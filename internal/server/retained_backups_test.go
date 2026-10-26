// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/api"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

// Only RFC1918 IPv4 addresses are suitable for this allowlisted local fixture.
// Global-unicast also includes CGNAT/Tailscale addresses, which the production
// endpoint policy rejects even when explicitly allowlisted.
func retainedS3IP(addresses []net.Addr) net.IP {
	for _, address := range addresses {
		if n, ok := address.(*net.IPNet); ok && n.IP.To4() != nil && n.IP.IsPrivate() {
			return n.IP
		}
	}
	return nil
}

func TestRetainedS3IP(t *testing.T) {
	addresses := []net.Addr{
		&net.IPNet{IP: net.ParseIP("127.0.0.1")},
		&net.IPNet{IP: net.ParseIP("100.116.236.21")},
		&net.IPNet{IP: net.ParseIP("168.63.129.16")},
		&net.IPNet{IP: net.ParseIP("fd00::1")},
	}
	if got := retainedS3IP(addresses); got != nil {
		t.Fatalf("selected an unsuitable fixture address: %s", got)
	}
	for _, ip := range []string{"10.1.2.3", "172.16.1.2", "192.168.2.208"} {
		t.Run(ip, func(t *testing.T) {
			want := net.ParseIP(ip)
			got := retainedS3IP(append(addresses, &net.IPNet{IP: want}))
			if !got.Equal(want) {
				t.Fatalf("selected %s, want %s after rejected addresses", got, want)
			}
		})
	}
}

// Exercise the real signed S3 client, including its outbound policy. A private
// interface is explicitly allowlisted; loopback remains forbidden in production.
func retainedS3(t *testing.T, keys []string) (string, func() map[string]bool, func(string)) {
	t.Helper()
	addresses, err := net.InterfaceAddrs()
	if err != nil {
		t.Fatal(err)
	}
	ip := retainedS3IP(addresses)
	if ip == nil {
		t.Fatal("S3 policy test needs a private RFC1918 IPv4 interface")
	}
	listener, err := net.Listen("tcp", net.JoinHostPort(ip.String(), "0"))
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("KARAKORAM_BACKUP_ENDPOINT_ALLOWLIST", listener.Addr().String())
	var mu sync.Mutex
	objects := map[string]bool{}
	for _, key := range keys {
		objects[key] = true
	}
	failKey := ""
	s3 := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if !strings.Contains(r.Header.Get("Authorization"), "AWS4-HMAC-SHA256") {
			t.Error("S3 cleanup request is not signed")
		}
		if r.Method == http.MethodPost && r.URL.Query().Has("delete") {
			var request struct {
				Objects []struct{ Key string } `xml:"Object"`
			}
			if err := xml.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Error(err)
				http.Error(w, "invalid deletion request", http.StatusBadRequest)
				return
			}
			result := struct {
				XMLName xml.Name                              `xml:"DeleteResult"`
				Deleted []struct{ Key string }                `xml:"Deleted"`
				Errors  []struct{ Key, Code, Message string } `xml:"Error"`
			}{}
			for _, object := range request.Objects {
				if object.Key == failKey {
					result.Errors = append(result.Errors, struct{ Key, Code, Message string }{object.Key, "AccessDenied", "denied"})
				} else {
					delete(objects, object.Key)
					result.Deleted = append(result.Deleted, object)
				}
			}
			w.Header().Set("Content-Type", "application/xml")
			_ = xml.NewEncoder(w).Encode(result)
			return
		}
		if r.Method != "GET" || r.URL.Path != "/bucket/" {
			t.Errorf("unexpected S3 request %s %s", r.Method, r.URL.Path)
			http.NotFound(w, r)
			return
		}
		type object struct {
			Key  string
			Size int64
		}
		result := struct {
			XMLName     xml.Name `xml:"ListVersionsResult"`
			Name        string
			Prefix      string
			IsTruncated bool
			Contents    []object `xml:"Version"`
		}{Name: "bucket", Prefix: r.URL.Query().Get("prefix")}
		ordered := make([]string, 0, len(objects))
		for key := range objects {
			ordered = append(ordered, key)
		}
		sort.Strings(ordered)
		for _, key := range ordered {
			if strings.HasPrefix(key, result.Prefix) {
				result.Contents = append(result.Contents, object{Key: key, Size: 1})
			}
		}
		w.Header().Set("Content-Type", "application/xml")
		_ = xml.NewEncoder(w).Encode(result)
	}))
	s3.Listener = listener
	s3.Start()
	t.Cleanup(s3.Close)
	return s3.URL, func() map[string]bool {
			mu.Lock()
			defer mu.Unlock()
			copy := make(map[string]bool, len(objects))
			for key := range objects {
				copy[key] = true
			}
			return copy
		}, func(key string) {
			mu.Lock()
			defer mu.Unlock()
			failKey = key
		}
}

func TestRetainedBackupLifecycle(t *testing.T) {
	s, _, admin := testServer(t)
	ctx := context.Background()
	prefix := "archive/karakoram/deleted-old-uid"
	snapshot := "2024-01-06T08:51:39Z"
	base := prefix + "/" + snapshot
	owned := []string{base + "/rs0/data.s2", base + "/rs0/meta.pbm", base + ".pbm.json"}
	unrelated := []string{base + "-other/rs0/data.s2", base + ".pbm.json.other", prefix + "/pbmPitr/rs0/oplog.s2", prefix + "/.pbm.init", "archive/karakoram/other/" + snapshot + "/data.s2"}
	endpoint, remaining, failKey := retainedS3(t, append(owned, unrelated...))
	storage := domain.BackupStorage{Name: "retained", Type: "s3", EndpointURL: endpoint, Region: "us-east-1", Bucket: "bucket", Prefix: "archive", ForcePathStyle: true, VerifyTLS: true, Credentials: domain.StorageCredentials{AccessKeyID: "access", SecretAccessKey: "secret"}}
	if err := s.Store.CreateBackupStorage(ctx, &storage); err != nil {
		t.Fatal(err)
	}
	backup := domain.Backup{Name: "saved", Cluster: "deleted", Storage: storage.Name, Type: "physical", Trigger: "manual", PBMName: snapshot, PBMPrefix: prefix, SourceTopology: domain.Topology{Type: "replicaSet"}, SourceVersion: "8.0", RecoveryIdentity: "backup-recovery", Phase: domain.PhaseSucceeded}
	if err := s.Store.CreateBackup(ctx, &backup); err != nil {
		t.Fatal(err)
	}
	if err := s.Store.SetClusterSecret(ctx, domain.ClusterRef{Name: backup.RecoveryIdentity}, "users/userAdmin", []byte("snapshot-password")); err != nil {
		t.Fatal(err)
	}
	path := "/api/v1/clusters/deleted/backups"
	w := call(s, "GET", path, "", admin)
	var list api.BackupList
	if err := json.Unmarshal(w.Body.Bytes(), &list); err != nil || w.Code != 200 || len(list.Items) != 1 || list.Items[0].Name != backup.Name {
		t.Fatal(w.Code, w.Body.String(), err)
	}
	if w := call(s, "GET", path+"/saved", "", admin); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	if w := call(s, "DELETE", "/api/v1/backup-storages/retained", "", admin); w.Code != 409 {
		t.Fatal(w.Code, w.Body.String())
	}
	// A role scoped to a different cluster cannot inspect or delete this backup.
	if _, err := s.Auth.CreateAccount(ctx, domain.Account{Name: "limited", Roles: []string{"role:retained-test"}}, "long-test-password"); err != nil {
		t.Fatal(err)
	}
	if err := s.RBAC.ReplacePolicy(ctx, s.RBAC.Policy()+"\np, role:retained-test, backups, read, other, allow\np, role:retained-test, backups, delete, other, allow\n"); err != nil {
		t.Fatal(err)
	}
	limited, _, err := s.Auth.CreateToken(ctx, "limited", "test", nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, request := range []struct{ method, path string }{{"GET", path}, {"GET", path + "/saved"}, {"DELETE", path + "/saved"}} {
		if w := call(s, request.method, request.path, "", limited); w.Code != 403 {
			t.Fatal(request, w.Code, w.Body.String())
		}
	}
	// Failure after data removal but before metadata removal retains the durable
	// Deleting record and identity. A fresh HTTP request resumes idempotently.
	failKey(base + ".pbm.json")
	if w := call(s, "DELETE", path+"/saved", "", admin); w.Code != 500 {
		t.Fatal(w.Code, w.Body.String())
	}
	b, err := s.Store.GetBackup(ctx, domain.ClusterRef{Name: backup.Cluster}, backup.Name)
	if err != nil || b.Phase != domain.PhaseDeleting || !remaining()[base+".pbm.json"] {
		t.Fatal("partial deletion lost its retry checkpoint", b, err)
	}
	for _, key := range owned[:2] {
		if remaining()[key] {
			t.Fatal("partial deletion did not reach S3 data removal", key)
		}
	}
	if _, err := s.Store.GetClusterSecret(ctx, domain.ClusterRef{Name: backup.RecoveryIdentity}, "users/userAdmin"); err != nil {
		t.Fatal("partial deletion removed recovery identity", err)
	}
	if w := call(s, "POST", "/api/v1/clusters/prod/restores", `{"name":"unsafe","source":{"sourceCluster":"deleted","backup":"saved"}}`, admin); w.Code != 409 {
		t.Fatal("restore accepted a deleting snapshot", w.Code, w.Body.String())
	}
	failKey("")
	for range 2 {
		if w := call(s, "DELETE", path+"/saved", "", admin); w.Code != 204 {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	got := remaining()
	for _, key := range owned {
		if got[key] {
			t.Fatal("snapshot object remains", key)
		}
	}
	for _, key := range unrelated {
		if !got[key] {
			t.Fatal("unrelated S3 object deleted", key)
		}
	}
	if _, err := s.Store.GetBackup(ctx, domain.ClusterRef{Name: backup.Cluster}, backup.Name); !errors.Is(err, store.ErrNotFound) {
		t.Fatal("backup record remains", err)
	}
	if _, err := s.Store.GetClusterSecret(ctx, domain.ClusterRef{Name: backup.RecoveryIdentity}, "users/userAdmin"); !errors.Is(err, store.ErrNotFound) {
		t.Fatal("snapshot recovery identity remains", err)
	}
	if w := call(s, "GET", path+"/saved", "", admin); w.Code != 404 {
		t.Fatal(w.Code, w.Body.String())
	}
	if w := call(s, "GET", path, "", admin); w.Code != 404 {
		t.Fatal(w.Code, w.Body.String())
	}
	if w := call(s, "DELETE", "/api/v1/backup-storages/retained", "", admin); w.Code != 204 {
		t.Fatal(w.Code, w.Body.String())
	}
}

func TestRetainedRestorePreflight(t *testing.T) {
	for _, kind := range []string{"logical", "physical"} {
		t.Run(kind, func(t *testing.T) {
			s, _, admin := testServer(t)
			ctx := context.Background()
			if err := s.Registry.Register(&restoreTestProvider{fakeProvider: &fakeProvider{}, name: "psmdb"}); err != nil {
				t.Fatal(err)
			}
			c, err := s.Store.GetCluster(ctx, domain.ClusterRef{Name: "prod"})
			if err != nil {
				t.Fatal(err)
			}
			c.Spec.Provider = "psmdb"
			if err = s.Store.UpdateClusterSpec(ctx, c.Ref(), c.Spec); err != nil {
				t.Fatal(err)
			}
			b := domain.Backup{Name: "saved", Cluster: "deleted", Storage: "storage", Type: kind, PBMName: "snapshot", PBMPrefix: "archive/old", SourceTopology: domain.Topology{Type: "replicaSet"}, SourceVersion: "8.0", Phase: domain.PhaseSucceeded}
			if kind == "physical" {
				b.RecoveryIdentity = "backup-recovery"
				if err := s.Store.SetClusterSecret(ctx, domain.ClusterRef{Name: b.RecoveryIdentity}, "users/userAdmin", []byte("snapshot-password")); err != nil {
					t.Fatal(err)
				}
			}
			if err := s.Store.CreateBackup(ctx, &b); err != nil {
				t.Fatal(err)
			}
			body := `{"name":"retained","source":{"sourceCluster":"deleted","backup":"saved"}}`
			if w := call(s, "POST", "/api/v1/clusters/prod/restores", body, admin); w.Code != 202 {
				t.Fatal(w.Code, w.Body.String())
			}
			cloneBody := `{"name":"copy","spec":{"provider":"psmdb","version":"8.0","topology":{"type":"replicaSet"},"components":{"engine":{"replicas":1}},"dataSource":{"cluster":"deleted","backup":"saved"}}}`
			if w := call(s, "POST", "/api/v1/clusters", cloneBody, admin); w.Code != 201 {
				t.Fatal("retained clone rejected", w.Code, w.Body.String())
			}
			b.PBMPrefix = ""
			if err := s.Store.UpdateBackup(ctx, &b); err != nil {
				t.Fatal(err)
			}
			if w := call(s, "POST", "/api/v1/clusters/prod/restores", body, admin); w.Code != 409 || !strings.Contains(w.Body.String(), "provenance") {
				t.Fatal(w.Code, w.Body.String())
			}
			if kind == "physical" {
				b.PBMPrefix, b.RecoveryIdentity = "archive/old", ""
				if err := s.Store.UpdateBackup(ctx, &b); err != nil {
					t.Fatal(err)
				}
				if w := call(s, "POST", "/api/v1/clusters/prod/restores", body, admin); w.Code != 409 || !strings.Contains(w.Body.String(), "recovery identity") {
					t.Fatal(w.Code, w.Body.String())
				}
				if err := s.Store.DeleteCluster(ctx, domain.ClusterRef{Name: "copy"}); err != nil {
					t.Fatal(err)
				}
				if w := call(s, "POST", "/api/v1/clusters", cloneBody, admin); w.Code != 409 || !strings.Contains(w.Body.String(), "recovery identity") {
					t.Fatal("unrecoverable physical clone accepted", w.Code, w.Body.String())
				}
			}
		})
	}
}

type retainedWithoutMongoIdentity struct {
	engine.Provider
	called bool
}

func (*retainedWithoutMongoIdentity) Name() string { return "native-test" }
func (*retainedWithoutMongoIdentity) Describe() domain.ProviderInfo {
	return domain.ProviderInfo{Name: "native-test"}
}
func (p *retainedWithoutMongoIdentity) DeleteRetainedBackup(context.Context, domain.BackupStorage, domain.Backup) error {
	p.called = true
	return nil
}

func TestRetainedNonMongoBackupDoesNotDeleteMongoRecoverySecret(t *testing.T) {
	s, _, admin := testServer(t)
	ctx := t.Context()
	provider := &retainedWithoutMongoIdentity{}
	if err := s.Registry.Register(provider); err != nil {
		t.Fatal(err)
	}
	storage := &domain.BackupStorage{Name: "retained-native", Type: "s3", EndpointURL: "https://objects.example", Region: "region", Bucket: "bucket", Credentials: domain.StorageCredentials{AccessKeyID: "access", SecretAccessKey: "secret"}}
	if err := s.Store.CreateBackupStorage(ctx, storage); err != nil {
		t.Fatal(err)
	}
	backup := &domain.Backup{Name: "saved", Cluster: "deleted", Storage: storage.Name, Provider: provider.Name(), Location: "artifact", Phase: domain.PhaseSucceeded, RecoveryIdentity: "shared-identity"}
	if err := s.Store.CreateBackup(ctx, backup); err != nil {
		t.Fatal(err)
	}
	ref := domain.ClusterRef{Name: backup.RecoveryIdentity}
	if err := s.Store.SetClusterSecret(ctx, ref, "users/userAdmin", []byte("mongo-owned")); err != nil {
		t.Fatal(err)
	}
	if response := call(s, "DELETE", "/api/v1/clusters/deleted/backups/saved", "", admin); response.Code != http.StatusNoContent {
		t.Fatal(response.Code, response.Body.String())
	}
	if !provider.called {
		t.Fatal("provider retained deleter was not invoked")
	}
	if _, err := s.Store.GetClusterSecret(ctx, ref, "users/userAdmin"); err != nil {
		t.Fatal("non-Mongo provider deleted a Mongo recovery credential", err)
	}
}

type retryRetainedProvider struct {
	*fakeProvider
	name   string
	delete func(domain.Backup) error
}

func (p *retryRetainedProvider) Name() string { return p.name }
func (p *retryRetainedProvider) DeleteRetainedBackup(_ context.Context, _ domain.BackupStorage, b domain.Backup) error {
	return p.delete(b)
}

func TestRetainedFailedRemoteBackupDeletionDoesNotQueueAbsentCluster(t *testing.T) {
	for _, provider := range []string{"rabbitmq", "postgres"} {
		t.Run(provider, func(t *testing.T) {
			s, _, admin := testServer(t)
			ctx := t.Context()
			calls, fail := 0, true
			p := &retryRetainedProvider{fakeProvider: &fakeProvider{}, name: provider}
			p.delete = func(b domain.Backup) error {
				calls++
				if (provider == "rabbitmq" && b.Location == "") || (provider == "postgres" && b.Postgres == nil) {
					t.Fatal("remote identity was discarded", b)
				}
				if fail {
					return errors.New("remote cleanup unavailable")
				}
				return nil
			}
			if err := s.Registry.Register(p); err != nil {
				t.Fatal(err)
			}
			if err := s.Store.CreateBackupStorage(ctx, &domain.BackupStorage{Name: "archive", Type: "s3"}); err != nil {
				t.Fatal(err)
			}
			b := &domain.Backup{Name: "saved", Cluster: "prod", Provider: provider, Storage: "archive", Phase: domain.PhaseSucceeded}
			if provider == "rabbitmq" {
				b.Location = "s3://archive/definitions.json"
			} else {
				b.Postgres = &domain.PostgresBackup{Label: "full", Repository: "repo"}
			}
			if err := s.Store.CreateBackup(ctx, b); err != nil {
				t.Fatal(err)
			}
			op := &domain.Operation{Cluster: b.Cluster, Kind: domain.OperationDeleteBackup}
			if err := s.Store.EnqueueBackupDeletion(ctx, b, op); err != nil {
				t.Fatal(err)
			}
			op.Phase = domain.PhaseFailed
			if err := s.Store.UpdateOperation(ctx, op); err != nil {
				t.Fatal(err)
			}
			if err := s.Store.DeleteCluster(ctx, domain.ClusterRef{Name: b.Cluster}); err != nil {
				t.Fatal(err)
			}
			path := "/api/v1/clusters/prod/backups/saved"
			if response := call(s, "DELETE", path, "", admin); response.Code != 500 || calls != 1 {
				t.Fatal("remote failure was not executed/preserved", response.Code, response.Body.String(), calls)
			}
			retained, err := s.Store.GetBackup(ctx, domain.ClusterRef{Name: b.Cluster}, b.Name)
			if err != nil || retained.Phase != domain.PhaseDeleting {
				t.Fatal("remote failure lost inventory", retained, err)
			}
			fail = false
			if response := call(s, "DELETE", path, "", admin); response.Code != 204 || calls != 2 {
				t.Fatal("retained deletion did not retry directly", response.Code, response.Body.String(), calls)
			}
			if _, err := s.Store.GetBackup(ctx, domain.ClusterRef{Name: b.Cluster}, b.Name); !errors.Is(err, store.ErrNotFound) {
				t.Fatal("remote success retained inventory", err)
			}
			ops, err := s.Store.ListOperations(ctx, domain.ClusterRef{Name: b.Cluster}, domain.PhasePending, domain.PhaseRunning)
			if err != nil || len(ops) != 0 {
				t.Fatal("absent cluster owns stranded cleanup", ops, err)
			}
		})
	}
}
