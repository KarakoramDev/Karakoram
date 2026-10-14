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
	"github.com/KarakoramDev/Karakoram/internal/store"
)

// Exercise the real signed S3 client, including its outbound policy. A private
// interface is explicitly allowlisted; loopback remains forbidden in production.
func retainedS3(t *testing.T, keys []string) (string, func() map[string]bool, func(string)) {
	t.Helper()
	addresses, err := net.InterfaceAddrs()
	if err != nil {
		t.Fatal(err)
	}
	var ip net.IP
	for _, address := range addresses {
		if n, ok := address.(*net.IPNet); ok && n.IP.To4() != nil && n.IP.IsGlobalUnicast() && !n.IP.IsLoopback() {
			ip = n.IP
			break
		}
	}
	if ip == nil {
		t.Fatal("S3 policy test needs a non-loopback IPv4 interface")
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
		key := strings.TrimPrefix(r.URL.Path, "/bucket/")
		if r.Method == "DELETE" {
			if key == failKey {
				w.WriteHeader(403)
				_, _ = w.Write([]byte("<Error><Code>AccessDenied</Code><Message>denied</Message></Error>"))
				return
			}
			delete(objects, key)
			w.WriteHeader(204)
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
			XMLName     xml.Name `xml:"ListBucketResult"`
			Name        string
			Prefix      string
			IsTruncated bool
			Contents    []object
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
			cloneBody := `{"name":"copy","spec":{"provider":"fake","version":"8.0","topology":{"type":"replicaSet"},"components":{"engine":{"replicas":1}},"dataSource":{"cluster":"deleted","backup":"saved"}}}`
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
