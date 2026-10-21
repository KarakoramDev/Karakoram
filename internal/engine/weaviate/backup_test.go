// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package weaviate

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/weaviate/wvapi"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

type backupNative struct {
	backup                   wvapi.BackupState
	restore                  wvapi.BackupState
	posts, restores, cancels int
	request                  wvapi.BackupRequest
	restoreRequest           wvapi.RestoreRequest
	collections              []string
	order                    []string
	restoreMetadata          *restoreIdentity
	size                     float64
}

func (n *backupNative) serve(w http.ResponseWriter, r *http.Request) bool {
	if r.URL.Path == "/v1/schema" {
		n.order = append(n.order, "collections")
		classes := []map[string]string{}
		for _, c := range n.collections {
			classes = append(classes, map[string]string{"class": c})
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"classes": classes})
		return true
	}
	if strings.HasPrefix(r.URL.Path, "/v1/schema/") && r.Method == "DELETE" {
		n.order = append(n.order, "delete")
		n.collections = n.collections[1:]
		w.WriteHeader(204)
		return true
	}
	if !strings.HasPrefix(r.URL.Path, "/v1/backups/") {
		return false
	}
	restore := strings.HasSuffix(r.URL.Path, "/restore")
	switch r.Method {
	case "POST":
		if restore {
			n.restores++
			n.order = append(n.order, "restore")
			_ = json.NewDecoder(r.Body).Decode(&n.restoreRequest)
			n.restore = wvapi.BackupStarted
			n.restoreMetadata = &restoreIdentity{ID: strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v1/backups/s3/"), "/restore"), StartedAt: time.Now().UTC(), Status: n.restore}
		} else {
			n.posts++
			_ = json.NewDecoder(r.Body).Decode(&n.request)
			n.backup = wvapi.BackupStarted
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "STARTED"})
	case "DELETE":
		n.cancels++
		n.backup = wvapi.BackupCanceled
		w.WriteHeader(204)
	default:
		status := n.backup
		if restore {
			status = n.restore
		}
		if status == "" {
			http.NotFound(w, r)
			return true
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"status": status, "size": n.size, "error": "signed secret URL must not persist", "completedAt": "2026-10-20T12:00:00Z"})
	}
	return true
}
func backupFixture(t *testing.T, replicas int) (*coreFixture, *state, *backupNative, *domain.Backup) {
	t.Helper()
	n := &backupNative{size: 1.25}
	f := coreSetup(t, replicas, n.serve)
	s := coreConverge(t, f)
	storage := &domain.BackupStorage{Name: "archive", Type: "s3", EndpointURL: "https://s3.example.com", Bucket: "bucket", Prefix: "old-prefix", Region: "us-east-1", Credentials: domain.StorageCredentials{AccessKeyID: "test", SecretAccessKey: "test-secret"}}
	if err := f.rc.Store.(*store.Store).CreateBackupStorage(context.Background(), storage); err != nil {
		t.Fatal(err)
	}
	f.rc.Cluster.Spec.Backup.Storage = storage.Name
	b := &domain.Backup{Namespace: f.rc.Cluster.Ref().Namespace, Cluster: f.rc.Cluster.Name, Name: "Full-UPPER/+", Storage: storage.Name, Provider: "weaviate", Type: "full", Phase: domain.PhasePending, CreatedAt: time.Now()}
	if err := f.rc.Store.CreateBackup(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	return f, s, n, b
}
func backupStep(t *testing.T, f *coreFixture, s *state, b *domain.Backup, cp operationState) (operationState, engine.OpResult, error) {
	t.Helper()
	r, e := f.p.runBackup(context.Background(), f.rc, s, &domain.Operation{ID: "backup-op"}, cp, b.Name)
	if json.Unmarshal(r.Checkpoint, &cp) != nil {
		t.Fatal("invalid checkpoint")
	}
	return cp, r, e
}
func completeBackup(t *testing.T, f *coreFixture, s *state, n *backupNative, b *domain.Backup) *domain.Backup {
	t.Helper()
	cp, _, e := backupStep(t, f, s, b, operationState{})
	if e != nil {
		t.Fatal(e)
	}
	cp, _, e = backupStep(t, f, s, b, cp)
	if e != nil {
		t.Fatal(e)
	}
	n.backup = wvapi.BackupSuccess
	_, r, e := backupStep(t, f, s, b, cp)
	if e != nil || !r.Done {
		t.Fatalf("backup completion %+v %v", r, e)
	}
	saved, e := f.rc.Store.GetBackup(context.Background(), f.rc.Cluster.Ref(), b.Name)
	if e != nil {
		t.Fatal(e)
	}
	return saved
}
func TestBackupDurableRecordReplayAndFailure(t *testing.T) {
	for _, status := range []wvapi.BackupState{wvapi.BackupSuccess, wvapi.BackupFailed, wvapi.BackupCanceled} {
		t.Run(string(status), func(t *testing.T) {
			f, s, n, b := backupFixture(t, 1)
			cp, _, err := backupStep(t, f, s, b, operationState{})
			if err != nil {
				t.Fatal(err)
			}
			saved, err := f.rc.Store.GetBackup(context.Background(), f.rc.Cluster.Ref(), b.Name)
			if err != nil {
				t.Fatal(err)
			}
			if saved.Weaviate == nil || saved.Location != backupLocation(saved.Weaviate) || !reflect.DeepEqual(saved.Weaviate.Members, []string{s.Members[0].Name}) || saved.Weaviate.ClusterUID != s.UID || saved.SourceVersion != "1.40.1" || n.posts != 0 {
				t.Fatalf("provenance not durable: %+v", saved)
			}
			// Lose operation checkpoint after record write; replay preserves location.
			_, _, err = backupStep(t, f, s, b, operationState{})
			if err != nil {
				t.Fatal(err)
			}
			// Lose checkpoint after successful POST; GET must prevent a duplicate POST.
			_, _, err = backupStep(t, f, s, b, cp)
			if err != nil {
				t.Fatal(err)
			}
			cp, _, err = backupStep(t, f, s, b, cp)
			if err != nil {
				t.Fatal(err)
			}
			if n.posts != 1 || n.request.ID != saved.Weaviate.ID || n.request.Config.Bucket != "bucket" {
				t.Fatalf("submission %+v posts=%d", n.request, n.posts)
			}
			n.backup = status
			_, result, err := backupStep(t, f, s, b, cp)
			if !result.Done || (err != nil) != (status != wvapi.BackupSuccess) {
				t.Fatalf("result %+v %v", result, err)
			}
			saved, _ = f.rc.Store.GetBackup(context.Background(), f.rc.Cluster.Ref(), b.Name)
			if saved.Size != 1342177280 || saved.CompletedAt == nil || strings.Contains(saved.Error, "secret") {
				t.Fatalf("bad terminal metadata %+v", saved)
			}
		})
	}
}

func TestBackupSizeOverflowRejected(t *testing.T) {
	f, s, n, b := backupFixture(t, 1)
	n.size = 1e30
	saved := completeBackup(t, f, s, n, b)
	if saved.Size != 0 {
		t.Fatalf("overflowing native GiB size persisted as %d bytes", saved.Size)
	}
}
func TestBackupStorageAdmission(t *testing.T) {
	f, s, n, b := backupFixture(t, 1)
	f.rc.Cluster.Spec.Backup.Storage = "other"
	_, _, err := backupStep(t, f, s, b, operationState{})
	if err == nil || n.posts != 0 {
		t.Fatal("accepted unbound storage")
	}
	f.rc.Cluster.Spec.Backup.Storage = "archive"
	f.rc.Cluster.Spec.Backup.Schedules = []domain.BackupSchedule{{Name: "daily", Cron: "0 0 * * *", Type: "full", Storage: "other", RetainCount: 1}}
	if f.p.Validate(context.Background(), nil, f.rc.Cluster) == nil {
		t.Fatal("accepted unbound schedule")
	}
}
func TestNativeBackupIDCollisionSafety(t *testing.T) {
	seen := map[string]bool{}
	for _, name := range []string{"X", "x", "/", "_", strings.Repeat("a", 1000)} {
		id := nativeBackupID(name)
		if !backupIDPattern.MatchString(id) || seen[id] {
			t.Fatal(id)
		}
		seen[id] = true
	}
}

// Use a real private interface plus the production operator allowlist. Loopback
// remains forbidden by the generic S3 client's outbound policy.
func fakeBackupS3(t *testing.T, handlers ...func(http.ResponseWriter, *http.Request) bool) (string, map[string]bool, *[]string) {
	t.Helper()
	addresses, err := net.InterfaceAddrs()
	if err != nil {
		t.Fatal(err)
	}
	host := ""
	for _, a := range addresses {
		ip, _, _ := net.ParseCIDR(a.String())
		if ip.To4() != nil && ip.IsPrivate() && !ip.IsLoopback() {
			host = ip.String()
			break
		}
	}
	if host == "" {
		t.Skip("no private IPv4 interface for production S3 outbound policy")
	}
	keys := map[string]bool{}
	prefixes := []string{}
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for _, handler := range handlers {
			if handler(w, r) {
				return
			}
		}
		if r.Method == "DELETE" {
			delete(keys, strings.TrimPrefix(r.URL.Path, "/bucket/"))
			w.WriteHeader(204)
			return
		}
		prefix := r.URL.Query().Get("prefix")
		prefixes = append(prefixes, prefix)
		type object struct {
			Key  string `xml:"Key"`
			Size int    `xml:"Size"`
		}
		v := struct {
			XMLName     xml.Name `xml:"ListBucketResult"`
			Name        string   `xml:"Name"`
			IsTruncated bool     `xml:"IsTruncated"`
			Contents    []object `xml:"Contents"`
		}{Name: "bucket"}
		for k := range keys {
			if strings.HasPrefix(k, prefix) {
				v.Contents = append(v.Contents, object{Key: k, Size: 1})
			}
		}
		w.Header().Set("Content-Type", "application/xml")
		_ = xml.NewEncoder(w).Encode(v)
	}))
	listener, err := net.Listen("tcp", net.JoinHostPort(host, "0"))
	if err != nil {
		t.Fatal(err)
	}
	srv.Listener.Close()
	srv.Listener = listener
	srv.Start()
	t.Cleanup(srv.Close)
	t.Setenv("KARAKORAM_BACKUP_ENDPOINT_ALLOWLIST", listener.Addr().String())
	return srv.URL, keys, &prefixes
}
func TestDeleteBackupAndRetainedUsesRecordedPrefix(t *testing.T) {
	for _, retained := range []bool{false, true} {
		t.Run(fmt.Sprint(retained), func(t *testing.T) {
			endpoint, keys, prefixes := fakeBackupS3(t)
			f, s, n, b := backupFixture(t, 1)
			b = completeBackup(t, f, s, n, b)
			storage, _ := f.rc.Store.GetBackupStorage(context.Background(), b.Namespace, b.Storage)
			storage.EndpointURL = endpoint
			storage.ForcePathStyle = true
			storage.VerifyTLS = false
			storage.Prefix = "new-prefix"
			b.Weaviate.Endpoint = endpoint
			b.Location = backupLocation(b.Weaviate)
			if err := f.rc.Store.(*store.Store).UpdateBackupStorage(context.Background(), storage); err != nil {
				t.Fatal(err)
			}
			if err := f.rc.Store.UpdateBackup(context.Background(), b); err != nil {
				t.Fatal(err)
			}
			prefix := b.Weaviate.Path + "/" + b.Weaviate.ID + "/"
			keys[prefix+"backup.json"] = true
			keys[prefix+"demo-0/backup.json"] = true
			keys[prefix+"demo-0/Article/chunk-0"] = true
			other := b.Weaviate.Path + "/another/backup.json"
			keys[other] = true
			if retained {
				if err := f.rc.Store.(*store.Store).DeleteCluster(context.Background(), f.rc.Cluster.Ref()); err != nil {
					t.Fatal(err)
				}
				if err := New(Options{}).DeleteRetainedBackup(context.Background(), *storage, *b); err != nil {
					t.Fatal(err)
				}
			} else {
				r, e := f.p.runDeleteBackup(context.Background(), f.rc, s, &domain.Operation{}, operationState{}, b.Name)
				if e != nil || !r.Done {
					t.Fatalf("delete %+v %v", r, e)
				}
			}
			if len(keys) != 1 || !keys[other] {
				t.Fatalf("objects %+v", keys)
			}
			for _, got := range *prefixes {
				if got != prefix {
					t.Fatalf("listed %q want %q", got, prefix)
				}
			}
		})
	}
}
func TestCleanupBackupKeepAndUnreachable(t *testing.T) {
	f, s, n, b := backupFixture(t, 1)
	b = completeBackup(t, f, s, n, b)
	if _, e := f.p.cleanupOwnedBackups(context.Background(), f.rc, true); e != nil {
		t.Fatal(e)
	}
	if _, e := f.rc.Store.GetBackup(context.Background(), f.rc.Cluster.Ref(), b.Name); e != nil {
		t.Fatal(e)
	}
	f.rc.Dialer = coreDialer{"127.0.0.1:1"}
	if done, e := f.p.cleanupOwnedBackups(context.Background(), f.rc, false); e != nil || !done {
		t.Fatalf("cleanup blocked %v %v", done, e)
	}
	if _, e := f.rc.Store.GetBackup(context.Background(), f.rc.Cluster.Ref(), b.Name); e != nil {
		t.Fatal("lost inventory")
	}
}

func TestCleanupDeletesOwnedBackupAndCancelsWriter(t *testing.T) {
	endpoint, keys, _ := fakeBackupS3(t)
	f, s, n, b := backupFixture(t, 1)
	b = completeBackup(t, f, s, n, b)
	storage, _ := f.rc.Store.GetBackupStorage(context.Background(), b.Namespace, b.Storage)
	storage.EndpointURL = endpoint
	storage.ForcePathStyle = true
	b.Weaviate.Endpoint = endpoint
	b.Location = backupLocation(b.Weaviate)
	if e := f.rc.Store.(*store.Store).UpdateBackupStorage(context.Background(), storage); e != nil {
		t.Fatal(e)
	}
	b.Phase = domain.PhaseRunning
	if e := f.rc.Store.UpdateBackup(context.Background(), b); e != nil {
		t.Fatal(e)
	}
	key := b.Weaviate.Path + "/" + b.Weaviate.ID + "/backup_config.json"
	keys[key] = true
	n.backup = wvapi.BackupTransferring
	if done, e := f.p.cleanupOwnedBackups(context.Background(), f.rc, false); e != nil || done {
		t.Fatalf("cancel checkpoint %v %v", done, e)
	}
	if n.cancels != 1 || !keys[key] {
		t.Fatal("deleted before cancellation was observed")
	}
	if done, e := f.p.cleanupOwnedBackups(context.Background(), f.rc, false); e != nil || !done {
		t.Fatalf("cleanup %v %v", done, e)
	}
	if len(keys) != 0 {
		t.Fatal("objects remain")
	}
	if _, e := f.rc.Store.GetBackup(context.Background(), f.rc.Cluster.Ref(), b.Name); !missing(e) {
		t.Fatal("inventory remains", e)
	}
}
