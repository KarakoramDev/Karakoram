// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package elastic

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

type cleanupStorage struct {
	engine.ClusterStore
	storage domain.BackupStorage
}

func (s *cleanupStorage) GetBackupStorage(_ context.Context, _, _ string) (*domain.BackupStorage, error) {
	return &s.storage, nil
}

func TestArtifactCleanupBoundAndRestart(t *testing.T) {
	for _, transient := range []bool{false, true} {
		t.Run(fmt.Sprint(transient), func(t *testing.T) {
			p, rc, f, _ := coreSetup(t, 1)
			s := coreConverge(t, p, rc, f)
			base := "saved/" + rc.Cluster.Ref().Namespace + "/demo/" + s.UID
			// Use an allowlisted private endpoint, never bypass the production egress guard.
			addresses, err := net.InterfaceAddrs()
			if err != nil {
				t.Fatal(err)
			}
			address := ""
			for _, addr := range addresses {
				ip, _, _ := net.ParseCIDR(addr.String())
				if ip != nil && ip.To4() != nil && ip.IsPrivate() {
					address = ip.String()
					break
				}
			}
			if address == "" {
				t.Skip("private test interface unavailable")
			}
			listener, err := net.Listen("tcp", net.JoinHostPort(address, "0"))
			if err != nil {
				t.Fatal(err)
			}
			denied, present, calls := true, true, 0
			server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				if len(f.services) != 0 {
					t.Error("artifact access before runtime/volume retirement")
				}
				w.Header().Set("Content-Type", "application/xml")
				if denied {
					w.WriteHeader(403)
					fmt.Fprint(w, `<Error><Code>AccessDenied</Code></Error>`)
					return
				}
				if r.Method == "POST" {
					present = false
					fmt.Fprintf(w, `<DeleteResult><Deleted><Key>%s/blob</Key></Deleted></DeleteResult>`, base)
					return
				}
				if r.URL.Query().Get("prefix") != base+"/" {
					t.Error("unsafe prefix", r.URL.RawQuery)
				}
				if r.URL.Query().Has("versions") {
					fmt.Fprint(w, `<ListVersionsResult><IsTruncated>false</IsTruncated>`)
					if present {
						fmt.Fprintf(w, `<Version><Key>%s/blob</Key><VersionId>one</VersionId><IsLatest>true</IsLatest></Version>`, base)
					}
					fmt.Fprint(w, `</ListVersionsResult>`)
				} else {
					fmt.Fprint(w, `<ListBucketResult><IsTruncated>false</IsTruncated>`)
					if present {
						fmt.Fprintf(w, `<Contents><Key>%s/blob</Key></Contents>`, base)
					}
					fmt.Fprint(w, `</ListBucketResult>`)
				}
			}))
			server.Listener = listener
			server.Start()
			defer server.Close()
			t.Setenv("KARAKORAM_BACKUP_ENDPOINT_ALLOWLIST", listener.Addr().String())
			storage := domain.BackupStorage{Namespace: rc.Cluster.Ref().Namespace, Name: "archive", Type: "s3", EndpointURL: server.URL, Bucket: "backups", Prefix: "saved", Credentials: domain.StorageCredentials{AccessKeyID: "test-access", SecretAccessKey: "test-key"}}
			rc.Store = &cleanupStorage{ClusterStore: rc.Store, storage: storage}
			s.Repositories[rc.Cluster.Ref().Namespace+"/archive"] = &repository{Name: "kk-archive", BasePath: base}
			backup := &domain.Backup{Namespace: rc.Cluster.Ref().Namespace, Cluster: "demo", Name: "one", Storage: "archive", Provider: "elasticsearch", Location: snapshotLocation(&storage, base, "kk-archive", "one")}
			if err := rc.Store.CreateBackup(t.Context(), backup); err != nil {
				t.Fatal(err)
			}
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			for range 30 {
				done, err := p.Cleanup(t.Context(), rc, domain.DeleteOptions{})
				if err != nil || done {
					t.Fatal("unexpected cleanup completion", done, err)
				}
				if calls > 0 {
					break
				}
			}
			if calls == 0 || len(f.services) != 0 {
				t.Fatal("storage failure never reached after retirement")
			}
			// Rehydrate every pass and replace the Provider: no process-local deadline state.
			s, err = load(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			if s.BackupCleanupStarted == nil {
				t.Fatal("missing durable start")
			}
			if transient {
				denied = false
			} else {
				expired := time.Now().Add(-3 * time.Minute)
				s.BackupCleanupStarted = &expired
			}
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			p = New(ProviderOptions{})
			done := false
			for range 15 {
				done, err = p.Cleanup(t.Context(), rc, domain.DeleteOptions{})
				if err != nil {
					t.Fatal(err)
				}
				if done {
					break
				}
			}
			if !done {
				t.Fatal("bounded cleanup did not terminate")
			}
			_, inventoryErr := rc.Store.GetBackup(t.Context(), rc.Cluster.Ref(), "one")
			events := rc.Events.(*coreEvents).events
			if transient {
				if present || !missing(inventoryErr) || len(events) != 0 {
					t.Fatal("transient cleanup failed", present, inventoryErr, events)
				}
			} else {
				if !present || inventoryErr != nil || len(events) != 1 || events[0].Reason != "SnapshotsRetained" || !strings.Contains(events[0].Message, base+"/") {
					t.Fatal("untruthful retained outcome", events)
				}
			}
			before := calls
			if _, err := p.Cleanup(t.Context(), rc, domain.DeleteOptions{}); err != nil {
				t.Fatal(err)
			}
			if calls != before {
				t.Fatal("completed cleanup repeated storage work")
			}
			durable := rc.Store.(*cleanupStorage).ClusterStore.(*store.Store)
			if err := durable.DeleteCluster(t.Context(), rc.Cluster.Ref()); err != nil {
				t.Fatal(err)
			}
			if _, err := durable.GetCluster(t.Context(), rc.Cluster.Ref()); !missing(err) {
				t.Fatal("cluster record remains after completed cleanup", err)
			}
			if !transient {
				if _, err := durable.GetBackup(t.Context(), rc.Cluster.Ref(), "one"); err != nil {
					t.Fatal("retained inventory lost with cluster record", err)
				}
			}
		})
	}
}

func TestElasticDeletionPreservesDependencyFence(t *testing.T) {
	p, rc, f, _ := coreSetup(t, 1)
	coreConverge(t, p, rc, f)
	st := rc.Store.(*store.Store)
	b := &domain.Backup{Name: "base", Cluster: rc.Cluster.Name, Storage: "archive", Phase: domain.PhaseSucceeded}
	if err := st.CreateBackup(t.Context(), b); err != nil {
		t.Fatal(err)
	}
	target := &domain.Cluster{Name: "deptarget"}
	if err := st.CreateCluster(t.Context(), target); err != nil {
		t.Fatal(err)
	}
	restore := &domain.Restore{Name: "restore", Cluster: target.Name, Source: domain.RestoreSource{SourceCluster: rc.Cluster.Name, Backup: b.Name}}
	op := &domain.Operation{ID: "dependent-operation", Cluster: target.Name, Kind: domain.OperationRestore, Payload: json.RawMessage(`{"restore":"restore"}`)}
	if err := st.EnqueueRestore(t.Context(), restore, op); err != nil {
		t.Fatal(err)
	}
	before := len(f.services)
	err := st.MarkClusterDeleting(t.Context(), rc.Cluster.Ref(), domain.DeleteOptions{})
	var conflict *domain.ResourceError
	if !errors.Is(err, store.ErrConflict) || !errors.As(err, &conflict) || conflict.Reason != domain.ReasonInvalidState || len(f.services) != before {
		t.Fatal("lost API409 dependency fence", err)
	}
}
