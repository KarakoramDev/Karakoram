// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/store"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type cleanupArtifactStore struct {
	engine.ClusterStore
	backup  *domain.Backup
	storage domain.BackupStorage
	deletes int
}

func (s *cleanupArtifactStore) ListBackups(context.Context, domain.ClusterRef) ([]domain.Backup, error) {
	if s.backup == nil {
		return nil, nil
	}
	return []domain.Backup{*s.backup}, nil
}
func (s *cleanupArtifactStore) GetBackupStorage(context.Context, string, string) (*domain.BackupStorage, error) {
	return &s.storage, nil
}
func (s *cleanupArtifactStore) DeleteBackup(context.Context, domain.ClusterRef, string) error {
	s.deletes++
	s.backup = nil
	return nil
}

func artifactStore(rc *engine.Context, uid string) *cleanupArtifactStore {
	s := &cleanupArtifactStore{ClusterStore: rc.Store,
		storage: domain.BackupStorage{Namespace: "default", Name: "archive", Bucket: "backups", Credentials: domain.StorageCredentials{AccessKeyID: "fixture", SecretAccessKey: "fixture"}},
		backup:  &domain.Backup{Namespace: "default", Cluster: rc.Cluster.Name, Name: "daily", Storage: "archive", Provider: "rabbitmq", Type: "definitions", Location: "prefix/" + uid + "/definitions/daily.json.gz"},
	}
	rc.Store = s
	return s
}

func TestCleanupArtifactsBoundedAfterRuntimeRetirement(t *testing.T) {
	for _, keepVolumes := range []bool{false, true} {
		for _, keepBackups := range []bool{false, true} {
			t.Run(fmt.Sprintf("volumes=%v/backups=%v", keepVolumes, keepBackups), func(t *testing.T) {
				p, rc, backend, _ := setup(t)
				ready(t, p, rc)
				members := len(state(t, rc).Members)
				st := artifactStore(rc, state(t, rc).UID)
				events := &definitionsTestEvents{}
				rc.Events = events
				calls := 0
				original := definitionsS3Client
				definitionsS3Client = func(domain.BackupStorage, domain.StorageCredentials) (*minio.Client, func(), error) {
					calls++
					wantServices := 0
					if keepVolumes {
						wantServices = members
					}
					if len(backend.services) != wantServices {
						t.Fatal("storage cleanup preceded runtime/volume retirement")
					}
					return nil, nil, errors.New("storage-password must not appear in outcome")
				}
				t.Cleanup(func() { definitionsS3Client = original })
				opts := domain.DeleteOptions{KeepVolumes: keepVolumes, KeepBackups: keepBackups}
				if keepBackups {
					cleanupCluster(t, p, rc, opts)
					if calls != 0 || st.backup == nil || len(events.events) != 0 || state(t, rc).BackupCleanupStarted != nil {
						t.Fatal("keepBackups changed artifact inventory or contacted storage")
					}
				} else {
					for range 100 {
						rc.Cluster = cloneCluster(t, rc.Cluster)
						done, err := p.Cleanup(t.Context(), rc, opts)
						if done || err != nil {
							t.Fatalf("cleanup before bound done=%v err=%v", done, err)
						}
						if calls > 0 {
							break
						}
					}
					if calls == 0 || state(t, rc).BackupCleanupStarted == nil {
						t.Fatal("cleanup did not reach durable artifact stage")
					}
					started := *state(t, rc).BackupCleanupStarted
					rc.Clock.(*testClock).at = started.Add(119 * time.Second)
					done, err := p.Cleanup(t.Context(), rc, opts)
					if done || err != nil || len(events.events) != 0 {
						t.Fatal("storage error exhausted before two-minute bound", done, err)
					}
					rc.Cluster = cloneCluster(t, rc.Cluster)
					rc.Clock.(*testClock).at = started.Add(2 * time.Minute)
					cleanupCluster(t, p, rc, opts)
					cleanupCluster(t, p, rc, opts)
					if st.backup == nil || st.deletes != 0 || len(events.events) != 1 {
						t.Fatal("retained outcome lost inventory or repeated warning")
					}
					event := events.events[0]
					if event.Reason != "BackupArtifactsRetained" || !strings.Contains(event.Message, st.backup.Location) || !strings.Contains(event.Message, "remove only that object") || strings.Contains(event.Message, "storage-password") {
						t.Fatalf("untruthful or unsafe warning: %+v", event)
					}
				}
				if (rc.Cluster.Status.Phase == domain.PhaseSuspended) != keepVolumes {
					t.Fatal("keepVolumes did not control suspension independently")
				}
				if keepVolumes {
					if len(backend.services) != members {
						t.Fatal("suspension removed retained volumes")
					}
					// Sync must clear finalization state before recreating runtimes.
					if _, err := p.Sync(t.Context(), rc); err != nil {
						t.Fatal(err)
					}
					s := state(t, rc)
					if s.Suspended || s.CleanupBackups || s.BackupCleanupStarted != nil || len(s.RetainedBackups) != 0 {
						t.Fatal("resume reused artifact finalization checkpoints")
					}
				} else if len(backend.services) != 0 {
					t.Fatal("full deletion left runtime/volume services")
				}
			})
		}
	}
}

func TestCleanupArtifactsTransientFailureRecoversWithinBound(t *testing.T) {
	p, rc, backend, _ := setup(t)
	ready(t, p, rc)
	st := artifactStore(rc, state(t, rc).UID)
	events := &definitionsTestEvents{}
	rc.Events = events
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.Method != http.MethodDelete || r.URL.Path != "/backups/"+st.backup.Location || len(backend.services) != 0 {
			t.Errorf("unsafe object delete: %s %s", r.Method, r.URL.Path)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	u, _ := url.Parse(server.URL)
	calls := 0
	original := definitionsS3Client
	definitionsS3Client = func(_ domain.BackupStorage, c domain.StorageCredentials) (*minio.Client, func(), error) {
		calls++
		if calls == 1 {
			return nil, nil, errors.New("transient storage failure")
		}
		client, err := minio.New(u.Host, &minio.Options{Creds: credentials.NewStaticV4(c.AccessKeyID, c.SecretAccessKey, ""), Secure: false, Region: "us-east-1", BucketLookup: minio.BucketLookupPath})
		return client, func() {}, err
	}
	t.Cleanup(func() { definitionsS3Client = original })
	for range 100 {
		rc.Cluster = cloneCluster(t, rc.Cluster)
		done, err := p.Cleanup(t.Context(), rc, domain.DeleteOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if done {
			break
		}
	}
	if !state(t, rc).CleanupBackups || calls != 2 || requests != 1 || st.backup != nil || st.deletes != 1 || len(events.events) != 0 {
		t.Fatalf("transient cleanup failed: calls=%d requests=%d deletes=%d", calls, requests, st.deletes)
	}
}

func TestCleanupArtifactsPreservesPriorIncarnation(t *testing.T) {
	p, rc, _, _ := setup(t)
	ready(t, p, rc)
	st := artifactStore(rc, "earlier-incarnation")
	original := definitionsS3Client
	definitionsS3Client = func(domain.BackupStorage, domain.StorageCredentials) (*minio.Client, func(), error) {
		t.Fatal("contacted storage for prior-incarnation backup")
		return nil, nil, nil
	}
	t.Cleanup(func() { definitionsS3Client = original })
	cleanupCluster(t, p, rc, domain.DeleteOptions{})
	if st.backup == nil || st.deletes != 0 {
		t.Fatal("prior-incarnation inventory removed")
	}
}

func TestRabbitDeletionKeepsRestoreDependencyConflict(t *testing.T) {
	p, rc, backend, _ := setup(t)
	ready(t, p, rc)
	st := rc.Store.(*store.Store)
	b := &domain.Backup{Name: "base", Cluster: rc.Cluster.Name, Storage: "archive", Phase: domain.PhaseSucceeded, Provider: "rabbitmq", Type: "definitions"}
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
	services := len(backend.services)
	err := st.MarkClusterDeleting(t.Context(), rc.Cluster.Ref(), domain.DeleteOptions{})
	var conflict *domain.ResourceError
	if !errors.Is(err, store.ErrConflict) || !errors.As(err, &conflict) || conflict.Reason != domain.ReasonInvalidState || !strings.Contains(err.Error(), op.ID) {
		t.Fatal("lost API409 dependency conflict", err)
	}
	if len(backend.services) != services {
		t.Fatal("refused delete changed backend inventory")
	}
}
