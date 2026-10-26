// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/backupstorage"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

type backupCleanupClock struct{ at time.Time }

func (c *backupCleanupClock) Now() time.Time { return c.at }

func repositoryCleanupFixture(t *testing.T) (*Core, *engine.Context, *fakeBackend, *backupCleanupClock, *domain.Backup) {
	t.Helper()
	p, rc, backend := setup(t, "single")
	s := converge(t, p, rc)
	storage := &domain.BackupStorage{Namespace: "default", Name: "archive", Type: "s3", EndpointURL: "https://storage.example.test", Bucket: "backups", Prefix: "prefix", Credentials: domain.StorageCredentials{AccessKeyID: "access", SecretAccessKey: "fixture"}}
	if err := rc.Store.(*store.Store).CreateBackupStorage(t.Context(), storage); err != nil {
		t.Fatal(err)
	}
	rc.Cluster.Spec.Backup.Storage = storage.Name
	b := &domain.Backup{Namespace: "default", Cluster: rc.Cluster.Name, Name: "base", Provider: "postgres", Storage: storage.Name, Phase: domain.PhaseSucceeded, Postgres: &domain.PostgresBackup{Repository: repoPrefix(storage, rc.Cluster, s), Label: "20261017-021859F"}}
	if err := rc.Store.CreateBackup(t.Context(), b); err != nil {
		t.Fatal(err)
	}
	clock := &backupCleanupClock{at: time.Now().UTC()}
	rc.Clock = clock
	rc.Events = &deploymentGuardEvents{}
	return p, rc, backend, clock, b
}

func repositoryCleanupStep(t *testing.T, p *Core, rc *engine.Context, opts domain.DeleteOptions) bool {
	t.Helper()
	done, err := p.Cleanup(t.Context(), rc, opts)
	if err != nil {
		t.Fatal(err)
	}
	clone := *rc.Cluster
	clone.Internal = slices.Clone(rc.Cluster.Internal)
	rc.Cluster = &clone
	return done
}

func finishRepositoryCleanup(t *testing.T, p *Core, rc *engine.Context, opts domain.DeleteOptions) {
	t.Helper()
	for range 100 {
		if repositoryCleanupStep(t, p, rc, opts) {
			return
		}
	}
	t.Fatal("repository cleanup did not complete")
}

func TestRepositoryCleanupFailureRetainsArtifactsOnlyAfterDurableBound(t *testing.T) {
	for _, failure := range []string{"storage authentication rejected", "storage unavailable", "binding unavailable"} {
		t.Run(failure, func(t *testing.T) {
			p, rc, backend, clock, b := repositoryCleanupFixture(t)
			calls := 0
			p.deleteRepository = func(_ context.Context, storage domain.BackupStorage, namespace, cluster, prefix string) error {
				calls++
				if len(backend.services) != 0 {
					t.Fatal("object deletion before runtime/volume removal")
				}
				directory, err := backupstorage.PostgresDirectory(storage, namespace, cluster, prefix)
				if err != nil || directory != b.Postgres.Repository+"/" {
					t.Fatal("wrong repository", directory, err)
				}
				return errors.New(failure)
			}
			if failure == "binding unavailable" {
				rc.Store = missingRepositoryBinding{rc.Store}
			}
			for range 100 {
				if repositoryCleanupStep(t, p, rc, domain.DeleteOptions{}) {
					t.Fatal("failure finished before its bound")
				}
				s, err := load(rc.Cluster)
				if err != nil {
					t.Fatal(err)
				}
				if s.BackupCleanupStarted != nil && len(backend.services) == 0 {
					break
				}
			}
			s, _ := load(rc.Cluster)
			if s.BackupCleanupStarted == nil || len(backend.services) != 0 {
				t.Fatal("cleanup did not reach storage stage")
			}
			clock.at = s.BackupCleanupStarted.Add(119 * time.Second)
			if repositoryCleanupStep(t, p, rc, domain.DeleteOptions{}) {
				t.Fatal("storage failure retained before deadline")
			}
			if len(rc.Events.(*deploymentGuardEvents).events) != 0 {
				t.Fatal("premature retained warning")
			}
			// Reconstruct the provider and checkpoint as on controller restart.
			resumed := New(Options{})
			resumed.deleteRepository = p.deleteRepository
			clock.at = s.BackupCleanupStarted.Add(121 * time.Second)
			finishRepositoryCleanup(t, resumed, rc, domain.DeleteOptions{})
			events := rc.Events.(*deploymentGuardEvents).events
			if len(events) != 1 || events[0].Reason != "BackupArtifactsRetained" || !strings.Contains(events[0].Message, "archive") || !strings.Contains(events[0].Message, "object-store tooling") {
				t.Fatal("missing retained outcome", events)
			}
			st := rc.Secrets.(*store.Store)
			if _, err := st.GetBackup(t.Context(), rc.Cluster.Ref(), b.Name); err != nil {
				t.Fatal("lost retained inventory", err)
			}
			if err := st.DeleteCluster(t.Context(), rc.Cluster.Ref()); err != nil {
				t.Fatal(err)
			}
			if _, err := st.GetCluster(t.Context(), rc.Cluster.Ref()); !errors.Is(err, store.ErrNotFound) {
				t.Fatal("cluster record remains", err)
			}
			if _, err := st.GetBackup(t.Context(), rc.Cluster.Ref(), b.Name); err != nil {
				t.Fatal("record deletion lost retained inventory", err)
			}
			if failure != "binding unavailable" && calls != 1 {
				t.Fatal("storage called after deadline", calls)
			}
		})
	}
}

type missingRepositoryBinding struct{ engine.ClusterStore }

func (s missingRepositoryBinding) GetBackupStorage(context.Context, string, string) (*domain.BackupStorage, error) {
	return nil, errors.New("binding unavailable")
}

func TestRepositoryCleanupTransientFailureThenSuccess(t *testing.T) {
	p, rc, backend, _, b := repositoryCleanupFixture(t)
	calls := 0
	p.deleteRepository = func(context.Context, domain.BackupStorage, string, string, string) error {
		calls++
		if len(backend.services) != 0 {
			t.Fatal("writers still exist")
		}
		if calls == 1 {
			return errors.New("transient authentication/store failure")
		}
		return nil
	}
	finishRepositoryCleanup(t, p, rc, domain.DeleteOptions{})
	if calls != 2 {
		t.Fatal("transient cleanup not retried", calls)
	}
	if _, err := rc.Store.GetBackup(t.Context(), rc.Cluster.Ref(), b.Name); !missing(err) {
		t.Fatal("successful cleanup retained inventory", err)
	}
	if len(rc.Events.(*deploymentGuardEvents).events) != 0 {
		t.Fatal("transient failure reported retained")
	}
	finishRepositoryCleanup(t, p, rc, domain.DeleteOptions{})
	if calls != 2 {
		t.Fatal("completed repository replayed")
	}
}

func TestRepositoryCleanupKeepBackupsAndResume(t *testing.T) {
	p, rc, _, _, b := repositoryCleanupFixture(t)
	p.deleteRepository = func(context.Context, domain.BackupStorage, string, string, string) error {
		t.Fatal("keepBackups deleted artifacts")
		return nil
	}
	finishRepositoryCleanup(t, p, rc, domain.DeleteOptions{KeepBackups: true, KeepVolumes: true})
	if _, err := rc.Store.GetBackup(t.Context(), rc.Cluster.Ref(), b.Name); err != nil {
		t.Fatal(err)
	}
	s, _ := load(rc.Cluster)
	stamp := time.Now().Add(-time.Hour)
	s.BackupCleanupStarted = &stamp
	s.CleanedBackupRepositories = []string{"old-checkpoint"}
	s.RetainedBackupStorages = []string{"archive"}
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Sync(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	s, _ = load(rc.Cluster)
	if s.Suspended || s.BackupCleanupStarted != nil || len(s.CleanedBackupRepositories) != 0 || len(s.RetainedBackupStorages) != 0 {
		t.Fatal("resume kept deletion checkpoints", s)
	}
}

func TestRepositoryCleanupPreservesDependency409(t *testing.T) {
	_, rc, backend, _, b := repositoryCleanupFixture(t)
	st := rc.Store.(*store.Store)
	target := &domain.Cluster{Name: "dependent", Spec: domain.ClusterSpec{DataSource: &domain.DataSource{Cluster: rc.Cluster.Name, Backup: b.Name}}}
	if err := st.CreateCluster(store.WithOperation(t.Context(), &domain.Operation{ID: "clone-dependency", Kind: domain.OperationCreate}), target); err != nil {
		t.Fatal(err)
	}
	before := len(backend.services)
	err := st.MarkClusterDeleting(t.Context(), rc.Cluster.Ref(), domain.DeleteOptions{})
	var conflict *domain.ResourceError
	if !errors.Is(err, store.ErrConflict) || !errors.As(err, &conflict) || conflict.Reason != domain.ReasonInvalidState {
		t.Fatal("lost HTTP409 dependency fence", err)
	}
	if len(backend.services) != before {
		t.Fatal("refused deletion changed runtime")
	}
}
