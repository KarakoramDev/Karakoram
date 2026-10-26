// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package psmdb

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/backupstorage"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

type unavailableBackupAgents struct {
	calls   int
	failure string
}

func (d *unavailableBackupAgents) Dial(context.Context, domain.ClusterRef, string) (*agentapi.Client, error) {
	d.calls++
	if d.failure != "" {
		return nil, errors.New(d.failure)
	}
	return nil, errors.New("member is unavailable")
}

func backupCleanupSetup(t *testing.T) (*Provider, *engine.Context, *fakeInfra, *unavailableBackupAgents) {
	t.Helper()
	p, rc, f, _ := setup(t)
	if _, err := p.Sync(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	s := retryState(t, rc)
	if err := p.ensureSecrets(t.Context(), rc, s); err != nil {
		t.Fatal(err)
	}
	st := rc.Store.(*store.Store)
	for _, name := range []string{"archive", "alternate"} {
		if err := st.CreateBackupStorage(t.Context(), &domain.BackupStorage{Name: name, Type: "s3", EndpointURL: "https://storage.example.test", Bucket: "test-bucket", Prefix: name, Credentials: domain.StorageCredentials{AccessKeyID: "access", SecretAccessKey: "secret"}}); err != nil {
			t.Fatal(err)
		}
	}
	s.PBMStorage = "archive"
	s.PBMHash = "configured"
	s.UsedStorages = []string{"archive"}
	m := s.Members[0]
	m.ServiceID, m.VolumeID = m.Name+"-id", m.Volume+"-id"
	f.services[m.Name] = &infra.Service{ID: m.ServiceID, Hostname: m.Name}
	f.services[m.Volume] = &infra.Service{ID: m.VolumeID, Hostname: m.Volume}
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	agents := &unavailableBackupAgents{}
	rc.Agents = agents
	rc.Events = &orphanEvents{}
	return p, rc, f, agents
}

func suspendForBackupCleanup(t *testing.T, p *Provider, rc *engine.Context) {
	t.Helper()
	for range 20 {
		done, err := p.Cleanup(t.Context(), rc, domain.DeleteOptions{KeepVolumes: true, KeepBackups: true})
		if err != nil {
			t.Fatal(err)
		}
		if done {
			if !retryState(t, rc).Suspended {
				t.Fatal("cluster was not suspended")
			}
			return
		}
	}
	t.Fatal("suspension did not finish")
}

func TestFullDeletionOfSuspendedClusterUsesStorageWithoutPBM(t *testing.T) {
	for _, inventory := range []string{"empty", "snapshot", "failed"} {
		t.Run(inventory, func(t *testing.T) {
			p, rc, f, agents := backupCleanupSetup(t)
			s := retryState(t, rc)
			if inventory != "empty" {
				b := &domain.Backup{Name: "backup", Cluster: rc.Cluster.Name, Storage: "alternate", Phase: domain.PhaseFailed, RecoveryIdentity: "backup-recovery", PBMPrefix: "alternate/" + s.PBMPrefix}
				if inventory == "snapshot" {
					b.Phase = domain.PhaseSucceeded
					b.PBMName = "snapshot"
				}
				if err := rc.Store.CreateBackup(t.Context(), b); err != nil {
					t.Fatal(err)
				}
				if err := rc.Secrets.SetClusterSecret(t.Context(), domain.ClusterRef{Name: b.RecoveryIdentity}, "users/userAdmin", []byte("recovery")); err != nil {
					t.Fatal(err)
				}
			}
			suspendForBackupCleanup(t, p, rc)
			calls := map[string]int{}
			p.deleteClusterObjects = func(_ context.Context, storage domain.BackupStorage, cluster, uid string) error {
				calls[storage.Name]++
				if f.services[s.Members[0].Name] != nil {
					t.Fatal("direct deletion while runtime exists")
				}
				directory, err := backupstorage.ClusterDirectory(storage, cluster, uid)
				if err != nil || directory != storage.Prefix+"/"+s.PBMPrefix+"/" {
					t.Fatal("wrong storage namespace", directory, err)
				}
				return nil
			}
			done := false
			for range 30 {
				var err error
				done, err = p.Cleanup(t.Context(), rc, domain.DeleteOptions{})
				if err != nil {
					t.Fatal(err)
				}
				if done {
					break
				}
			}
			if !done || len(f.services) != 0 || agents.calls != 0 || calls["archive"] != 1 {
				t.Fatal("full deletion did not finish without a PBM node", done, f.services, agents.calls, calls)
			}
			if inventory != "empty" && calls["alternate"] != 1 {
				t.Fatal("backup's alternate storage was not cleaned", calls)
			}
			backups, err := rc.Store.ListBackups(t.Context(), rc.Cluster.Ref())
			if err != nil || len(backups) != 0 {
				t.Fatal("backup inventory retained after storage cleanup", backups, err)
			}
			if inventory != "empty" {
				if _, err := rc.Secrets.GetClusterSecret(t.Context(), domain.ClusterRef{Name: "backup-recovery"}, "users/userAdmin"); !missing(err) {
					t.Fatal("recovery identity survived deletion", err)
				}
			}
		})
	}
}

func TestOfflineCleanupRemovesInventoryAfterOlderStorageCheckpoint(t *testing.T) {
	p, rc, _, agents := backupCleanupSetup(t)
	suspendForBackupCleanup(t, p, rc)
	s := retryState(t, rc)
	s.CleanedStorages = []string{"archive"}
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	b := &domain.Backup{Name: "failed", Cluster: rc.Cluster.Name, Storage: "archive", Phase: domain.PhaseFailed}
	if err := rc.Store.CreateBackup(t.Context(), b); err != nil {
		t.Fatal(err)
	}
	p.deleteClusterObjects = func(context.Context, domain.BackupStorage, string, string) error { return nil }
	for range 30 {
		done, err := p.Cleanup(t.Context(), rc, domain.DeleteOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if done {
			if _, err := rc.Store.GetBackup(t.Context(), rc.Cluster.Ref(), b.Name); !missing(err) {
				t.Fatal("older storage checkpoint retained failed inventory", err)
			}
			if agents.calls != 0 {
				t.Fatal("offline cleanup required a PBM node")
			}
			return
		}
	}
	t.Fatal("cleanup did not finish")
}

func TestDeleteWithoutNativeCredentialsFencesRuntimesFirst(t *testing.T) {
	for _, failure := range []string{"AuthenticationFailed", "all members unreachable"} {
		t.Run(failure, func(t *testing.T) {
			p, rc, backend, agents := backupCleanupSetup(t)
			s := retryState(t, rc)
			agents.failure = failure
			b := &domain.Backup{Name: "owned", Cluster: rc.Cluster.Name, Storage: "archive", PBMName: "snapshot", PBMPrefix: "archive/" + s.PBMPrefix}
			if err := rc.Store.CreateBackup(t.Context(), b); err != nil {
				t.Fatal(err)
			}
			calls := 0
			p.deleteClusterObjects = func(_ context.Context, storage domain.BackupStorage, cluster, uid string) error {
				calls++
				if backend.services[s.Members[0].Name] != nil {
					t.Fatal("runtime can still write backups")
				}
				prefix, err := backupstorage.ClusterDirectory(storage, cluster, uid)
				if err != nil || prefix != b.PBMPrefix+"/" {
					t.Fatal("wrong owned prefix", prefix, err)
				}
				return nil
			}
			finishBackupCleanup(t, p, rc, domain.DeleteOptions{})
			if calls != 1 || agents.calls != 0 || len(backend.services) != 0 {
				t.Fatal("deletion depended on native authentication", calls, agents.calls, backend.services)
			}
			if _, err := rc.Store.GetBackup(t.Context(), rc.Cluster.Ref(), b.Name); !missing(err) {
				t.Fatal("success retained backup inventory", err)
			}
		})
	}
}

func finishBackupCleanup(t *testing.T, p *Provider, rc *engine.Context, opts domain.DeleteOptions) {
	t.Helper()
	for range 40 {
		done, err := p.Cleanup(t.Context(), rc, opts)
		if err != nil {
			t.Fatal(err)
		}
		// Rehydrate every checkpoint, including retained outcomes and deadlines.
		clone := *rc.Cluster
		clone.Internal = slices.Clone(rc.Cluster.Internal)
		rc.Cluster = &clone
		if done {
			return
		}
	}
	t.Fatal("cleanup did not finish")
}

func TestArtifactFailureRetainsInventoryButFinishesDeletion(t *testing.T) {
	for _, failure := range []string{"permission", "prefix", "identity", "deadline"} {
		t.Run(failure, func(t *testing.T) {
			p, rc, backend, agents := backupCleanupSetup(t)
			s := retryState(t, rc)
			b := &domain.Backup{Name: "owned", Cluster: rc.Cluster.Name, Storage: "archive", PBMName: "snapshot", PBMPrefix: "archive/" + s.PBMPrefix}
			if failure == "prefix" {
				b.PBMPrefix = "old/" + s.PBMPrefix
			}
			if failure == "identity" {
				s.UID = ""
			}
			if failure == "deadline" {
				start := now(rc).Add(-3 * time.Minute)
				s.BackupCleanupStarted = &start
			}
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			if err := rc.Store.CreateBackup(t.Context(), b); err != nil {
				t.Fatal(err)
			}
			calls := 0
			p.deleteClusterObjects = func(context.Context, domain.BackupStorage, string, string) error {
				calls++
				return errors.New("storage permission denied")
			}
			finishBackupCleanup(t, p, rc, domain.DeleteOptions{})
			if failure != "permission" && calls != 0 {
				t.Fatal("unsafe or expired deletion attempted")
			}
			if len(backend.services) != 0 || agents.calls != 0 {
				t.Fatal("artifact failure stranded runtime", backend.services, agents.calls)
			}
			if _, err := rc.Store.GetBackup(t.Context(), rc.Cluster.Ref(), b.Name); err != nil {
				t.Fatal("lost retained artifact inventory", err)
			}
			events := rc.Events.(*orphanEvents).items
			if len(events) != 1 || events[0].Reason != "BackupArtifactsRetained" || !strings.Contains(events[0].Message, b.PBMPrefix) {
				t.Fatal("missing truthful retained outcome", events)
			}
		})
	}
}

func TestDeletingReplacementPreservesRetainedSourceBackup(t *testing.T) {
	p, rc, _, _ := backupCleanupSetup(t)
	old := retryState(t, rc)
	b := &domain.Backup{Name: "retained", Cluster: rc.Cluster.Name, Storage: "archive", Phase: domain.PhaseSucceeded, PBMName: "snapshot", PBMPrefix: "archive/" + old.PBMPrefix, RecoveryIdentity: "retained-recovery"}
	if err := rc.Store.CreateBackup(t.Context(), b); err != nil {
		t.Fatal(err)
	}
	identity := domain.ClusterRef{Name: b.RecoveryIdentity}
	if err := rc.Secrets.SetClusterSecret(t.Context(), identity, "users/userAdmin", []byte("recovery")); err != nil {
		t.Fatal(err)
	}
	deleted := false
	for range 30 {
		done, err := p.Cleanup(t.Context(), rc, domain.DeleteOptions{KeepBackups: true})
		if err != nil {
			t.Fatal(err)
		}
		if done {
			deleted = true
			break
		}
	}
	if !deleted {
		t.Fatal("original cluster deletion did not finish")
	}
	st := rc.Store.(*store.Store)
	if err := st.DeleteCluster(t.Context(), rc.Cluster.Ref()); err != nil {
		t.Fatal(err)
	}
	replacement := &domain.Cluster{Name: rc.Cluster.Name, Spec: rc.Cluster.Spec}
	replacement.Spec.Backup.Storage = "archive"
	replacement.Spec.DataSource = &domain.DataSource{Cluster: b.Cluster, Backup: b.Name}
	if err := st.CreateCluster(t.Context(), replacement); err != nil {
		t.Fatal(err)
	}
	rc.Cluster = replacement
	if _, err := p.Sync(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	s := retryState(t, rc)
	if s.UID == old.UID {
		t.Fatal("replacement reused its source incarnation")
	}
	// A replacement using this retained restore source owns only its new
	// destination prefix, whether deleted before or after runtime provisioning.
	s.PBMStorage, s.PBMHash = "archive", "configured"
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	calls := 0
	p.deleteClusterObjects = func(_ context.Context, _ domain.BackupStorage, _ string, uid string) error {
		calls++
		if uid != s.UID || uid == old.UID {
			t.Fatal("deleting retained source artifacts", uid)
		}
		return nil
	}
	for range 30 {
		done, err := p.Cleanup(t.Context(), rc, domain.DeleteOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if done {
			items, err := st.ListBackups(t.Context(), rc.Cluster.Ref())
			if err != nil || len(items) != 1 || items[0].PBMPrefix != b.PBMPrefix {
				t.Fatal("replacement deletion discarded retained source", items, err)
			}
			if _, err := rc.Secrets.GetClusterSecret(t.Context(), identity, "users/userAdmin"); err != nil {
				t.Fatal("retained recovery credential was deleted", err)
			}
			if calls != 1 {
				t.Fatal("unexpected storage deletion passes", calls)
			}
			return
		}
	}
	t.Fatal("replacement deletion did not finish")
}

func TestMongoDeletionKeepsRestoreDependencyConflict(t *testing.T) {
	_, rc, backend, _ := backupCleanupSetup(t)
	st := rc.Store.(*store.Store)
	b := &domain.Backup{Name: "base", Cluster: rc.Cluster.Name, Storage: "archive", Phase: domain.PhaseSucceeded, PBMName: "snapshot"}
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
	err := st.MarkClusterDeleting(t.Context(), rc.Cluster.Ref(), domain.DeleteOptions{})
	var conflict *domain.ResourceError
	if !errors.Is(err, store.ErrConflict) || !errors.As(err, &conflict) || conflict.Reason != domain.ReasonInvalidState || !strings.Contains(err.Error(), op.ID) {
		t.Fatal("lost API409 dependency conflict", err)
	}
	if len(backend.services) != 2 {
		t.Fatal("refused deletion altered runtime")
	}
}
