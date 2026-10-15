// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package psmdb

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/backupstorage"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

type unavailableBackupAgents struct{ calls int }

func (d *unavailableBackupAgents) Dial(context.Context, domain.ClusterRef, string) (*agentapi.Client, error) {
	d.calls++
	return nil, errors.New("member is unavailable")
}

type interruptedBackupInventory struct {
	engine.ClusterStore
	fail bool
}

func (s *interruptedBackupInventory) DeleteBackup(ctx context.Context, ref domain.ClusterRef, name string) error {
	if s.fail {
		s.fail = false
		return errors.New("inventory temporarily unavailable")
	}
	return s.ClusterStore.DeleteBackup(ctx, ref, name)
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

func TestOfflineBackupCleanupPreservesInventoryAndResourcesUntilSuccess(t *testing.T) {
	for _, failure := range []string{"storage", "inventory"} {
		t.Run(failure, func(t *testing.T) {
			p, rc, f, agents := backupCleanupSetup(t)
			s := retryState(t, rc)
			b := &domain.Backup{Name: "backup", Cluster: rc.Cluster.Name, Storage: "archive", PBMName: "snapshot", PBMPrefix: "archive/" + s.PBMPrefix, RecoveryIdentity: "backup-recovery"}
			if err := rc.Store.CreateBackup(t.Context(), b); err != nil {
				t.Fatal(err)
			}
			if err := rc.Secrets.SetClusterSecret(t.Context(), domain.ClusterRef{Name: b.RecoveryIdentity}, "users/userAdmin", []byte("recovery")); err != nil {
				t.Fatal(err)
			}
			suspendForBackupCleanup(t, p, rc)
			fail := failure == "storage"
			calls := 0
			p.deleteClusterObjects = func(context.Context, domain.BackupStorage, string, string) error {
				calls++
				if fail {
					return errors.New("storage permission denied")
				}
				return nil
			}
			if failure == "inventory" {
				rc.Store = &interruptedBackupInventory{ClusterStore: rc.Store, fail: true}
			}
			if done, err := p.Cleanup(t.Context(), rc, domain.DeleteOptions{}); done || err == nil {
				t.Fatal("cleanup failure was ignored", done, err)
			}
			if failure == "storage" && (!strings.Contains(rc.Cluster.Status.Message, "repair storage access") || !strings.Contains(rc.Cluster.Status.Message, "--keep-backups")) {
				t.Fatal("failure is not actionable", rc.Cluster.Status.Message)
			}
			if _, err := rc.Store.GetBackup(t.Context(), rc.Cluster.Ref(), b.Name); err != nil {
				t.Fatal("failure lost retry inventory", err)
			}
			if f.services[s.Members[0].Volume] == nil || len(retryState(t, rc).CleanedStorages) != 0 {
				t.Fatal("failure advanced persistent resource deletion")
			}
			if failure == "storage" {
				if _, err := rc.Secrets.GetClusterSecret(t.Context(), domain.ClusterRef{Name: b.RecoveryIdentity}, "users/userAdmin"); err != nil {
					t.Fatal("storage failure discarded recovery credential", err)
				}
			}
			fail = false
			// Deserialize the checkpoint, as after a controller restart.
			clone := *rc.Cluster
			clone.Internal = append([]byte(nil), rc.Cluster.Internal...)
			rc.Cluster = &clone
			for range 30 {
				done, err := p.Cleanup(t.Context(), rc, domain.DeleteOptions{})
				if err != nil {
					t.Fatal(err)
				}
				if done {
					if calls != 2 || len(f.services) != 0 || agents.calls != 0 {
						t.Fatal("retry did not finish idempotently", calls, f.services, agents.calls)
					}
					if _, err := rc.Store.GetBackup(t.Context(), rc.Cluster.Ref(), b.Name); !missing(err) {
						t.Fatal("retry left backup inventory", err)
					}
					return
				}
			}
			t.Fatal("retry never completed")
		})
	}
}

func TestUnreachableRuntimeDoesNotAuthorizeDirectBackupDeletion(t *testing.T) {
	p, rc, f, agents := backupCleanupSetup(t)
	direct := false
	p.deleteClusterObjects = func(context.Context, domain.BackupStorage, string, string) error { direct = true; return nil }
	done, err := p.Cleanup(t.Context(), rc, domain.DeleteOptions{})
	if done || err == nil || !strings.Contains(err.Error(), "restore member availability") || !strings.Contains(err.Error(), "--keep-backups") {
		t.Fatal("PBM failure must be actionable", done, err)
	}
	if direct || agents.calls == 0 || len(f.services) != 2 {
		t.Fatal("unreachable runtime caused unsafe direct deletion", direct, agents.calls, f.services)
	}
}

func TestOfflineBackupDeletionRequiresRecordedClusterNamespace(t *testing.T) {
	p, rc, _, _ := backupCleanupSetup(t)
	suspendForBackupCleanup(t, p, rc)
	s := retryState(t, rc)
	s.PBMPrefix = "karakoram/another-incarnation"
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	direct := false
	p.deleteClusterObjects = func(context.Context, domain.BackupStorage, string, string) error { direct = true; return nil }
	done, err := p.Cleanup(t.Context(), rc, domain.DeleteOptions{})
	if done || err == nil || direct || !strings.Contains(err.Error(), "resume the cluster") {
		t.Fatal("unsafe namespace accepted", done, err, direct)
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
