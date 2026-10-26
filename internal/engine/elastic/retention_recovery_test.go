// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/elastic/agentspec"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

type interruptedSnapshotCleanupStore struct {
	engine.ClusterStore
	interrupted bool
}

func (s *interruptedSnapshotCleanupStore) DeleteBackup(ctx context.Context, ref domain.ClusterRef, name string) error {
	if !s.interrupted {
		s.interrupted = true
		return errors.New("inventory deletion interrupted")
	}
	return s.ClusterStore.DeleteBackup(ctx, ref, name)
}

func TestCleanupSnapshotInventoryAfterConfirmedDeletion(t *testing.T) {
	p, rc, f, _ := coreSetup(t, 1)
	ctx := context.Background()
	s := coreConverge(t, p, rc, f)
	storage := &domain.BackupStorage{Name: "archive", Bucket: "backups"}
	s.Repositories[rc.Cluster.Ref().Namespace+"/archive"] = &repository{Name: "kk-archive", BasePath: "owned"}
	s.CleanedRepositories = []string{rc.Cluster.Ref().Namespace + "/archive"}
	started := time.Now()
	s.BackupCleanupStarted = &started
	b := &domain.Backup{Namespace: rc.Cluster.Ref().Namespace, Cluster: rc.Cluster.Name, Name: "one", Storage: "archive", Provider: "elasticsearch", Type: "snapshot", Phase: domain.PhaseSucceeded, Location: snapshotLocation(storage, "owned", "kk-archive", "one")}
	if err := rc.Store.CreateBackup(ctx, b); err != nil {
		t.Fatal(err)
	}
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	inventory := &interruptedSnapshotCleanupStore{ClusterStore: rc.Store}
	rc.Store = inventory
	for range 30 {
		_, err := p.Cleanup(ctx, rc, domain.DeleteOptions{})
		if err != nil {
			if !inventory.interrupted {
				t.Fatal(err)
			}
			break
		}
	}
	if !inventory.interrupted {
		t.Fatal("expected interrupted inventory deletion")
	}
	if _, err := rc.Store.GetBackup(ctx, rc.Cluster.Ref(), "one"); err != nil {
		t.Fatal("inventory removed before its checkpoint", err)
	}
	if _, err := p.Cleanup(ctx, rc, domain.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := rc.Store.GetBackup(ctx, rc.Cluster.Ref(), "one"); !missing(err) {
		t.Fatal("deleted repository remained in inventory", err)
	}
}

func TestCleanupPreservesRetainedSnapshotInventory(t *testing.T) {
	for _, mode := range []string{"keep", "unreachable", "readonly"} {
		t.Run(mode, func(t *testing.T) {
			p, rc, f, es := coreSetup(t, 1)
			ctx := context.Background()
			s := coreConverge(t, p, rc, f)
			s.Repositories["owned"] = &repository{Name: "kk-archive", BasePath: "owned", ReadOnly: mode == "readonly"}
			started := time.Now().Add(-3 * time.Minute)
			s.BackupCleanupStarted = &started
			b := &domain.Backup{Namespace: rc.Cluster.Ref().Namespace, Cluster: rc.Cluster.Name, Name: "one", Storage: "archive", Provider: "elasticsearch", Phase: domain.PhaseSucceeded, Location: snapshotLocation(&domain.BackupStorage{Bucket: "backups"}, "owned", "kk-archive", "one")}
			if err := rc.Store.CreateBackup(ctx, b); err != nil {
				t.Fatal(err)
			}
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			es.snapshots = []string{"one"}
			if mode == "unreachable" {
				rc.Dialer = coreDialer{address: "127.0.0.1:1"}
			}
			done := false
			for range 30 {
				var err error
				done, err = p.Cleanup(ctx, rc, domain.DeleteOptions{KeepBackups: mode == "keep"})
				if err != nil {
					t.Fatal(err)
				}
				if done {
					break
				}
			}
			if !done || len(es.snapshots) != 1 {
				t.Fatal("retention cleanup did not finish safely")
			}
			if _, err := rc.Store.GetBackup(ctx, rc.Cluster.Ref(), "one"); err != nil {
				t.Fatal("retained snapshot inventory removed", err)
			}
		})
	}
}

func TestRetainedResumeAbandonsInterruptedRestore(t *testing.T) {
	for _, mode := range []string{"restore", "initial", "queued-initial"} {
		t.Run(mode, func(t *testing.T) {
			p, rc, f, es := coreSetup(t, 1)
			ctx := context.Background()
			kb := rc.Cluster.Spec.Components.Extra[kindKibana]
			kb.Replicas = 1
			rc.Cluster.Spec.Components.Extra[kindKibana] = kb
			s := coreConverge(t, p, rc, f)
			s.RestoreActive = mode != "queued-initial"
			if mode != "restore" {
				rc.Cluster.Spec.DataSource = &domain.DataSource{Cluster: "source", Backup: "one"}
				s.InitialRestore = "initial-restore"
			}
			es.transient = map[string]any{"action.destructive_requires_name": false}
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			done := false
			for range 40 {
				var err error
				done, err = p.Cleanup(ctx, rc, domain.DeleteOptions{KeepVolumes: true, KeepBackups: true})
				if err != nil {
					t.Fatal(err)
				}
				if done {
					break
				}
			}
			if !done {
				t.Fatal("cleanup did not suspend")
			}
			if mode == "restore" {
				s = coreConverge(t, p, rc, f)
			} else {
				// Retained runtimes and destructive settings still recover, but an
				// abandoned initial snapshot must never establish cluster readiness.
				failed := false
				for range 300 {
					result, err := p.Sync(ctx, rc)
					if err != nil {
						t.Fatal(err)
					}
					if result.ConvergenceError != nil {
						failed = true
						break
					}
				}
				if !failed {
					t.Fatal("abandoned initial restore never reached terminal failure")
				}
				var err error
				s, err = load(rc.Cluster)
				if err != nil {
					t.Fatal(err)
				}
				status, _ := p.Status(ctx, rc)
				if status.Phase != domain.PhaseFailed {
					t.Fatal("abandoned initial restore became ready", status)
				}
			}
			if s.RestoreActive || s.ResumeRestoreReset || s.find("demokb").Stage != stageJoined || f.services["demokb"] == nil {
				t.Fatal("resume did not restore normal component convergence", s)
			}
			if mode != "restore" && s.InitialRestore != "abandoned" {
				t.Fatal("initial restore was not abandoned", s.InitialRestore)
			}
			if _, ok := es.transient["action.destructive_requires_name"]; ok {
				t.Fatal("restore destructive setting survived resume")
			}
			cfg, err := p.MemberConfig(ctx, rc, "demokb")
			if err != nil || !slices.Contains(cfg.DesiredProcesses, "kibana") {
				t.Fatal("component process intent not released", cfg.DesiredProcesses, err)
			}
			events := rc.Events.(*coreEvents).events
			warnings := slices.DeleteFunc(slices.Clone(events), func(e domain.Event) bool { return e.Reason != "RestoreAbandoned" })
			if len(warnings) != 1 || warnings[0].Type != "Warning" {
				t.Fatal("missing interrupted-restore warning", events)
			}
		})
	}
}

func TestUnusedDeletedStorageRollsOutClientRemoval(t *testing.T) {
	p, rc, f, es := coreSetup(t, 1)
	ctx := context.Background()
	storage := &domain.BackupStorage{Namespace: rc.Cluster.Ref().Namespace, Name: "archive", Type: "s3", EndpointURL: "https://storage.example", Region: "us-east-1", Bucket: "backups", Credentials: domain.StorageCredentials{AccessKeyID: "access", SecretAccessKey: "secret"}}
	st := rc.Store.(*store.Store)
	if err := st.CreateBackupStorage(ctx, storage); err != nil {
		t.Fatal(err)
	}
	rc.Cluster.Spec.Backup.Storage = storage.Name
	s := coreConverge(t, p, rc, f)
	if len(s.Storages) != 1 || len(s.Repositories) != 1 {
		t.Fatal("storage not configured", s)
	}
	configVersion, configHash := s.ConfigVersion, s.find("demon0").ConfigHash
	rc.Cluster.Spec.Backup.Storage = ""
	if err := st.DeleteBackupStorage(ctx, storage.Namespace, storage.Name); err != nil {
		t.Fatal(err)
	}
	if _, err := p.MemberConfig(ctx, rc, "demon0"); err != nil {
		t.Fatal("config pull failed on an unreferenced deleted storage", err)
	}
	s = coreConverge(t, p, rc, f)
	if len(s.Storages) != 0 || len(s.Repositories) != 0 || !slices.Contains(es.mutations, "DELETE /_snapshot/"+RepositoryName(storage.Name)) {
		t.Fatal("unused repository/client was not retired", s, es.mutations)
	}
	if s.ConfigVersion <= configVersion || s.find("demon0").ConfigHash == configHash || !slices.Contains(es.mutations, "POST /_flush") {
		t.Fatal("client removal bypassed rolling configuration")
	}
	cfg, err := p.MemberConfig(ctx, rc, "demon0")
	if err != nil {
		t.Fatal(err)
	}
	var spec agentspec.Spec
	if err = json.Unmarshal(cfg.EngineSpec, &spec); err != nil {
		t.Fatal(err)
	}
	for key := range spec.Elasticsearch.Settings {
		if strings.HasPrefix(key, "s3.client.") {
			t.Fatal("removed static client still published", key)
		}
	}
	if len(spec.Elasticsearch.SecureSettings) != 0 {
		t.Fatal("removed client credentials still published")
	}
}

func TestStoragePruningProtectsRestoreSourcesAndBackupInventory(t *testing.T) {
	p, rc, f, _ := coreSetup(t, 1)
	ctx := context.Background()
	s := coreConverge(t, p, rc, f)
	s.Storages = []string{rc.Cluster.Ref().Namespace + "/archive"}
	b := &domain.Backup{Namespace: rc.Cluster.Ref().Namespace, Cluster: rc.Cluster.Name, Name: "one", Storage: "archive", Provider: "elasticsearch", Phase: domain.PhaseSucceeded}
	if err := rc.Store.CreateBackup(ctx, b); err != nil {
		t.Fatal(err)
	}
	if changed, err := p.reconcileStorages(ctx, rc, s); err != nil || changed || len(s.Storages) != 1 {
		t.Fatal("backup inventory client pruned", changed, err)
	}
	b.Cluster = "source"
	if err := rc.Store.CreateBackup(ctx, b); err != nil {
		t.Fatal(err)
	}
	if err := rc.Store.DeleteBackup(ctx, rc.Cluster.Ref(), "one"); err != nil {
		t.Fatal(err)
	}
	r := &domain.Restore{Namespace: rc.Cluster.Ref().Namespace, Cluster: rc.Cluster.Name, Name: "restore", Source: domain.RestoreSource{SourceCluster: "source", Backup: "one"}, Phase: domain.PhasePending}
	op := &domain.Operation{ID: "source-restore", Namespace: rc.Cluster.Ref().Namespace, Cluster: rc.Cluster.Name, Kind: domain.OperationRestore, Payload: json.RawMessage(`{"restore":"restore"}`)}
	if err := rc.Store.EnqueueRestore(ctx, r, op); err != nil {
		t.Fatal(err)
	}
	if changed, err := p.reconcileStorages(ctx, rc, s); err != nil || changed || len(s.Storages) != 1 {
		t.Fatal("queued restore source client pruned", changed, err)
	}
	if _, _, err := p.staticSettings(ctx, rc, s); !missing(err) {
		t.Fatal("deleted but actively referenced storage was silently ignored", err)
	}
	op.Phase = domain.PhaseFailed
	if err := rc.Store.(*store.Store).UpdateOperation(ctx, op); err != nil {
		t.Fatal(err)
	}
	rc.Cluster.Spec.DataSource = &domain.DataSource{Cluster: "source", Backup: "one"}
	if changed, err := p.reconcileStorages(ctx, rc, s); err != nil || changed {
		t.Fatal("initial restore source client pruned", changed, err)
	}
	s.InitialRestore = "done"
	if changed, err := p.reconcileStorages(ctx, rc, s); err != nil || !changed || len(s.Storages) != 0 || s.Rolling == nil {
		t.Fatal("completed source reference prevented retirement", changed, err, s)
	}
}
