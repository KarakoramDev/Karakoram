// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package psmdb

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

type unprovenSourceReads struct {
	engine.ClusterStore
	reads int
}

func (s *unprovenSourceReads) GetCluster(ctx context.Context, ref domain.ClusterRef) (*domain.Cluster, error) {
	if ref.Name == "missing-source" {
		s.reads++
	}
	return s.ClusterStore.GetCluster(ctx, ref)
}
func (s *unprovenSourceReads) GetBackup(ctx context.Context, ref domain.ClusterRef, name string) (*domain.Backup, error) {
	if ref.Name == "missing-source" {
		s.reads++
	}
	return s.ClusterStore.GetBackup(ctx, ref, name)
}

func TestLegacyUnprovenRestoreKeepsRuntimeConvergenceAfterReopen(t *testing.T) {
	for _, queued := range []bool{false, true} {
		name := "never-queued"
		if queued {
			name = "legacy-queue-receipt"
		}
		t.Run(name, func(t *testing.T) {
			p, rc, backend, _ := setup(t)
			if _, err := p.Sync(t.Context(), rc); err != nil {
				t.Fatal(err)
			}
			s := retryState(t, rc)
			// Recreate the legacy runtime in its own reopenable store. Its source no
			// longer exists; there is no initial Restore or native checkpoint at all.
			sealer, err := secrets.New(strings.Repeat("x", 32))
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(t.TempDir(), "unproven.db")
			st, err := store.Open(path, sealer)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = st.Close() })
			rc.Store, rc.Secrets = st, st
			rc.Cluster.Spec.DataSource = &domain.DataSource{Cluster: "missing-source"}
			rc.Cluster.Spec.Backup.Storage = ""
			rc.Cluster.Status.Conditions = []domain.Condition{{Type: "InitialDataSource", Status: "False", Reason: "LegacyRestoreUnproven", Message: "Legacy create operation legacy-create was superseded without initial restore evidence; verify data or recreate target"}}
			create := &domain.Operation{ID: "legacy-create", Kind: domain.OperationCreate}
			if err := st.CreateCluster(store.WithOperation(t.Context(), create), rc.Cluster); err != nil {
				t.Fatal(err)
			}
			create.Phase, create.Message = domain.PhaseCancelled, "Superseded by generation 2"
			if err := st.UpdateOperation(t.Context(), create); err != nil {
				t.Fatal(err)
			}
			s.CloneQueued, s.InitialRestore = queued, nil
			s.Phase = "agents"
			for _, m := range s.Members {
				m.Stage = "deployed"
			}
			seedFormedServices(t, p, rc, backend, s)
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			if err := st.UpdateClusterState(t.Context(), rc.Cluster); err != nil {
				t.Fatal(err)
			}
			if err := st.Close(); err != nil {
				t.Fatal(err)
			}
			st, err = store.Open(path, sealer)
			if err != nil {
				t.Fatal(err)
			}
			rc.Cluster, err = st.GetCluster(t.Context(), rc.Cluster.Ref())
			if err != nil {
				t.Fatal(err)
			}
			spy := &unprovenSourceReads{ClusterStore: st}
			rc.Store, rc.Secrets = spy, st
			if !engine.InitialDataSourceUnproven(rc.Cluster) {
				t.Fatal("hold lost across store reopen")
			}
			if _, err := p.Sync(t.Context(), rc); err != nil {
				t.Fatal(err)
			}
			s = retryState(t, rc)
			for _, m := range s.Members {
				if m.Stage != "healthy" {
					t.Fatalf("ordinary agent/runtime convergence blocked: stage=%s", m.Stage)
				}
			}
			if complete, err := reconcileInitialRestore(t.Context(), rc, s); err != nil || !complete {
				t.Fatalf("runtime convergence blocked: complete=%v err=%v", complete, err)
			}
			if applied, _ := p.InitialDataSourceApplied(rc.Cluster); applied {
				t.Fatal("hold forged completion evidence")
			}
			if s.InitialRestore != nil {
				t.Fatalf("hold synthesized restore state: %+v", s.InitialRestore)
			}
			if _, err := st.GetRestore(t.Context(), rc.Cluster.Ref(), "initial-restore"); !missing(err) {
				t.Fatalf("unexpected restore record/error: %v", err)
			}
			if _, err := st.GetOperation(t.Context(), initialRestoreID(rc.Cluster, s)); !missing(err) {
				t.Fatalf("unexpected restore operation/error: %v", err)
			}
			// No own storage is configured: PBM must not bootstrap from the historical
			// source even when CloneQueued=false. Its absence must never be consulted.
			_, _ = p.pbmEnv(t.Context(), rc, s)
			if spy.reads != 0 {
				t.Fatalf("historical source consulted %d times", spy.reads)
			}
			if err := st.CreateBackupStorage(t.Context(), &domain.BackupStorage{Name: "own", Type: "s3", Bucket: "own-backups", EndpointURL: "https://storage.example.test"}); err != nil {
				t.Fatal(err)
			}
			rc.Cluster.Spec.Backup.Storage = "own"
			env, err := p.pbmEnv(t.Context(), rc, s)
			if err != nil || env.Storage.Name != "own" {
				t.Fatalf("own backup configuration blocked: storage=%+v err=%v", env.Storage, err)
			}
			// No replica set has entered native bootstrap. Availability-only status
			// also skips backup probes; the shared gate must retain the durable hold.
			rc.AvailabilityOnly = true
			status, err := p.Status(t.Context(), rc)
			if err != nil {
				t.Fatal(err)
			}
			if status.Phase == domain.PhaseReady {
				t.Fatalf("unproven restore became Ready: %+v", status)
			}
			held, readyFalse := false, false
			for _, c := range status.Conditions {
				if c.Type == "InitialDataSource" && c.Reason == "LegacyRestoreUnproven" && c.Status == "False" {
					held = true
				}
				if c.Type == "Ready" && c.Status == "False" && c.Reason == "LegacyRestoreUnproven" {
					readyFalse = true
				}
			}
			if !held {
				t.Fatalf("status dropped durable hold: %+v", status.Conditions)
			}
			if !readyFalse {
				t.Fatalf("unproven restore did not fence Ready: %+v", status.Conditions)
			}
			if spy.reads != 0 {
				t.Fatalf("runtime/status consulted missing source %d times", spy.reads)
			}
		})
	}
}
