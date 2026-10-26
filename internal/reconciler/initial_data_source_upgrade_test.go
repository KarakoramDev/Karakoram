// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package reconciler

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres"
	"github.com/KarakoramDev/Karakoram/internal/engine/supabase"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

// Native recovery is simulated, but migration/marker validation uses the real
// PostgreSQL/Supabase core and reconciliation uses the real reopened store.
type legacyRecoveryProvider struct {
	engine.Provider
	syncCalls int
}

func (p *legacyRecoveryProvider) InitialDataSourceApplied(c *domain.Cluster) (bool, error) {
	return p.Provider.(engine.InitialDataSourceObserver).InitialDataSourceApplied(c)
}
func (p *legacyRecoveryProvider) MigrateInitialDataSource(ctx context.Context, rc *engine.Context) error {
	return p.Provider.(engine.InitialDataSourceMigrator).MigrateInitialDataSource(ctx, rc)
}
func (p *legacyRecoveryProvider) Sync(_ context.Context, rc *engine.Context) (engine.Result, error) {
	p.syncCalls++
	applied, err := p.InitialDataSourceApplied(rc.Cluster)
	if !applied && !engine.InitialDataSourceUnproven(rc.Cluster) {
		return engine.Result{RequeueAfter: time.Second}, err
	}
	return engine.Result{}, err
}
func (*legacyRecoveryProvider) Status(context.Context, *engine.Context) (domain.ClusterStatus, error) {
	return domain.ClusterStatus{Phase: domain.PhaseReady, Conditions: []domain.Condition{{Type: "Ready", Status: "True", Reason: "MembersReady"}}}, nil
}
func (*legacyRecoveryProvider) RunOperation(ctx context.Context, rc *engine.Context, op *domain.Operation) (engine.OpResult, error) {
	r, err := rc.Store.GetRestore(ctx, rc.Cluster.Ref(), "initial-restore")
	if err != nil {
		return engine.OpResult{}, err
	}
	stamp := time.Now().UTC()
	r.Phase, r.CompletedAt = domain.PhaseSucceeded, &stamp
	if err = rc.Store.UpdateRestore(ctx, r); err != nil {
		return engine.OpResult{}, err
	}
	return engine.OpResult{Done: true, Step: "done", Checkpoint: json.RawMessage(`{"step":"done"}`)}, nil
}

func TestLegacyPGInitialRestoreMigratesBeforeHistoricalCreateRetirementAfterReopen(t *testing.T) {
	for _, name := range []string{"postgres", "supabase"} {
		for _, state := range []string{"completed", "pending", "running", "invalid-checkpoint", "explicit-cancel"} {
			t.Run(name+"/"+state, func(t *testing.T) {
				var native engine.Provider = postgres.New(postgres.Options{})
				if name == "supabase" {
					native = supabase.New(postgres.Options{})
				}
				p := &legacyRecoveryProvider{Provider: native}
				path := filepath.Join(t.TempDir(), "legacy.db")
				_, st := openAttributionManager(t, p, path)
				if err := st.CreateBackup(t.Context(), &domain.Backup{Cluster: "source", Name: "base", Provider: name, Phase: domain.PhaseSucceeded}); err != nil {
					t.Fatal(err)
				}
				c := &domain.Cluster{Name: "target", Spec: domain.ClusterSpec{Provider: name, DataSource: &domain.DataSource{Cluster: "source", Backup: "base"}}, Internal: json.RawMessage(`{"postgresBackup":{"initialQueued":true,"cloneComplete":true}}`)}
				native.ApplyDefaults(&c.Spec)
				create := &domain.Operation{ID: "oldcreate", Kind: domain.OperationCreate}
				if err := st.CreateCluster(store.WithOperation(t.Context(), create), c); err != nil {
					t.Fatal(err)
				}
				r := &domain.Restore{Name: "initial-restore", Cluster: c.Name, Source: domain.RestoreSource{SourceCluster: "source", Backup: "base"}}
				op := &domain.Operation{ID: "oldnative", Cluster: c.Name, Kind: domain.OperationRestore, Payload: json.RawMessage(`{"restore":"initial-restore"}`)}
				if err := st.EnqueueRestore(t.Context(), r, op); err != nil {
					t.Fatal(err)
				}
				stamp := time.Now().UTC()
				if state == "completed" || state == "invalid-checkpoint" {
					r.Phase, r.CompletedAt = domain.PhaseSucceeded, &stamp
					if err := st.UpdateRestore(t.Context(), r); err != nil {
						t.Fatal(err)
					}
					op.Phase, op.CompletedAt, op.Step = domain.PhaseSucceeded, &stamp, "done"
					op.Checkpoint = json.RawMessage(`{"step":"done"}`)
					if state == "invalid-checkpoint" {
						op.Checkpoint = json.RawMessage(`{"step":"clone-primary"}`)
					}
					if err := st.UpdateOperation(t.Context(), op); err != nil {
						t.Fatal(err)
					}
				} else if state == "running" {
					if _, err := st.ClaimOperation(t.Context(), c.Ref()); err != nil {
						t.Fatal(err)
					}
				}
				create.Phase, create.CompletedAt, create.Message = domain.PhaseCancelled, &stamp, "Superseded by operation legacy-patch (update)"
				if state == "explicit-cancel" {
					create.Message = "Create cancelled by operator"
				}
				if err := st.UpdateOperation(t.Context(), create); err != nil {
					t.Fatal(err)
				}
				c.Status.Phase, c.Status.ObservedGeneration = domain.PhaseReady, c.Generation
				if err := st.UpdateClusterState(t.Context(), c); err != nil {
					t.Fatal(err)
				}
				if err := st.Close(); err != nil {
					t.Fatal(err)
				}
				m, st := openAttributionManager(t, p, path)
				_, _ = m.reconcile(t.Context(), c.Ref())
				if state == "pending" || state == "running" {
					first, err := st.GetCluster(t.Context(), c.Ref())
					if err != nil {
						t.Fatal(err)
					}
					for _, condition := range first.Status.Conditions {
						if condition.Reason == "RestoreAborted" {
							t.Fatalf("in-flight legacy restore aborted: %+v", first.Status)
						}
					}
					if err := st.Close(); err != nil {
						t.Fatal(err)
					}
					m, st = openAttributionManager(t, p, path)
					_, _ = m.reconcile(t.Context(), c.Ref())
				}
				stored, err := st.GetCluster(t.Context(), c.Ref())
				if err != nil {
					t.Fatal(err)
				}
				applied, err := p.InitialDataSourceApplied(stored)
				valid := state != "invalid-checkpoint" && state != "explicit-cancel"
				if valid && (err != nil || !applied || stored.Status.Phase != domain.PhaseReady) {
					t.Fatalf("validated legacy recovery lost on upgrade: applied=%v status=%+v err=%v", applied, stored.Status, err)
				}
				if !valid && (applied || stored.Status.Phase == domain.PhaseReady) {
					t.Fatalf("unproven/cancelled legacy recovery became Ready: applied=%v status=%+v", applied, stored.Status)
				}
			})
		}
	}
}

func TestLegacyUnprovenRestoreKeepsSyncAfterStoreReopen(t *testing.T) {
	for _, name := range []string{"postgres", "supabase"} {
		t.Run(name, func(t *testing.T) {
			var native engine.Provider = postgres.New(postgres.Options{})
			if name == "supabase" {
				native = supabase.New(postgres.Options{})
			}
			p := &legacyRecoveryProvider{Provider: native}
			path := filepath.Join(t.TempDir(), "unproven.db")
			_, st := openAttributionManager(t, p, path)
			if err := st.CreateBackup(t.Context(), &domain.Backup{Cluster: "source", Name: "base", Phase: domain.PhaseSucceeded}); err != nil {
				t.Fatal(err)
			}
			c := &domain.Cluster{Name: "target", Spec: domain.ClusterSpec{Provider: name, DataSource: &domain.DataSource{Cluster: "source", Backup: "base"}}}
			native.ApplyDefaults(&c.Spec)
			create := &domain.Operation{ID: "historical", Kind: domain.OperationCreate}
			if err := st.CreateCluster(store.WithOperation(t.Context(), create), c); err != nil {
				t.Fatal(err)
			}
			create.Phase, create.Message = domain.PhaseCancelled, "Superseded by operation old-patch (update)"
			if err := st.UpdateOperation(t.Context(), create); err != nil {
				t.Fatal(err)
			}
			c.Status.Phase = domain.PhaseReady
			if err := st.UpdateClusterState(t.Context(), c); err != nil {
				t.Fatal(err)
			}
			// Legacy source loss is not a prerequisite for serving/healing the
			// existing runtime; it cannot trigger an implicit initial replay.
			if err := st.DeleteBackup(t.Context(), domain.ClusterRef{Name: "source"}, "base"); err != nil {
				t.Fatal(err)
			}
			if err := st.Close(); err != nil {
				t.Fatal(err)
			}
			m, reopened := openAttributionManager(t, p, path)
			for range 2 {
				if _, err := m.reconcile(t.Context(), c.Ref()); err != nil {
					t.Fatal(err)
				}
			}
			stored, err := reopened.GetCluster(t.Context(), c.Ref())
			if err != nil {
				t.Fatal(err)
			}
			if p.syncCalls != 2 || !engine.InitialDataSourceUnproven(stored) || stored.Status.Phase == domain.PhaseReady {
				t.Fatalf("legacy readiness hold stopped runtime convergence: calls=%d status=%+v", p.syncCalls, stored.Status)
			}
			if !strings.Contains(stored.Status.Message, create.ID) || !strings.Contains(stored.Status.Message, "delete this target") {
				t.Fatalf("missing owner/remediation diagnostic: %+v", stored.Status)
			}
			ops, err := reopened.ListOperations(t.Context(), c.Ref())
			if err != nil || len(ops) != 1 || ops[0].ID != create.ID || ops[0].Phase != domain.PhaseCancelled {
				t.Fatalf("unproven legacy restore was replayed: %+v %v", ops, err)
			}
		})
	}
}
