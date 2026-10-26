// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package reconciler

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/psmdb"
)

type maintenanceClaimProvider struct{ *operationProvider }

func (*maintenanceClaimProvider) CanClaimOperation(c *domain.Cluster) (bool, error) {
	return string(c.Internal) != `{"maintenance":true}`, nil
}

func TestMaintenanceClaimGateDefersWithoutConsumingOperationBudget(t *testing.T) {
	p := &maintenanceClaimProvider{&operationProvider{fakeProvider: &fakeProvider{}}}
	maintenance := true
	syncs, runs := 0, 0
	p.syncFn = func(rc *engine.Context) (engine.Result, error) {
		syncs++
		rc.Cluster.Internal = json.RawMessage(`{"maintenance":false}`)
		if maintenance {
			rc.Cluster.Internal = json.RawMessage(`{"maintenance":true}`)
		}
		return engine.Result{RequeueAfter: time.Second}, nil
	}
	p.runFn = func(_ *engine.Context, op *domain.Operation) (engine.OpResult, error) {
		runs++
		if op.Attempts != 1 || op.Failures != 0 {
			t.Fatalf("queue wait consumed budget: %+v", op)
		}
		return engine.OpResult{Done: true}, nil
	}
	m, s := newTestManager(t, p)
	createOperationCluster(t, s)
	created := time.Now().Add(-48 * time.Hour)
	op := &domain.Operation{ID: "backup", Cluster: "demo", Kind: domain.OperationBackup, CreatedAt: created, Actor: "scheduler"}
	if err := s.EnqueueOperation(t.Context(), op); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		delay, err := m.reconcile(t.Context(), domain.ClusterRef{Name: "demo"})
		if err != nil || delay != time.Second {
			t.Fatal(delay, err)
		}
		queued, err := s.GetOperation(t.Context(), op.ID)
		if err != nil || queued.Phase != domain.PhasePending || queued.Attempts != 0 || queued.Failures != 0 || queued.StartedAt != nil {
			t.Fatalf("deferred operation changed: %+v %v", queued, err)
		}
	}
	if syncs != 3 || runs != 0 {
		t.Fatal("imperative work starved maintenance", syncs, runs)
	}
	maintenance = false
	if _, err := m.reconcile(t.Context(), domain.ClusterRef{Name: "demo"}); err != nil {
		t.Fatal(err)
	}
	completed, err := s.GetOperation(t.Context(), op.ID)
	if err != nil || completed.Phase != domain.PhaseSucceeded || runs != 1 || !completed.CreatedAt.Equal(created) {
		t.Fatalf("deferred operation did not receive its execution window: %+v %v", completed, err)
	}
}

func TestOperationClaimPolicyIsOptional(t *testing.T) {
	for _, old := range []bool{false, true} {
		t.Run(map[bool]string{false: "fresh", true: "expired"}[old], func(t *testing.T) {
			p := &operationProvider{fakeProvider: &fakeProvider{}}
			runs := 0
			p.runFn = func(*engine.Context, *domain.Operation) (engine.OpResult, error) {
				runs++
				return engine.OpResult{Done: true}, nil
			}
			m, s := newTestManager(t, p)
			createOperationCluster(t, s)
			created := time.Now()
			if old {
				created = created.Add(-48 * time.Hour)
			}
			op := &domain.Operation{ID: "restart", Cluster: "demo", Kind: domain.OperationRestart, CreatedAt: created}
			if old {
				op.Elapsed = 48 * time.Hour
			}
			if err := s.EnqueueOperation(t.Context(), op); err != nil {
				t.Fatal(err)
			}
			if _, err := m.reconcile(t.Context(), domain.ClusterRef{Name: "demo"}); err != nil {
				t.Fatal(err)
			}
			got, err := s.GetOperation(t.Context(), op.ID)
			want, wantRuns := domain.PhaseSucceeded, 1
			if old {
				want, wantRuns = domain.PhaseFailed, 0
			}
			if err != nil || got.Phase != want || runs != wantRuns {
				t.Fatalf("legacy provider behavior changed: %+v runs=%d err=%v", got, runs, err)
			}
		})
	}
}

func TestRunningOperationStillPrecedesMaintenanceClaimPolicy(t *testing.T) {
	p := &maintenanceClaimProvider{&operationProvider{fakeProvider: &fakeProvider{}}}
	p.syncFn = func(*engine.Context) (engine.Result, error) {
		t.Fatal("Sync preempted running operation")
		return engine.Result{}, nil
	}
	p.runFn = func(*engine.Context, *domain.Operation) (engine.OpResult, error) {
		return engine.OpResult{Done: true}, nil
	}
	m, s := newTestManager(t, p)
	createOperationCluster(t, s)
	c, err := s.GetCluster(t.Context(), domain.ClusterRef{Name: "demo"})
	if err != nil {
		t.Fatal(err)
	}
	c.Internal = json.RawMessage(`{"maintenance":true}`)
	if err := s.UpdateClusterState(t.Context(), c); err != nil {
		t.Fatal(err)
	}
	if err := s.EnqueueOperation(t.Context(), &domain.Operation{ID: "running", Cluster: "demo", Kind: domain.OperationRestart, Phase: domain.PhaseRunning}); err != nil {
		t.Fatal(err)
	}
	if _, err := m.reconcile(t.Context(), domain.ClusterRef{Name: "demo"}); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetOperation(t.Context(), "running")
	if err != nil || got.Phase != domain.PhaseSucceeded {
		t.Fatal(got, err)
	}
}

type runtimeRecoveryClaimProvider struct{ *operationProvider }

func (*runtimeRecoveryClaimProvider) CanClaimOperation(c *domain.Cluster) (bool, error) {
	return (&psmdb.Provider{}).CanClaimOperation(c)
}

func TestAcceptedRuntimeRecoveryDefersQueuedActions(t *testing.T) {
	for _, kind := range []string{domain.OperationRestart, domain.OperationRestore} {
		t.Run(kind, func(t *testing.T) {
			p := &runtimeRecoveryClaimProvider{&operationProvider{fakeProvider: &fakeProvider{}}}
			p.runFn = func(*engine.Context, *domain.Operation) (engine.OpResult, error) {
				t.Fatal("queued action overtook accepted runtime recovery")
				return engine.OpResult{}, nil
			}
			m, st := newTestManager(t, p)
			c := &domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: "fake"}, Internal: json.RawMessage(`{"uid":"owned","members":[{"name":"demom0","stage":"active","runtimeRecovery":{"action":"import","op":{"ID":"accepted-import"}}}]}`)}
			if err := st.CreateCluster(t.Context(), c); err != nil {
				t.Fatal(err)
			}
			op := &domain.Operation{Cluster: c.Name, Kind: kind, CreatedAt: time.Now().Add(-48 * time.Hour)}
			if err := st.EnqueueOperation(t.Context(), op); err != nil {
				t.Fatal(err)
			}
			for range 2 {
				if _, err := m.reconcile(t.Context(), c.Ref()); err != nil {
					t.Fatal(err)
				}
			}
			queued, err := st.GetOperation(t.Context(), op.ID)
			if err != nil || queued.Phase != domain.PhasePending || queued.Attempts != 0 || queued.Elapsed != 0 || queued.StartedAt != nil {
				t.Fatal("runtime recovery did not retain claim priority", queued, err)
			}
		})
	}
}
