// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package reconciler

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres"
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

func TestFailedSyncStillClaimsAgentRemediation(t *testing.T) {
	for _, actor := range []string{"operator", "system:agent-rollout"} {
		t.Run(actor, func(t *testing.T) {
			p := &operationProvider{fakeProvider: lifecycleProvider().fakeProvider}
			syncs, runs := 0, 0
			p.syncFn = func(*engine.Context) (engine.Result, error) {
				syncs++
				return engine.Result{}, errors.New("old agent extension action fails persistently")
			}
			p.runFn = func(_ *engine.Context, op *domain.Operation) (engine.OpResult, error) {
				runs++
				if op.Kind != domain.OperationUpgradeAgents || op.Actor != actor || op.Failures != 0 {
					t.Fatalf("wrong remediation or budget: %+v", op)
				}
				return engine.OpResult{Done: runs == 2, Step: "rolling", Checkpoint: json.RawMessage(`{"member":1}`)}, nil
			}
			m, s := newTestManager(t, p)
			createOperationCluster(t, s)
			for i, kind := range []string{domain.OperationBackup, domain.OperationUpgradeAgents} {
				op := &domain.Operation{ID: kind, Cluster: "demo", Kind: kind, Actor: actor, CreatedAt: time.Now().Add(time.Duration(i) * time.Second)}
				if err := s.EnqueueOperation(t.Context(), op); err != nil {
					t.Fatal(err)
				}
			}
			for range 2 {
				if _, err := m.reconcile(t.Context(), domain.ClusterRef{Name: "demo"}); err != nil {
					t.Fatal(err)
				}
			}
			got, err := s.GetOperation(t.Context(), domain.OperationUpgradeAgents)
			if err != nil || got.Phase != domain.PhaseSucceeded || got.Attempts != 2 || got.Failures != 0 || syncs != 1 || runs != 2 {
				t.Fatalf("rollout did not progress independently: %+v syncs=%d runs=%d err=%v", got, syncs, runs, err)
			}
			other, err := s.GetOperation(t.Context(), domain.OperationBackup)
			if err != nil || other.Phase != domain.PhasePending || other.Attempts != 0 {
				t.Fatalf("unrelated work bypassed failed sync: %+v %v", other, err)
			}
			if _, err := m.reconcile(t.Context(), domain.ClusterRef{Name: "demo"}); err == nil {
				t.Fatal("persistent sync error disappeared without remediation work")
			}
		})
	}
}

func TestFailedSyncAgentRemediationRetainsAdmissionGates(t *testing.T) {
	for _, gate := range []string{"window", "policy", "running"} {
		t.Run(gate, func(t *testing.T) {
			p := &maintenanceClaimProvider{&operationProvider{fakeProvider: lifecycleProvider().fakeProvider}}
			p.syncFn = func(*engine.Context) (engine.Result, error) {
				return engine.Result{}, errors.New("persistent sync failure")
			}
			p.runFn = func(_ *engine.Context, op *domain.Operation) (engine.OpResult, error) {
				if gate != "running" || op.ID != "existing" {
					t.Fatalf("rollout bypassed %s gate", gate)
				}
				return engine.OpResult{}, nil
			}
			m, s := newTestManager(t, p)
			m.opts.Context.Clock = fixedClock{now: time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC)}
			c := &domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: "fake"}}
			if gate == "window" {
				c.Spec.Maintenance = &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: "0 2 * * *", Duration: "1h"}}}
			}
			if gate == "policy" {
				c.Internal = json.RawMessage(`{"maintenance":true}`)
			}
			if err := s.CreateCluster(t.Context(), c); err != nil {
				t.Fatal(err)
			}
			if gate == "running" {
				if err := s.EnqueueOperation(t.Context(), &domain.Operation{ID: "existing", Cluster: c.Name, Kind: domain.OperationRestart, Phase: domain.PhaseRunning}); err != nil {
					t.Fatal(err)
				}
			}
			if err := s.EnqueueOperation(t.Context(), &domain.Operation{ID: "agents", Cluster: c.Name, Kind: domain.OperationUpgradeAgents}); err != nil {
				t.Fatal(err)
			}
			_, err := m.reconcile(t.Context(), c.Ref())
			if gate == "policy" && err == nil {
				t.Fatal("denied claim hid sync failure")
			}
			got, err := s.GetOperation(t.Context(), "agents")
			if err != nil || got.Phase != domain.PhasePending || got.Attempts != 0 || got.Failures != 0 || got.StartedAt != nil {
				t.Fatalf("admission wait consumed rollout budget: %+v %v", got, err)
			}
		})
	}
}

func TestFailedSyncPendingPostgresRolloutRejectsPreviousControlPlaneBuild(t *testing.T) {
	p := &operationProvider{fakeProvider: lifecycleProvider().fakeProvider}
	p.syncFn = func(*engine.Context) (engine.Result, error) {
		return engine.Result{}, errors.New("old agent extension action fails")
	}
	core := postgres.New(postgres.Options{})
	p.runFn = func(rc *engine.Context, op *domain.Operation) (engine.OpResult, error) {
		return core.RunOperation(t.Context(), rc, op)
	}
	m, s := newTestManager(t, p)
	createOperationCluster(t, s)
	payload, _ := json.Marshal(engine.AgentUpgradePayload{TargetBuild: engine.CurrentAgentBuild().Build + "-previous"})
	if err := s.EnqueueOperation(t.Context(), &domain.Operation{ID: "old-target", Cluster: "demo", Kind: domain.OperationUpgradeAgents, Payload: payload}); err != nil {
		t.Fatal(err)
	}
	if _, err := m.reconcile(t.Context(), domain.ClusterRef{Name: "demo"}); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetOperation(t.Context(), "old-target")
	if err != nil || got.Phase != domain.PhaseFailed || got.Attempts != 1 || !strings.Contains(got.Message, "differs from running control-plane build") || !strings.Contains(got.Message, "re-run upgrade-agents") || string(got.Payload) != string(payload) {
		t.Fatalf("previous frozen target was not explicitly retired: %+v %v", got, err)
	}
}
