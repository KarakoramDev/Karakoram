// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package reconciler

import (
	"errors"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

func TestMaintenanceClaimGate(t *testing.T) {
	p := &operationProvider{fakeProvider: lifecycleProvider().fakeProvider}
	var ran []string
	p.runFn = func(rc *engine.Context, op *domain.Operation) (engine.OpResult, error) {
		ran = append(ran, op.ID)
		if rc.Operation != op {
			t.Error("operation context missing")
		}
		return engine.OpResult{Done: true}, nil
	}
	m, st := newTestManager(t, p)
	now := time.Date(2026, 1, 1, 1, 59, 0, 0, time.UTC)
	m.opts.Context.Clock = fixedClock{now: now}
	c := &domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: "fake", Maintenance: &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: "0 2 * * *", Duration: "1h"}}}}}
	if err := st.CreateCluster(t.Context(), c); err != nil {
		t.Fatal(err)
	}
	for i, kind := range []string{domain.OperationRestart, domain.OperationBackup} {
		op := &domain.Operation{ID: kind, Kind: kind, Cluster: c.Name, CreatedAt: now.Add(time.Duration(i) * time.Second)}
		if err := st.EnqueueOperation(t.Context(), op); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := m.reconcile(t.Context(), c.Ref()); err != nil {
		t.Fatal(err)
	}
	if len(ran) != 1 || ran[0] != domain.OperationBackup {
		t.Fatalf("ran=%v", ran)
	}
	if delay, err := m.reconcile(t.Context(), c.Ref()); err != nil || delay > time.Minute || delay <= 0 {
		t.Fatalf("delay=%s err=%v", delay, err)
	}
	op, err := st.GetOperation(t.Context(), domain.OperationRestart)
	if err != nil || op.Attempts != 0 || op.Elapsed != 0 || op.Phase != domain.PhasePending {
		t.Fatal(op, err)
	}
	m.opts.Context.Clock = fixedClock{now: now.Add(time.Minute)}
	if _, err := m.reconcile(t.Context(), c.Ref()); err != nil {
		t.Fatal(err)
	}
	if len(ran) != 2 || ran[1] != domain.OperationRestart {
		t.Fatalf("ran=%v", ran)
	}
}
func TestMaintenanceWaitExcludedFromBudgets(t *testing.T) {
	for _, tracking := range []bool{false, true} {
		t.Run(map[bool]string{false: "imperative", true: "tracking"}[tracking], func(t *testing.T) {
			p := &operationProvider{fakeProvider: lifecycleProvider().fakeProvider}
			p.runFn = func(rc *engine.Context, op *domain.Operation) (engine.OpResult, error) {
				result := rc.WaitForWindow()
				return engine.OpResult{RequeueAfter: result.RequeueAfter}, nil
			}
			p.syncFn = func(rc *engine.Context) (engine.Result, error) { return rc.WaitForWindow(), nil }
			m, st := newTestManager(t, p)
			now := time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC)
			m.opts.Context.Clock = fixedClock{now: now}
			c := &domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: "fake", Maintenance: &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: "0 2 * * *", Duration: "1h"}}}}}
			op := &domain.Operation{ID: "work", Kind: domain.OperationRestart, Phase: domain.PhaseRunning, Cluster: c.Name}
			ctx := t.Context()
			if tracking {
				op.Kind = domain.OperationUpdate
				op.Phase = ""
				ctx = store.WithOperation(ctx, op)
			}
			if err := st.CreateCluster(ctx, c); err != nil {
				t.Fatal(err)
			}
			if !tracking {
				if err := st.EnqueueOperation(ctx, op); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := m.reconcile(t.Context(), c.Ref()); err != nil {
				t.Fatal(err)
			}
			key := c.Ref().Key()
			budget := m.budgets[key][op.ID]
			if !budget.paused {
				t.Fatal("budget not paused")
			}
			budget.observed = time.Now().Add(-24 * time.Hour)
			m.budgets[key][op.ID] = budget
			if _, err := m.reconcile(t.Context(), c.Ref()); err != nil {
				t.Fatal(err)
			}
			got, err := st.GetOperation(t.Context(), op.ID)
			wantPhase := domain.PhaseRunning
			if tracking {
				wantPhase = domain.PhasePending
			}
			if err != nil || got.Elapsed > time.Second || got.Phase != wantPhase {
				t.Fatal(got, err)
			}
			observed, err := st.GetCluster(t.Context(), c.Ref())
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, condition := range observed.Status.Conditions {
				if condition.Type == "MaintenanceWindow" && condition.Status == "False" {
					found = true
				}
			}
			if !found {
				t.Fatal("status observation discarded maintenance condition")
			}
		})
	}
}

type maintenanceStepClock struct{ now time.Time }

func (c *maintenanceStepClock) Now() time.Time { return c.now }

func TestMaintenanceClaimUsesPostSyncTime(t *testing.T) {
	clock := &maintenanceStepClock{now: time.Date(2026, 1, 1, 2, 59, 0, 0, time.UTC)}
	p := &operationProvider{fakeProvider: lifecycleProvider().fakeProvider}
	p.syncFn = func(*engine.Context) (engine.Result, error) {
		clock.now = clock.now.Add(2 * time.Minute)
		return engine.Result{}, nil
	}
	p.runFn = func(*engine.Context, *domain.Operation) (engine.OpResult, error) {
		t.Fatal("window closed during Sync but operation was claimed")
		return engine.OpResult{}, nil
	}
	m, st := newTestManager(t, p)
	m.opts.Context.Clock = clock
	c := &domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: "fake", Maintenance: &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: "0 2 * * *", Duration: "1h"}}}}}
	if err := st.CreateCluster(t.Context(), c); err != nil {
		t.Fatal(err)
	}
	op := &domain.Operation{Cluster: c.Name, Kind: domain.OperationRestart}
	if err := st.EnqueueOperation(t.Context(), op); err != nil {
		t.Fatal(err)
	}
	if delay, err := m.reconcile(t.Context(), c.Ref()); err != nil || delay != 5*time.Minute {
		t.Fatal(delay, err)
	}
	got, err := st.GetOperation(t.Context(), op.ID)
	if err != nil || got.Phase != domain.PhasePending || got.Attempts != 0 {
		t.Fatal(got, err)
	}
}

func TestStaleDesiredMaintenanceWaitDoesNotPauseImperativeBudgets(t *testing.T) {
	for _, kind := range []string{domain.OperationBackup, domain.OperationRestore, domain.OperationRestart} {
		t.Run(kind, func(t *testing.T) {
			p := &operationProvider{fakeProvider: lifecycleProvider().fakeProvider}
			p.syncFn = func(rc *engine.Context) (engine.Result, error) {
				return rc.WaitForWindow(), nil
			}
			p.runFn = func(*engine.Context, *domain.Operation) (engine.OpResult, error) {
				// Backup/restore are exempt. A restart already past its stop
				// boundary must recover without calling the disruption gate.
				return engine.OpResult{RequeueAfter: time.Second}, nil
			}
			m, st := newTestManager(t, p)
			m.opts.Context.Clock = fixedClock{now: time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC)}
			c := &domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{
				Provider:    "fake",
				Maintenance: &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: "0 2 * * *", Duration: "1h"}}},
			}, Status: domain.ClusterStatus{Conditions: []domain.Condition{{Type: "MaintenanceWindow", Status: "False", Reason: "WaitingForWindow"}}}}
			ctx := t.Context()
			if err := st.CreateCluster(ctx, c); err != nil {
				t.Fatal(err)
			}
			op := &domain.Operation{ID: "work", Cluster: c.Name, Kind: kind, Phase: domain.PhaseRunning}
			if kind == domain.OperationRestore {
				if err := st.CreateBackup(ctx, &domain.Backup{Name: "saved", Cluster: c.Name, Provider: "fake", Phase: domain.PhaseSucceeded}); err != nil {
					t.Fatal(err)
				}
				op.Payload = []byte(`{"restore":"restore"}`)
				r := &domain.Restore{Name: "restore", Cluster: c.Name, Source: domain.RestoreSource{Backup: "saved"}, Phase: domain.PhasePending}
				if err := st.EnqueueRestore(ctx, r, op); err != nil {
					t.Fatal(err)
				}
			} else if err := st.EnqueueOperation(ctx, op); err != nil {
				t.Fatal(err)
			}
			if _, err := m.reconcile(ctx, c.Ref()); err != nil {
				t.Fatal(err)
			}
			budget := m.budgets[c.Ref().Key()][op.ID]
			if budget.paused {
				t.Fatal("unrelated desired-state wait paused imperative work")
			}
			budget.observed = time.Now().Add(-time.Minute)
			m.budgets[c.Ref().Key()][op.ID] = budget
			if _, err := m.reconcile(ctx, c.Ref()); err != nil {
				t.Fatal(err)
			}
			got, err := st.GetOperation(ctx, op.ID)
			if err != nil || got.Elapsed < time.Minute || got.Phase != domain.PhaseRunning {
				t.Fatalf("execution budget did not progress: operation=%+v err=%v", got, err)
			}
		})
	}
}

func TestMaintenanceBudgetOverrideAndCleanup(t *testing.T) {
	for _, scenario := range []string{"imperative-override", "generation-override", "cleanup", "cancellation", "system-recovery"} {
		t.Run(scenario, func(t *testing.T) {
			m := &Manager{}
			c := &domain.Cluster{Name: "demo", Generation: 3}
			op := &domain.Operation{ID: "work", Cluster: c.Name, Kind: domain.OperationRestart, Phase: domain.PhaseRunning}
			switch scenario {
			case "imperative-override":
				op.IgnoreMaintenanceWindow = true
			case "generation-override":
				op.Kind, op.TargetGeneration = domain.OperationUpdate, c.Generation
				c.MaintenanceOverrideGeneration = c.Generation
			case "cleanup":
				op.Step = failureCleanupStep
			case "cancellation":
				op.CancelRequested = true
			case "system-recovery":
				op.Kind, op.Actor = domain.OperationReplaceMember, "system"
			}
			m.maintenanceBudget(op, c, true)
			budget := m.budgets[c.Ref().Key()][op.ID]
			if budget.paused {
				t.Fatal("maintenance masked exempt work or recovery budget")
			}
			cleanup := scenario == "cleanup"
			m.elapsedOperationTime(op, cleanup)
			budget = m.budgets[c.Ref().Key()][op.ID]
			budget.observed = time.Now().Add(-time.Minute)
			// Even a stale in-memory pause cannot exempt compensation.
			budget.paused = cleanup
			m.budgets[c.Ref().Key()][op.ID] = budget
			if elapsed := m.elapsedOperationTime(op, cleanup); elapsed < time.Minute {
				t.Fatalf("budget failed to progress: %s", elapsed)
			}
		})
	}
}

func TestRunningRolloutWaitBudgetResumesForRecovery(t *testing.T) {
	c := &domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Maintenance: &domain.MaintenancePolicy{
		Windows: []domain.MaintenanceWindow{{Schedule: "0 2 * * *", Duration: "1h"}},
	}}}
	op := &domain.Operation{ID: "rollout", Cluster: c.Name, Kind: domain.OperationUpgradeAgents, Phase: domain.PhaseRunning}
	rc := &engine.Context{Cluster: c, Operation: op, Clock: fixedClock{now: time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC)}}
	if wait := rc.WaitForWindow(); wait.RequeueAfter <= 0 {
		t.Fatal("rollout did not reach the maintenance gate")
	}
	m := &Manager{}
	m.maintenanceBudget(op, c, rc.WaitingForMaintenance(op))
	budget := m.budgets[c.Ref().Key()][op.ID]
	if !budget.paused {
		t.Fatal("running rollout wait consumed execution budget")
	}
	budget.observed = time.Now().Add(-24 * time.Hour)
	m.budgets[c.Ref().Key()][op.ID] = budget
	if elapsed := m.elapsedOperationTime(op, false); elapsed != 0 {
		t.Fatalf("maintenance wait charged %s", elapsed)
	}
	// The next invocation recovers an already disrupted member without a
	// gate call. Its persisted cluster condition is still WaitingForWindow.
	recovery := &engine.Context{Cluster: c, Operation: op}
	m.maintenanceBudget(op, c, recovery.WaitingForMaintenance(op))
	budget = m.budgets[c.Ref().Key()][op.ID]
	budget.observed = time.Now().Add(-time.Minute)
	m.budgets[c.Ref().Key()][op.ID] = budget
	if elapsed := m.elapsedOperationTime(op, false); elapsed < time.Minute {
		t.Fatalf("member recovery execution budget did not resume: %s", elapsed)
	}
}

func TestDesiredGenerationMaintenanceWaitReceipt(t *testing.T) {
	for _, started := range []bool{false, true} {
		t.Run(map[bool]string{false: "pending", true: "already-running"}[started], func(t *testing.T) {
			p := &operationProvider{fakeProvider: lifecycleProvider().fakeProvider}
			m, st := newTestManager(t, p)
			now := m.opts.Context.Clock.Now()
			c := &domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: "fake"}, Status: domain.ClusterStatus{
				Phase: domain.PhaseReady, Message: "Temporal is Ready",
				Conditions: []domain.Condition{{Type: "MaintenanceWindow", Status: "False", Reason: "WaitingForWindow", Message: "Waiting for maintenance window; next start 2026-11-01T02:00:00Z"}},
			}}
			op := &domain.Operation{Kind: domain.OperationUpdate}
			if err := st.CreateCluster(store.WithOperation(t.Context(), op), c); err != nil {
				t.Fatal(err)
			}
			if started {
				op.Phase, op.StartedAt = domain.PhaseRunning, &now
				if err := st.UpdateOperation(t.Context(), op); err != nil {
					t.Fatal(err)
				}
			}
			for range 3 {
				if err := m.trackingOperations(t.Context(), p, c, nil, true, ""); err != nil {
					t.Fatal(err)
				}
				got, err := st.GetOperation(t.Context(), op.ID)
				if err != nil || got.Phase != op.Phase || (got.StartedAt != nil) != started ||
					got.Attempts != 0 || got.Elapsed != 0 || got.Step != "WaitingForWindow" ||
					got.Message != c.Status.Conditions[0].Message {
					t.Fatal("maintenance wait consumed execution or lost diagnostics", got, err)
				}
			}
			events, err := st.ListEvents(t.Context(), store.EventFilter{Cluster: c.Name, Limit: 100})
			if err != nil {
				t.Fatal(err)
			}
			waits := 0
			for _, event := range events {
				if event.Reason == "MaintenanceWindowWait" && event.Operation == op.ID {
					waits++
				}
			}
			if waits != 1 {
				t.Fatal("maintenance event must be durable and deduplicated", waits)
			}
			c.Status.Conditions = nil
			c.Status.ObservedGeneration = c.Generation
			if err := m.trackingOperations(t.Context(), p, c, nil, true, ""); err != nil {
				t.Fatal(err)
			}
			got, err := st.GetOperation(t.Context(), op.ID)
			if err != nil || got.Phase != domain.PhaseSucceeded || got.StartedAt == nil {
				t.Fatal("open-window convergence did not finish the same receipt", got, err)
			}
		})
	}
}

func TestReconcileDoesNotStartDeferredDesiredGeneration(t *testing.T) {
	p := &operationProvider{fakeProvider: lifecycleProvider().fakeProvider}
	p.syncFn = func(rc *engine.Context) (engine.Result, error) {
		return rc.WaitForWindow(), nil
	}
	p.statusFn = func(rc *engine.Context) (domain.ClusterStatus, error) {
		return domain.ClusterStatus{Phase: domain.PhaseReady, Message: "Temporal is Ready", ObservedGeneration: rc.Cluster.Status.ObservedGeneration}, nil
	}
	m, st := newTestManager(t, p)
	m.opts.Context.Clock = fixedClock{now: time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC)}
	c := &domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{
		Provider: "fake", Maintenance: &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: "0 2 * * *", Duration: "1h"}}},
	}}
	op := &domain.Operation{Kind: domain.OperationUpdate}
	if err := st.CreateCluster(store.WithOperation(t.Context(), op), c); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if _, err := m.reconcile(t.Context(), c.Ref()); err != nil {
			t.Fatal(err)
		}
		got, err := st.GetOperation(t.Context(), op.ID)
		if err != nil || got.Phase != domain.PhasePending || got.StartedAt != nil || got.Attempts != 0 || got.Elapsed != 0 || got.Step != "WaitingForWindow" {
			t.Fatal("pre-sync tracking started deferred desired work", got, err)
		}
	}
	// Availability/transport failures during a known deferral must not turn
	// never-started desired work into Running or consume an execution attempt.
	p.syncFn = func(rc *engine.Context) (engine.Result, error) {
		return engine.Result{}, errors.New("agent connection refused")
	}
	_, _ = m.reconcile(t.Context(), c.Ref())
	got, err := st.GetOperation(t.Context(), op.ID)
	if err != nil || got.Phase != domain.PhasePending || got.StartedAt != nil || got.Attempts != 0 || got.Step != "WaitingForWindow" {
		t.Fatal("health failure started deferred desired work", got, err)
	}
	m.opts.Context.Clock = fixedClock{now: time.Date(2026, 1, 1, 2, 1, 0, 0, time.UTC)}
	p.syncFn = func(rc *engine.Context) (engine.Result, error) {
		got, err := st.GetOperation(t.Context(), op.ID)
		if err != nil || got.Phase != domain.PhaseRunning || got.StartedAt == nil {
			t.Fatal("opening window did not durably start deferred work before Sync", got, err)
		}
		return rc.WaitForWindow(), nil
	}
	if _, err := m.reconcile(t.Context(), c.Ref()); err != nil {
		t.Fatal(err)
	}
}

func TestConvergenceExecutionStartIsDurableBeforeProviderWork(t *testing.T) {
	for _, kind := range []string{domain.OperationCreate, domain.OperationUpdate, domain.OperationDelete} {
		t.Run(kind, func(t *testing.T) {
			p := &operationProvider{fakeProvider: lifecycleProvider().fakeProvider}
			m, st := newTestManager(t, p)
			c := &domain.Cluster{Name: "demo", Deleting: kind == domain.OperationDelete, Spec: domain.ClusterSpec{
				Provider: "fake", Maintenance: &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: "* * * * *", Duration: "1h"}}},
			}}
			op := &domain.Operation{Kind: kind}
			if err := st.CreateCluster(store.WithOperation(t.Context(), op), c); err != nil {
				t.Fatal(err)
			}
			called := false
			check := func() {
				called = true
				got, err := st.GetOperation(t.Context(), op.ID)
				if err != nil || got.Phase != domain.PhaseRunning || got.StartedAt == nil {
					t.Fatal("provider work started before durable Running receipt", got, err)
				}
			}
			p.syncFn = func(*engine.Context) (engine.Result, error) {
				check()
				return engine.Result{RequeueAfter: time.Second}, nil
			}
			p.cleanupFn = func(*engine.Context) (bool, error) {
				check()
				return false, nil
			}
			if _, err := m.reconcile(t.Context(), c.Ref()); err != nil {
				t.Fatal(err)
			}
			if !called {
				t.Fatal("execution boundary was not exercised")
			}
		})
	}
}
