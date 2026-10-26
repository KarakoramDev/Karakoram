// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package reconciler

import (
	"context"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

type clockCleanupProvider struct {
	*fakeProvider
	cleanupCalls int
}

func (*clockCleanupProvider) SupportsFailureCleanup(*domain.Operation) bool               { return true }
func (*clockCleanupProvider) CanCancelOperation(context.Context, *domain.Operation) error { return nil }
func (p *clockCleanupProvider) CancelOperation(context.Context, *engine.Context, *domain.Operation) (engine.OpResult, error) {
	p.cleanupCalls++
	return engine.OpResult{RequeueAfter: time.Second}, nil
}

func TestClockCorrectionDoesNotAbandonCompensation(t *testing.T) {
	p := &clockCleanupProvider{fakeProvider: lifecycleProvider().fakeProvider}
	m, st := newTestManager(t, p)
	ctx := context.Background()
	stamp := time.Now().UTC()
	c := &domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: "fake"}}
	if err := st.CreateCluster(ctx, c); err != nil {
		t.Fatal(err)
	}
	op := &domain.Operation{Cluster: c.Name, Kind: domain.OperationRestore, Phase: domain.PhaseRunning, Step: failureCleanupStep, LastError: "original failure", FailureCleanupStartedAt: &stamp}
	if err := st.EnqueueOperation(ctx, op); err != nil {
		t.Fatal(err)
	}
	for _, at := range []time.Time{stamp, stamp.Add(8 * time.Hour), stamp.Add(time.Minute)} {
		m.opts.Context.Clock = fixedClock{now: at}
		if _, err := m.reconcile(ctx, c.Ref()); err != nil {
			t.Fatal(err)
		}
		got, err := st.GetOperation(ctx, op.ID)
		if err != nil || got.Phase != domain.PhaseRunning || got.Step != failureCleanupStep {
			t.Fatalf("clock step abandoned cleanup: %+v %v", got, err)
		}
	}
	if p.cleanupCalls != 3 {
		t.Fatalf("cleanup calls=%d", p.cleanupCalls)
	}
	next := &domain.Operation{Cluster: c.Name, Kind: domain.OperationRestart}
	if err := st.EnqueueOperation(ctx, next); err != nil {
		t.Fatal(err)
	}
	claimed, err := st.ClaimOperation(ctx, c.Ref())
	if err != nil || claimed.ID != op.ID {
		t.Fatalf("lost cleanup ownership: %+v %v", claimed, err)
	}
}

func TestClockJumpDoesNotExpireRunningOperation(t *testing.T) {
	p := &operationProvider{fakeProvider: lifecycleProvider().fakeProvider, runFn: func(*engine.Context, *domain.Operation) (engine.OpResult, error) {
		return engine.OpResult{RequeueAfter: time.Second}, nil
	}}
	m, st := newTestManager(t, p)
	ctx := context.Background()
	stamp := time.Now().UTC()
	c := &domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: "fake"}}
	if err := st.CreateCluster(ctx, c); err != nil {
		t.Fatal(err)
	}
	op := &domain.Operation{Cluster: c.Name, Kind: domain.OperationRestart, Phase: domain.PhaseRunning}
	if err := st.EnqueueOperation(ctx, op); err != nil {
		t.Fatal(err)
	}
	for _, at := range []time.Time{stamp, stamp.Add(8 * time.Hour), stamp.Add(time.Minute)} {
		m.opts.Context.Clock = fixedClock{now: at}
		if _, err := m.reconcile(ctx, c.Ref()); err != nil {
			t.Fatal(err)
		}
		got, err := st.GetOperation(ctx, op.ID)
		if err != nil || got.Phase != domain.PhaseRunning {
			t.Fatalf("clock step retired active work: %+v %v", got, err)
		}
	}
}

func TestOperationBudgetPersistsMonotonicElapsedTime(t *testing.T) {
	p := &operationProvider{fakeProvider: lifecycleProvider().fakeProvider, runFn: func(*engine.Context, *domain.Operation) (engine.OpResult, error) {
		return engine.OpResult{RequeueAfter: time.Second}, nil
	}}
	m, st := newTestManager(t, p)
	ctx := t.Context()
	c := &domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: "fake"}}
	if err := st.CreateCluster(ctx, c); err != nil {
		t.Fatal(err)
	}
	op := &domain.Operation{Cluster: c.Name, Kind: domain.OperationRestart, Phase: domain.PhaseRunning}
	if err := st.EnqueueOperation(ctx, op); err != nil {
		t.Fatal(err)
	}
	if _, err := m.reconcile(ctx, c.Ref()); err != nil {
		t.Fatal(err)
	}
	// Advance only the monotonic observation anchor. The host clock is unchanged.
	m.budgets[c.Ref().Key()][op.ID] = operationBudget{observed: time.Now().Add(-time.Minute)}
	if _, err := m.reconcile(ctx, c.Ref()); err != nil {
		t.Fatal(err)
	}
	got, err := st.GetOperation(ctx, op.ID)
	if err != nil || got.Elapsed < time.Minute || got.Phase != domain.PhaseRunning {
		t.Fatal(got, err)
	}
	restarted, err := New(m.opts)
	if err != nil {
		t.Fatal(err)
	}
	restarted.opts.OperationTimeouts = map[string]time.Duration{domain.OperationRestart: 30 * time.Second}
	if _, err = restarted.reconcile(ctx, c.Ref()); err != nil {
		t.Fatal(err)
	}
	got, err = st.GetOperation(ctx, op.ID)
	if err != nil || got.Phase != domain.PhaseFailed || got.Elapsed < time.Minute {
		t.Fatal("restart lost the observed budget", got, err)
	}
}
