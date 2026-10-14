// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package reconciler

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

type boundedProvider struct {
	*fakeProvider
	stuck   bool
	invalid bool
}

func (p *boundedProvider) RunOperation(ctx context.Context, rc *engine.Context, op *domain.Operation) (engine.OpResult, error) {
	if p.invalid {
		return engine.OpResult{}, engine.InvalidField("version", domain.ReasonInvalid, "version is invalid")
	}
	if p.stuck {
		return engine.OpResult{}, errors.New("PBM remains unavailable")
	}
	return p.fakeProvider.RunOperation(ctx, rc, op)
}
func (*boundedProvider) CanCancelOperation(*domain.Operation) error { return nil }
func (*boundedProvider) CancelOperation(context.Context, *engine.Context, *domain.Operation) (engine.OpResult, error) {
	return engine.OpResult{Done: true, Step: "cancelled"}, nil
}
func lifecycleProvider() *boundedProvider {
	return &boundedProvider{fakeProvider: &fakeProvider{calls: map[string]int{}, active: map[string]bool{}}}
}

func TestCreateOperationSucceedsOnObservedGeneration(t *testing.T) {
	m, s := newTestManager(t, lifecycleProvider())
	ctx := context.Background()
	op := &domain.Operation{Kind: domain.OperationCreate}
	c := &domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: "fake"}}
	if err := s.CreateCluster(store.WithOperation(ctx, op), c); err != nil {
		t.Fatal(err)
	}
	if _, err := m.reconcile(ctx, c.Ref()); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetOperation(ctx, op.ID)
	if err != nil || got.Phase != domain.PhaseSucceeded || got.StartedAt == nil || got.CompletedAt == nil || got.Message == "" {
		t.Fatal(got, err)
	}
	events, err := s.ListEvents(ctx, store.EventFilter{Cluster: c.Name})
	if err != nil || len(events) == 0 || events[0].Operation != op.ID {
		t.Fatal(events, err)
	}
}

func TestOperationRetryBudgetUnblocksQueueAndValidationFailsImmediately(t *testing.T) {
	for _, invalid := range []bool{false, true} {
		p := lifecycleProvider()
		p.stuck, p.invalid = true, invalid
		m, s := newTestManager(t, p)
		m.opts.OperationRetryLimit = 2
		ctx := context.Background()
		c := &domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: "fake"}}
		if err := s.CreateCluster(ctx, c); err != nil {
			t.Fatal(err)
		}
		op := &domain.Operation{Cluster: c.Name, Kind: domain.OperationRestart}
		queued := &domain.Operation{Cluster: c.Name, Kind: domain.OperationRestart}
		if err := s.EnqueueOperation(ctx, op); err != nil {
			t.Fatal(err)
		}
		if err := s.EnqueueOperation(ctx, queued); err != nil {
			t.Fatal(err)
		}
		_, _ = m.reconcile(ctx, c.Ref())
		got, _ := s.GetOperation(ctx, op.ID)
		if invalid && got.Phase != domain.PhaseFailed {
			t.Fatal(got)
		}
		if !invalid {
			if got.Phase != domain.PhaseRunning {
				t.Fatal(got)
			}
			if _, err := m.reconcile(ctx, c.Ref()); err != nil {
				t.Fatal(err)
			}
			got, _ = s.GetOperation(ctx, op.ID)
			if got.Phase != domain.PhaseFailed || !strings.Contains(got.Message, "PBM remains unavailable") {
				t.Fatal(got)
			}
		}
		p.stuck, p.invalid = false, false
		for range 2 {
			if _, err := m.reconcile(ctx, c.Ref()); err != nil {
				t.Fatal(err)
			}
		}
		done, _ := s.GetOperation(ctx, queued.ID)
		if done.Phase != domain.PhaseSucceeded || done.Message == "" {
			t.Fatal(done)
		}
	}
}

func TestRunningCancellationAndRestorePhase(t *testing.T) {
	m, s := newTestManager(t, lifecycleProvider())
	ctx := context.Background()
	c := &domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: "fake"}}
	if err := s.CreateCluster(ctx, c); err != nil {
		t.Fatal(err)
	}
	op := &domain.Operation{Cluster: c.Name, Kind: domain.OperationRestore}
	if err := s.EnqueueOperation(ctx, op); err != nil {
		t.Fatal(err)
	}
	if _, err := m.reconcile(ctx, c.Ref()); err != nil {
		t.Fatal(err)
	}
	c, err := s.GetCluster(ctx, c.Ref())
	if err != nil || c.Status.Phase != domain.PhaseRestoring || c.Status.Message == "" {
		t.Fatal(c, err)
	}
	if _, err = s.RequestOperationCancellation(ctx, op.ID); err != nil {
		t.Fatal(err)
	}
	if _, err = m.reconcile(ctx, c.Ref()); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetOperation(ctx, op.ID)
	if err != nil || got.Phase != domain.PhaseCancelled || got.Message == "" || got.CompletedAt == nil {
		t.Fatal(got, err)
	}
	c, _ = s.GetCluster(ctx, c.Ref())
	if c.Status.Phase == domain.PhaseRestoring {
		t.Fatal(c.Status)
	}
}

func TestDeadlineExpiresPendingBehindUnformedCluster(t *testing.T) {
	p := lifecycleProvider()
	m, s := newTestManager(t, p)
	ctx := context.Background()
	c := &domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: "fake"}}
	if err := s.CreateCluster(ctx, c); err != nil {
		t.Fatal(err)
	}
	op := &domain.Operation{Cluster: c.Name, Kind: domain.OperationRestart, CreatedAt: time.Now().Add(-3 * time.Hour)}
	if err := s.EnqueueOperation(ctx, op); err != nil {
		t.Fatal(err)
	}
	if _, err := m.reconcile(ctx, c.Ref()); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetOperation(ctx, op.ID)
	if err != nil || got.Phase != domain.PhaseFailed || !strings.Contains(got.Message, "deadline") {
		t.Fatal(got, err)
	}
}

func TestConditionTransitionTimeDependsOnStatusAndReason(t *testing.T) {
	old := time.Now().Add(-time.Hour)
	previous := []domain.Condition{{Type: "Healthy", Status: "True", Reason: "Observed", LastTransitionTime: old}}
	next := []domain.Condition{{Type: "Healthy", Status: "True", Reason: "Observed", Message: "Refreshed observation", LastTransitionTime: time.Now()}}
	preserveTransitions(previous, next)
	if !next[0].LastTransitionTime.Equal(old) {
		t.Fatal(next)
	}
	next[0].Reason = "Changed"
	next[0].LastTransitionTime = time.Now()
	preserveTransitions(previous, next)
	if next[0].LastTransitionTime.Equal(old) {
		t.Fatal(next)
	}
}

func TestSharedStatusReportsObservationNotDesiredVersion(t *testing.T) {
	p := &operationProvider{fakeProvider: lifecycleProvider().fakeProvider}
	p.syncFn = func(*engine.Context) (engine.Result, error) { return engine.Result{RequeueAfter: time.Second}, nil }
	p.statusFn = func(*engine.Context) (domain.ClusterStatus, error) {
		return domain.ClusterStatus{Phase: domain.PhaseScaling, Version: "10.0.1", Members: []domain.MemberStatus{{Version: "9.5.5"}, {Version: "8.19.23"}}, Conditions: []domain.Condition{{Type: "Bootstrapped", Status: "False", Reason: "Formation"}}}, nil
	}
	m, s := newTestManager(t, p)
	ctx := context.Background()
	c := &domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: "fake", Version: "10.0.1"}}
	if err := s.CreateCluster(ctx, c); err != nil {
		t.Fatal(err)
	}
	if _, err := m.reconcile(ctx, c.Ref()); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetCluster(ctx, c.Ref())
	if err != nil || got.Status.Version != "8.19.23" || got.Status.Phase != domain.PhaseCreating || got.Status.Message == "" || got.Status.Conditions[0].Message == "" {
		t.Fatal(got, err)
	}
	if observedVersion(nil) != "" {
		t.Fatal("unavailable versions must not be guessed")
	}
}

func TestDeadlineRetainsLastErrorAcrossProgressUpdates(t *testing.T) {
	m, _ := newTestManager(t, lifecycleProvider())
	op := &domain.Operation{Kind: domain.OperationBackup, CreatedAt: time.Now()}
	if m.operationFailure(op, errors.New("Storage is unavailable")) {
		t.Fatal("first failure retired operation")
	}
	op.Message = "Waiting for inventory"
	op.CreatedAt = time.Now().Add(-3 * time.Hour)
	if !m.operationFailure(op, nil) || !strings.Contains(op.Message, "Storage is unavailable") {
		t.Fatal(op)
	}
}

func TestRestorePhaseIsPersistedBeforeProviderCall(t *testing.T) {
	var s *store.Store
	p := &operationProvider{fakeProvider: lifecycleProvider().fakeProvider}
	p.runFn = func(rc *engine.Context, _ *domain.Operation) (engine.OpResult, error) {
		current, err := s.GetCluster(context.Background(), rc.Cluster.Ref())
		if err != nil || current.Status.Phase != domain.PhaseRestoring || current.Status.Message == "" {
			t.Fatal(current, err)
		}
		return engine.OpResult{Step: "source-inventory", Message: "Waiting for PBM source inventory"}, nil
	}
	m, s := newTestManager(t, p)
	c := &domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: "fake"}, Status: domain.ClusterStatus{Phase: domain.PhaseReady}}
	ctx := context.Background()
	if err := s.CreateCluster(ctx, c); err != nil {
		t.Fatal(err)
	}
	if err := s.EnqueueOperation(ctx, &domain.Operation{Cluster: c.Name, Kind: domain.OperationRestore}); err != nil {
		t.Fatal(err)
	}
	if _, err := m.reconcile(ctx, c.Ref()); err != nil {
		t.Fatal(err)
	}
}
