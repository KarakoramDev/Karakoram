// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package reconciler

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

func TestConvergenceFailureDoesNotBlockIndependentOperations(t *testing.T) {
	for _, kind := range []string{domain.OperationBackup, domain.OperationDeleteBackup, domain.OperationRestore, domain.OperationRestart} {
		for _, convergenceFailure := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/failure=%t", kind, convergenceFailure), func(t *testing.T) {
				p := &operationProvider{fakeProvider: lifecycleProvider().fakeProvider}
				p.syncFn = func(*engine.Context) (engine.Result, error) {
					result := engine.Result{RequeueAfter: time.Second}
					if convergenceFailure {
						result.ConvergenceError = fmt.Errorf("%w: stateless component configuration failed", engine.ErrInvalid)
					}
					return result, nil
				}
				p.statusFn = func(*engine.Context) (domain.ClusterStatus, error) {
					return domain.ClusterStatus{Phase: domain.PhaseInitializing, Message: "stateless component configuration failed"}, nil
				}
				calls := 0
				p.runFn = func(_ *engine.Context, op *domain.Operation) (engine.OpResult, error) {
					if op.Kind != kind {
						t.Fatalf("unexpected operation: %s", op.Kind)
					}
					calls++
					return engine.OpResult{Done: true}, nil
				}
				m, st := newTestManager(t, p)
				create := &domain.Operation{Kind: domain.OperationCreate}
				c := &domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: "fake"}}
				if err := st.CreateCluster(store.WithOperation(t.Context(), create), c); err != nil {
					t.Fatal(err)
				}
				if kind == domain.OperationRestore {
					if err := st.CreateRestore(t.Context(), &domain.Restore{Cluster: c.Name, Name: "restore"}); err != nil {
						t.Fatal(err)
					}
				}
				for attempt := range 3 {
					queued := &domain.Operation{Cluster: c.Name, Kind: kind, Payload: json.RawMessage(`{"restore":"restore"}`)}
					if err := st.EnqueueOperation(t.Context(), queued); err != nil {
						t.Fatal(err)
					}
					if _, err := m.reconcile(t.Context(), c.Ref()); err != nil {
						t.Fatal("component failure escaped reconciliation", err)
					}
					actual, err := st.GetOperation(t.Context(), queued.ID)
					if err != nil || actual.Phase != domain.PhaseSucceeded || calls != attempt+1 {
						t.Fatalf("queue starved: %+v %v, calls=%d", actual, err, calls)
					}
				}
				actual, err := st.GetOperation(t.Context(), create.ID)
				want := domain.PhaseRunning
				if convergenceFailure {
					want = domain.PhaseFailed
				}
				if err != nil || actual.Phase != want {
					t.Fatalf("tracking operation: %+v %v", actual, err)
				}
				events, err := st.ListEvents(t.Context(), store.EventFilter{})
				if err != nil {
					t.Fatal(err)
				}
				for _, event := range events {
					if event.Reason == "ReconcileError" {
						t.Fatalf("component failure repeated a reconciliation warning: %+v", event)
					}
				}
			})
		}
	}
}

func TestConvergenceFailureDoesNotObserveTheGenerationAsApplied(t *testing.T) {
	p := &operationProvider{fakeProvider: lifecycleProvider().fakeProvider}
	p.syncFn = func(rc *engine.Context) (engine.Result, error) {
		rc.Cluster.Status.ObservedGeneration = rc.Cluster.Generation
		return engine.Result{ConvergenceError: engine.ErrInvalid}, nil
	}
	p.statusFn = func(*engine.Context) (domain.ClusterStatus, error) {
		return domain.ClusterStatus{Phase: domain.PhaseReady}, nil
	}
	m, _ := newTestManager(t, p)
	c := &domain.Cluster{Generation: 2, Status: domain.ClusterStatus{ObservedGeneration: 1}}
	rc := m.opts.Context
	rc.Cluster = c
	if _, err := m.synchronize(t.Context(), p, &rc, nil); err != nil {
		t.Fatal(err)
	}
	if c.Status.ObservedGeneration != 1 {
		t.Fatalf("failed generation was observed as applied: %d", c.Status.ObservedGeneration)
	}
}
