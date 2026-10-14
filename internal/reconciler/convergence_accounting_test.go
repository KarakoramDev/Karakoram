// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package reconciler

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

func TestQueueOperationErrorsDoNotConsumeConvergenceBudget(t *testing.T) {
	for _, priorFailures := range []int{0, DefaultOperationRetryLimit - 1} {
		t.Run(strconv.Itoa(priorFailures), func(t *testing.T) {
			ctx := context.Background()
			p := &operationProvider{fakeProvider: lifecycleProvider().fakeProvider}
			p.syncFn = func(*engine.Context) (engine.Result, error) {
				t.Fatal("running restore invoked Sync")
				return engine.Result{}, nil
			}
			p.runFn = func(*engine.Context, *domain.Operation) (engine.OpResult, error) {
				return engine.OpResult{}, errors.New("initial restore temporarily unavailable")
			}
			m, st := newTestManager(t, p)
			create := &domain.Operation{Kind: domain.OperationCreate}
			c := &domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: "fake"}}
			if err := st.CreateCluster(store.WithOperation(ctx, create), c); err != nil {
				t.Fatal(err)
			}
			create.Failures, create.LastError = priorFailures, "earlier convergence error"
			if err := st.UpdateOperation(ctx, create); err != nil {
				t.Fatal(err)
			}
			restore := &domain.Operation{Cluster: c.Name, Kind: domain.OperationRestore, Phase: domain.PhaseRunning}
			if err := st.EnqueueOperation(ctx, restore); err != nil {
				t.Fatal(err)
			}
			for range 5 {
				if _, err := m.reconcile(ctx, c.Ref()); err == nil {
					t.Fatal("restore failure disappeared")
				}
				got, err := st.GetOperation(ctx, create.ID)
				if err != nil || got.Phase != domain.PhaseRunning || got.Failures != priorFailures || got.LastError != "earlier convergence error" {
					t.Fatal(got, err)
				}
			}
			got, err := st.GetOperation(ctx, restore.ID)
			if err != nil || got.Failures != 5 || got.Phase != domain.PhaseRunning {
				t.Fatal(got, err)
			}
		})
	}
}

func TestSuccessfulConvergenceResetsItsOwnFailureBudget(t *testing.T) {
	for _, deleting := range []bool{false, true} {
		name := "sync"
		if deleting {
			name = "cleanup"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			calls := 0
			failure := func() error {
				calls++
				if calls%2 == 1 {
					return errors.New("platform temporarily unavailable")
				}
				return nil
			}
			p := &operationProvider{fakeProvider: lifecycleProvider().fakeProvider}
			p.syncFn = func(*engine.Context) (engine.Result, error) {
				return engine.Result{RequeueAfter: time.Second}, failure()
			}
			p.statusFn = func(*engine.Context) (domain.ClusterStatus, error) {
				return domain.ClusterStatus{Phase: domain.PhaseCreating}, nil
			}
			p.cleanupFn = func(*engine.Context) (bool, error) { return false, failure() }
			m, st := newTestManager(t, p)
			m.opts.OperationRetryLimit = 2
			op := &domain.Operation{Kind: domain.OperationCreate}
			c := &domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: "fake"}}
			if err := st.CreateCluster(store.WithOperation(ctx, op), c); err != nil {
				t.Fatal(err)
			}
			if deleting {
				op = &domain.Operation{Kind: domain.OperationDelete}
				if err := st.MarkClusterDeleting(store.WithOperation(ctx, op), c.Ref(), domain.DeleteOptions{KeepVolumes: true}); err != nil {
					t.Fatal(err)
				}
			}
			for attempt := range 6 {
				_, _ = m.reconcile(ctx, c.Ref())
				got, err := st.GetOperation(ctx, op.ID)
				wantFailures := (attempt + 1) % 2
				if err != nil || got.Phase != domain.PhaseRunning || got.Failures != wantFailures {
					t.Fatal(got, err, wantFailures)
				}
			}
		})
	}
}
