// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package reconciler

import (
	"context"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

type convergenceContextProvider struct {
	*fakeProvider
	t        *testing.T
	deadline time.Time
	calls    int
}

func (p *convergenceContextProvider) check(ctx context.Context) {
	p.t.Helper()
	deadline, ok := ctx.Deadline()
	if !ok || !deadline.Equal(p.deadline) {
		p.t.Fatalf("tracking deadline changed provider context: deadline=%s want=%s", deadline, p.deadline)
	}
	p.calls++
}
func (p *convergenceContextProvider) Sync(ctx context.Context, _ *engine.Context) (engine.Result, error) {
	p.check(ctx)
	return engine.Result{RequeueAfter: time.Second}, nil
}
func (p *convergenceContextProvider) Cleanup(ctx context.Context, _ *engine.Context, _ domain.DeleteOptions) (bool, error) {
	p.check(ctx)
	return false, nil
}

func TestConvergenceTrackingDeadlineDoesNotShortenProviderContext(t *testing.T) {
	for _, deleting := range []bool{false, true} {
		name := "sync"
		if deleting {
			name = "cleanup"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			deadline, _ := ctx.Deadline()
			p := &convergenceContextProvider{fakeProvider: lifecycleProvider().fakeProvider, t: t, deadline: deadline}
			m, st := newTestManager(t, p)
			c := &domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: "fake"}}
			op := &domain.Operation{Kind: domain.OperationCreate, CreatedAt: time.Now().Add(-DefaultOperationTimeout + time.Second)}
			if err := st.CreateCluster(store.WithOperation(ctx, op), c); err != nil {
				t.Fatal(err)
			}
			if deleting {
				op = &domain.Operation{Kind: domain.OperationDelete, CreatedAt: time.Now().Add(-DefaultOperationTimeout + time.Second)}
				if err := st.MarkClusterDeleting(store.WithOperation(ctx, op), c.Ref(), domain.DeleteOptions{KeepVolumes: true}); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := m.reconcile(ctx, c.Ref()); err != nil {
				t.Fatal(err)
			}
			if p.calls != 1 {
				t.Fatalf("provider calls = %d", p.calls)
			}
		})
	}
}

type eventualDeleteProvider struct {
	*fakeProvider
	calls    int
	complete bool
}

func (p *eventualDeleteProvider) Cleanup(_ context.Context, rc *engine.Context, _ domain.DeleteOptions) (bool, error) {
	p.calls++
	rc.Cluster.Status.Message = "Swarm inconsistency: desired 0 but a task is desired-running"
	return p.complete, nil
}

func TestDeleteTrackingDeadlineRetainsCleanupAndEventuallySucceeds(t *testing.T) {
	p := &eventualDeleteProvider{fakeProvider: lifecycleProvider().fakeProvider}
	m, st := newTestManager(t, p)
	ctx := t.Context()
	c := &domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: "fake"}}
	if err := st.CreateCluster(ctx, c); err != nil {
		t.Fatal(err)
	}
	op := &domain.Operation{Kind: domain.OperationDelete}
	if err := st.MarkClusterDeleting(store.WithOperation(ctx, op), c.Ref(), domain.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := m.reconcile(ctx, c.Ref()); err != nil {
		t.Fatal(err)
	}
	if DefaultOperationTimeout != 2*time.Hour {
		t.Fatal("unexpected generic tracking deadline")
	}
	m.budgets[c.Ref().Key()][op.ID] = operationBudget{observed: time.Now().Add(-DefaultOperationTimeout - time.Minute)}
	if _, err := m.reconcile(ctx, c.Ref()); err != nil {
		t.Fatal(err)
	}
	failed, err := st.GetOperation(ctx, op.ID)
	if err != nil || failed.Phase != domain.PhaseFailed {
		t.Fatalf("tracking did not expire: %+v %v", failed, err)
	}
	retained, err := st.GetCluster(ctx, c.Ref())
	if err != nil || !retained.Deleting || retained.DeleteOptions.KeepVolumes {
		t.Fatalf("deadline erased desired cleanup: %+v %v", retained, err)
	}
	before := p.calls
	m, err = New(m.opts)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.reconcile(ctx, c.Ref()); err != nil {
		t.Fatal(err)
	}
	if p.calls <= before {
		t.Fatal("cleanup stopped after failed tracking and reconstruction")
	}
	p.complete = true
	if _, err := m.reconcile(ctx, c.Ref()); err != errClusterGone {
		t.Fatal(err)
	}
	done, err := st.GetOperation(ctx, op.ID)
	if err != nil || done.Phase != domain.PhaseSucceeded {
		t.Fatalf("authoritative removal did not complete original operation: %+v %v", done, err)
	}
}
