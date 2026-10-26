// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package reconciler

import (
	"errors"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

type unavailableCleanupNamespaces struct{ engine.StaticNamespaces }

func (unavailableCleanupNamespaces) Backend(string) (infra.Backend, error) {
	return nil, errors.New("namespace offline")
}

func TestSuspendedCleanupNamespaceFailurePreservesLifecycle(t *testing.T) {
	for _, resume := range []bool{false, true} {
		t.Run(map[bool]string{false: "recover-cleanup", true: "explicit-resume"}[resume], func(t *testing.T) {
			p := &operationProvider{fakeProvider: lifecycleProvider().fakeProvider}
			p.syncFn = func(*engine.Context) (engine.Result, error) {
				t.Fatal("suspended runtime was synchronized")
				return engine.Result{}, nil
			}
			p.statusFn = func(*engine.Context) (domain.ClusterStatus, error) {
				t.Fatal("suspended runtime was observed")
				return domain.ClusterStatus{}, nil
			}
			p.runFn = func(rc *engine.Context, _ *domain.Operation) (engine.OpResult, error) {
				if rc.Cluster.Status.Phase != domain.PhaseSuspended {
					t.Fatal("cleanup lost suspension", rc.Cluster.Status)
				}
				return engine.OpResult{Done: true}, nil
			}
			m, st := newTestManager(t, p)
			ctx := t.Context()
			c := &domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: "fake"}}
			if err := st.CreateCluster(ctx, c); err != nil {
				t.Fatal(err)
			}
			if err := st.MarkClusterDeleting(ctx, c.Ref(), domain.DeleteOptions{KeepVolumes: true, KeepBackups: true}); err != nil {
				t.Fatal(err)
			}
			c, err := st.GetCluster(ctx, c.Ref())
			if err != nil {
				t.Fatal(err)
			}
			if err = st.SuspendCluster(ctx, c); err != nil {
				t.Fatal(err)
			}
			backup := &domain.Backup{Cluster: c.Name, Name: "daily", PBMName: "remote", Phase: domain.PhaseSucceeded}
			if err = st.CreateBackup(ctx, backup); err != nil {
				t.Fatal(err)
			}
			op := &domain.Operation{Cluster: c.Name, Kind: domain.OperationDeleteBackup}
			if err = st.EnqueueBackupDeletion(store.WithClusterSnapshot(ctx, c), backup, op); err != nil {
				t.Fatal(err)
			}
			m.opts.Namespaces = unavailableCleanupNamespaces{}
			if _, err = m.reconcile(ctx, c.Ref()); err == nil {
				t.Fatal("namespace failure was ignored")
			}
			current, err := st.GetCluster(ctx, c.Ref())
			if err != nil || current.Status.Phase != domain.PhaseSuspended || len(current.Status.Conditions) != 1 || current.Status.Conditions[0].Reason != "NamespaceUnavailable" {
				t.Fatal(current, err)
			}
			if resume {
				if err = st.ResumeCluster(store.WithClusterSnapshot(ctx, current), c.Ref()); err != nil {
					t.Fatal("resume was no longer admissible", err)
				}
				return
			}
			m.opts.Namespaces = engine.StaticNamespaces{}
			for range 2 {
				if _, err = m.reconcile(ctx, c.Ref()); err != nil {
					t.Fatal(err)
				}
			}
			done, err := st.GetOperation(ctx, op.ID)
			if err != nil || done.Phase != domain.PhaseSucceeded {
				t.Fatal(done, err)
			}
			current, err = st.GetCluster(ctx, c.Ref())
			if err != nil || current.Status.Phase != domain.PhaseSuspended {
				t.Fatal(current, err)
			}
		})
	}
}

func TestConvergenceBudgetPersistsBetweenReconciliations(t *testing.T) {
	p := &operationProvider{fakeProvider: lifecycleProvider().fakeProvider}
	p.statusFn = func(*engine.Context) (domain.ClusterStatus, error) {
		return domain.ClusterStatus{Phase: domain.PhaseCreating}, nil
	}
	m, st := newTestManager(t, p)
	m.opts.OperationTimeouts = map[string]time.Duration{domain.OperationCreate: 90 * time.Second}
	ctx := t.Context()
	op := &domain.Operation{Kind: domain.OperationCreate}
	c := &domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: "fake"}}
	if err := st.CreateCluster(store.WithOperation(ctx, op), c); err != nil {
		t.Fatal(err)
	}
	if _, err := m.reconcile(ctx, c.Ref()); err != nil {
		t.Fatal(err)
	}
	for i := range 2 {
		m.budgets[c.Ref().Key()][op.ID] = operationBudget{observed: time.Now().Add(-time.Minute)}
		if _, err := m.reconcile(ctx, c.Ref()); err != nil {
			t.Fatal(err)
		}
		got, err := st.GetOperation(ctx, op.ID)
		if err != nil || got.Elapsed < time.Duration(i+1)*time.Minute {
			t.Fatal("lost scheduled wait interval", got, err)
		}
		if i == 0 {
			if got.Phase != domain.PhaseRunning {
				t.Fatal(got)
			}
			// A new manager models restart: only persisted spent time survives.
			m, err = New(m.opts)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = m.reconcile(ctx, c.Ref()); err != nil {
				t.Fatal(err)
			}
		} else if got.Phase != domain.PhaseFailed {
			t.Fatal("convergence exceeded its budget without retiring", got)
		}
	}
}
