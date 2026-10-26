// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package reconciler

import (
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

func TestSuspendedClusterExecutesOnlyBackupCleanup(t *testing.T) {
	runs := 0
	p := &operationProvider{fakeProvider: lifecycleProvider().fakeProvider}
	p.syncFn = func(*engine.Context) (engine.Result, error) {
		t.Fatal("suspended runtime was synchronized")
		return engine.Result{}, nil
	}
	p.runFn = func(rc *engine.Context, op *domain.Operation) (engine.OpResult, error) {
		runs++
		if op.Kind != domain.OperationDeleteBackup || rc.Cluster.Status.Phase != domain.PhaseSuspended {
			t.Fatalf("unsafe suspended action: %+v", op)
		}
		if err := rc.Store.DeleteBackup(t.Context(), rc.Cluster.Ref(), "daily"); err != nil {
			t.Fatal(err)
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
	backup := &domain.Backup{Cluster: c.Name, Name: "daily", PBMName: "remote-backup", Phase: domain.PhaseSucceeded}
	if err = st.CreateBackup(ctx, backup); err != nil {
		t.Fatal(err)
	}
	deletion := &domain.Operation{Cluster: c.Name, Kind: domain.OperationDeleteBackup}
	if err = st.EnqueueBackupDeletion(store.WithClusterSnapshot(ctx, c), backup, deletion); err != nil {
		t.Fatal(err)
	}
	if _, err = m.reconcile(ctx, c.Ref()); err != nil {
		t.Fatal(err)
	}
	done, err := st.GetOperation(ctx, deletion.ID)
	if err != nil || done.Phase != domain.PhaseSucceeded || runs != 1 {
		t.Fatalf("suspended cleanup not executed: %+v runs=%d err=%v", done, runs, err)
	}
	current, err := st.GetCluster(ctx, c.Ref())
	if err != nil || current.Status.Phase != domain.PhaseSuspended {
		t.Fatal(current, err)
	}
}
