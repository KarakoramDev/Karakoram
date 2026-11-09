// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package reconciler

import (
	"context"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func snapshotForced(m *Manager, w *worker) forcedReconcileState {
	m.mu.Lock()
	defer m.mu.Unlock()
	return w.forced
}

func TestForceReconcileClearsSteadyAndCoalesces(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ref := domain.ClusterRef{Name: "demo"}
		w := &worker{trigger: make(chan struct{}, 1)}
		w.steady.Store(&steadyState{})
		m := &Manager{workers: map[string]*worker{ref.Key(): w}}
		m.ForceReconcile(ref)
		if w.steady.Load() != nil || w.triggers.Load() != 1 || len(w.trigger) != 1 {
			t.Fatal("force did not clear steady and trigger")
		}
		<-w.trigger
		for range 100 {
			m.ForceReconcile(ref)
		}
		if w.triggers.Load() != 1 || len(w.trigger) != 0 || !snapshotForced(m, w).pending {
			t.Fatal("burst did not coalesce")
		}
		timer := snapshotForced(m, w).timer
		time.Sleep(ForcedReconcileMinInterval - time.Second)
		for range 100 {
			m.ForceReconcile(ref)
		}
		if snapshotForced(m, w).timer != timer || w.triggers.Load() != 1 {
			t.Fatal("pending calls changed timer or triggered early")
		}
		w.steady.Store(&steadyState{})
		time.Sleep(time.Second)
		synctest.Wait()
		state := snapshotForced(m, w)
		if w.triggers.Load() != 2 || len(w.trigger) != 1 || w.steady.Load() != nil || state.pending || state.timer != nil {
			t.Fatal("expected exactly one deferred fresh trigger")
		}
		<-w.trigger
		time.Sleep(2 * ForcedReconcileMinInterval)
		synctest.Wait()
		if w.triggers.Load() != 2 || len(w.trigger) != 0 {
			t.Fatal("force created recurring polling")
		}
		m.ForceReconcile(ref)
		if w.triggers.Load() != 3 {
			t.Fatal("new window did not trigger immediately")
		}
	})
}

func TestForceReconcileStartsMissingWorker(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := &steadyProvider{fakeProvider: &fakeProvider{}}
		m, st := newSteadyTestManager(t, p)
		m.opts.ResyncInterval = time.Hour
		stop := runSteadyManager(t, m)
		defer stop()
		c := &domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: "fake"}, Status: domain.ClusterStatus{Phase: domain.PhaseReady}}
		if err := st.CreateCluster(t.Context(), c); err != nil {
			t.Fatal(err)
		}
		m.ForceReconcile(c.Ref())
		synctest.Wait()
		m.mu.Lock()
		w := m.workers[c.Ref().Key()]
		started := w != nil && w.started
		m.mu.Unlock()
		if !started || p.syncs.Load() != 1 {
			t.Fatal("force did not start a missing worker", p.syncs.Load())
		}
		for range 100 {
			m.ForceReconcile(c.Ref())
		}
		time.Sleep(ForcedReconcileMinInterval)
		synctest.Wait()
		if p.syncs.Load() != 2 {
			t.Fatal("expected exactly one immediate and one deferred pass", p.syncs.Load())
		}
	})
}

func TestForceReconcileStopCancelsDeferred(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := &steadyProvider{fakeProvider: &fakeProvider{}}
		m, _, c := steadyFixture(t, p)
		m.ForceReconcile(c.Ref())
		stop := runSteadyManager(t, m)
		m.ForceReconcile(c.Ref())
		stop()
		m.mu.Lock()
		w := m.workers[c.Ref().Key()]
		pending, timer, epoch := w.forced.pending, w.forced.timer, w.triggers.Load()
		m.mu.Unlock()
		if pending || timer != nil {
			t.Fatal("shutdown retained timer")
		}
		time.Sleep(ForcedReconcileMinInterval)
		synctest.Wait()
		m.ForceReconcile(c.Ref())
		if w.triggers.Load() != epoch {
			t.Fatal("trigger after manager shutdown")
		}
	})
}

func TestForceReconcileGoneCancelsDeferred(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := &steadyProvider{fakeProvider: &fakeProvider{}}
		m, _ := newSteadyTestManager(t, p)
		ref := domain.ClusterRef{Name: "missing"}
		m.ForceReconcile(ref)
		m.ForceReconcile(ref)
		w := m.workers[ref.Key()]
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		go func() { done <- m.Run(ctx) }()
		synctest.Wait()
		m.mu.Lock()
		gone := m.workers[ref.Key()] == nil
		pending, timer := w.forced.pending, w.forced.timer
		m.mu.Unlock()
		if !gone || pending || timer != nil {
			t.Fatal("gone worker retained forced timer")
		}
		time.Sleep(ForcedReconcileMinInterval)
		synctest.Wait()
		if w.triggers.Load() != 1 {
			t.Fatal("removed worker triggered")
		}
		cancel()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	})
}

func TestForceReconcileConcurrentBurst(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ref := domain.ClusterRef{Name: "demo"}
		m := &Manager{workers: make(map[string]*worker)}
		var wg sync.WaitGroup
		for range 100 {
			wg.Go(func() { m.ForceReconcile(ref) })
		}
		wg.Wait()
		w := m.workers[ref.Key()]
		if w.triggers.Load() != 1 || !snapshotForced(m, w).pending {
			t.Fatal("concurrent requests did not coalesce")
		}
		<-w.trigger
		time.Sleep(ForcedReconcileMinInterval)
		synctest.Wait()
		if w.triggers.Load() != 2 || len(w.trigger) != 1 || snapshotForced(m, w).pending {
			t.Fatal("concurrent burst did not produce exactly one deferred trigger")
		}
	})
}
