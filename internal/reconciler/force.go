// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package reconciler

import (
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

// ForcedReconcileMinInterval bounds operator-driven fresh platform reads.
const ForcedReconcileMinInterval = 30 * time.Second

// All fields are protected by Manager.mu.
type forcedReconcileState struct {
	last    time.Time
	pending bool
	timer   *time.Timer
}

// ForceReconcile requests a fresh pass without changing desired state or retry
// budgets. Bursts share one immediate trigger and at most one deferred trigger.
func (m *Manager) ForceReconcile(ref domain.ClusterRef) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.ctx != nil && m.ctx.Err() != nil {
		return
	}
	key := ref.Key()
	w := m.workers[key]
	if w == nil {
		w = &worker{trigger: make(chan struct{}, 1)}
		m.workers[key] = w
	}
	f := &w.forced
	if f.pending {
		return
	}
	now := time.Now()
	if f.last.IsZero() || !now.Before(f.last.Add(ForcedReconcileMinInterval)) {
		f.last = now
		m.triggerLocked(ref)
		return
	}
	f.pending = true
	fire := func() {
		m.mu.Lock()
		defer m.mu.Unlock()
		if !f.pending || m.workers[key] != w || (m.ctx != nil && m.ctx.Err() != nil) {
			return
		}
		f.pending, f.timer = false, nil
		f.last = time.Now()
		m.triggerLocked(ref)
	}
	delay := f.last.Add(ForcedReconcileMinInterval).Sub(now)
	f.timer = time.AfterFunc(delay, fire)
}

func (m *Manager) stopForcedLocked(w *worker) {
	f := &w.forced
	f.pending = false
	if f.timer != nil {
		f.timer.Stop()
		f.timer = nil
	}
}
