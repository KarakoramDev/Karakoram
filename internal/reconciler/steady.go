// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package reconciler

import (
	"context"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

// Immutable snapshots let discovery and availability inspect worker state without
// sharing the lifecycle worker's mutable cluster or reconciliation context.
type steadyState struct {
	generation int64
	drift      uint64
}

type workerContextKey struct{}
type steadyPassKey struct{}
type steadyPass struct {
	eligible   bool
	generation int64
	drift      uint64
	requeue    time.Duration
}

func (m *Manager) workerFor(ctx context.Context, ref domain.ClusterRef) *worker {
	if w, _ := ctx.Value(workerContextKey{}).(*worker); w != nil {
		return w
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.workers[ref.Key()]
}

func (m *Manager) skipSteadyDiscovery(ctx context.Context, c *domain.Cluster) bool {
	w := m.workerFor(ctx, c.Ref())
	if w == nil || c.Deleting {
		return false
	}
	s := w.steady.Load()
	if s == nil || s.generation != c.Generation {
		return false
	}
	ops, err := m.opts.Store.ListOperations(ctx, c.Ref(), domain.PhasePending, domain.PhaseRunning)
	current := w.steady.Load()
	return err == nil && len(ops) == 0 && current != nil && current.generation == s.generation
}

func (m *Manager) finishSteady(ctx context.Context, ref domain.ClusterRef, w *worker, epoch uint64, generation int64, drift uint64) bool {
	// A negative renewal must not slip between the final durable read and
	// entering steady mode, where it could otherwise miss clearing the flag.
	if err := holdObservation(ctx); err != nil {
		return false
	}
	defer releaseObservation(ctx)
	c, err := m.opts.Store.GetCluster(ctx, ref)
	if err != nil || c.Deleting || c.Generation != generation ||
		c.Status.ObservedGeneration != c.Generation || c.Status.Phase != domain.PhaseReady ||
		len(c.Status.DeploymentFailures) != 0 {
		return false
	}
	ops, err := m.opts.Store.ListOperations(ctx, ref, domain.PhasePending, domain.PhaseRunning)
	if err != nil || len(ops) != 0 {
		return false
	}
	backend, err := m.opts.Namespaces.Backend(domain.NormalizedNamespace(ref.Namespace))
	if err != nil {
		return false
	}
	// A sibling's listing check can discover drift after the pass's last fresh
	// observation. Only enter steady under the generation captured before the
	// pass, so that discovery cannot be absorbed into the steady baseline.
	if target, ok := backend.(infra.PlatformSnapshotter); ok {
		if target.PlatformSnapshot().DriftGeneration() != drift {
			return false
		}
	}
	// Trigger and this transition share the lock: a request arriving during a
	// pass must never be overwritten by that pass's otherwise healthy result.
	m.mu.Lock()
	defer m.mu.Unlock()
	if w.triggers.Load() != epoch {
		return false
	}
	if w.steady.Load() == nil {
		w.steady.Store(&steadyState{generation: generation, drift: drift})
	}
	return true
}

func (m *Manager) publishedAvailability(ctx context.Context, ref domain.ClusterRef, status domain.ClusterStatus) {
	w := m.workerFor(ctx, ref)
	if w != nil && status.Phase != domain.PhaseReady && w.steady.Swap(nil) != nil {
		m.Trigger(ref)
	}
}

// Only disposable availability observations are eligible. Lifecycle observations
// can persist provider-owned state, so they must remain fresh just like Sync.
func (m *Manager) observeSteadyStatus(ctx context.Context, p engine.Provider, rc *engine.Context) (domain.ClusterStatus, error) {
	w := m.workerFor(ctx, rc.Cluster.Ref())
	if w == nil || w.steady.Load() == nil {
		return m.observeStatus(ctx, p, rc)
	}
	observeCtx, outcome := engine.WithSteadyOutcome(engine.WithSteadyEligible(ctx))
	status, err := m.observeStatusWithOutcome(observeCtx, p, rc, outcome)
	if outcome.Attempted {
		drift := outcome.DriftGeneration
		if target, ok := rc.Backend.(infra.PlatformSnapshotter); ok {
			drift = max(drift, target.PlatformSnapshot().DriftGeneration())
		}
		for s := w.steady.Load(); s != nil; s = w.steady.Load() {
			if drift > s.drift {
				if w.steady.CompareAndSwap(s, nil) {
					m.Trigger(rc.Cluster.Ref())
					break
				}
				continue
			}
			break
		}
	}
	return status, err
}
