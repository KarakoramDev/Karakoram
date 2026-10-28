// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package reconciler

import (
	"bytes"
	"context"
	"slices"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

// renewAvailability does not share the lifecycle worker's mutable Context or
// Cluster. Provider-owned changes made by Status are deliberately discarded;
// only a fenced availability sample can be published. Imperative operations
// retain their existing serialized observation/checkpoint ownership.
func (m *Manager) renewAvailability(ctx context.Context, ref domain.ClusterRef) error {
	if err := holdObservation(ctx); err != nil {
		return err
	}
	defer releaseObservation(ctx)
	c, previousStatus, err := m.opts.Store.GetClusterObservation(ctx, ref)
	if err != nil {
		return err
	}
	if !canObserveAvailability(c) {
		return nil
	}
	ops, err := m.opts.Store.ListOperations(ctx, ref, domain.PhaseRunning)
	if err != nil {
		return err
	}
	for _, op := range ops {
		if !domain.IsConvergenceOperation(op.Kind) {
			return nil
		}
	}
	before := *c
	before.Internal = bytes.Clone(c.Internal)
	before.Status.Conditions = slices.Clone(c.Status.Conditions)
	p, err := m.opts.Registry.Get(c.Spec.Provider)
	if err != nil {
		return err
	}
	rc := m.opts.Context
	rc.Cluster, rc.Events, rc.AvailabilityOnly = c, m, true
	for _, condition := range c.Status.Conditions {
		if condition.Type == "PlatformDeploymentWaiting" {
			rc.DeploymentWait = condition.Message
			rc.DeploymentWaitReason, rc.DeploymentWaitKey = condition.Reason, condition.Key
			break
		}
	}
	// Do not call resolve here: its dependency-failure path owns lifecycle
	// state and events. A missing namespace simply cannot renew this lease.
	rc.Namespace = domain.NormalizedNamespace(c.Namespace)
	rc.Backend, err = m.opts.Namespaces.Backend(rc.Namespace)
	if err != nil {
		return err
	}
	rc.Dialer = m.opts.Namespaces.Dialer(rc.Namespace)
	rc.AgentURL = m.opts.Namespaces.AgentURL(rc.Namespace)
	status, _ := m.observeStatus(ctx, p, &rc)
	status.ObservedGeneration = before.Status.ObservedGeneration
	engine.ApplyDeploymentStatus(&rc, &status)
	preservePublishedEndpointFailure(before.Status.Conditions, &status)
	preserveMaintenance(&before, &status)
	preserveAvailabilityLifecycle(before.Status, &status, rc.Clock.Now())
	preserveTransitions(before.Status.Conditions, status.Conditions)
	started := time.Now()
	published, err := m.opts.Store.UpdateClusterObservation(ctx, &before, previousStatus, status)
	m.logStage(ref, "availability.publish", started)
	if err == nil && !published {
		m.opts.Context.Logger.Debug("discard concurrent availability sample", "cluster", ref.Key())
	}
	return err
}

// Lifecycle phase does not determine whether existing members need fresh health.
// Only stopped/destructive intent forbids this independent read. A brand-new
// cluster with neither members nor observations has no availability to renew.
func canObserveAvailability(c *domain.Cluster) bool {
	if c.Deleting || c.Status.Phase == domain.PhaseDeleting ||
		c.Status.Phase == domain.PhaseSuspended || c.Status.Phase == domain.PhaseRestoring {
		return false
	}
	return c.Status.Phase == domain.PhaseReady || c.Status.Phase == domain.PhaseDegraded ||
		len(c.Status.Members) > 0 || !c.Status.ObservedAt.IsZero()
}

// Availability does not complete provisioning, deferred maintenance or a failed
// lifecycle request. Keep that progress and any negative readiness fence while
// allowing fresh member health (including fresh failures) to be published.
func preserveAvailabilityLifecycle(before domain.ClusterStatus, status *domain.ClusterStatus, at time.Time) {
	if before.Phase == domain.PhaseReady || before.Phase == domain.PhaseDegraded {
		return
	}
	status.Phase, status.Message = before.Phase, before.Message
	ready := domain.Condition{Type: "Ready", Status: "False", Reason: "LifecyclePending",
		Message: string(before.Phase) + ": lifecycle reconciliation has not completed", LastTransitionTime: at}
	for _, condition := range before.Conditions {
		if condition.Type == "Ready" && condition.Status != "True" {
			ready = condition
			break
		}
	}
	for i, condition := range status.Conditions {
		if condition.Type == "Ready" {
			if condition.Status == "True" {
				status.Conditions[i] = ready
			}
			return
		}
	}
	status.Conditions = append(status.Conditions, ready)
}
func (m *Manager) availabilityLoop(ctx context.Context, ref domain.ClusterRef, w *worker) {
	ticker := time.NewTicker(engine.ReadinessInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !w.reconciling.Load() {
				continue
			}
			if err := m.renewAvailability(ctx, ref); err != nil && ctx.Err() == nil {
				m.opts.Context.Logger.Error("renew cluster availability", "cluster", ref.Key(), "error", err)
			}
		}
	}
}

// time.Since uses the monotonic clock; a wall-clock correction must not change
// whether a stage exhausted the readiness lease. Never log provider diagnostics,
// request bodies, credentials or private observation epochs here.
func (m *Manager) logStage(ref domain.ClusterRef, stage string, started time.Time) {
	duration := time.Since(started)
	if duration > engine.ReadinessMaxAge {
		m.opts.Context.Logger.Warn("reconcile stage exceeds availability lease", "cluster", ref.Key(), "stage", stage, "duration", duration)
	} else {
		m.opts.Context.Logger.Debug("reconcile stage completed", "cluster", ref.Key(), "stage", stage, "duration", duration)
	}
}

type observationPublicationKey struct{}

// Each caller has its own scope over the same per-worker gate. held is accessed
// only by that caller's goroutine; never share a scope with the renewal loop.
// The gate spans observation AND publication, not Sync. Serializing complete
// samples avoids merging mixed health/lifecycle status or ordering failures by
// ObservedAt, which is intentionally zero on failed sweeps.
type observationPublication struct {
	gate chan struct{}
	held bool
}

func withObservationPublication(ctx context.Context, gate chan struct{}) context.Context {
	return context.WithValue(ctx, observationPublicationKey{}, &observationPublication{gate: gate})
}

func holdObservation(ctx context.Context) error {
	scope, _ := ctx.Value(observationPublicationKey{}).(*observationPublication)
	if scope == nil || scope.held {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case scope.gate <- struct{}{}:
		scope.held = true
		return nil
	}
}

func releaseObservation(ctx context.Context) {
	scope, _ := ctx.Value(observationPublicationKey{}).(*observationPublication)
	if scope != nil && scope.held {
		scope.held = false
		<-scope.gate
	}
}
