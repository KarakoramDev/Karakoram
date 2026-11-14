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
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

// renewAvailability does not share the lifecycle worker's mutable Context or
// Cluster. Provider changes to that Cluster are deliberately discarded; only a
// fenced availability sample can be published. Existing authoritative continuity
// checks may still persist fail-closed safety-journal evidence independently.
// Observation-safe operations release publication ownership during their attempt.
func (m *Manager) renewAvailability(ctx context.Context, ref domain.ClusterRef) error {
	ctx = infra.WithRequestCaller(ctx, infra.CallerAvailability)
	ctx = engine.WithObservationPass(ctx)
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
	p, err := m.opts.Registry.Get(c.Spec.Provider)
	if err != nil {
		return err
	}
	ops, err := m.opts.Store.ListOperations(ctx, ref, domain.PhaseRunning)
	if err != nil {
		return err
	}
	for _, op := range ops {
		if !canObserveDuringOperation(p, op.Kind) {
			return nil
		}
	}
	before := *c
	before.Internal = bytes.Clone(c.Internal)
	before.Status.Conditions = slices.Clone(c.Status.Conditions)
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
	status, observationErr := m.observeSteadyStatus(ctx, p, &rc)
	if observationErr != nil {
		rc.Logger.Debug("availability observation failed", "cluster", ref.Key(), "stage", "availability.observe")
	}
	status.ObservedGeneration = before.Status.ObservedGeneration
	engine.ApplyDeploymentStatus(&rc, &status)
	preservePublishedEndpointFailure(before.Status.Conditions, &status)
	preserveMaintenance(&before, &status)
	preserveAvailabilityLifecycle(before.Status, &status, rc.Clock.Now())
	for _, op := range ops {
		if !domain.IsConvergenceOperation(op.Kind) {
			// Progress belongs to the operation even when its phase happens to
			// be Ready. Availability may still withdraw health and Ready.
			status.Message = before.Status.Message
			status.Conditions = rebaseOperationConditions(before.Status.Conditions, before.Status.Conditions, status.Conditions)
			break
		}
	}
	preserveTransitions(before.Status.Conditions, status.Conditions)
	started := time.Now()
	published, err := m.opts.Store.UpdateClusterObservation(ctx, &before, previousStatus, status)
	m.logStage(ref, "availability.publish", started)
	if err == nil && !published {
		m.opts.Context.Logger.Debug("discard concurrent availability sample", "cluster", ref.Key())
	}
	if err == nil && published {
		m.publishedAvailability(ctx, ref, status)
	}
	return err
}

func canObserveDuringOperation(p engine.Provider, kind string) bool {
	if !domain.AllowsIndependentObservation(kind) {
		return false
	}
	policy, ok := p.(engine.OperationObservationPolicy)
	return !ok || policy.ObserveDuringOperation(kind)
}

// Rebase observation-owned fields under the publication gate. Internal, applied
// generation and explicitly classified lifecycle receipts belong to the operation;
// all other conditions come exclusively from the newest complete observation.
func rebaseOperationAvailability(before domain.ClusterStatus, operation *domain.ClusterStatus, current domain.ClusterStatus) {
	// A lifecycle phase retained by renewal is not a newer health verdict:
	// it must not undo the provider's explicit completion of that lifecycle.
	// Ready/Degraded observations still supersede an earlier provider verdict.
	if operation.Phase == before.Phase ||
		(operation.Phase == domain.PhaseReady || operation.Phase == domain.PhaseDegraded) &&
			(current.Phase == domain.PhaseReady || current.Phase == domain.PhaseDegraded) {
		operation.Phase = current.Phase
	}
	if operation.Message == before.Message {
		operation.Message = current.Message
	}
	operation.Conditions = rebaseOperationConditions(before.Conditions, operation.Conditions, current.Conditions)
	operation.ObservedAt, operation.ObservationEpoch = current.ObservedAt, current.ObservationEpoch
	operation.Agents, operation.Members = current.Agents, current.Members
	operation.Version, operation.FCV = current.Version, current.FCV
	operation.Connection, operation.Backup = current.Connection, current.Backup
	operation.Placement, operation.DataMcp = current.Placement, current.DataMcp
}

// Unknown condition types are observation-owned: a diagnostic omitted after
// recovery must disappear. Only these explicit lifecycle/operation receipts may
// survive a health sweep; this is deliberately not the readiness-expiry predicate.
func operationOwnedCondition(condition domain.Condition) bool {
	switch condition.Type {
	case "OperationReceipt", "MaintenanceWindow":
		return true
	case "Ready":
		if condition.Status == "True" {
			return false
		}
		switch condition.Reason {
		case "LifecyclePending", "WaitingForWindow", "Restoring", "RestoreFinished":
			return true
		}
	case "InitialDataSource":
		return condition.Status == "False" && condition.Reason == "RestoreAborted"
	case "PublishedEndpoint":
		return condition.Status == "False" && condition.Reason == "PublishedEndpointRepairExhausted"
	}
	return false
}

func rebaseOperationConditions(before, operation, current []domain.Condition) []domain.Condition {
	for _, old := range before {
		if !operationOwnedCondition(old) {
			continue
		}
		if !slices.ContainsFunc(operation, func(condition domain.Condition) bool { return condition.Type == old.Type }) {
			current = slices.DeleteFunc(current, func(condition domain.Condition) bool {
				return condition.Type == old.Type && operationOwnedCondition(condition)
			})
		}
	}
	for _, condition := range operation {
		if !operationOwnedCondition(condition) {
			continue
		}
		index := slices.IndexFunc(before, func(old domain.Condition) bool { return old.Type == condition.Type })
		if index >= 0 && before[index] == condition &&
			slices.ContainsFunc(current, func(observed domain.Condition) bool { return observed.Type == condition.Type }) {
			continue
		}
		index = slices.IndexFunc(current, func(old domain.Condition) bool { return old.Type == condition.Type })
		if index < 0 {
			current = append(current, condition)
		} else {
			current[index] = condition
		}
	}
	return current
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
	var watch <-chan struct{}
	if m.opts.AgentPush != nil {
		var stop func()
		watch, stop = m.opts.AgentPush.Watch(domain.NormalizedNamespace(ref.Namespace), ref.Name)
		defer stop()
	}
	var lastRenewal time.Time
	var deferred <-chan time.Time
	var timer *time.Timer
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !w.reconciling.Load() && w.steady.Load() == nil {
				continue
			}
		case <-watch:
		case <-deferred:
			deferred = nil
		}
		if !lastRenewal.IsZero() && time.Since(lastRenewal) < time.Second {
			if deferred == nil {
				if timer == nil {
					timer = time.NewTimer(time.Second - time.Since(lastRenewal))
				} else {
					timer.Reset(time.Second - time.Since(lastRenewal))
				}
				deferred = timer.C
			}
			continue
		}
		if deferred != nil {
			timer.Stop()
			deferred = nil
		}
		lastRenewal = time.Now()
		if err := m.renewAvailability(ctx, ref); err != nil && ctx.Err() == nil {
			m.opts.Context.Logger.Error("renew cluster availability", "cluster", ref.Key(), "error", err)
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
