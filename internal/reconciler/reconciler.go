// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package reconciler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

// A missing backup or secret is a provider error, not a reason to stop a worker.
var errClusterGone = fmt.Errorf("cluster removed: %w", store.ErrNotFound)

// Carry the invocation's identity to the worker without reloading mutable
// desired state after an error (which could belong to a newer request).
type attributedError struct {
	error
	actor     string
	operation string
}

func (e *attributedError) Unwrap() error { return e.error }

// Options configures serialized reconciliation and retry delays.
type Options struct {
	Store                 *store.Store
	Registry              *engine.Registry
	Namespaces            engine.Namespaces
	Context               engine.Context
	ResyncInterval        time.Duration
	BackoffMin            time.Duration
	BackoffMax            time.Duration
	OperationRetryLimit   int
	OperationTimeouts     map[string]time.Duration
	FailureCleanupTimeout time.Duration
}
type worker struct {
	trigger chan struct{}
	started bool
}

// Manager owns one serialized, retrying reconciliation worker per cluster.
type Manager struct {
	opts    Options
	mu      sync.Mutex
	workers map[string]*worker
	ctx     context.Context
	wg      sync.WaitGroup
}

// New constructs a per-cluster reconciliation manager.
func New(opts Options) (*Manager, error) {
	if opts.Store == nil || opts.Registry == nil {
		return nil, errors.New("store and provider registry are required")
	}
	if opts.ResyncInterval <= 0 {
		opts.ResyncInterval = 30 * time.Second
	}
	if opts.BackoffMin <= 0 {
		opts.BackoffMin = time.Second
	}
	if opts.BackoffMax <= 0 {
		opts.BackoffMax = time.Minute
	}
	if opts.OperationRetryLimit <= 0 {
		opts.OperationRetryLimit = DefaultOperationRetryLimit
	}
	if opts.FailureCleanupTimeout <= 0 {
		opts.FailureCleanupTimeout = DefaultFailureCleanupTimeout
	}
	if opts.BackoffMin > opts.BackoffMax {
		opts.BackoffMin = opts.BackoffMax
	}
	if opts.Context.Logger == nil {
		opts.Context.Logger = slog.Default()
	}
	if opts.Context.Clock == nil {
		opts.Context.Clock = engine.RealClock{}
	}
	if opts.Context.Store == nil {
		opts.Context.Store = opts.Store
	}
	if opts.Context.Secrets == nil {
		opts.Context.Secrets = opts.Store
	}
	if opts.Namespaces == nil {
		opts.Namespaces = engine.StaticNamespaces{DefaultBackend: opts.Context.Backend, DefaultAgentURL: opts.Context.AgentURL}
	}
	return &Manager{opts: opts, workers: make(map[string]*worker)}, nil
}

// Trigger coalesces reconciliation requests for a cluster without blocking.
func (m *Manager) Trigger(ref domain.ClusterRef) {
	cluster := ref.Key()
	m.mu.Lock()
	w := m.workers[cluster]
	if w == nil {
		w = &worker{trigger: make(chan struct{}, 1)}
		m.workers[cluster] = w
	}
	if m.ctx != nil && !w.started && m.ctx.Err() == nil {
		w.started = true
		m.wg.Add(1)
		go m.work(m.ctx, ref, w)
	}
	select {
	case w.trigger <- struct{}{}:
	default:
	}
	m.mu.Unlock()
}

// Run runs workers until cancellation and waits for them to stop.
func (m *Manager) Run(ctx context.Context) error {
	m.mu.Lock()
	if m.ctx != nil {
		m.mu.Unlock()
		return errors.New("reconciler manager already started")
	}
	runCtx, cancel := context.WithCancel(ctx)
	m.ctx = runCtx
	for name, w := range m.workers {
		w.started = true
		m.wg.Add(1)
		go m.work(runCtx, domain.ParseClusterKey(name), w)
	}
	m.mu.Unlock()
	defer func() {
		m.mu.Lock()
		cancel()
		m.mu.Unlock()
		m.wg.Wait()
	}()
	if err := m.discover(runCtx); err != nil {
		return err
	}
	ticker := time.NewTicker(m.opts.ResyncInterval)
	defer ticker.Stop()
	for {
		select {
		case <-runCtx.Done():
			return nil
		case <-ticker.C:
			if err := m.discover(runCtx); err != nil {
				m.opts.Context.Logger.Error("discover clusters", "error", err)
			}
		}
	}
}
func (m *Manager) discover(ctx context.Context) error {
	clusters, err := m.opts.Store.ListClusters(ctx, "")
	if err != nil {
		return err
	}
	for _, c := range clusters {
		m.Trigger(c.Ref())
	}
	return nil
}
func (m *Manager) work(ctx context.Context, ref domain.ClusterRef, w *worker) {
	name := ref.Key()
	defer m.wg.Done()
	timer := time.NewTimer(0)
	defer timer.Stop()
	backoff := m.opts.BackoffMin
	retrying := false
	for {
		triggers := w.trigger
		if retrying {
			triggers = nil
		}
		select {
		case <-ctx.Done():
			return
		case <-triggers:
		case <-timer.C:
		}
		delay, err := m.reconcile(ctx, ref)
		if errors.Is(err, errClusterGone) {
			m.mu.Lock()
			if m.workers[name] == w {
				delete(m.workers, name)
			}
			m.mu.Unlock()
			return
		}
		waiting := scheduledPlatformWait(err)
		scheduled := waiting != nil
		retrying = err != nil && !scheduled
		if scheduled {
			delay = max(time.Millisecond, waiting.RetryAt.Sub(m.opts.Context.Clock.Now()))
			backoff = m.opts.BackoffMin
		} else if err != nil {
			if ctx.Err() != nil {
				return
			}
			m.opts.Context.Logger.Error("reconcile cluster", "cluster", name, "error", err)
			event := domain.Event{Namespace: domain.NormalizedNamespace(ref.Namespace), Cluster: ref.Name, Type: "Warning", Reason: "ReconcileError", Message: err.Error(), Actor: "reconciler"}
			var cause *attributedError
			if errors.As(err, &cause) {
				event.Actor = cause.actor
				event.Operation = cause.operation
			}
			if eventErr := m.record(ctx, event); eventErr != nil {
				m.opts.Context.Logger.Error("record reconcile error", "error", eventErr)
			}
			delay = backoff
			if backoff < m.opts.BackoffMax/2 {
				backoff *= 2
			} else {
				backoff = m.opts.BackoffMax
			}
		} else {
			backoff = m.opts.BackoffMin
			if delay <= 0 {
				delay = m.opts.ResyncInterval
			}
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(delay)
	}
}
func (m *Manager) record(ctx context.Context, event domain.Event) error {
	if event.Actor == "" {
		event.Actor = store.Actor(ctx)
	}
	if event.Cluster != "" {
		if event.Operation == "" {
			event.Operation, _ = ctx.Value(operationEventKey{}).(string)
		}
		if event.Resource == "" {
			event.Resource = "clusters"
		}
		if event.Target == "" {
			event.Target = event.Cluster
		}
	}
	if m.opts.Context.Events != nil {
		return m.opts.Context.Events.Record(ctx, event)
	}
	return m.opts.Store.AppendEvent(ctx, &event)
}

// Record attributes provider lifecycle events to the current reconciliation or
// operation before forwarding them to the durable recorder.
func (m *Manager) Record(ctx context.Context, event domain.Event) error {
	return m.record(ctx, event)
}
func (m *Manager) state(ctx context.Context, c *domain.Cluster, previous domain.Phase) error {
	if err := m.opts.Store.UpdateClusterState(ctx, c); err != nil {
		return err
	}
	if previous != c.Status.Phase {
		return m.record(ctx, domain.Event{Namespace: c.Namespace, Cluster: c.Name, Type: "Normal", Reason: "PhaseChanged", Message: fmt.Sprintf("%s → %s", previous, c.Status.Phase)})
	}
	return nil
}
func (m *Manager) reconcile(ctx context.Context, ref domain.ClusterRef) (delay time.Duration, err error) {
	actor := "reconciler"
	operation := ""
	defer func() {
		if err != nil && !errors.Is(err, errClusterGone) {
			err = &attributedError{error: err, actor: actor, operation: operation}
		}
	}()
	c, err := m.opts.Store.GetCluster(ctx, ref)
	if errors.Is(err, store.ErrNotFound) {
		return 0, errClusterGone
	}
	if err != nil {
		return 0, err
	}
	ops, lookupErr := m.opts.Store.ListOperations(ctx, c.Ref(), domain.PhasePending, domain.PhaseRunning)
	if lookupErr != nil {
		return 0, lookupErr
	}
	for i := range ops {
		op := &ops[i]
		if !domain.IsConvergenceOperation(op.Kind) || op.TargetGeneration != c.Generation {
			continue
		}
		if m.operationFailure(op, nil) {
			if err = m.opts.Store.UpdateOperation(ctx, op); err != nil {
				return 0, err
			}
			continue
		}
		operation = op.ID
		if op.Phase == domain.PhasePending {
			now := m.opts.Context.Clock.Now()
			op.Phase, op.StartedAt = domain.PhaseRunning, &now
			if err = m.opts.Store.UpdateOperation(ctx, op); err != nil {
				return 0, err
			}
		}
	}
	var convergenceErr error
	convergenceAttempted := false
	defer func() {
		if !errors.Is(err, errClusterGone) {
			err = errors.Join(err, m.trackingOperations(ctx, c, convergenceErr, convergenceAttempted))
		}
	}()
	// Capture attribution before synchronization advances ObservedGeneration.
	// A completed generation must not claim later automatic health changes.
	if c.Deleting || c.Status.ObservedGeneration < c.Generation {
		if c.RequestedBy != "" {
			actor = c.RequestedBy
		}
	}
	ctx = store.WithActor(ctx, actor)
	if operation != "" {
		ctx = context.WithValue(ctx, operationEventKey{}, operation)
	}
	p, err := m.opts.Registry.Get(c.Spec.Provider)
	if err != nil {
		return 0, err
	}
	rc := m.opts.Context
	rc.Cluster = c
	rc.Events = m
	previous := c.Status.Phase
	// Convergence records bound observation, not the platform calls needed to
	// finish applying desired state or cleaning up after a failed record.
	if c.Deleting {
		c.Status.Phase = domain.PhaseDeleting
		if c.Status.Message == "" {
			c.Status.Message = "Removing platform runtime services and owned backup data"
		}
		if err = m.state(ctx, c, previous); err != nil {
			return 0, err
		}
		convergenceAttempted = true
		if err = m.resolve(ctx, &rc); err != nil {
			convergenceErr = err
			return 0, err
		}
		done, cleanupErr := p.Cleanup(ctx, &rc, c.DeleteOptions)
		convergenceErr = cleanupErr
		if err = m.opts.Store.UpdateClusterState(ctx, c); err != nil {
			return 0, err
		}
		if cleanupErr != nil {
			return 0, cleanupErr
		}
		if !done {
			return time.Second, nil
		}
		if c.DeleteOptions.KeepVolumes {
			if err = m.opts.Store.SuspendCluster(ctx, c); err != nil {
				return 0, err
			}
			return 0, m.record(ctx, domain.Event{Namespace: c.Namespace, Cluster: c.Name, Type: "Normal", Reason: "PhaseChanged", Message: "Deleting → Suspended"})
		}
		if err = m.record(ctx, domain.Event{Namespace: c.Namespace, Cluster: c.Name, Type: "Normal", Reason: "Deleted", Message: "Cluster deleted"}); err != nil {
			return 0, err
		}
		if err = m.opts.Store.DeleteCluster(ctx, ref); err != nil {
			return 0, err
		}
		return 0, errClusterGone
	}
	if c.Status.Phase == domain.PhaseSuspended {
		return 0, nil
	}
	// Claim before resolving dependencies so namespace failures consume the
	// operation's bounded budget rather than leaving it Running indefinitely.
	// Resume active work before convergence: restore may deliberately have every
	// database process stopped, making ordinary Sync/Status unsafe or impossible.
	op, err := m.opts.Store.ClaimRunningOperation(ctx, ref)
	if err == nil {
		actor, operation = op.Actor, op.ID
		if actor == "" {
			actor = "reconciler"
		}
		return m.runOperation(ctx, p, &rc, op)
	}
	if !errors.Is(err, store.ErrNotFound) {
		return 0, err
	}
	result, syncErr := m.synchronize(ctx, p, &rc)
	convergenceErr, convergenceAttempted = syncErr, true
	if err = m.state(ctx, c, previous); err != nil {
		return 0, err
	}
	if syncErr != nil {
		return 0, syncErr
	}
	op, err = m.opts.Store.ClaimOperation(ctx, ref)
	if errors.Is(err, store.ErrNotFound) {
		return result.RequeueAfter, nil
	}
	if err != nil {
		return 0, err
	}
	actor, operation = op.Actor, op.ID
	if actor == "" {
		actor = "reconciler"
	}
	return m.runOperation(ctx, p, &rc, op)
}

// resolve refreshes project dependencies immediately before each provider call.
func (m *Manager) resolve(ctx context.Context, rc *engine.Context) error {
	rc.Namespace = domain.NormalizedNamespace(rc.Cluster.Namespace)
	backend, err := m.opts.Namespaces.Backend(rc.Namespace)
	if err != nil {
		c := rc.Cluster
		message := fmt.Sprintf("namespace %q unavailable: %v", rc.Namespace, err)
		c.Status.Phase = domain.PhaseDegraded
		c.Status.Message = message
		condition := domain.Condition{Type: "NamespaceReady", Status: "False", Reason: "NamespaceUnavailable", Message: message, LastTransitionTime: rc.Clock.Now()}
		found := false
		for i := range c.Status.Conditions {
			if c.Status.Conditions[i].Type == condition.Type {
				if c.Status.Conditions[i].Status == condition.Status && c.Status.Conditions[i].Reason == condition.Reason {
					condition.LastTransitionTime = c.Status.Conditions[i].LastTransitionTime
				}
				c.Status.Conditions[i] = condition
				found = true
				break
			}
		}
		if !found {
			c.Status.Conditions = append(c.Status.Conditions, condition)
		}
		saveErr := m.opts.Store.UpdateClusterState(ctx, c)
		eventErr := m.record(ctx, domain.Event{Namespace: rc.Namespace, Cluster: c.Name, Type: "Warning", Reason: "NamespaceUnavailable", Message: message})
		return errors.Join(fmt.Errorf("%s: %w", message, err), saveErr, eventErr)
	}
	rc.Backend = backend
	rc.Dialer = m.opts.Namespaces.Dialer(rc.Namespace)
	rc.AgentURL = m.opts.Namespaces.AgentURL(rc.Namespace)
	for i := range rc.Cluster.Status.Conditions {
		condition := &rc.Cluster.Status.Conditions[i]
		if condition.Type == "NamespaceReady" && condition.Status != "True" {
			if rc.Cluster.Status.Message == condition.Message {
				rc.Cluster.Status.Message = ""
			}
			*condition = domain.Condition{Type: "NamespaceReady", Status: "True", Reason: "NamespaceAvailable", LastTransitionTime: rc.Clock.Now()}
		}
	}
	return nil
}

func (m *Manager) synchronize(ctx context.Context, p engine.Provider, rc *engine.Context) (engine.Result, error) {
	if err := m.resolve(ctx, rc); err != nil {
		return engine.Result{}, err
	}
	observedGeneration := rc.Cluster.Status.ObservedGeneration
	result, syncErr := p.Sync(ctx, rc)
	// A successful Sync may record the generation it applied while still
	// requesting periodic requeues; that observation only moves forward.
	if applied := rc.Cluster.Status.ObservedGeneration; syncErr == nil && applied > observedGeneration && applied <= rc.Cluster.Generation {
		observedGeneration = applied
	}
	if err := m.resolve(ctx, rc); err != nil {
		return result, errors.Join(syncErr, err)
	}
	status, statusErr := p.Status(ctx, rc)
	if statusErr == nil {
		status.Version = observedVersion(status.Members)
		if rc.Cluster.Generation == 1 && observedGeneration == 0 && (status.Phase == domain.PhaseScaling || status.Phase == domain.PhaseUpdating) {
			status.Phase = domain.PhaseCreating
		}
		if status.Message == "" && status.Phase != domain.PhaseReady && status.Phase != domain.PhaseDegraded {
			status.Message = fmt.Sprintf("%s: observing platform services and member formation", status.Phase)
		}
		preserveTransitions(rc.Cluster.Status.Conditions, status.Conditions)
		if status.Phase == domain.PhaseDegraded && status.Message == "" {
			status.Message = "Member health is degraded; inspect conditions and member observations"
		}
		for i := range status.Conditions {
			if status.Conditions[i].Message == "" {
				status.Conditions[i].Message = fmt.Sprintf("%s: %s (%s)", status.Phase, status.Conditions[i].Type, status.Conditions[i].Reason)
			}
		}
		// Observation is owned by reconciliation, not provider health snapshots.
		// Automatic retries must not make an applied request appear pending again.
		status.ObservedGeneration = observedGeneration
		rc.Cluster.Status = status
		if syncErr == nil && result.RequeueAfter <= 0 {
			rc.Cluster.Status.ObservedGeneration = rc.Cluster.Generation
		}
	}
	return result, errors.Join(syncErr, statusErr)
}

func (m *Manager) runOperation(ctx context.Context, p engine.Provider, rc *engine.Context, op *domain.Operation) (time.Duration, error) {
	actor := op.Actor
	if actor == "" {
		actor = "reconciler"
	}
	ctx = store.WithActor(ctx, actor)
	ctx = context.WithValue(ctx, operationEventKey{}, op.ID)
	c := rc.Cluster
	previous := c.Status.Phase
	callCtx := ctx
	var opResult engine.OpResult
	var opErr error
	if op.Kind == domain.OperationRestore {
		c.Status.Phase, c.Status.Message = domain.PhaseRestoring, "Restoring data; normal database traffic may be unavailable"
		if previous != domain.PhaseRestoring {
			if err := m.state(ctx, c, previous); err != nil {
				return 0, err
			}
			previous = domain.PhaseRestoring
		}
	}
	terminal := false
	if op.Step != failureCleanupStep {
		terminal = m.failQueueOperation(p, op, nil)
	}
	failing := op.Step == failureCleanupStep
	cleanupExpired := false
	if failing {
		// Legacy cleanup records receive a fresh, durable recovery window.
		if op.FailureCleanupStartedAt == nil {
			stamp := rc.Clock.Now()
			op.FailureCleanupStartedAt = &stamp
		}
		deadline := op.FailureCleanupStartedAt.Add(m.opts.FailureCleanupTimeout)
		cleanupExpired = !rc.Clock.Now().Before(deadline)
		if cleanupExpired {
			terminal = true
			op.Message = fmt.Sprintf("%s; recovery exceeded its %s cleanup deadline at compensation step %q; manual recovery is required", op.LastError, m.opts.FailureCleanupTimeout, compensationStep(op))
		} else {
			var cancel context.CancelFunc
			callCtx, cancel = context.WithTimeout(ctx, deadline.Sub(rc.Clock.Now()))
			defer cancel()
		}
	}
	// Compensation has its own window, not the exhausted work deadline.
	if !terminal && !failing {
		deadline := op.CreatedAt.Add(m.operationTimeout(op))
		var cancel context.CancelFunc
		callCtx, cancel = context.WithTimeout(ctx, deadline.Sub(rc.Clock.Now()))
		defer cancel()
	}
	if terminal {
		opResult.Done = true
		opErr = errors.New(op.Message)
	} else if err := m.resolve(callCtx, rc); err != nil {
		opErr = err
	} else if failing || op.CancelRequested {
		if canceller, ok := p.(engine.OperationCanceller); ok {
			opResult, opErr = canceller.CancelOperation(callCtx, rc, op)
			if opResult.Done && opErr == nil && !failing {
				op.Phase = domain.PhaseCancelled
			}
		} else {
			opErr = errors.New("Provider no longer supports cancellation of this operation")
		}
	} else {
		opResult, opErr = p.RunOperation(callCtx, rc, op)
	}
	if opResult.Checkpoint != nil {
		op.Checkpoint = opResult.Checkpoint
	}
	if opResult.Step != "" {
		op.Step = opResult.Step
	}
	if op.Kind == domain.OperationRestore && !opResult.Done && opResult.Message != "" {
		c.Status.Message = "Restoring: " + opResult.Message
	}
	if opResult.Message != "" {
		op.Message = opResult.Message
	}
	if opResult.Message == "" {
		op.Message = op.Kind + ": executing checkpoint " + op.Step
	}
	if opErr != nil {
		op.Message = opErr.Error()
	}
	if op.Kind == domain.OperationRestore && !opResult.Done {
		c.Status.Phase = domain.PhaseRestoring
		if opErr != nil {
			c.Status.Message = "Restoring: " + opErr.Error()
		}
	}
	if opErr == nil && !terminal {
		op.Failures = 0
	}
	if failing && !cleanupExpired {
		// Cleanup errors retain the checkpoint and retry with worker backoff,
		// including errors that would be permanent during ordinary work.
		opResult.Done = opResult.Done && opErr == nil
		if opResult.Done {
			opErr = errors.New(op.LastError)
			op.Message = op.LastError
		} else {
			op.Step = failureCleanupStep
		}
	} else if !opResult.Done {
		if m.failQueueOperation(p, op, opErr) {
			opResult.Done = true
			opErr = errors.New(op.Message)
		}
	}
	if opResult.Done {
		if op.Phase != domain.PhaseCancelled {
			op.Phase = domain.PhaseSucceeded
		}
		if opErr != nil {
			op.Phase = domain.PhaseFailed
		}
		now := rc.Clock.Now()
		op.CompletedAt = &now
		if opErr == nil {
			if op.Phase == domain.PhaseCancelled {
				op.Message = op.Kind + " cancelled safely at checkpoint " + op.Step
			} else {
				op.Message = op.Kind + " completed successfully"
			}
		}
		if op.Kind == domain.OperationRestore {
			c.Status.Phase, c.Status.Message = domain.PhaseUpdating, "Restore finished; observing database health"
			if opErr != nil {
				c.Status.Phase, c.Status.Message = domain.PhaseDegraded, op.Message
			}
		}
	}
	// Provisioning needed by an operation is opt-in and follows RunOperation,
	// using its new checkpoint rather than unrelated ordinary convergence.
	var result engine.Result
	var syncErr error
	if policy, ok := p.(engine.OperationSyncPolicy); ok && op.Step != failureCleanupStep && !opResult.Done && opErr == nil && policy.SyncDuringOperation(op) {
		result, syncErr = m.synchronize(ctx, p, rc)
	}
	// Persist even on provider error, atomically with the state referenced by
	// the checkpoint, so a completed external step is never forgotten.
	if err := m.opts.Store.UpdateOperationAndClusterState(ctx, op, c); err != nil {
		return 0, err
	}
	if previous != c.Status.Phase {
		if err := m.record(ctx, domain.Event{Namespace: c.Namespace, Cluster: c.Name, Operation: op.ID, Type: "Normal", Reason: "PhaseChanged", Message: fmt.Sprintf("%s → %s", previous, c.Status.Phase)}); err != nil {
			return 0, err
		}
	}
	if opResult.Done {
		event := domain.Event{Namespace: c.Namespace, Cluster: c.Name, Operation: op.ID, Type: "Normal", Reason: "OperationSucceeded", Message: op.Kind + " completed"}
		if op.Phase == domain.PhaseCancelled {
			event.Reason, event.Message = "OperationCancelled", op.Message
		}
		if opErr != nil {
			event.Type = "Warning"
			event.Reason = "OperationFailed"
			event.Message = opErr.Error()
			rc.Logger.Error("operation failed", "cluster", c.Name, "operation", op.ID, "kind", op.Kind, "attempts", op.Attempts, "error", opErr)
		}
		if err := m.record(ctx, event); err != nil {
			return 0, err
		}
		// Terminal failures unblock the queue; only nonterminal errors enter
		// the worker's bounded exponential backoff.
		return time.Millisecond, nil
	}
	if opErr != nil {
		if scheduledPlatformWait(opErr) != nil {
			return 0, opErr
		}
		if err := m.record(ctx, domain.Event{Namespace: c.Namespace, Cluster: c.Name, Operation: op.ID, Type: "Warning", Reason: "OperationError", Message: opErr.Error()}); err != nil {
			return 0, err
		}
		return 0, opErr
	}
	if syncErr != nil {
		return 0, syncErr
	}
	delay := opResult.RequeueAfter
	if delay <= 0 {
		delay = time.Second
	}
	if result.RequeueAfter > 0 && result.RequeueAfter < delay {
		delay = result.RequeueAfter
	}
	return delay, nil
}

// scheduledPlatformWait recognizes only error trees whose leaves are scheduled
// waits. errors.As alone also matches a wait joined with a genuine observation
// failure, which must remain visible and use ordinary error backoff.
func scheduledPlatformWait(err error) *engine.PlatformRetryWait {
	switch e := err.(type) {
	case *engine.PlatformRetryWait:
		return e
	case interface{ Unwrap() []error }:
		var earliest *engine.PlatformRetryWait
		for _, child := range e.Unwrap() {
			if child == nil {
				continue
			}
			wait := scheduledPlatformWait(child)
			if wait == nil {
				return nil
			}
			if earliest == nil || wait.RetryAt.Before(earliest.RetryAt) {
				earliest = wait
			}
		}
		return earliest
	case interface{ Unwrap() error }:
		return scheduledPlatformWait(e.Unwrap())
	default:
		return nil
	}
}
