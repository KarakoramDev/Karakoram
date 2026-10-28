// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package reconciler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/datamcp/companion"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/maintenance"
	"github.com/KarakoramDev/Karakoram/internal/redact"
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
	Companion             *companion.Controller
	ResyncInterval        time.Duration
	BackoffMin            time.Duration
	BackoffMax            time.Duration
	OperationRetryLimit   int
	OperationTimeouts     map[string]time.Duration
	FailureCleanupTimeout time.Duration
}
type worker struct {
	trigger     chan struct{}
	started     bool
	reconciling atomic.Bool
}

// Manager owns one serialized, retrying reconciliation worker per cluster.
type Manager struct {
	opts    Options
	mu      sync.Mutex
	workers map[string]*worker
	ctx     context.Context
	wg      sync.WaitGroup
	budgets map[string]map[string]operationBudget
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
	if opts.Companion == nil {
		opts.Companion = &companion.Controller{Registry: opts.Registry}
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
		// Keep inspection and unrelated controllers available while storage
		// recovers. No work is invented from an unavailable authoritative store.
		m.opts.Context.Logger.Error("discover clusters; preserving state and retrying", "error", err)
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
		var damaged *store.ClusterDataError
		if !errors.As(err, &damaged) {
			return err
		}
		m.opts.Context.Logger.Error("isolating unreadable clusters; restore verified control-plane state", "error", err)
	}
	for _, c := range clusters {
		m.Trigger(c.Ref())
	}
	return nil
}
func (m *Manager) work(ctx context.Context, ref domain.ClusterRef, w *worker) {
	name := ref.Key()
	defer m.wg.Done()
	publicationGate := make(chan struct{}, 1)
	ctx = withObservationPublication(ctx, publicationGate)
	availabilityCtx, stopAvailability := context.WithCancel(withObservationPublication(ctx, publicationGate))
	availabilityDone := make(chan struct{})
	go func() {
		defer close(availabilityDone)
		m.availabilityLoop(availabilityCtx, ref, w)
	}()
	defer func() {
		stopAvailability()
		<-availabilityDone
	}()
	timer := time.NewTimer(0)
	defer timer.Stop()
	observations := time.NewTicker(engine.ReadinessInterval)
	defer observations.Stop()
	backoff := m.opts.BackoffMin
	retrying := false
	// Identical attributed failures share one event until success or a changed
	// message, actor or operation; scheduled waits do not reset suppression.
	lastFailure := [3]string{}
	for {
		triggers := w.trigger
		if retrying {
			triggers = nil
		}
		select {
		case <-ctx.Done():
			return
		case <-observations.C:
			if retrying {
				if err := m.observeDuringBackoff(ctx, ref); err != nil && ctx.Err() == nil {
					m.opts.Context.Logger.Error("observe during reconcile backoff", "cluster", name, "error", err)
				}
			}
			continue
		case <-triggers:
		case <-timer.C:
		}
		w.reconciling.Store(true)
		delay, err := m.reconcile(ctx, ref)
		w.reconciling.Store(false)
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
			message := boundedMessage(redact.Diagnostic(err.Error()), 1024)
			event := domain.Event{Namespace: domain.NormalizedNamespace(ref.Namespace), Cluster: ref.Name, Type: "Warning", Reason: "ReconcileError", Message: message, Actor: "reconciler"}
			var cause *attributedError
			if errors.As(err, &cause) {
				event.Actor = cause.actor
				event.Operation = cause.operation
			}
			failureKey := [3]string{message, event.Actor, event.Operation}
			if failureKey != lastFailure {
				if eventErr := m.record(ctx, event); eventErr != nil {
					m.opts.Context.Logger.Error("record reconcile error", "error", eventErr)
				} else {
					lastFailure = failureKey
				}
			}
			delay = backoff
			if backoff < m.opts.BackoffMax/2 {
				backoff *= 2
			} else {
				backoff = m.opts.BackoffMax
			}
		} else {
			lastFailure = [3]string{}
			backoff = m.opts.BackoffMin
			if delay <= 0 {
				delay = m.opts.ResyncInterval
			}
		}
		if retryAt := infra.RetryAt(err); retryAt.After(m.opts.Context.Clock.Now()) {
			delay = max(delay, retryAt.Sub(m.opts.Context.Clock.Now()))
			// Discovery/user triggers cannot bypass a backend rate-limit reset.
			retrying = true
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

// observeDuringBackoff renews only availability, on the same serialized worker.
// Operation and platform retry deadlines remain untouched; a fenced native
// receipt must not suppress health sampling or cause an early mutation retry.
func (m *Manager) observeDuringBackoff(ctx context.Context, ref domain.ClusterRef) error {
	if err := holdObservation(ctx); err != nil {
		return err
	}
	defer releaseObservation(ctx)
	c, err := m.opts.Store.GetCluster(ctx, ref)
	if err != nil {
		return err
	}
	if c.Deleting || c.Status.Phase == domain.PhaseSuspended {
		return nil
	}
	ops, err := m.opts.Store.ListOperations(ctx, ref, domain.PhaseRunning)
	if err != nil {
		return err
	}
	restoring := c.Status.Phase == domain.PhaseRestoring
	for _, op := range ops {
		restoring = restoring || op.Kind == domain.OperationRestore
	}
	if restoring {
		restoreStatus(&c.Status, domain.PhaseRestoring, "Restoring", c.Status.Message, m.opts.Context.Clock.Now())
	}
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
	if err := m.resolve(ctx, &rc); err != nil {
		return err
	}
	status, _ := m.observeStatus(ctx, p, &rc)
	status.ObservedGeneration = c.Status.ObservedGeneration
	engine.ApplyDeploymentStatus(&rc, &status)
	preservePublishedEndpointFailure(c.Status.Conditions, &status)
	preserveMaintenance(c, &status)
	preserveAvailabilityLifecycle(c.Status, &status, rc.Clock.Now())
	preserveTransitions(c.Status.Conditions, status.Conditions)
	if restoring {
		restoreStatus(&status, domain.PhaseRestoring, "Restoring", c.Status.Message, rc.Clock.Now())
	}
	previous := c.Status.Phase
	c.Status = status
	if err := m.opts.Store.UpdateClusterState(ctx, c); err != nil {
		return err
	}
	return m.healthTransition(ctx, c, previous)
}
func preservePublishedEndpointFailure(previous []domain.Condition, status *domain.ClusterStatus) {
	for _, condition := range status.Conditions {
		if condition.Type == "PublishedEndpoint" {
			return
		}
	}
	for _, condition := range previous {
		if condition.Type == "PublishedEndpoint" && condition.Reason == "PublishedEndpointRepairExhausted" && condition.Status == "False" {
			status.Conditions = append(status.Conditions, condition)
			// Keep the historical diagnostic without hiding a newer failure.
			// Only fence an observation that would otherwise claim readiness.
			if status.Phase == domain.PhaseReady {
				for _, current := range status.Conditions {
					if current.Type == "Ready" && current.Status != "True" {
						return
					}
				}
				restoreStatus(status, domain.PhaseDegraded, condition.Reason, condition.Message, condition.LastTransitionTime)
			}
			return
		}
	}
}

func (m *Manager) record(ctx context.Context, event domain.Event) error {
	event.Message = boundedMessage(redact.Diagnostic(event.Message), 1024)
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

func phaseChangeMessage(c *domain.Cluster, previous domain.Phase) string {
	message := fmt.Sprintf("%s → %s", previous, c.Status.Phase)
	if c.Status.Message != "" {
		message += ": " + boundedMessage(redact.Diagnostic(c.Status.Message), 512)
	}
	var conditions strings.Builder
	for _, condition := range c.Status.Conditions {
		if condition.Status == "True" {
			continue
		}
		if conditions.Len() > 0 {
			conditions.WriteString("; ")
		}
		fmt.Fprintf(&conditions, "%s / %s / %s", boundedMessage(redact.Diagnostic(condition.Type), 64), boundedMessage(redact.Diagnostic(condition.Status), 64), boundedMessage(redact.Diagnostic(condition.Reason), 64))
		if conditions.Len() >= 384 {
			break
		}
	}
	if conditions.Len() > 0 {
		message += " [conditions: " + boundedMessage(conditions.String(), 384) + "]"
	}
	return boundedMessage(message, 1024)
}
func (m *Manager) state(ctx context.Context, c *domain.Cluster, previous domain.Phase) error {
	started := time.Now()
	defer m.logStage(c.Ref(), "state.publish", started)
	c.Status.Message = boundedMessage(redact.Diagnostic(c.Status.Message), 512)
	if err := m.opts.Store.UpdateClusterState(ctx, c); err != nil {
		return err
	}
	if previous != c.Status.Phase {
		if err := m.healthTransition(ctx, c, previous); err != nil {
			return err
		}
		return m.record(ctx, domain.Event{Namespace: c.Namespace, Cluster: c.Name, Type: "Normal", Reason: "PhaseChanged", Message: phaseChangeMessage(c, previous)})
	}
	return nil
}
func (m *Manager) reconcile(ctx context.Context, ref domain.ClusterRef) (delay time.Duration, err error) {
	started := time.Now()
	defer m.logStage(ref, "reconcile", started)
	if err := holdObservation(ctx); err != nil {
		return 0, err
	}
	defer releaseObservation(ctx)
	actor := "reconciler"
	operation := ""
	defer func() {
		if err != nil && !errors.Is(err, errClusterGone) {
			err = &attributedError{error: err, actor: actor, operation: operation}
		}
	}()
	c, err := m.opts.Store.GetCluster(ctx, ref)
	if errors.Is(err, store.ErrNotFound) {
		m.retainOperationBudgets(ref, nil)
		return 0, errClusterGone
	}
	if err != nil {
		return 0, err
	}
	ctx = store.WithClusterSnapshot(ctx, c)
	ops, lookupErr := m.opts.Store.ListOperations(ctx, c.Ref(), domain.PhasePending, domain.PhaseRunning)
	if lookupErr != nil {
		return 0, lookupErr
	}
	m.retainOperationBudgets(ref, ops)
	for i := range ops {
		op := &ops[i]
		if !domain.IsConvergenceOperation(op.Kind) || op.TargetGeneration != c.Generation {
			continue
		}
		if m.operationFailure(op, nil) {
			m.enrichInitialCreateFailure(ctx, c, op, nil)
			if err = m.opts.Store.UpdateOperation(ctx, op); err != nil {
				return 0, err
			}
			continue
		}
		operation = op.ID
		// Persist elapsed observations, but leave Pending work unstarted until
		// trackingOperations sees whether this sweep reached a maintenance gate.
		if err = m.opts.Store.UpdateOperation(ctx, op); err != nil {
			return 0, err
		}
	}
	var convergenceErr error
	var p engine.Provider
	convergenceAttempted := false
	defer func() {
		if !errors.Is(err, errClusterGone) {
			err = errors.Join(err, m.trackingOperations(ctx, p, c, convergenceErr, convergenceAttempted))
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
	p, err = m.opts.Registry.Get(c.Spec.Provider)
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
		// All providers must be able to observe owned runtime identity during
		// teardown without restoring a failed initial deployment first. This
		// does not grant exact-ID authorization for escalated removal; providers
		// may add their stronger persisted cleanup intent below.
		ctx = infra.WithCleanupIntent(ctx, infra.CleanupIntent{})
		if c.Spec.DataSource != nil {
			_ = m.observeInitialDataSourceLifecycle(ctx, p, c, nil)
			applied := false
			if observer, ok := p.(engine.InitialDataSourceObserver); ok {
				applied, _ = observer.InitialDataSourceApplied(c)
			}
			if !applied {
				markInitialDataSourceAborted(c, errors.New("target teardown requested before initial restore completion"))
			}
		}
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
		if err = m.opts.Store.StartClusterCleanup(ctx, c); err != nil {
			convergenceErr = err
			return 0, err
		}
		done, cleanupErr := p.Cleanup(ctx, &rc, c.DeleteOptions)
		companionDone, companionErr := m.opts.Companion.Cleanup(ctx, &rc)
		done = done && companionDone
		cleanupErr = errors.Join(cleanupErr, companionErr)
		cleanupErr = errors.Join(cleanupErr, engine.ObserveRuntimeStops(ctx, &rc, &c.Status, true))
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
			return 0, m.record(ctx, domain.Event{Namespace: c.Namespace, Cluster: c.Name, Type: "Normal", Reason: "PhaseChanged", Message: phaseChangeMessage(c, domain.PhaseDeleting)})
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
		// Backup-object cleanup does not resume the suspended runtime. Never
		// claim unrelated legacy work or perform ordinary Sync/Status here.
		op, claimErr := m.opts.Store.ClaimBackupDeletion(ctx, ref)
		if claimErr == nil {
			actor, operation = op.Actor, op.ID
			if actor == "" {
				actor = "reconciler"
			}
			return m.runOperation(ctx, p, &rc, op)
		}
		if !errors.Is(claimErr, store.ErrNotFound) {
			return 0, claimErr
		}
		return 0, nil
	}
	// Claim before resolving dependencies so namespace failures consume the
	// operation's bounded budget rather than leaving it Running indefinitely.
	// Resume active work before convergence: restore may deliberately have every
	// database process stopped, making ordinary Sync/Status unsafe or impossible.
	ctx = store.WithMaintenanceTime(ctx, rc.Clock.Now())
	op, err := m.opts.Store.ClaimRunningOperation(store.WithClusterSnapshot(ctx, c), ref)
	if err == nil {
		actor, operation = op.Actor, op.ID
		if actor == "" {
			actor = "reconciler"
		}
		if c.Spec.DataSource != nil {
			// Persist genuine-abort evidence before compensation, but never
			// use a failure observation to deny its existing Running owner.
			_ = m.observeInitialDataSourceLifecycle(ctx, p, c, op)
			_ = engine.ApplyInitialDataSourceStatus(p, c, &c.Status)
			if c.Status.InitialDataSource != nil {
				if err = m.opts.Store.UpdateClusterState(ctx, c); err != nil {
					return 0, err
				}
			}
		}
		return m.runOperation(ctx, p, &rc, op)
	}
	if !errors.Is(err, store.ErrNotFound) {
		return 0, err
	}
	// Once Running work has drained, upgrade completed native receipts before
	// interpreting historical tracker retirement or allowing ordinary Sync.
	// These observation gates must never preempt an active compensator.
	if c.Spec.DataSource != nil {
		if migrator, ok := p.(engine.InitialDataSourceMigrator); ok {
			if err = migrator.MigrateInitialDataSource(ctx, &rc); err != nil {
				return 0, err
			}
		}
		initialErr := m.initialDataSourceFailure(ctx, p, c)
		if errors.Is(initialErr, engine.ErrInvalid) {
			markInitialDataSourceAborted(c, initialErr)
			_ = engine.ApplyInitialDataSourceStatus(p, c, &c.Status)
			if err = m.opts.Store.UpdateClusterState(ctx, c); err != nil {
				return 0, err
			}
		} else if initialErr != nil {
			return 0, initialErr
		}
	}
	result, syncErr := m.synchronize(ctx, p, &rc, nil)
	convergenceErr, convergenceAttempted = syncErr, true
	if convergenceErr == nil {
		convergenceErr = result.ConvergenceError
	} else if result.ConvergenceError != nil {
		convergenceErr = errors.Join(convergenceErr, result.ConvergenceError)
	}
	if err = m.state(ctx, c, previous); err != nil {
		return 0, err
	}
	if policy, ok := p.(engine.OperationClaimPolicy); ok {
		allowed, err := policy.CanClaimOperation(c)
		if err != nil {
			return 0, err
		}
		if !allowed {
			return result.RequeueAfter, syncErr
		}
	}
	claimCtx := store.WithMaintenanceTime(store.WithClusterSnapshot(ctx, c), rc.Clock.Now())
	if syncErr != nil {
		// An old agent can make Sync fail while its replacement is the remedy.
		// Only agent rollout may bypass failed convergence; Running ownership
		// was checked above and provider/native safety gates still apply.
		op, err = m.opts.Store.ClaimAgentUpgrade(claimCtx, ref)
	} else {
		op, err = m.opts.Store.ClaimOperation(claimCtx, ref)
	}
	var windowWait *store.MaintenanceWindowWait
	if errors.As(err, &windowWait) {
		return maintenance.Delay(rc.Clock.Now(), windowWait.NextStart), nil
	}
	if errors.Is(err, store.ErrNotFound) {
		return result.RequeueAfter, syncErr
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
	started := time.Now()
	defer m.logStage(rc.Cluster.Ref(), "resolve", started)
	rc.Namespace = domain.NormalizedNamespace(rc.Cluster.Namespace)
	backend, err := m.opts.Namespaces.Backend(rc.Namespace)
	if err != nil {
		c := rc.Cluster
		// A post-Sync dependency failure may still carry the pre-Sync sample.
		// It is a new failure, not permission to republish cached positives
		// over an intervening renewal.
		hasAvailability := !c.Status.ObservedAt.IsZero() || c.Status.ObservationEpoch != ""
		for _, member := range c.Status.Members {
			hasAvailability = hasAvailability || member.Health || member.Agent.Healthy
		}
		if hasAvailability {
			c.Status = engine.UnknownObservation(c.Status, rc.Clock.Now(), "NamespaceUnavailable")
		}
		c.Status.ObservedAt = time.Time{}
		c.Status.ObservationEpoch = ""
		message := fmt.Sprintf("namespace %q unavailable: %v", rc.Namespace, err)
		// Dependency failure cannot release an in-progress restore's lifecycle
		// fence, including during read-only error-backoff observations.
		if c.Status.Phase != domain.PhaseSuspended && c.Status.Phase != domain.PhaseRestoring && !(c.Spec.DataSource != nil && c.Status.Phase == domain.PhaseFailed) {
			c.Status.Phase = domain.PhaseDegraded
		}
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

func (m *Manager) synchronize(ctx context.Context, p engine.Provider, rc *engine.Context, operation *domain.Operation) (engine.Result, error) {
	if err := m.resolve(ctx, rc); err != nil {
		return engine.Result{}, err
	}
	initialFailure := m.initialDataSourceFailure(ctx, p, rc.Cluster)
	if errors.Is(initialFailure, engine.ErrInvalid) {
		markInitialDataSourceAborted(rc.Cluster, initialFailure)
	}
	recoveringInitial := false
	if initialFailure != nil {
		if policy, ok := p.(engine.InitialDataSourceRecoveryPolicy); ok {
			var recoveryErr error
			recoveringInitial, recoveryErr = policy.InitialDataSourceRecoveryPending(rc.Cluster)
			initialFailure = errors.Join(initialFailure, recoveryErr)
		}
	}
	for _, condition := range rc.Cluster.Status.Conditions {
		if condition.Type == "PlatformDeploymentWaiting" {
			rc.DeploymentWait = condition.Message
			rc.DeploymentWaitReason, rc.DeploymentWaitKey = condition.Reason, condition.Key
			break
		}
	}
	// Publish health before any recovery/platform work can block. This runs on
	// the existing per-cluster worker/operation lease, not a second observer.
	if canObserveAvailability(rc.Cluster) {
		before := rc.Cluster.Status
		before.Conditions = slices.Clone(before.Conditions)
		status, _ := m.observeStatus(ctx, p, rc)
		status.ObservedGeneration = rc.Cluster.Status.ObservedGeneration
		engine.ApplyDeploymentStatus(rc, &status)
		preservePublishedEndpointFailure(rc.Cluster.Status.Conditions, &status)
		preserveMaintenance(rc.Cluster, &status)
		preserveAvailabilityLifecycle(before, &status, rc.Clock.Now())
		preserveTransitions(rc.Cluster.Status.Conditions, status.Conditions)
		rc.Cluster.Status = status
		// This is a distinct completed health sample, not the final Sync
		// checkpoint. Publish successful retries too, so recovery cannot hide a
		// freshly renewed lease behind long-running work.
		var saveErr error
		started := time.Now()
		if operation == nil {
			saveErr = m.opts.Store.UpdateClusterState(ctx, rc.Cluster)
		} else {
			// Opt-in imperative provisioning has already advanced its native
			// checkpoint; never publish its cluster state without that receipt.
			saveErr = m.opts.Store.UpdateOperationAndClusterState(ctx, operation, rc.Cluster)
		}
		m.logStage(rc.Cluster.Ref(), "availability.pre-sync.publish", started)
		if saveErr != nil {
			// The caller still publishes this snapshot on error. Retain the
			// gate through that write and its deferred release.
			return engine.Result{}, saveErr
		}
		// Release only after this complete sample is durably published. Sync
		// remains outside the gate so renewal can run during slow convergence.
		releaseObservation(ctx)
	}
	observedGeneration := rc.Cluster.Status.ObservedGeneration
	rc.DeploymentWait = ""
	rc.DeploymentWaitReason, rc.DeploymentWaitKey = "", ""
	rc.SyncWait = ""
	exposureStarted := time.Now()
	exposureCondition, exposureErr := engine.CheckExposureSupport(ctx, rc.Backend, rc.Cluster.Spec.Exposure)
	m.logStage(rc.Cluster.Ref(), "exposure.check", exposureStarted)
	var result engine.Result
	syncErr := errors.Join(exposureErr, initialFailure)
	if recoveringInitial {
		// Failed/non-Ready is an observation, not revocation of an existing
		// frozen recovery journal. No new native work is admitted here.
		syncErr = nil
	}
	if syncErr == nil && !recoveringInitial && rc.Cluster.Status.ObservedGeneration == 0 && !engine.InitialDataSourceUnproven(rc.Cluster) {
		if d := rc.Cluster.Spec.DataSource; d != nil && d.Backup != "" {
			source := domain.ClusterRef{Namespace: d.Namespace, Name: d.Cluster}
			if source.Namespace == "" {
				source.Namespace = rc.Cluster.Namespace
			}
			if source.Name == "" {
				source.Name = rc.Cluster.Name
			}
			_, syncErr = rc.Store.GetBackup(ctx, source, d.Backup)
			if errors.Is(syncErr, store.ErrNotFound) || errors.Is(syncErr, engine.ErrNotFound) {
				syncErr = engine.InvalidField("source.backup", domain.ReasonNotFound, "Source backup %s/%s is no longer available; select another successful backup and create a new target cluster", source.Name, d.Backup)
			}
		}
	}
	if syncErr == nil {
		started := time.Now()
		result, syncErr = p.Sync(ctx, rc)
		m.logStage(rc.Cluster.Ref(), "provider.sync", started)
	}
	if _, supportsDataMcp := p.(engine.DataMcpProvider); syncErr == nil && !recoveringInitial && supportsDataMcp {
		companionResult, companionErr := m.opts.Companion.Sync(ctx, rc)
		syncErr = companionErr
		if companionResult.RequeueAfter > 0 && (result.RequeueAfter == 0 || companionResult.RequeueAfter < result.RequeueAfter) {
			result.RequeueAfter = companionResult.RequeueAfter
		}
		result.ConvergenceError = errors.Join(result.ConvergenceError, companionResult.ConvergenceError)
	}
	if recoveringInitial {
		syncErr = errors.Join(syncErr, initialFailure, exposureErr)
	}
	if errors.Is(syncErr, infra.ErrPublicIPTypeUnsupported) {
		exposureCondition = domain.Condition{Type: "ExposureSupported", Status: "False", Reason: "PublicIPTypeUnsupported",
			Message: "Backend refused spec.exposure.ipType; select an available IP family, provision that public address, or disable public exposure"}
		exposureErr = fmt.Errorf("%w: %s", engine.ErrInvalid, exposureCondition.Message)
		syncErr = errors.Join(syncErr, exposureErr)
	}
	var portConflict *infra.PublicPortConflictError
	if errors.As(syncErr, &portConflict) {
		exposureCondition = domain.Condition{Type: "ExposureSupported", Status: "False", Reason: "PublicPortInUse", Message: portConflict.Error()}
		exposureErr = fmt.Errorf("%w: %s", engine.ErrInvalid, exposureCondition.Message)
		syncErr = errors.Join(syncErr, exposureErr)
	}
	var publishFailure *infra.PublicPortPublishError
	if errors.As(syncErr, &publishFailure) {
		exposureCondition = domain.Condition{Type: "ExposureSupported", Status: "False", Reason: "PublicPortPublishFailed", Message: publishFailure.Error()}
		exposureErr = fmt.Errorf("%w: %s", engine.ErrInvalid, exposureCondition.Message)
		syncErr = errors.Join(syncErr, exposureErr)
	}
	var endpointExhausted *infra.EndpointRepairExhaustedError
	if errors.As(syncErr, &endpointExhausted) {
		exposureCondition = domain.Condition{Type: "PublishedEndpoint", Status: "False", Reason: "PublishedEndpointRepairExhausted", Message: endpointExhausted.Error()}
		exposureErr = endpointExhausted
	}
	// A successful Sync may record the generation it applied while still
	// requesting periodic requeues; that observation only moves forward.
	if applied := rc.Cluster.Status.ObservedGeneration; syncErr == nil && result.ConvergenceError == nil && applied > observedGeneration && applied <= rc.Cluster.Generation {
		observedGeneration = applied
	}
	if err := holdObservation(ctx); err != nil {
		return result, errors.Join(syncErr, err)
	}
	if err := m.resolve(ctx, rc); err != nil {
		return result, errors.Join(syncErr, err)
	}
	status, statusErr := m.observeStatus(ctx, p, rc)
	engine.ApplyDeploymentStatus(rc, &status)
	preserveMaintenance(rc.Cluster, &status)
	if exposureCondition.Type != "" {
		status.Conditions = append(status.Conditions, exposureCondition)
	}
	if exposureErr != nil {
		status.Phase, status.Message = domain.PhaseDegraded, exposureCondition.Message
		readyFound := false
		for i := range status.Conditions {
			if status.Conditions[i].Type == "Ready" {
				readyFound = true
				status.Conditions[i].Status = "False"
				status.Conditions[i].Reason = exposureCondition.Reason
				status.Conditions[i].Message = exposureCondition.Message
			}
		}
		if !readyFound {
			status.Conditions = append(status.Conditions, domain.Condition{Type: "Ready", Status: "False", Reason: exposureCondition.Reason, Message: exposureCondition.Message})
		}
	}
	// Provider health cannot clear a reconciliation-owned exhaustion fence.
	// Only a successful Sync and fresh Ready observation establish recovery.
	if syncErr != nil || statusErr != nil || result.ConvergenceError != nil || rc.SyncWait != "" || status.Phase != domain.PhaseReady {
		preservePublishedEndpointFailure(rc.Cluster.Status.Conditions, &status)
	}
	// A nonempty phase marks an independently useful partial observation.
	// Preserve fresh safety conditions even when a secondary observation fails;
	// a zero snapshot with an error must not erase the last trustworthy state.
	if statusErr == nil || status.Phase != "" {
		if statusErr != nil && status.Phase == domain.PhaseReady {
			status.Phase = domain.PhaseDegraded
		}
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
		if syncErr == nil && statusErr == nil && result.ConvergenceError == nil && result.RequeueAfter <= 0 {
			rc.Cluster.Status.ObservedGeneration = rc.Cluster.Generation
		}
	}
	if canObserveAvailability(rc.Cluster) && (result.RequeueAfter <= 0 || result.RequeueAfter > engine.ReadinessInterval) {
		result.RequeueAfter = engine.ReadinessInterval
	}
	if len(rc.Cluster.Status.DeploymentFailures) > 0 && !rc.Cluster.Deleting {
		for _, f := range rc.Cluster.Status.DeploymentFailures {
			if rc.Cluster.Spec.RetryEpoch <= f.RetryEpoch {
				syncErr = errors.Join(syncErr, fmt.Errorf("%s: %w", f.Message, infra.ErrDeploymentRetriesExhausted))
				break
			}
		}
	}
	if syncErr == nil && rc.Cluster.Status.ObservedGeneration < rc.Cluster.Generation && (rc.SyncWait != "" || rc.Cluster.Status.Message == "") {
		reason := rc.SyncWait
		if reason == "" {
			switch {
			case result.ConvergenceError != nil:
				reason = "provider reported a convergence failure"
			case result.RequeueAfter > 0:
				reason = "provider requested another convergence pass without acknowledging the generation"
			case statusErr != nil:
				reason = "status observation failed before generation acknowledgement"
			default:
				reason = "waiting for a usable status observation"
			}
		}
		rc.Cluster.Status.Message = boundedMessage(redact.Diagnostic(fmt.Sprintf("%s: waiting for generation %d: %s", rc.Cluster.Status.Phase, rc.Cluster.Generation, reason)), 512)
	}
	failure := errors.Join(syncErr, statusErr, result.ConvergenceError)
	if failure != nil && scheduledPlatformWait(failure) == nil {
		rc.Cluster.Status.Message = boundedMessage(redact.Diagnostic(fmt.Sprintf("%s: reconcile failed: %s", rc.Cluster.Status.Phase, failure)), 512)
	}
	return result, errors.Join(syncErr, statusErr)
}

func (m *Manager) runOperation(ctx context.Context, p engine.Provider, rc *engine.Context, op *domain.Operation) (requeue time.Duration, attemptErr error) {
	rc.Operation = op
	defer func() { rc.Operation = nil }()
	actor := op.Actor
	if actor == "" {
		actor = "reconciler"
	}
	ctx = store.WithActor(ctx, actor)
	ctx = context.WithValue(ctx, operationEventKey{}, op.ID)
	previousCheckpoint := op.Checkpoint
	invalidator, invalidates := p.(engine.OperationObservationInvalidator)
	defer func() {
		if !invalidates || attemptErr == nil {
			return
		}
		before := op.Checkpoint
		if err := invalidator.InvalidateOperationObservation(op); err != nil {
			attemptErr = errors.Join(attemptErr, err)
			return
		}
		if !bytes.Equal(before, op.Checkpoint) {
			// Even a later status/event/persistence failure must not carry a
			// successful poll's window into the next attempt or CP restart.
			persistCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			defer cancel()
			attemptErr = errors.Join(attemptErr, m.opts.Store.UpdateOperation(persistCtx, op))
		}
	}()
	if invalidates {
		if err := invalidator.InvalidateOperationObservation(op); err != nil {
			return 0, err
		}
		if !bytes.Equal(previousCheckpoint, op.Checkpoint) {
			// Fail closed across worker/CP crashes and failures before provider
			// entry. The previous proof is available only to this fresh attempt.
			if err := m.opts.Store.UpdateOperation(ctx, op); err != nil {
				return 0, err
			}
		}
	}
	c := rc.Cluster
	previous := c.Status.Phase
	callCtx := ctx
	var opResult engine.OpResult
	var opErr error
	var restoreTransition time.Time
	if op.Kind == domain.OperationRestore {
		restoreStatus(&c.Status, domain.PhaseRestoring, "Restoring", "Restoring data; normal database traffic may be unavailable", rc.Clock.Now())
		for _, condition := range c.Status.Conditions {
			if condition.Type == "Ready" {
				restoreTransition = condition.LastTransitionTime
			}
		}
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
		remaining := m.opts.FailureCleanupTimeout - m.elapsedOperationTime(op, true)
		cleanupExpired = remaining <= 0
		if cleanupExpired {
			terminal = true
			op.Message = fmt.Sprintf("%s; recovery exceeded its %s cleanup deadline at compensation step %q; manual recovery is required", op.LastError, m.opts.FailureCleanupTimeout, compensationStep(op))
		} else {
			var cancel context.CancelFunc
			callCtx, cancel = context.WithTimeout(ctx, remaining)
			defer cancel()
		}
	}
	// Compensation has its own window, not the exhausted work deadline.
	if !terminal && !failing {
		var cancel context.CancelFunc
		callCtx, cancel = context.WithTimeout(ctx, m.operationTimeout(op)-op.Elapsed)
		defer cancel()
	}
	dependenciesResolved := false
	preparationStopped := false
	if failing && !terminal {
		if preparer, ok := p.(operationFailureCleanupPreparer); ok {
			opResult, opErr = preparer.PrepareOperationFailureCleanup(callCtx, rc, op)
			if opResult.Checkpoint != nil {
				op.Checkpoint = opResult.Checkpoint
			}
			preparationStopped = opResult.Done || opErr != nil
		}
	}
	if terminal {
		opResult.Done = true
		opErr = errors.New(op.Message)
	} else if preparationStopped {
		// Preflight owns this attempt, including terminal operator diagnostics.
	} else if err := m.resolve(callCtx, rc); err != nil {
		opErr = err
	} else if failing || op.CancelRequested {
		dependenciesResolved = true
		if canceller, ok := p.(engine.OperationCanceller); ok {
			if invalidates {
				op.Checkpoint = previousCheckpoint
			}
			opResult, opErr = canceller.CancelOperation(callCtx, rc, op)
			if opResult.Done && opErr == nil && !failing {
				op.Phase = domain.PhaseCancelled
			}
		} else {
			opErr = errors.New("Provider no longer supports cancellation of this operation")
		}
	} else {
		dependenciesResolved = true
		opResult, opErr = m.validateRestoreProvider(callCtx, rc, op)
		if opErr == nil {
			if invalidates {
				op.Checkpoint = previousCheckpoint
			}
			opResult, opErr = p.RunOperation(callCtx, rc, op)
		}
	}

	if opResult.Checkpoint != nil {
		op.Checkpoint = opResult.Checkpoint
	}
	if invalidates && (opErr != nil || opResult.Checkpoint == nil) {
		if err := invalidator.InvalidateOperationObservation(op); err != nil {
			opErr = errors.Join(opErr, err)
		}
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
	m.maintenanceBudget(op, c, rc.WaitingForMaintenance(op))
	if failing && !cleanupExpired {
		// Cleanup errors retain the checkpoint and retry with worker backoff,
		// including errors that would be permanent during ordinary work.
		opResult.Done = opResult.Done && opErr == nil
		if opResult.Done && opResult.Recovered && op.Kind == domain.OperationBackup {
			op.LastError = ""
		} else if opResult.Done {
			if op.Kind == domain.OperationBackup && opResult.Message != "" {
				op.LastError = opResult.Message
			}
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
	synchronized := false
	restoreMessage := c.Status.Message
	if policy, ok := p.(engine.OperationSyncPolicy); ok && previous != domain.PhaseSuspended && op.Step != failureCleanupStep && !opResult.Done && opErr == nil && policy.SyncDuringOperation(op) {
		result, syncErr = m.synchronize(ctx, p, rc, op)
		synchronized = true
	}
	// Imperative work owns convergence, not observation. Every provider must
	// continue publishing bounded availability samples while an operation is
	// polling (including on operation errors); ordinary Sync remains opt-in.
	// Keep lifecycle progress authoritative even if a native health probe says
	// Ready during a restore/restart/initialization checkpoint.
	if !synchronized && !opResult.Done && previous != domain.PhaseSuspended && previous != domain.PhaseDeleting {
		phase, message := c.Status.Phase, c.Status.Message
		status := engine.UnknownObservation(c.Status, rc.Clock.Now(), "NamespaceUnavailable")
		status.ObservedAt = time.Time{}
		status.ObservationEpoch = ""
		status.Message = message
		if dependenciesResolved {
			availability := *rc
			availability.AvailabilityOnly = true
			status, _ = m.observeStatus(ctx, p, &availability)
		}
		if !dependenciesResolved && status.DataMcp != nil {
			snapshot := *status.DataMcp
			snapshot.Phase, snapshot.Message = domain.DataMcpPhaseDegraded, "Namespace unavailable"
			status.DataMcp = &snapshot
		}
		status.ObservedGeneration = c.Status.ObservedGeneration
		engine.ApplyDeploymentStatus(rc, &status)
		preserveMaintenance(c, &status)
		preserveTransitions(c.Status.Conditions, status.Conditions)
		if phase != domain.PhaseReady && phase != domain.PhaseDegraded {
			status.Phase, status.Message = phase, message
		}
		c.Status = status
	}
	if op.Kind == domain.OperationRestore {
		// Availability probes and operation-specific Sync may see healthy old
		// processes. Neither can certify a destructive restore as ready.
		if !opResult.Done {
			restoreStatus(&c.Status, domain.PhaseRestoring, "Restoring", restoreMessage, restoreTransition)
		} else {
			restoreStatus(&c.Status, c.Status.Phase, "RestoreFinished", c.Status.Message, rc.Clock.Now())
		}
	}
	_ = m.observeInitialDataSourceLifecycle(ctx, p, c, op)
	if c.Spec.DataSource != nil && op.Kind == domain.OperationRestore && (op.Phase == domain.PhaseFailed || op.Phase == domain.PhaseCancelled) {
		applied := false
		if observer, ok := p.(engine.InitialDataSourceObserver); ok {
			applied, _ = observer.InitialDataSourceApplied(c)
		}
		if !applied {
			markInitialDataSourceAborted(c, fmt.Errorf("restore operation %s ended %s: %s", op.ID, op.Phase, op.Message))
		}
	}
	// A failed initial restore is still failed while its Running operation
	// repairs process/maintenance/router holds. Observation errors gate Ready,
	// not execution of the already-owned compensating lifecycle.
	_ = engine.ApplyInitialDataSourceStatus(p, c, &c.Status)
	// Persist even on provider error, atomically with the state referenced by
	// the checkpoint, so a completed external step is never forgotten.
	if previous == domain.PhaseSuspended && op.Kind == domain.OperationDeleteBackup {
		c.Status.Phase = domain.PhaseSuspended
	}
	c.Status.Message = boundedMessage(redact.Diagnostic(c.Status.Message), 512)
	op.Message = boundedMessage(redact.Diagnostic(op.Message), 1024)
	op.LastError = boundedMessage(redact.Diagnostic(op.LastError), 1024)
	if err := m.opts.Store.UpdateOperationAndClusterState(ctx, op, c); err != nil {
		return 0, err
	}
	if previous != c.Status.Phase {
		if err := m.healthTransition(ctx, c, previous); err != nil {
			return 0, err
		}
		if err := m.record(ctx, domain.Event{Namespace: c.Namespace, Cluster: c.Name, Operation: op.ID, Type: "Normal", Reason: "PhaseChanged", Message: phaseChangeMessage(c, previous)}); err != nil {
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
	if delay > engine.ReadinessInterval {
		delay = engine.ReadinessInterval
	}
	return delay, nil
}

// restoreStatus keeps lifecycle phase and the API readiness condition atomic.
// Completion stays non-ready until ordinary reconciliation observes health.
func restoreStatus(status *domain.ClusterStatus, phase domain.Phase, reason, message string, now time.Time) {
	status.Phase, status.Message = phase, message
	ready := domain.Condition{Type: "Ready", Status: "False", Reason: reason, Message: message, LastTransitionTime: now}
	for i, condition := range status.Conditions {
		if condition.Type == "Ready" {
			if condition.Status == ready.Status && condition.Reason == ready.Reason {
				ready.LastTransitionTime = condition.LastTransitionTime
			}
			status.Conditions[i] = ready
			return
		}
	}
	status.Conditions = append(status.Conditions, ready)
}

// Reject incompatible sources before any engine can mutate the target. This
// also covers restores queued before admission checked backup provenance.
func (m *Manager) validateRestoreProvider(ctx context.Context, rc *engine.Context, op *domain.Operation) (engine.OpResult, error) {
	if op.Kind != domain.OperationRestore {
		return engine.OpResult{}, nil
	}
	var payload struct {
		Restore string `json:"restore"`
	}
	if err := json.Unmarshal(op.Payload, &payload); err != nil {
		return engine.OpResult{}, err
	}
	restore, err := rc.Store.GetRestore(ctx, rc.Cluster.Ref(), payload.Restore)
	if err != nil {
		return engine.OpResult{}, err
	}
	if restore.Source.Backup == "" {
		return engine.OpResult{}, nil
	}
	source := domain.ClusterRef{Namespace: restore.SourceNamespace, Name: restore.Source.SourceCluster}
	if source.Namespace == "" {
		source.Namespace = rc.Cluster.Namespace
	}
	if source.Name == "" {
		source.Name = rc.Cluster.Name
	}
	backup, err := rc.Store.GetBackup(ctx, source, restore.Source.Backup)
	if errors.Is(err, store.ErrNotFound) || errors.Is(err, engine.ErrNotFound) {
		return engine.OpResult{}, engine.InvalidField("source.backup", domain.ReasonNotFound, "Source backup %s/%s is no longer available; select another successful backup and retry the restore", source.Name, restore.Source.Backup)
	}
	if err != nil {
		return engine.OpResult{}, err
	}
	if backup.ProviderMatches(rc.Cluster.Spec.Provider) {
		return engine.OpResult{}, nil
	}
	err = engine.InvalidField("source.backup", domain.ReasonConflict, "backup provider does not match target provider")
	now := rc.Clock.Now()
	restore.Phase, restore.Error, restore.CompletedAt = domain.PhaseFailed, err.Error(), &now
	if updateErr := rc.Store.UpdateRestore(ctx, restore); updateErr != nil {
		return engine.OpResult{}, updateErr
	}
	return engine.OpResult{Done: true}, err
}

// scheduledPlatformWait recognizes only error trees whose leaves are scheduled
// waits. errors.As alone also matches a wait joined with a genuine observation
// failure, which must remain visible and use ordinary error backoff.
func scheduledPlatformWait(err error) *engine.PlatformRetryWait {
	switch e := err.(type) {
	case *infra.RetryAfterError:
		wait := scheduledPlatformWait(e.Cause)
		if wait != nil {
			// Preserve the outer platform constraint without mutating an error
			// instance shared by provider state or joined error branches.
			at := wait.RetryAt
			if e.At.After(at) {
				at = e.At
			}
			return &engine.PlatformRetryWait{RetryAt: at, Message: e.Error()}
		}
		return nil
	case *infra.DeploymentExhaustedError:
		return &engine.PlatformRetryWait{RetryAt: time.Now().Add(engine.ReadinessInterval), Message: e.Error()}
	case interface{ DeploymentWaiting() bool }:
		if e.DeploymentWaiting() {
			at := infra.RetryAt(err)
			if at.IsZero() {
				at = time.Now().Add(15 * time.Second)
			}
			return &engine.PlatformRetryWait{RetryAt: at, Message: infra.SafeDetail(err)}
		}
		return nil
	case *engine.PlatformRetryWait:
		if at := infra.RetryAt(e); at.After(e.RetryAt) {
			return &engine.PlatformRetryWait{RetryAt: at, Message: e.Message, Cause: e}
		}
		return e
	case interface{ Unwrap() []error }:
		var latest *engine.PlatformRetryWait
		for _, child := range e.Unwrap() {
			if child == nil {
				continue
			}
			wait := scheduledPlatformWait(child)
			if wait == nil {
				return nil
			}
			if latest == nil || wait.RetryAt.After(latest.RetryAt) {
				latest = wait
			}
		}
		return latest
	case interface{ Unwrap() error }:
		return scheduledPlatformWait(e.Unwrap())
	default:
		if err == infra.ErrDeploymentRetriesExhausted {
			return &engine.PlatformRetryWait{RetryAt: time.Now().Add(engine.ReadinessInterval), Message: err.Error()}
		}
		return nil
	}
}

func (m *Manager) healthTransition(ctx context.Context, c *domain.Cluster, previous domain.Phase) error {
	unhealthy := func(phase domain.Phase) bool { return phase == domain.PhaseDegraded || phase == domain.PhaseError }
	if unhealthy(previous) == unhealthy(c.Status.Phase) {
		return nil
	}
	kind, reason := "Warning", "ClusterDegraded"
	if !unhealthy(c.Status.Phase) {
		kind, reason = "Normal", "ClusterRecovered"
	}
	return m.record(ctx, domain.Event{Namespace: c.Namespace, Cluster: c.Name, Type: kind, Reason: reason, Message: fmt.Sprintf("%s → %s", previous, c.Status.Phase)})
}
