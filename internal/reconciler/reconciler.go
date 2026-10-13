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

// Options configures serialized reconciliation and retry delays.
type Options struct {
	Store          *store.Store
	Registry       *engine.Registry
	Context        engine.Context
	ResyncInterval time.Duration
	BackoffMin     time.Duration
	BackoffMax     time.Duration
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
	return &Manager{opts: opts, workers: make(map[string]*worker)}, nil
}

// Trigger coalesces reconciliation requests for a cluster without blocking.
func (m *Manager) Trigger(cluster string) {
	m.mu.Lock()
	w := m.workers[cluster]
	if w == nil {
		w = &worker{trigger: make(chan struct{}, 1)}
		m.workers[cluster] = w
	}
	if m.ctx != nil && !w.started && m.ctx.Err() == nil {
		w.started = true
		m.wg.Add(1)
		go m.work(m.ctx, cluster, w)
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
		go m.work(runCtx, name, w)
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
	clusters, err := m.opts.Store.ListClusters(ctx)
	if err != nil {
		return err
	}
	for _, c := range clusters {
		m.Trigger(c.Name)
	}
	return nil
}
func (m *Manager) work(ctx context.Context, name string, w *worker) {
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
		delay, err := m.reconcile(ctx, name)
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
			if eventErr := m.record(ctx, domain.Event{Cluster: name, Type: "Warning", Reason: "ReconcileError", Message: err.Error()}); eventErr != nil {
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
	if m.opts.Context.Events != nil {
		return m.opts.Context.Events.Record(ctx, event)
	}
	return m.opts.Store.AppendEvent(ctx, &event)
}
func (m *Manager) state(ctx context.Context, c *domain.Cluster, previous domain.Phase) error {
	if err := m.opts.Store.UpdateClusterState(ctx, c); err != nil {
		return err
	}
	if previous != c.Status.Phase {
		return m.record(ctx, domain.Event{Cluster: c.Name, Type: "Normal", Reason: "PhaseChanged", Message: fmt.Sprintf("%s → %s", previous, c.Status.Phase)})
	}
	return nil
}
func (m *Manager) reconcile(ctx context.Context, name string) (time.Duration, error) {
	c, err := m.opts.Store.GetCluster(ctx, name)
	if errors.Is(err, store.ErrNotFound) {
		return 0, errClusterGone
	}
	if err != nil {
		return 0, err
	}
	p, err := m.opts.Registry.Get(c.Spec.Provider)
	if err != nil {
		return 0, err
	}
	rc := m.opts.Context
	rc.Cluster = c
	previous := c.Status.Phase
	if c.Deleting {
		c.Status.Phase = domain.PhaseDeleting
		if err = m.state(ctx, c, previous); err != nil {
			return 0, err
		}
		done, cleanupErr := p.Cleanup(ctx, &rc, c.DeleteOptions)
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
			return 0, m.record(ctx, domain.Event{Cluster: name, Type: "Normal", Reason: "PhaseChanged", Message: "Deleting → Suspended"})
		}
		if err = m.record(ctx, domain.Event{Cluster: name, Type: "Normal", Reason: "Deleted", Message: "Cluster deleted"}); err != nil {
			return 0, err
		}
		if err = m.opts.Store.DeleteCluster(ctx, name); err != nil {
			return 0, err
		}
		return 0, errClusterGone
	}
	if c.Status.Phase == domain.PhaseSuspended {
		return 0, nil
	}
	// Resume active work before convergence: restore may deliberately have every
	// database process stopped, making ordinary Sync/Status unsafe or impossible.
	op, err := m.opts.Store.ClaimRunningOperation(ctx, name)
	if err == nil {
		return m.runOperation(ctx, p, &rc, op)
	}
	if !errors.Is(err, store.ErrNotFound) {
		return 0, err
	}
	result, syncErr := synchronize(ctx, p, &rc)
	if err = m.state(ctx, c, previous); err != nil {
		return 0, err
	}
	if syncErr != nil {
		return 0, syncErr
	}
	op, err = m.opts.Store.ClaimOperation(ctx, name)
	if errors.Is(err, store.ErrNotFound) {
		return result.RequeueAfter, nil
	}
	if err != nil {
		return 0, err
	}
	return m.runOperation(ctx, p, &rc, op)
}

func synchronize(ctx context.Context, p engine.Provider, rc *engine.Context) (engine.Result, error) {
	result, syncErr := p.Sync(ctx, rc)
	status, statusErr := p.Status(ctx, rc)
	if statusErr == nil {
		rc.Cluster.Status = status
		if syncErr == nil && result.RequeueAfter <= 0 {
			rc.Cluster.Status.ObservedGeneration = rc.Cluster.Generation
		}
	}
	return result, errors.Join(syncErr, statusErr)
}

func (m *Manager) runOperation(ctx context.Context, p engine.Provider, rc *engine.Context, op *domain.Operation) (time.Duration, error) {
	c := rc.Cluster
	previous := c.Status.Phase
	opResult, opErr := p.RunOperation(ctx, rc, op)
	if opResult.Checkpoint != nil {
		op.Checkpoint = opResult.Checkpoint
	}
	if opResult.Step != "" {
		op.Step = opResult.Step
	}
	op.Message = opResult.Message
	if opErr != nil {
		op.Message = opErr.Error()
	}
	if opResult.Done {
		op.Phase = domain.PhaseSucceeded
		if opErr != nil {
			op.Phase = domain.PhaseFailed
		}
		now := rc.Clock.Now()
		op.CompletedAt = &now
	}
	// Provisioning needed by an operation is opt-in and follows RunOperation,
	// using its new checkpoint rather than unrelated ordinary convergence.
	var result engine.Result
	var syncErr error
	if policy, ok := p.(engine.OperationSyncPolicy); ok && !opResult.Done && opErr == nil && policy.SyncDuringOperation(op) {
		result, syncErr = synchronize(ctx, p, rc)
	}
	// Persist even on provider error, atomically with the state referenced by
	// the checkpoint, so a completed external step is never forgotten.
	if err := m.opts.Store.UpdateOperationAndClusterState(ctx, op, c); err != nil {
		return 0, err
	}
	if previous != c.Status.Phase {
		if err := m.record(ctx, domain.Event{Cluster: c.Name, Type: "Normal", Reason: "PhaseChanged", Message: fmt.Sprintf("%s → %s", previous, c.Status.Phase)}); err != nil {
			return 0, err
		}
	}
	if opResult.Done {
		event := domain.Event{Cluster: c.Name, Operation: op.ID, Type: "Normal", Reason: "OperationSucceeded", Message: op.Kind + " completed"}
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
		if err := m.record(ctx, domain.Event{Cluster: c.Name, Operation: op.ID, Type: "Warning", Reason: "OperationError", Message: opErr.Error()}); err != nil {
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
