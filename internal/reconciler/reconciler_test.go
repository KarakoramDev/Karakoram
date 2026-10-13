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
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

type fakeProvider struct {
	mu      sync.Mutex
	calls   map[string]int
	active  map[string]bool
	overlap bool
	cleanup int
	resume  chan string
	fail    bool
}

func (*fakeProvider) Name() string                                                     { return "fake" }
func (*fakeProvider) Describe() domain.ProviderInfo                                    { return domain.ProviderInfo{Name: "fake"} }
func (*fakeProvider) ApplyDefaults(*domain.ClusterSpec)                                {}
func (*fakeProvider) Validate(context.Context, *domain.Cluster, *domain.Cluster) error { return nil }
func (p *fakeProvider) Sync(ctx context.Context, rc *engine.Context) (engine.Result, error) {
	p.mu.Lock()
	if p.active[rc.Cluster.Name] {
		p.overlap = true
	}
	p.active[rc.Cluster.Name] = true
	p.calls[rc.Cluster.Name]++
	p.mu.Unlock()
	time.Sleep(time.Millisecond)
	p.mu.Lock()
	p.active[rc.Cluster.Name] = false
	fail := p.fail
	p.fail = false
	p.mu.Unlock()
	rc.Cluster.Internal = json.RawMessage(`{"synced":true}`)
	if fail {
		return engine.Result{}, errors.New("temporary backend error")
	}
	return engine.Result{}, nil
}
func (*fakeProvider) Status(context.Context, *engine.Context) (domain.ClusterStatus, error) {
	return domain.ClusterStatus{Phase: domain.PhaseReady}, nil
}
func (p *fakeProvider) RunOperation(_ context.Context, _ *engine.Context, op *domain.Operation) (engine.OpResult, error) {
	if len(op.Checkpoint) == 0 {
		return engine.OpResult{Checkpoint: json.RawMessage(`{"step":1}`), Step: "first", RequeueAfter: time.Millisecond}, nil
	}
	if p.resume != nil {
		select {
		case p.resume <- string(op.Checkpoint):
		default:
		}
	}
	return engine.OpResult{Done: true, Step: "complete"}, nil
}
func (p *fakeProvider) Cleanup(context.Context, *engine.Context, domain.DeleteOptions) (bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.cleanup++
	return p.cleanup >= 2, nil
}
func newTestManager(t *testing.T, p engine.Provider) (*Manager, *store.Store) {
	t.Helper()
	key, _ := secrets.New(strings.Repeat("k", 32))
	s, err := store.Open(filepath.Join(t.TempDir(), "reconcile.db"), key)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	registry, _ := engine.NewRegistry(p)
	m, err := New(Options{Store: s, Registry: registry, ResyncInterval: 20 * time.Millisecond, BackoffMin: time.Millisecond, BackoffMax: 5 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	return m, s
}
func eventually(t *testing.T, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if f() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("timed out waiting for reconciliation")
}
func TestWorkersCheckpointFIFOAndErrors(t *testing.T) {
	p := &fakeProvider{calls: map[string]int{}, active: map[string]bool{}, fail: true, resume: make(chan string, 10)}
	m, s := newTestManager(t, p)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, name := range []string{"one", "two"} {
		if err := s.CreateCluster(ctx, &domain.Cluster{Name: name, Spec: domain.ClusterSpec{Provider: "fake"}}); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range []string{"first", "second"} {
		if err := s.EnqueueOperation(ctx, &domain.Operation{ID: id, Cluster: "one", Kind: "backup"}); err != nil {
			t.Fatal(err)
		}
	}
	done := make(chan error, 1)
	go func() { done <- m.Run(ctx) }()
	for i := 0; i < 20; i++ {
		m.Trigger(domain.ClusterRef{Name: "one"})
	}
	eventually(t, func() bool {
		op, err := s.GetOperation(ctx, "second")
		return err == nil && op.Phase == domain.PhaseSucceeded
	})
	c, err := s.GetCluster(ctx, domain.ClusterRef{Name: "one"})
	if err != nil || c.Status.Phase != domain.PhaseReady || c.Status.ObservedGeneration != c.Generation || string(c.Internal) != `{"synced":true}` {
		t.Fatal(c, err)
	}
	op, _ := s.GetOperation(ctx, "first")
	if op.Attempts != 2 || string(op.Checkpoint) != `{"step":1}` {
		t.Fatal(op)
	}
	events, _ := s.ListEvents(ctx, store.EventFilter{})
	warnings := 0
	for _, e := range events {
		if e.Reason == "ReconcileError" {
			warnings++
		}
	}
	if warnings == 0 {
		t.Fatal("no durable error event")
	}
	p.mu.Lock()
	overlap := p.overlap
	two := p.calls["two"]
	p.mu.Unlock()
	if overlap || two == 0 {
		t.Fatal("worker serialization/discovery")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
func TestResumeRunningOperationAndDeleteSuspend(t *testing.T) {
	p := &fakeProvider{calls: map[string]int{}, active: map[string]bool{}, resume: make(chan string, 1)}
	m, s := newTestManager(t, p)
	ctx := context.Background()
	c := domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: "fake"}}
	if err := s.CreateCluster(ctx, &c); err != nil {
		t.Fatal(err)
	}
	op := domain.Operation{ID: "resume", Cluster: "demo", Kind: "restart", Phase: domain.PhaseRunning, Checkpoint: json.RawMessage(`{"persisted":true}`)}
	if err := s.EnqueueOperation(ctx, &op); err != nil {
		t.Fatal(err)
	}
	if _, err := m.reconcile(ctx, domain.ClusterRef{Name: "demo"}); err != nil {
		t.Fatal(err)
	}
	got, _ := s.GetOperation(ctx, op.ID)
	if got.Phase != domain.PhaseSucceeded {
		t.Fatal(got)
	}
	if value := <-p.resume; value != `{"persisted":true}` {
		t.Fatal(value)
	}
	if err := s.MarkClusterDeleting(ctx, domain.ClusterRef{Name: "demo"}, domain.DeleteOptions{KeepVolumes: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := m.reconcile(ctx, domain.ClusterRef{Name: "demo"}); err != nil {
		t.Fatal(err)
	}
	if _, err := m.reconcile(ctx, domain.ClusterRef{Name: "demo"}); err != nil {
		t.Fatal(err)
	}
	cluster, _ := s.GetCluster(ctx, domain.ClusterRef{Name: "demo"})
	if cluster.Deleting || cluster.Status.Phase != domain.PhaseSuspended {
		t.Fatal(cluster)
	}
	if err := s.MarkClusterDeleting(ctx, domain.ClusterRef{Name: "demo"}, domain.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := m.reconcile(ctx, domain.ClusterRef{Name: "demo"}); !errors.Is(err, store.ErrNotFound) {
		t.Fatal(err)
	}
	if _, err := s.GetCluster(ctx, domain.ClusterRef{Name: "demo"}); !errors.Is(err, store.ErrNotFound) {
		t.Fatal(err)
	}
}

type operationProvider struct {
	*fakeProvider
	syncFn    func(*engine.Context) (engine.Result, error)
	statusFn  func(*engine.Context) (domain.ClusterStatus, error)
	runFn     func(*engine.Context, *domain.Operation) (engine.OpResult, error)
	cleanupFn func(*engine.Context) (bool, error)
}

func (p *operationProvider) Sync(_ context.Context, rc *engine.Context) (engine.Result, error) {
	if p.syncFn != nil {
		return p.syncFn(rc)
	}
	return engine.Result{}, nil
}

func (p *operationProvider) Status(_ context.Context, rc *engine.Context) (domain.ClusterStatus, error) {
	if p.statusFn != nil {
		return p.statusFn(rc)
	}
	return domain.ClusterStatus{Phase: domain.PhaseReady}, nil
}

func (p *operationProvider) RunOperation(_ context.Context, rc *engine.Context, op *domain.Operation) (engine.OpResult, error) {
	return p.runFn(rc, op)
}

func (p *operationProvider) Cleanup(_ context.Context, rc *engine.Context, _ domain.DeleteOptions) (bool, error) {
	return p.cleanupFn(rc)
}

type operationSyncProvider struct {
	*operationProvider
	policy func(*domain.Operation) bool
}

func (p *operationSyncProvider) SyncDuringOperation(op *domain.Operation) bool {
	return p.policy(op)
}

func createOperationCluster(t *testing.T, s *store.Store) {
	t.Helper()
	if err := s.CreateCluster(context.Background(), &domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: "fake"}}); err != nil {
		t.Fatal(err)
	}
}

func TestRunningOperationPrecedesSynchronization(t *testing.T) {
	ctx := context.Background()
	var calls []string
	p := &operationProvider{fakeProvider: &fakeProvider{}}
	p.syncFn = func(*engine.Context) (engine.Result, error) {
		calls = append(calls, "sync")
		return engine.Result{}, errors.New("mongod intentionally stopped")
	}
	p.statusFn = func(*engine.Context) (domain.ClusterStatus, error) {
		calls = append(calls, "status")
		return domain.ClusterStatus{}, errors.New("mongod intentionally stopped")
	}
	p.runFn = func(rc *engine.Context, op *domain.Operation) (engine.OpResult, error) {
		calls = append(calls, op.ID)
		if op.ID != "restore" {
			t.Fatalf("pending operation displaced running restore: %s", op.ID)
		}
		rc.Cluster.Internal = json.RawMessage(`{"processes":"recovered"}`)
		rc.Cluster.Status.Phase = domain.PhaseRestoring
		return engine.OpResult{Done: op.Attempts == 2, Checkpoint: json.RawMessage(`{"stage":"recovery"}`)}, nil
	}
	m, s := newTestManager(t, p)
	createOperationCluster(t, s)
	for _, op := range []domain.Operation{
		{ID: "pending", Cluster: "demo", Kind: "restart"},
		{ID: "restore", Cluster: "demo", Kind: "restore", Phase: domain.PhaseRunning},
	} {
		if err := s.EnqueueOperation(ctx, &op); err != nil {
			t.Fatal(err)
		}
	}
	for range 2 {
		if _, err := m.reconcile(ctx, domain.ClusterRef{Name: "demo"}); err != nil {
			t.Fatal(err)
		}
	}
	if strings.Join(calls, ",") != "restore,restore" {
		t.Fatal(calls)
	}
	op, err := s.GetOperation(ctx, "restore")
	if err != nil || op.Phase != domain.PhaseSucceeded || op.Attempts != 2 {
		t.Fatal(op, err)
	}
	c, err := s.GetCluster(ctx, domain.ClusterRef{Name: "demo"})
	if err != nil || c.Status.Phase != domain.PhaseRestoring || string(c.Internal) != `{"processes":"recovered"}` {
		t.Fatal(c, err)
	}
	if _, err = m.reconcile(ctx, domain.ClusterRef{Name: "demo"}); err == nil {
		t.Fatal("ordinary synchronization did not resume")
	}
	if strings.Join(calls, ",") != "restore,restore,sync,status" {
		t.Fatal(calls)
	}
}

func TestOperationSyncPolicyProvisionsReplacement(t *testing.T) {
	ctx := context.Background()
	var calls []string
	p := &operationSyncProvider{operationProvider: &operationProvider{fakeProvider: &fakeProvider{}}}
	p.policy = func(op *domain.Operation) bool {
		return op.Kind == domain.OperationReplaceMember && string(op.Checkpoint) == `{"stage":"join"}`
	}
	p.runFn = func(rc *engine.Context, op *domain.Operation) (engine.OpResult, error) {
		calls = append(calls, "operation")
		if op.Attempts == 1 {
			rc.Cluster.Internal = json.RawMessage(`{"newMember":"demom3"}`)
			return engine.OpResult{Checkpoint: json.RawMessage(`{"stage":"join"}`), RequeueAfter: 5 * time.Second}, nil
		}
		if string(rc.Cluster.Internal) != `{"newMember":"demom3","provisioned":true}` {
			t.Fatal("replacement provider state lost", string(rc.Cluster.Internal))
		}
		return engine.OpResult{Done: true}, nil
	}
	p.syncFn = func(rc *engine.Context) (engine.Result, error) {
		calls = append(calls, "sync")
		if string(rc.Cluster.Internal) != `{"newMember":"demom3"}` {
			t.Fatal("Sync ran before replacement requested provisioning")
		}
		rc.Cluster.Internal = json.RawMessage(`{"newMember":"demom3","provisioned":true}`)
		return engine.Result{RequeueAfter: time.Second}, nil
	}
	p.statusFn = func(*engine.Context) (domain.ClusterStatus, error) {
		calls = append(calls, "status")
		return domain.ClusterStatus{Phase: domain.PhaseScaling}, nil
	}
	m, s := newTestManager(t, p)
	createOperationCluster(t, s)
	op := domain.Operation{ID: "replace", Cluster: "demo", Kind: domain.OperationReplaceMember, Phase: domain.PhaseRunning}
	if err := s.EnqueueOperation(ctx, &op); err != nil {
		t.Fatal(err)
	}
	delay, err := m.reconcile(ctx, domain.ClusterRef{Name: "demo"})
	if err != nil || delay != time.Second {
		t.Fatal(delay, err)
	}
	got, err := s.GetOperation(ctx, op.ID)
	if err != nil || string(got.Checkpoint) != `{"stage":"join"}` {
		t.Fatal(got, err)
	}
	if _, err = m.reconcile(ctx, domain.ClusterRef{Name: "demo"}); err != nil {
		t.Fatal(err)
	}
	if strings.Join(calls, ",") != "operation,sync,status,operation" {
		t.Fatal(calls)
	}
}

type fixedClock struct{ now time.Time }

func (c fixedClock) Now() time.Time { return c.now }

func TestCompletedFailureUnblocksQueueAndRecordsEvent(t *testing.T) {
	ctx := context.Background()
	p := &operationProvider{fakeProvider: &fakeProvider{}}
	p.runFn = func(rc *engine.Context, op *domain.Operation) (engine.OpResult, error) {
		rc.Cluster.Internal = json.RawMessage(`{"terminal":true}`)
		if op.ID == "failed" {
			return engine.OpResult{Done: true, Step: "done", Checkpoint: json.RawMessage(`{"stage":"done"}`)}, errors.New("PBM backup failed")
		}
		return engine.OpResult{Done: true}, nil
	}
	m, s := newTestManager(t, p)
	createOperationCluster(t, s)
	var logs bytes.Buffer
	m.opts.Context.Logger = slog.New(slog.NewTextHandler(&logs, nil))
	now := time.Date(2024, 1, 5, 10, 0, 0, 0, time.UTC)
	m.opts.Context.Clock = fixedClock{now}
	for _, op := range []domain.Operation{
		{ID: "failed", Cluster: "demo", Kind: "backup", Phase: domain.PhaseRunning},
		{ID: "next", Cluster: "demo", Kind: "restart"},
	} {
		if err := s.EnqueueOperation(ctx, &op); err != nil {
			t.Fatal(err)
		}
	}
	if delay, err := m.reconcile(ctx, domain.ClusterRef{Name: "demo"}); err != nil || delay != time.Millisecond {
		t.Fatal("terminal failure should not retry", delay, err)
	}
	op, err := s.GetOperation(ctx, "failed")
	if err != nil || op.Phase != domain.PhaseFailed || op.CompletedAt == nil || !op.CompletedAt.Equal(now) || op.Attempts != 1 || op.Message != "PBM backup failed" || op.Step != "done" || string(op.Checkpoint) != `{"stage":"done"}` {
		t.Fatal(op, err)
	}
	events, err := s.ListEvents(ctx, store.EventFilter{Cluster: "demo"})
	if err != nil || len(events) != 1 || events[0].Operation != op.ID || events[0].Reason != "OperationFailed" || events[0].Type != "Warning" {
		t.Fatal(events, err)
	}
	if !strings.Contains(logs.String(), "operation failed") || !strings.Contains(logs.String(), "PBM backup failed") {
		t.Fatal(logs.String())
	}
	if _, err = m.reconcile(ctx, domain.ClusterRef{Name: "demo"}); err != nil {
		t.Fatal(err)
	}
	next, err := s.GetOperation(ctx, "next")
	if err != nil || next.Phase != domain.PhaseSucceeded {
		t.Fatal(next, err)
	}
	failed, err := s.GetOperation(ctx, "failed")
	if err != nil || failed.Attempts != 1 {
		t.Fatal("terminal operation retried", failed, err)
	}
}

func TestNonterminalErrorsPersistCheckpointAndAttempts(t *testing.T) {
	ctx := context.Background()
	temporary := errors.New("temporary PBM error")
	p := &operationProvider{fakeProvider: &fakeProvider{}}
	p.runFn = func(rc *engine.Context, op *domain.Operation) (engine.OpResult, error) {
		if op.Attempts > 1 && string(op.Checkpoint) != `{"stage":"poll"}` {
			t.Fatal("checkpoint lost", string(op.Checkpoint))
		}
		rc.Cluster.Internal = json.RawMessage(`{"restore":"polling"}`)
		result := engine.OpResult{Done: op.Attempts == 3, Checkpoint: json.RawMessage(`{"stage":"poll"}`)}
		if !result.Done {
			return result, temporary
		}
		return result, nil
	}
	m, s := newTestManager(t, p)
	createOperationCluster(t, s)
	op := domain.Operation{ID: "retry", Cluster: "demo", Kind: "restore", Phase: domain.PhaseRunning}
	if err := s.EnqueueOperation(ctx, &op); err != nil {
		t.Fatal(err)
	}
	for attempt := 1; attempt <= 3; attempt++ {
		_, err := m.reconcile(ctx, domain.ClusterRef{Name: "demo"})
		if attempt < 3 && !errors.Is(err, temporary) || attempt == 3 && err != nil {
			t.Fatal(attempt, err)
		}
		got, err := s.GetOperation(ctx, op.ID)
		if err != nil || got.Attempts != attempt || string(got.Checkpoint) != `{"stage":"poll"}` {
			t.Fatal(got, err)
		}
		if attempt < 3 && (got.Phase != domain.PhaseRunning || got.CompletedAt != nil) {
			t.Fatal("nonterminal failure retired", got)
		}
	}
}

func TestCleanupIgnoresUnfinishedOperations(t *testing.T) {
	ctx := context.Background()
	p := &operationProvider{fakeProvider: &fakeProvider{}}
	p.runFn = func(*engine.Context, *domain.Operation) (engine.OpResult, error) {
		t.Fatal("operation ran during cleanup")
		return engine.OpResult{}, nil
	}
	m, s := newTestManager(t, p)
	createOperationCluster(t, s)
	p.cleanupFn = func(*engine.Context) (bool, error) {
		ops, err := s.ListOperations(ctx, domain.ClusterRef{Name: "demo"}, domain.PhasePending, domain.PhaseRunning)
		if err != nil || len(ops) != 0 {
			t.Fatal("unfinished work blocked cleanup", ops, err)
		}
		return true, nil
	}
	for _, phase := range []domain.Phase{domain.PhasePending, domain.PhaseRunning} {
		op := domain.Operation{ID: string(phase), Cluster: "demo", Kind: "restore", Phase: phase}
		if err := s.EnqueueOperation(ctx, &op); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.MarkClusterDeleting(ctx, domain.ClusterRef{Name: "demo"}, domain.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := m.reconcile(ctx, domain.ClusterRef{Name: "demo"}); !errors.Is(err, errClusterGone) {
		t.Fatal(err)
	}
}

func TestOperationWorkerUsesBoundedBackoff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := make(chan time.Time, 8)
	p := &operationProvider{fakeProvider: &fakeProvider{}}
	p.runFn = func(*engine.Context, *domain.Operation) (engine.OpResult, error) {
		calls <- time.Now()
		return engine.OpResult{}, errors.New("retryable")
	}
	m, s := newTestManager(t, p)
	createOperationCluster(t, s)
	m.opts.BackoffMin = 20 * time.Millisecond
	m.opts.BackoffMax = 40 * time.Millisecond
	op := domain.Operation{ID: "retry", Cluster: "demo", Kind: "restore", Phase: domain.PhaseRunning}
	if err := s.EnqueueOperation(ctx, &op); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- m.Run(ctx) }()
	var times []time.Time
	for len(times) < 4 {
		select {
		case now := <-calls:
			times = append(times, now)
			m.Trigger(domain.ClusterRef{Name: "demo"})
		case <-time.After(3 * time.Second):
			t.Fatal("operation retry stalled")
		}
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	for i := 1; i < len(times); i++ {
		min := 35 * time.Millisecond
		if i == 1 {
			min = 15 * time.Millisecond
		}
		if times[i].Sub(times[i-1]) < min {
			t.Fatal("trigger bypassed backoff", times)
		}
	}
	got, err := s.GetOperation(context.Background(), op.ID)
	if err != nil || got.Attempts != 4 || got.Phase != domain.PhaseRunning {
		t.Fatal(got, err)
	}
}

func TestScheduledPlatformWaitDoesNotRepeatFailureEvents(t *testing.T) {
	var calls atomic.Int32
	retryAt := time.Now().Add(time.Hour)
	p := &operationProvider{fakeProvider: &fakeProvider{}}
	p.syncFn = func(*engine.Context) (engine.Result, error) {
		n := calls.Add(1)
		if n == 1 || n == 5 {
			return engine.Result{}, fmt.Errorf("platform action cleanup-volume failed (attempt %d)", n)
		}
		return engine.Result{}, &engine.PlatformRetryWait{RetryAt: retryAt, Message: "platform action cleanup-volume failed; waiting for retry"}
	}
	p.statusFn = func(*engine.Context) (domain.ClusterStatus, error) {
		return domain.ClusterStatus{Phase: domain.PhaseUpdating, Message: "waiting for retry"}, nil
	}
	m, s := newTestManager(t, p)
	m.opts.ResyncInterval = time.Hour
	createOperationCluster(t, s)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- m.Run(ctx) }()
	defer func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()
	// First failed attempt emits one event. Subsequent explicit triggers may
	// refresh status but must not manufacture failures while the deadline waits.
	eventually(t, func() bool { return calls.Load() >= 2 })
	for n := int32(3); n <= 6; n++ {
		m.Trigger(domain.ClusterRef{Name: "demo"})
		eventually(t, func() bool { return calls.Load() >= n })
	}
	events, err := s.ListEvents(context.Background(), store.EventFilter{Cluster: "demo", Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, event := range events {
		if event.Reason == "ReconcileError" {
			count++
		}
	}
	if count != 2 {
		t.Fatalf("ReconcileError count=%d want one per failed attempt (2)", count)
	}
}

func TestScheduledPlatformWaitRequiresOnlyWaitLeaves(t *testing.T) {
	first := &engine.PlatformRetryWait{RetryAt: time.Now(), Message: "first wait"}
	later := &engine.PlatformRetryWait{RetryAt: first.RetryAt.Add(time.Hour), Message: "later wait"}
	failure := errors.New("ListServices authentication failed")
	for _, tc := range []struct {
		name string
		err  error
		want *engine.PlatformRetryWait
	}{
		{"nil", nil, nil},
		{"direct", first, first},
		{"wrapped", fmt.Errorf("provider: %w", first), first},
		{"joined waits", errors.Join(later, fmt.Errorf("provider: %w", first)), first},
		{"plain failure", failure, nil},
		{"wait then failure", errors.Join(first, failure), nil},
		{"failure then wait", errors.Join(failure, first), nil},
		{"nested failure", fmt.Errorf("provider: %w", errors.Join(later, errors.Join(first, failure))), nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := scheduledPlatformWait(tc.err); got != tc.want {
				t.Fatalf("scheduledPlatformWait(%v)=%v want=%v", tc.err, got, tc.want)
			}
		})
	}
}

func TestScheduledWaitDoesNotSuppressStatusFailure(t *testing.T) {
	var calls atomic.Int32
	p := &operationProvider{fakeProvider: &fakeProvider{}}
	p.syncFn = func(*engine.Context) (engine.Result, error) {
		calls.Add(1)
		return engine.Result{}, &engine.PlatformRetryWait{RetryAt: time.Now().Add(time.Hour), Message: "CPU-mode cooldown"}
	}
	p.statusFn = func(*engine.Context) (domain.ClusterStatus, error) {
		if calls.Load() == 1 {
			return domain.ClusterStatus{}, errors.New("ListServices authentication failed")
		}
		return domain.ClusterStatus{Phase: domain.PhaseUpdating, Message: "CPU-mode cooldown"}, nil
	}
	m, s := newTestManager(t, p)
	m.opts.ResyncInterval = time.Hour
	var logs bytes.Buffer
	m.opts.Context.Logger = slog.New(slog.NewTextHandler(&logs, nil))
	createOperationCluster(t, s)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- m.Run(ctx) }()
	// A real status failure must retry via bounded error backoff, not wait an
	// hour. Once observation succeeds the ordinary scheduled wait is preserved.
	eventually(t, func() bool {
		c, err := s.GetCluster(context.Background(), domain.ClusterRef{Name: "demo"})
		return err == nil && c.Status.Message == "CPU-mode cooldown"
	})
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	events, err := s.ListEvents(context.Background(), store.EventFilter{Cluster: "demo"})
	if err != nil {
		t.Fatal(err)
	}
	failures := 0
	for _, event := range events {
		if event.Reason == "ReconcileError" {
			failures++
			if !strings.Contains(event.Message, "ListServices authentication failed") {
				t.Fatal("observation failure missing from event", event)
			}
		}
	}
	if failures != 1 || calls.Load() < 2 {
		t.Fatalf("failures=%d calls=%d want one failure followed by successful observation", failures, calls.Load())
	}
	if !strings.Contains(logs.String(), "ListServices authentication failed") {
		t.Fatal("observation failure missing from log", logs.String())
	}
}

type testNamespaces struct {
	engine.StaticNamespaces
	mu       sync.Mutex
	calls    map[string]int
	failures []time.Time
}

func (n *testNamespaces) Backend(namespace string) (infra.Backend, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.calls[namespace]++
	if namespace == "broken" {
		n.failures = append(n.failures, time.Now())
		return nil, errors.New("project token invalid")
	}
	return n.DefaultBackend, nil
}

func (n *testNamespaces) AgentURL(namespace string) string {
	return "https://" + namespace + ".example.com"
}

func TestNamespaceFailureIsolatedAndWorkersDistinct(t *testing.T) {
	p := &fakeProvider{calls: map[string]int{}, active: map[string]bool{}}
	m, s := newTestManager(t, p)
	m.opts.Namespaces = &testNamespaces{calls: map[string]int{}}
	m.opts.BackoffMin = 25 * time.Millisecond
	m.opts.BackoffMax = 50 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, namespace := range []string{"healthy", "broken"} {
		if err := s.CreateNamespace(ctx, &domain.Namespace{Name: namespace, Phase: "Ready"}); err != nil {
			t.Fatal(err)
		}
		c := domain.Cluster{Namespace: namespace, Name: "same", Spec: domain.ClusterSpec{Provider: "fake"}}
		if err := s.CreateCluster(ctx, &c); err != nil {
			t.Fatal(err)
		}
		m.Trigger(c.Ref())
	}
	if len(m.workers) != 2 || m.workers["healthy:same"] == m.workers["broken:same"] {
		t.Fatal("namespace workers collided", m.workers)
	}
	done := make(chan error, 1)
	go func() { done <- m.Run(ctx) }()
	eventually(t, func() bool {
		good, err := s.GetCluster(ctx, domain.ClusterRef{Namespace: "healthy", Name: "same"})
		bad, badErr := s.GetCluster(ctx, domain.ClusterRef{Namespace: "broken", Name: "same"})
		return err == nil && badErr == nil && good.Status.Phase == domain.PhaseReady && bad.Status.Phase == domain.PhaseDegraded
	})
	eventually(t, func() bool {
		ns := m.opts.Namespaces.(*testNamespaces)
		ns.mu.Lock()
		defer ns.mu.Unlock()
		return len(ns.failures) >= 2
	})
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	ns := m.opts.Namespaces.(*testNamespaces)
	if ns.failures[1].Sub(ns.failures[0]) < 20*time.Millisecond {
		t.Fatal("namespace retry bypassed backoff", ns.failures)
	}
	bad, err := s.GetCluster(context.Background(), domain.ClusterRef{Namespace: "broken", Name: "same"})
	if err != nil || len(bad.Status.Conditions) != 1 || bad.Status.Conditions[0].Reason != "NamespaceUnavailable" || !strings.Contains(bad.Status.Message, "project token invalid") {
		t.Fatal(bad, err)
	}
	items, err := s.ListEvents(context.Background(), store.EventFilter{Namespace: "broken"})
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, event := range items {
		if event.Namespace != "broken" || event.Cluster != "same" {
			t.Fatal(event)
		}
		found = found || event.Reason == "NamespaceUnavailable"
	}
	if !found || p.overlap {
		t.Fatal("missing namespace error or crossed workers", items)
	}
}

func TestNamespaceDependenciesResolvedBeforeProviderCalls(t *testing.T) {
	ns := &testNamespaces{calls: map[string]int{}}
	p := &operationProvider{fakeProvider: &fakeProvider{}}
	expected := 0
	check := func(rc *engine.Context) {
		t.Helper()
		if rc.Namespace != "healthy" || rc.AgentURL != "https://healthy.example.com" || ns.calls["healthy"] <= expected {
			t.Fatal("provider dependencies not refreshed", rc.Namespace, rc.AgentURL, ns.calls)
		}
		expected = ns.calls["healthy"]
	}
	p.syncFn = func(rc *engine.Context) (engine.Result, error) { check(rc); return engine.Result{}, nil }
	p.statusFn = func(rc *engine.Context) (domain.ClusterStatus, error) {
		check(rc)
		return domain.ClusterStatus{Phase: domain.PhaseReady}, nil
	}
	p.runFn = func(rc *engine.Context, _ *domain.Operation) (engine.OpResult, error) {
		check(rc)
		return engine.OpResult{Done: true}, nil
	}
	p.cleanupFn = func(rc *engine.Context) (bool, error) { check(rc); return true, nil }
	m, s := newTestManager(t, p)
	m.opts.Namespaces = ns
	c := domain.Cluster{Namespace: "healthy", Name: "same", Spec: domain.ClusterSpec{Provider: "fake"}}
	ctx := context.Background()
	if err := s.CreateNamespace(ctx, &domain.Namespace{Name: c.Namespace, Phase: "Ready"}); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateCluster(ctx, &c); err != nil {
		t.Fatal(err)
	}
	if err := s.EnqueueOperation(ctx, &domain.Operation{Namespace: c.Namespace, Cluster: c.Name, Kind: "backup"}); err != nil {
		t.Fatal(err)
	}
	if _, err := m.reconcile(ctx, c.Ref()); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkClusterDeleting(ctx, c.Ref(), domain.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := m.reconcile(ctx, c.Ref()); !errors.Is(err, errClusterGone) {
		t.Fatal(err)
	}
}
