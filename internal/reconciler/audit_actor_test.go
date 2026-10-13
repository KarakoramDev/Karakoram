// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package reconciler

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/scheduler"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

// Exercise provider-generated events as well as the manager's phase events.
type attributionProvider struct{ *operationProvider }

func (p *attributionProvider) Sync(ctx context.Context, rc *engine.Context) (engine.Result, error) {
	if err := rc.Events.Record(ctx, domain.Event{Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Type: "Normal", Reason: "ProviderSync"}); err != nil {
		return engine.Result{}, err
	}
	return p.operationProvider.Sync(ctx, rc)
}

func (p *attributionProvider) Cleanup(ctx context.Context, rc *engine.Context, opts domain.DeleteOptions) (bool, error) {
	if err := rc.Events.Record(ctx, domain.Event{Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Type: "Normal", Reason: "ProviderCleanup"}); err != nil {
		return false, err
	}
	return p.operationProvider.Cleanup(ctx, rc, opts)
}

func (p *attributionProvider) RunOperation(ctx context.Context, rc *engine.Context, op *domain.Operation) (engine.OpResult, error) {
	if err := rc.Events.Record(ctx, domain.Event{Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Operation: op.ID, Type: "Normal", Reason: "ProviderOperation"}); err != nil {
		return engine.OpResult{}, err
	}
	return p.operationProvider.RunOperation(ctx, rc, op)
}

func openAttributionManager(t *testing.T, p engine.Provider, path string) (*Manager, *store.Store) {
	t.Helper()
	key, err := secrets.New(strings.Repeat("k", 32))
	if err != nil {
		t.Fatal(err)
	}
	s, err := store.Open(path, key)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	registry, err := engine.NewRegistry(p)
	if err != nil {
		t.Fatal(err)
	}
	m, err := New(Options{Store: s, Registry: registry, Namespaces: &testNamespaces{calls: map[string]int{}}})
	if err != nil {
		t.Fatal(err)
	}
	return m, s
}

func assertAttributedEvents(t *testing.T, s *store.Store, since int64, actor string, reasons ...string) int64 {
	t.Helper()
	events, err := s.ListEvents(context.Background(), store.EventFilter{SinceID: since})
	if err != nil || len(events) != len(reasons) {
		t.Fatalf("events=%+v err=%v want reasons=%v", events, err, reasons)
	}
	for i, event := range events {
		if event.Actor != actor || event.Reason != reasons[i] || event.Resource != "clusters" || event.Target != "demo" {
			t.Fatalf("event=%+v want actor=%q reason=%q", event, actor, reasons[i])
		}
		since = event.ID
	}
	return since
}

func TestDesiredGenerationEventsKeepRequesterAcrossRestart(t *testing.T) {
	ctx := store.WithActor(context.Background(), "unrelated-worker-context")
	phase := domain.PhaseReady
	requeue := time.Duration(0)
	var syncFailure error
	cleanupDone := false
	p := &attributionProvider{&operationProvider{fakeProvider: &fakeProvider{}}}
	p.syncFn = func(*engine.Context) (engine.Result, error) {
		return engine.Result{RequeueAfter: requeue}, syncFailure
	}
	p.statusFn = func(*engine.Context) (domain.ClusterStatus, error) {
		return domain.ClusterStatus{Phase: phase}, nil
	}
	p.cleanupFn = func(*engine.Context) (bool, error) { return cleanupDone, nil }
	path := filepath.Join(t.TempDir(), "attribution.db")
	m, s := openAttributionManager(t, p, path)
	if err := s.CreateNamespace(ctx, &domain.Namespace{Name: "healthy", Phase: "Ready"}); err != nil {
		t.Fatal(err)
	}
	c := domain.Cluster{Namespace: "healthy", Name: "demo", Spec: domain.ClusterSpec{Provider: "fake"}}
	if err := s.CreateCluster(store.WithActor(ctx, "creator"), &c); err != nil {
		t.Fatal(err)
	}
	if _, err := m.reconcile(ctx, c.Ref()); err != nil {
		t.Fatal(err)
	}
	last := assertAttributedEvents(t, s, 0, "creator", "ProviderSync", "PhaseChanged")
	c.Spec.Version = "8.0"
	if err := s.UpdateClusterSpec(store.WithActor(ctx, "editor"), c.Ref(), c.Spec); err != nil {
		t.Fatal(err)
	}
	phase, requeue = domain.PhaseUpdating, time.Second
	if _, err := m.reconcile(ctx, c.Ref()); err != nil {
		t.Fatal(err)
	}
	last = assertAttributedEvents(t, s, last, "editor", "ProviderSync", "PhaseChanged")
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	m, s = openAttributionManager(t, p, path)
	phase, requeue = domain.PhaseReady, 0
	if _, err := m.reconcile(ctx, c.Ref()); err != nil {
		t.Fatal(err)
	}
	last = assertAttributedEvents(t, s, last, "editor", "ProviderSync", "PhaseChanged")
	got, err := s.GetCluster(ctx, c.Ref())
	if err != nil || got.RequestedBy != "editor" || got.Status.ObservedGeneration != got.Generation {
		t.Fatalf("applied generation=%+v err=%v", got, err)
	}
	// A provider health snapshot omits ObservedGeneration. A failed automatic
	// resync must not resurrect the editor's already-applied causal attribution.
	phase, syncFailure = domain.PhaseDegraded, errors.New("automatic health failure")
	if _, err := m.reconcile(ctx, c.Ref()); !errors.Is(err, syncFailure) {
		t.Fatal(err)
	}
	last = assertAttributedEvents(t, s, last, "reconciler", "ProviderSync", "PhaseChanged")
	phase, syncFailure = domain.PhaseReady, nil
	if _, err := m.reconcile(ctx, c.Ref()); err != nil {
		t.Fatal(err)
	}
	last = assertAttributedEvents(t, s, last, "reconciler", "ProviderSync", "PhaseChanged")
	got, err = s.GetCluster(ctx, c.Ref())
	if err != nil || got.RequestedBy != "editor" {
		t.Fatalf("automatic update overwrote requester: %+v err=%v", got, err)
	}
	// Deletion after Ready establishes its own generation and survives restart.
	if err := s.MarkClusterDeleting(store.WithActor(ctx, "deleter"), c.Ref(), domain.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	m, s = openAttributionManager(t, p, path)
	if _, err := m.reconcile(ctx, c.Ref()); err != nil {
		t.Fatal(err)
	}
	last = assertAttributedEvents(t, s, last, "deleter", "PhaseChanged", "ProviderCleanup")
	cleanupDone = true
	if _, err := m.reconcile(ctx, c.Ref()); !errors.Is(err, errClusterGone) {
		t.Fatal(err)
	}
	assertAttributedEvents(t, s, last, "deleter", "ProviderCleanup", "Deleted")
	if _, err := s.GetCluster(ctx, c.Ref()); !errors.Is(err, store.ErrNotFound) {
		t.Fatal(err)
	}
	events, err := s.ListEvents(ctx, store.EventFilter{})
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range events {
		if event.Namespace != c.Namespace {
			t.Fatalf("request actor crossed namespace: %+v", event)
		}
	}
}

func TestSuspendAndResumeLifecycleUsesRequestActors(t *testing.T) {
	ctx := context.Background()
	p := &attributionProvider{&operationProvider{fakeProvider: &fakeProvider{}}}
	p.cleanupFn = func(*engine.Context) (bool, error) { return true, nil }
	path := filepath.Join(t.TempDir(), "resume.db")
	m, s := openAttributionManager(t, p, path)
	c := domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: "fake"}}
	if err := s.CreateCluster(store.WithActor(ctx, "creator"), &c); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkClusterDeleting(store.WithActor(ctx, "suspender"), c.Ref(), domain.DeleteOptions{KeepVolumes: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := m.reconcile(ctx, c.Ref()); err != nil {
		t.Fatal(err)
	}
	last := assertAttributedEvents(t, s, 0, "suspender", "PhaseChanged", "ProviderCleanup", "PhaseChanged")
	if err := s.ResumeCluster(store.WithActor(ctx, "resumer"), c.Ref()); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	m, s = openAttributionManager(t, p, path)
	if _, err := m.reconcile(ctx, c.Ref()); err != nil {
		t.Fatal(err)
	}
	assertAttributedEvents(t, s, last, "resumer", "ProviderSync", "PhaseChanged")
}

func TestOperationEventsAndPhasesUseOperationActorAcrossRestart(t *testing.T) {
	for _, actor := range []string{"operator", "scheduler"} {
		t.Run(actor, func(t *testing.T) {
			ctx := context.Background()
			p := &attributionProvider{&operationProvider{fakeProvider: &fakeProvider{}}}
			p.runFn = func(rc *engine.Context, op *domain.Operation) (engine.OpResult, error) {
				if len(op.Checkpoint) == 0 {
					rc.Cluster.Status.Phase = domain.PhaseUpdating
					return engine.OpResult{Checkpoint: []byte(`{"started":true}`)}, errors.New("temporary operation failure")
				}
				rc.Cluster.Status.Phase = domain.PhaseReady
				return engine.OpResult{Done: true}, nil
			}
			path := filepath.Join(t.TempDir(), "operation.db")
			m, s := openAttributionManager(t, p, path)
			c := domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: "fake"}}
			if err := s.CreateCluster(store.WithActor(ctx, "creator"), &c); err != nil {
				t.Fatal(err)
			}
			if _, err := m.reconcile(ctx, c.Ref()); err != nil {
				t.Fatal(err)
			}
			last := assertAttributedEvents(t, s, 0, "creator", "ProviderSync", "PhaseChanged")
			op := domain.Operation{Cluster: c.Name, Kind: domain.OperationRestart, Actor: actor}
			if err := s.EnqueueOperation(ctx, &op); err != nil {
				t.Fatal(err)
			}
			if _, err := s.ClaimOperation(ctx, c.Ref()); err != nil {
				t.Fatal(err)
			}
			if _, err := m.reconcile(ctx, c.Ref()); err == nil {
				t.Fatal("expected nonterminal operation failure")
			}
			last = assertAttributedEvents(t, s, last, actor, "ProviderOperation", "PhaseChanged", "OperationError")
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			m, s = openAttributionManager(t, p, path)
			if _, err := m.reconcile(ctx, c.Ref()); err != nil {
				t.Fatal(err)
			}
			assertAttributedEvents(t, s, last, actor, "ProviderOperation", "PhaseChanged", "OperationSucceeded")
			events, err := s.ListEvents(ctx, store.EventFilter{Actor: actor})
			if err != nil {
				t.Fatal(err)
			}
			for _, event := range events {
				if event.Operation != op.ID {
					t.Fatalf("operation event missing ID: %+v", event)
				}
			}
		})
	}
}

func TestScheduledBackupLifecycleRetainsSchedulerActor(t *testing.T) {
	ctx := store.WithActor(context.Background(), "interactive-user")
	p := &attributionProvider{&operationProvider{fakeProvider: &fakeProvider{}}}
	p.runFn = func(rc *engine.Context, op *domain.Operation) (engine.OpResult, error) {
		rc.Cluster.Status.Phase = domain.PhaseUpdating
		return engine.OpResult{Done: true}, nil
	}
	m, s := openAttributionManager(t, p, filepath.Join(t.TempDir(), "scheduler.db"))
	if err := s.CreateNamespace(ctx, &domain.Namespace{Name: "healthy", Phase: "Ready"}); err != nil {
		t.Fatal(err)
	}
	c := domain.Cluster{Namespace: "healthy", Name: "demo", Spec: domain.ClusterSpec{Provider: "fake", Backup: domain.BackupConfig{Storage: "archive", Schedules: []domain.BackupSchedule{{Name: "daily", Cron: "@daily", Enabled: true}}}}}
	if err := s.CreateCluster(ctx, &c); err != nil {
		t.Fatal(err)
	}
	if _, err := m.reconcile(ctx, c.Ref()); err != nil {
		t.Fatal(err)
	}
	last := assertAttributedEvents(t, s, 0, "interactive-user", "ProviderSync", "PhaseChanged")
	scheduled, err := scheduler.New(scheduler.Options{Store: s})
	if err != nil {
		t.Fatal(err)
	}
	if err := scheduled.Fire(ctx, c.Ref(), "daily"); err != nil {
		t.Fatal(err)
	}
	op, err := s.ClaimOperation(ctx, c.Ref())
	if err != nil || op.Actor != "scheduler" {
		t.Fatalf("queued operation=%+v err=%v", op, err)
	}
	if _, err := m.reconcile(ctx, c.Ref()); err != nil {
		t.Fatal(err)
	}
	assertAttributedEvents(t, s, last, "scheduler", "ProviderOperation", "PhaseChanged", "OperationSucceeded")
	events, err := s.ListEvents(ctx, store.EventFilter{Namespace: c.Namespace, Actor: "scheduler", Resource: "clusters"})
	if err != nil || len(events) != 3 {
		t.Fatalf("scheduled namespace attribution: %+v %v", events, err)
	}
}

func TestReconcileErrorRetainsCausalActor(t *testing.T) {
	for _, tc := range []struct {
		name      string
		actor     string
		observed  int64
		operation bool
	}{
		{name: "desired-generation", actor: "requester"},
		{name: "automatic", actor: "reconciler", observed: 1},
		{name: "operation", actor: "operator", observed: 1, operation: true},
		{name: "scheduled-operation", actor: "scheduler", observed: 1, operation: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			p := &attributionProvider{&operationProvider{fakeProvider: &fakeProvider{}}}
			failure := errors.New("causal failure")
			p.syncFn = func(*engine.Context) (engine.Result, error) { return engine.Result{}, failure }
			p.runFn = func(*engine.Context, *domain.Operation) (engine.OpResult, error) {
				return engine.OpResult{}, failure
			}
			m, s := openAttributionManager(t, p, filepath.Join(t.TempDir(), "errors.db"))
			c := domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: "fake"}, Status: domain.ClusterStatus{Phase: domain.PhaseReady, ObservedGeneration: tc.observed}}
			if err := s.CreateCluster(store.WithActor(ctx, "requester"), &c); err != nil {
				cancel()
				t.Fatal(err)
			}
			op := domain.Operation{ID: "operation", Cluster: c.Name, Actor: tc.actor, Phase: domain.PhaseRunning}
			if tc.operation {
				if err := s.EnqueueOperation(ctx, &op); err != nil {
					cancel()
					t.Fatal(err)
				}
			}
			done := make(chan error, 1)
			go func() { done <- m.Run(ctx) }()
			t.Cleanup(func() {
				cancel()
				if err := <-done; err != nil {
					t.Error(err)
				}
			})
			eventually(t, func() bool {
				events, err := s.ListEvents(ctx, store.EventFilter{Actor: tc.actor})
				if err != nil {
					t.Fatal(err)
				}
				for _, event := range events {
					if event.Reason == "ReconcileError" {
						if tc.operation && event.Operation != op.ID {
							t.Fatalf("missing operation attribution: %+v", event)
						}
						return true
					}
				}
				return false
			})
		})
	}
}

func TestFailedOperationLifecycleUsesOperationActor(t *testing.T) {
	ctx := context.Background()
	p := &attributionProvider{&operationProvider{fakeProvider: &fakeProvider{}}}
	p.runFn = func(rc *engine.Context, op *domain.Operation) (engine.OpResult, error) {
		rc.Cluster.Status.Phase = domain.PhaseDegraded
		return engine.OpResult{Done: true}, errors.New("operation failed")
	}
	m, s := openAttributionManager(t, p, filepath.Join(t.TempDir(), "failed-operation.db"))
	c := domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: "fake"}, Status: domain.ClusterStatus{Phase: domain.PhaseReady, ObservedGeneration: 1}}
	if err := s.CreateCluster(store.WithActor(ctx, "creator"), &c); err != nil {
		t.Fatal(err)
	}
	op := domain.Operation{Cluster: c.Name, Phase: domain.PhaseRunning, Actor: "operator"}
	if err := s.EnqueueOperation(ctx, &op); err != nil {
		t.Fatal(err)
	}
	if _, err := m.reconcile(ctx, c.Ref()); err != nil {
		t.Fatal(err)
	}
	assertAttributedEvents(t, s, 0, "operator", "ProviderOperation", "PhaseChanged", "OperationFailed")
}
