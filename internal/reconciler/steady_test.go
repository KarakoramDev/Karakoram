// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package reconciler

import (
	"context"
	"encoding/json"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/agentpush"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	infrafake "github.com/KarakoramDev/Karakoram/internal/infra/fake"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

type steadyProvider struct {
	*fakeProvider
	syncs                    atomic.Int32
	renewals                 atomic.Int32
	steadyObservations       atomic.Int32
	freshSyncViolation       atomic.Bool
	lifecycleMarkerViolation atomic.Bool
	renewalMarkerViolation   atomic.Bool
	callerViolation          atomic.Bool
	degraded                 atomic.Bool
	blockClaim               bool
	requeue                  time.Duration
	notSteady                bool
	snapshot                 *infra.TargetSnapshot
}

func (p *steadyProvider) Sync(ctx context.Context, _ *engine.Context) (engine.Result, error) {
	if infra.RequestCaller(ctx) != infra.CallerReconcile {
		p.callerViolation.Store(true)
	}
	p.syncs.Add(1)
	if engine.SteadyEligible(ctx) {
		p.freshSyncViolation.Store(true)
	}
	return engine.Result{RequeueAfter: p.requeue, Steady: !p.notSteady}, nil
}

func (p *steadyProvider) CanClaimOperation(*domain.Cluster) (bool, error) {
	return !p.blockClaim, nil
}

func (p *steadyProvider) Status(ctx context.Context, rc *engine.Context) (domain.ClusterStatus, error) {
	wantCaller := infra.CallerReconcile
	if rc.AvailabilityOnly {
		wantCaller = infra.CallerAvailability
	}
	if infra.RequestCaller(ctx) != wantCaller {
		p.callerViolation.Store(true)
	}
	if p.snapshot != nil {
		p.snapshot.Lookup(ctx, "cluster")
	}
	if engine.SteadyEligible(ctx) {
		p.steadyObservations.Add(1)
		if !rc.AvailabilityOnly {
			p.lifecycleMarkerViolation.Store(true)
		}
	}
	if rc.AvailabilityOnly {
		p.renewals.Add(1)
		if !engine.SteadyEligible(ctx) && !p.degraded.Load() {
			p.renewalMarkerViolation.Store(true)
		}
	}
	phase := domain.PhaseReady
	ready := "True"
	if p.degraded.Load() {
		phase, ready = domain.PhaseDegraded, "False"
	}
	return domain.ClusterStatus{
		Phase:      phase,
		Conditions: []domain.Condition{{Type: "Ready", Status: ready, Reason: "MembersObserved"}},
		Members:    []domain.MemberStatus{{Name: "member", Health: !p.degraded.Load(), Agent: domain.AgentStatus{Healthy: !p.degraded.Load(), LastSeen: rc.Clock.Now()}}},
	}, nil
}

func newSteadyTestManager(t *testing.T, p engine.Provider) (*Manager, *store.Store) {
	t.Helper()
	key, err := secrets.New(strings.Repeat("k", 32))
	if err != nil {
		t.Fatal(err)
	}
	// Exercise the real publication CAS without paying for hundreds of disk
	// commits: durability is irrelevant to this simulated scheduling contract.
	st, err := store.Open(":memory:", key)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	registry, err := engine.NewRegistry(p)
	if err != nil {
		t.Fatal(err)
	}
	m, err := New(Options{Store: st, Registry: registry, ResyncInterval: 30 * time.Second,
		BackoffMin: time.Millisecond, BackoffMax: 5 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	return m, st
}

func steadyFixture(t *testing.T, p *steadyProvider) (*Manager, *store.Store, *domain.Cluster) {
	t.Helper()
	m, st := newSteadyTestManager(t, p)
	m.opts.Context.ObservationEpoch = "steady-test"
	c := &domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: "fake"}, Status: domain.ClusterStatus{Phase: domain.PhaseReady, ObservedGeneration: 1}}
	if err := st.CreateCluster(t.Context(), c); err != nil {
		t.Fatal(err)
	}
	return m, st, c
}

func runSteadyManager(t *testing.T, m *Manager) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- m.Run(ctx) }()
	synctest.Wait()
	return func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	}
}

func TestSteadyHourScheduleAndObservationIsolation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := &steadyProvider{fakeProvider: &fakeProvider{}}
		m, _, c := steadyFixture(t, p)
		m.opts.SteadyResyncInterval = 10 * time.Minute
		stop := runSteadyManager(t, m)
		defer stop()
		defer func() {
			if p.callerViolation.Load() {
				t.Error("steady observation or deep reconcile lost its request caller")
			}
		}()
		if p.syncs.Load() != 1 {
			t.Fatal("startup did not coalesce discovery and timer", p.syncs.Load())
		}
		for minute := 1; minute <= 60; minute++ {
			time.Sleep(time.Minute)
			synctest.Wait()
			want := int32(1 + minute/10)
			if got := p.syncs.Load(); got != want {
				t.Fatalf("minute %d: Sync=%d, want %d (discovery must not wake steady workers)", minute, got, want)
			}
		}
		if got := p.renewals.Load(); got < 719 || got > 720 {
			t.Fatalf("availability did not renew every five seconds: %d", got)
		}
		if p.renewalMarkerViolation.Load() || p.freshSyncViolation.Load() || p.lifecycleMarkerViolation.Load() ||
			p.steadyObservations.Load() != p.renewals.Load() {
			t.Fatal("steady observation marker escaped observation scope or was missing")
		}
		w := m.workerFor(t.Context(), c.Ref())
		if w == nil || w.steady.Load() == nil {
			t.Fatal("healthy cluster did not stay steady")
		}
	})
}

func TestNonSteadyKeepsReadinessCadence(t *testing.T) {
	for _, scenario := range []string{"provider-requeue", "pending-operation", "degraded"} {
		t.Run(scenario, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				p := &steadyProvider{fakeProvider: &fakeProvider{}}
				if scenario == "provider-requeue" {
					p.requeue = time.Minute
					p.notSteady = true
				}
				p.degraded.Store(scenario == "degraded")
				p.blockClaim = scenario == "pending-operation"
				m, st, c := steadyFixture(t, p)
				if p.blockClaim {
					if err := st.EnqueueOperation(t.Context(), &domain.Operation{Cluster: c.Name, Kind: domain.OperationBackup}); err != nil {
						t.Fatal(err)
					}
				}
				stop := runSteadyManager(t, m)
				defer stop()
				for second := 5; second <= 25; second += 5 {
					time.Sleep(5 * time.Second)
					synctest.Wait()
					if got, want := p.syncs.Load(), int32(1+second/5); got != want {
						t.Fatalf("at %ds: Sync=%d, want %d", second, got, want)
					}
				}
				if m.workerFor(t.Context(), c.Ref()).steady.Load() != nil {
					t.Fatal("non-steady cluster entered steady mode")
				}
			})
		})
	}
}

func TestSteadyNonReadyRenewalTriggersReconcile(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := &steadyProvider{fakeProvider: &fakeProvider{}}
		m, st, c := steadyFixture(t, p)
		stop := runSteadyManager(t, m)
		defer stop()
		p.degraded.Store(true)
		time.Sleep(engine.ReadinessInterval)
		synctest.Wait()
		if p.syncs.Load() < 2 {
			t.Fatal("published non-Ready renewal did not trigger an immediate reconcile")
		}
		stored, err := st.GetCluster(t.Context(), c.Ref())
		if err != nil || stored.Status.Phase != domain.PhaseDegraded || m.workerFor(t.Context(), c.Ref()).steady.Load() != nil {
			t.Fatal("non-Ready renewal retained steady readiness", stored, err)
		}
	})
}

func TestSteadyWatchRenewsImmediatelyAndCoalesces(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := &steadyProvider{fakeProvider: &fakeProvider{}}
		m, _, c := steadyFixture(t, p)
		m.opts.AgentPush = agentpush.NewStore(time.Now)
		stop := runSteadyManager(t, m)
		defer stop()
		key := agentpush.MemberKey{Namespace: domain.NormalizedNamespace(c.Namespace), Cluster: c.Name, Member: "member"}
		m.opts.AgentPush.Ingest(key, agentapi.HeartbeatRequest{Session: "first"})
		synctest.Wait()
		if got := p.renewals.Load(); got != 1 {
			t.Fatal("Watch did not renew immediately", got)
		}
		m.opts.AgentPush.Ingest(key, agentapi.HeartbeatRequest{Session: "second"})
		synctest.Wait()
		if got := p.renewals.Load(); got != 1 {
			t.Fatal("Watch bypassed the one-second renewal limit", got)
		}
		time.Sleep(time.Second)
		synctest.Wait()
		if got := p.renewals.Load(); got != 2 {
			t.Fatal("coalesced Watch was not serviced within one second", got)
		}
		if p.syncs.Load() != 1 {
			t.Fatal("healthy Watch renewal needlessly reconciled")
		}
	})
}

func TestSteadyDiscoveryFindsOperationAndGeneration(t *testing.T) {
	for _, scenario := range []string{"pending-operation", "generation-change"} {
		t.Run(scenario, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				p := &steadyProvider{fakeProvider: &fakeProvider{}}
				m, st, c := steadyFixture(t, p)
				stop := runSteadyManager(t, m)
				defer stop()
				if scenario == "pending-operation" {
					if err := st.EnqueueOperation(t.Context(), &domain.Operation{Cluster: c.Name, Kind: domain.OperationBackup}); err != nil {
						t.Fatal(err)
					}
				} else {
					stored, err := st.GetCluster(t.Context(), c.Ref())
					if err != nil {
						t.Fatal(err)
					}
					stored.Spec.Version = "updated"
					if err := st.UpdateClusterSpec(t.Context(), stored.Ref(), stored.Spec); err != nil {
						t.Fatal(err)
					}
				}
				if err := m.discover(t.Context()); err != nil {
					t.Fatal(err)
				}
				synctest.Wait()
				if p.syncs.Load() < 2 {
					t.Fatal("discovery skipped changed desired state or pending operation")
				}
			})
		})
	}
}

func TestSteadyDriftGenerationTriggersReconcile(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var changed atomic.Bool
		snapshot := &infra.TargetSnapshot{}
		snapshot.SetListing(func(context.Context) (string, error) {
			if changed.Load() {
				return "changed", nil
			}
			return "initial", nil
		})
		p := &steadyProvider{fakeProvider: &fakeProvider{}, snapshot: snapshot}
		m, _, _ := steadyFixture(t, p)
		m.opts.Namespaces = engine.StaticNamespaces{DefaultBackend: &steadySnapshotBackend{Backend: infrafake.New(infrafake.Options{}), snapshot: snapshot}}
		stop := runSteadyManager(t, m)
		defer stop()
		time.Sleep(engine.ReadinessInterval)
		synctest.Wait()
		if p.syncs.Load() != 1 {
			t.Fatal("an unchanged listing unexpectedly triggered reconciliation")
		}
		changed.Store(true)
		time.Sleep(time.Minute)
		synctest.Wait()
		if p.syncs.Load() != 2 {
			t.Fatal("listing drift did not trigger an immediate reconcile", p.syncs.Load())
		}
	})
}

func TestSteadyTriggerCannotBeOverwrittenByCompletedPass(t *testing.T) {
	p := &steadyProvider{fakeProvider: &fakeProvider{}}
	m, _, c := steadyFixture(t, p)
	w := &worker{trigger: make(chan struct{}, 1)}
	w.steady.Store(&steadyState{generation: c.Generation})
	m.workers[c.Ref().Key()] = w
	epoch := w.triggers.Load()
	m.Trigger(c.Ref())
	if w.steady.Load() != nil {
		t.Fatal("Trigger did not clear steady mode")
	}
	if m.finishSteady(t.Context(), c.Ref(), w, epoch, c.Generation, 0) {
		t.Fatal("a completed pass swallowed a concurrent trigger")
	}
	if !m.finishSteady(t.Context(), c.Ref(), w, w.triggers.Load(), c.Generation, 0) {
		t.Fatal("a subsequent healthy pass could not become steady")
	}
}

func TestSteadyHonorsGenuineTimedWakeup(t *testing.T) {
	for _, wakeup := range []time.Duration{15 * time.Second, 20 * time.Minute} {
		t.Run(wakeup.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				p := &steadyProvider{fakeProvider: &fakeProvider{}, requeue: wakeup}
				m, _, c := steadyFixture(t, p)
				stop := runSteadyManager(t, m)
				defer stop()
				if m.workerFor(t.Context(), c.Ref()).steady.Load() == nil {
					t.Fatal("a converged provider with a timed wake-up did not become steady")
				}
				delay := min(wakeup, engine.SteadyResyncInterval)
				time.Sleep(delay - time.Second)
				synctest.Wait()
				if p.syncs.Load() != 1 {
					t.Fatal("steady worker woke before the genuine deadline", p.syncs.Load())
				}
				time.Sleep(time.Second)
				synctest.Wait()
				if p.syncs.Load() != 2 {
					t.Fatal("steady worker missed its timed wake-up", p.syncs.Load())
				}
			})
		})
	}
}

type steadyCompanionProvider struct {
	*dataProvider
	steady *steadyProvider
}

func (p *steadyCompanionProvider) Sync(ctx context.Context, rc *engine.Context) (engine.Result, error) {
	return p.steady.Sync(ctx, rc)
}

func (p *steadyCompanionProvider) Status(ctx context.Context, rc *engine.Context) (domain.ClusterStatus, error) {
	return p.steady.Status(ctx, rc)
}

func TestSteadyRequiresInvokedCompanionConvergence(t *testing.T) {
	t.Setenv("KARAKORAM_ARTIFACT_MIRROR", "https://artifacts.invalid")
	for _, scenario := range []string{"disabled", "native", "pending"} {
		t.Run(scenario, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				kind := engine.DataMcpCompanion
				if scenario == "native" {
					kind = engine.DataMcpNative
				}
				p := &steadyCompanionProvider{
					dataProvider: &dataProvider{fakeProvider: &fakeProvider{}, kind: kind},
					steady:       &steadyProvider{fakeProvider: &fakeProvider{}},
				}
				m, st := newSteadyTestManager(t, p)
				m.opts.Context.ObservationEpoch = "companion-test"
				m.opts.Namespaces = engine.StaticNamespaces{DefaultBackend: infrafake.New(infrafake.Options{})}
				c := &domain.Cluster{Name: "demo",
					Spec:   domain.ClusterSpec{Provider: "fake", DataMcp: &domain.DataMcpSpec{Enabled: scenario != "disabled"}},
					Status: domain.ClusterStatus{Phase: domain.PhaseReady, ObservedGeneration: 1},
				}
				if err := st.CreateCluster(t.Context(), c); err != nil {
					t.Fatal(err)
				}
				stop := runSteadyManager(t, m)
				defer stop()
				got := m.workerFor(t.Context(), c.Ref()).steady.Load() != nil
				if got != (scenario != "pending") {
					t.Fatalf("companion %s: steady=%t", scenario, got)
				}
				if scenario == "pending" {
					raw, err := st.GetClusterSecret(t.Context(), c.Ref(), "datamcp/state")
					if err != nil {
						t.Fatal("pending companion did not persist its provisioning receipt", err)
					}
					var receipt struct {
						Stage string `json:"stage"`
					}
					if err := json.Unmarshal(raw, &receipt); err != nil || receipt.Stage != "import" {
						t.Fatal("companion did not reach genuine pending import work", string(raw), err)
					}
				}
			})
		})
	}
}

type steadySnapshotBackend struct {
	infra.Backend
	snapshot *infra.TargetSnapshot
}

func (b *steadySnapshotBackend) PlatformSnapshot() *infra.TargetSnapshot { return b.snapshot }

func TestSteadyDriftBeforeFirstRenewalTriggersSync(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var changed atomic.Bool
		snapshot := &infra.TargetSnapshot{}
		snapshot.SetListing(func(context.Context) (string, error) {
			if changed.Load() {
				return "changed", nil
			}
			return "initial", nil
		})
		p := &steadyProvider{fakeProvider: &fakeProvider{}, snapshot: snapshot}
		m, _, c := steadyFixture(t, p)
		m.opts.Namespaces = engine.StaticNamespaces{DefaultBackend: &steadySnapshotBackend{Backend: infrafake.New(infrafake.Options{}), snapshot: snapshot}}
		probe := infra.WithSteadyObservation(infra.ObservationContext(t.Context()))
		snapshot.Lookup(probe, "cluster")
		w := &worker{trigger: make(chan struct{}, 1)}
		m.workers[c.Ref().Key()] = w
		drift := snapshot.DriftGeneration()
		if _, err := m.reconcile(t.Context(), c.Ref()); err != nil {
			t.Fatal(err)
		}
		if !m.finishSteady(t.Context(), c.Ref(), w, w.triggers.Load(), c.Generation, drift) {
			t.Fatal("converged cluster did not become steady")
		}
		time.Sleep(infra.SnapshotListingInterval)
		changed.Store(true)
		snapshot.Lookup(probe, "cluster")
		if snapshot.DriftGeneration() == 0 {
			t.Fatal("fixture did not detect listing drift before the first renewal")
		}
		if err := m.renewAvailability(t.Context(), c.Ref()); err != nil {
			t.Fatal(err)
		}
		if w.steady.Load() != nil || len(w.trigger) != 1 {
			t.Fatal("the first renewal swallowed drift instead of triggering reconciliation")
		}
		stop := runSteadyManager(t, m)
		defer stop()
		if p.syncs.Load() < 2 {
			t.Fatal("first-renewal drift did not run Sync")
		}
	})
}

type steadyEntryDriftProvider struct {
	*steadyProvider
	afterFinalObservation func()
}

func (p *steadyEntryDriftProvider) Status(ctx context.Context, rc *engine.Context) (domain.ClusterStatus, error) {
	status, err := p.steadyProvider.Status(ctx, rc)
	if !rc.AvailabilityOnly && p.syncs.Load() == 1 {
		p.afterFinalObservation()
	}
	return status, err
}

func TestSteadyDriftAfterFinalObservationKeepsReadinessCadence(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var changed atomic.Bool
		snapshot := &infra.TargetSnapshot{}
		snapshot.SetListing(func(context.Context) (string, error) {
			if changed.Load() {
				return "changed", nil
			}
			return "initial", nil
		})
		probe := infra.WithSteadyObservation(infra.ObservationContext(t.Context()))
		snapshot.Lookup(probe, "sibling")
		// Expire the initial listing before starting the worker, so the sibling
		// can detect drift synchronously after the final fresh Status result.
		time.Sleep(infra.SnapshotListingInterval)
		initialDrift := snapshot.DriftGeneration()
		p := &steadyEntryDriftProvider{
			steadyProvider: &steadyProvider{fakeProvider: &fakeProvider{}, snapshot: snapshot},
			afterFinalObservation: func() {
				changed.Store(true)
				snapshot.Lookup(probe, "sibling")
			},
		}
		m, st := newSteadyTestManager(t, p)
		m.opts.Context.ObservationEpoch = "steady-entry-drift-test"
		m.opts.Namespaces = engine.StaticNamespaces{DefaultBackend: &steadySnapshotBackend{Backend: infrafake.New(infrafake.Options{}), snapshot: snapshot}}
		c := &domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: "fake"}, Status: domain.ClusterStatus{Phase: domain.PhaseReady, ObservedGeneration: 1}}
		if err := st.CreateCluster(t.Context(), c); err != nil {
			t.Fatal(err)
		}
		stop := runSteadyManager(t, m)
		defer stop()
		w := m.workerFor(t.Context(), c.Ref())
		drift := snapshot.DriftGeneration()
		if drift == initialDrift {
			t.Fatal("fixture did not detect sibling listing drift after the final observation")
		}
		if p.syncs.Load() != 1 || w.steady.Load() != nil {
			t.Fatal("the converging pass absorbed drift into its steady baseline")
		}
		time.Sleep(engine.ReadinessInterval - time.Nanosecond)
		synctest.Wait()
		if p.syncs.Load() != 1 {
			t.Fatal("drift-rejected pass reconciled before the normal readiness cadence")
		}
		time.Sleep(time.Nanosecond)
		synctest.Wait()
		if p.syncs.Load() != 2 {
			t.Fatal("drift-rejected pass did not reconcile at the normal readiness cadence", p.syncs.Load())
		}
		if steady := w.steady.Load(); steady == nil || steady.drift != drift {
			t.Fatal("fresh follow-up pass did not enter steady with the new drift baseline", steady)
		}
		time.Sleep(engine.ReadinessInterval)
		synctest.Wait()
		if p.syncs.Load() != 2 || w.steady.Load() == nil {
			t.Fatal("unchanged drift did not retain steady mode after the fresh follow-up pass")
		}
	})
}
