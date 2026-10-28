// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package reconciler

import (
	"context"
	"encoding/json"
	"testing"
	"testing/synctest"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

func TestAvailabilityRenewsNonReadyLifecycleDuringSlowSync(t *testing.T) {
	for _, phase := range []domain.Phase{domain.PhaseCreating, domain.PhaseInitializing, domain.PhaseUpdating,
		domain.PhaseScaling, domain.PhaseUpgrading, domain.PhaseError, domain.PhaseFailed} {
		for _, dead := range []bool{false, true} {
			t.Run(string(phase)+map[bool]string{false: "/healthy", true: "/dead-agent"}[dead], func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					p := &blockedSyncProvider{fakeProvider: &fakeProvider{}, started: make(chan struct{}), duration: 28 * time.Second}
					m, st := newTestManager(t, p)
					m.opts.Context.ObservationEpoch = "current-process"
					at := m.opts.Context.Clock.Now()
					c := &domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: "fake"}, Internal: json.RawMessage(`{"checkpoint":true}`),
						Status: domain.ClusterStatus{Phase: phase, Message: "Lifecycle change deferred by maintenance", ObservedGeneration: 0,
							ObservedAt: at, ObservationEpoch: "current-process",
							Members: []domain.MemberStatus{{Name: "member", Health: true, Agent: domain.AgentStatus{Healthy: true, LastSeen: at}}},
							Conditions: []domain.Condition{
								{Type: "Ready", Status: "False", Reason: "WaitingForWindow", LastTransitionTime: at},
								{Type: "MaintenanceWindow", Status: "False", Reason: "WaitingForWindow", LastTransitionTime: at},
							}}}
					if err := st.CreateCluster(t.Context(), c); err != nil {
						t.Fatal(err)
					}
					// This is a desired-state convergence tracker, not an imperative
					// operation owning native mutation/checkpoint observation.
					op := &domain.Operation{ID: "deferred-update", Cluster: c.Name, Kind: domain.OperationUpdate,
						Phase: domain.PhaseRunning, TargetGeneration: c.Generation}
					if err := st.EnqueueOperation(t.Context(), op); err != nil {
						t.Fatal(err)
					}
					ctx, cancel := context.WithCancel(t.Context())
					w := &worker{trigger: make(chan struct{}, 1)}
					m.wg.Add(1)
					go m.work(ctx, c.Ref(), w)
					defer func() { cancel(); m.wg.Wait() }()
					<-p.started
					p.unavailable.Store(dead)
					for second := 1; second <= 27; second++ {
						time.Sleep(time.Second)
						synctest.Wait()
						stored, err := st.GetCluster(t.Context(), c.Ref())
						if err != nil {
							t.Fatal(err)
						}
						fresh := engine.FreshStatus(stored.Status, m.opts.Context.Clock.Now())
						if fresh.Phase != phase || fresh.Message != c.Status.Message || fresh.ObservedGeneration != 0 || stored.Generation != c.Generation || string(stored.Internal) != `{"checkpoint":true}` {
							t.Fatalf("renewal changed lifecycle/intent at %ds: %+v", second, fresh)
						}
						if !dead && (!fresh.Members[0].Health || !fresh.Members[0].Agent.Healthy || fresh.ObservationEpoch != "current-process") {
							t.Fatalf("non-ready availability starved behind slow Sync at %ds: %+v", second, fresh)
						}
						if dead && second > int(engine.ReadinessMaxAge/time.Second) && (fresh.Members[0].Health || fresh.Members[0].Agent.Healthy || !fresh.ObservedAt.IsZero()) {
							t.Fatalf("failed observation renewed a positive lease at %ds: %+v", second, fresh)
						}
						ready, maintenance := false, false
						for _, condition := range fresh.Conditions {
							if condition.Type == "Ready" {
								ready = condition.Status == "False" && condition.Reason == "WaitingForWindow"
							}
							if condition.Type == "MaintenanceWindow" {
								maintenance = condition.Status == "False" && condition.Reason == "WaitingForWindow"
							}
						}
						if !ready || !maintenance {
							t.Fatal("availability completed deferred readiness or discarded its maintenance receipt", fresh.Conditions)
						}
					}
					if p.observations.Load() < 6 {
						t.Fatal("renewal did not run independently during the 28s Sync")
					}
					tracked, err := st.GetOperation(t.Context(), op.ID)
					if err != nil || tracked.Phase != domain.PhaseRunning || tracked.TargetGeneration != c.Generation {
						t.Fatal("availability completed the deferred convergence tracker", tracked, err)
					}
					// Complete the slow ordinary call, rather than only canceling
					// a blocked fixture. Its checkpoint remains authoritative.
					time.Sleep(2 * time.Second)
					synctest.Wait()
					completed, err := st.GetCluster(t.Context(), c.Ref())
					if err != nil || completed.Status.ObservedGeneration != 0 || string(completed.Internal) != `{"sync":"unpublished"}` {
						t.Fatal("renewal stole the ordinary Sync checkpoint or generation", completed, err)
					}
				})
			})
		}
	}
}

func TestNonReadyAvailabilityKeepsBoundedRequeue(t *testing.T) {
	p := &operationProvider{fakeProvider: &fakeProvider{}}
	m, st := newTestManager(t, p)
	at := m.opts.Context.Clock.Now()
	c := &domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: "fake"},
		Status: domain.ClusterStatus{Phase: domain.PhaseInitializing, Members: []domain.MemberStatus{{Name: "member"}}}}
	if err := st.CreateCluster(t.Context(), c); err != nil {
		t.Fatal(err)
	}
	p.syncFn = func(*engine.Context) (engine.Result, error) { return engine.Result{RequeueAfter: time.Minute}, nil }
	p.statusFn = func(*engine.Context) (domain.ClusterStatus, error) {
		return domain.ClusterStatus{Phase: domain.PhaseInitializing,
			Members: []domain.MemberStatus{{Name: "member", Health: true, Agent: domain.AgentStatus{Healthy: true, LastSeen: at}}}}, nil
	}
	rc := m.opts.Context
	rc.Cluster = c
	// A stable fixture clock keeps the positive member sample current for both
	// preliminary and final observations, without changing scheduling budgets.
	rc.Clock = fixedClock{now: at}
	result, err := m.synchronize(t.Context(), p, &rc, nil)
	if err != nil || result.RequeueAfter != engine.ReadinessInterval || c.Status.ObservedGeneration != 0 {
		t.Fatal("non-ready health outlives a minute-long ordinary requeue", result, err)
	}
}
