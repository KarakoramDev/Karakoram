// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package rabbitmq

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/reconciler"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

func TestRollingStartupTrackedOperation(t *testing.T) {
	for _, neverStarts := range []bool{false, true} {
		name := "recovers_after_eight_minutes"
		if neverStarts {
			name = "never_starts"
		}
		t.Run(name, func(t *testing.T) {
			p, rc, _, agents := setup(t)
			ready(t, p, rc)
			s := state(t, rc)
			m := ordered(s)[0]
			rc.Cluster.Generation++
			rc.Cluster.Spec.EngineConfig = "consumer_timeout = 1800000"
			// Peers have already applied this config; only the final member is
			// still restarting. Keep real all-peer membership convergence gates.
			for _, peer := range ordered(s)[1:] {
				peer.EngineConfig = rc.Cluster.Spec.EngineConfig
			}
			beginChange(rc, s, m, "config")
			m.EngineConfig = m.TargetConfig
			m.ChangeStep = "reload"
			m.ChangeDisruptedAt = rc.Clock.Now()
			if err := s.save(rc.Cluster); err != nil {
				t.Fatal(err)
			}
			st := rc.Store.(*store.Store)
			if err := st.UpdateClusterState(t.Context(), rc.Cluster); err != nil {
				t.Fatal(err)
			}
			if err := st.UpdateClusterSpec(t.Context(), rc.Cluster.Ref(), rc.Cluster.Spec); err != nil {
				t.Fatal(err)
			}
			op := &domain.Operation{Cluster: rc.Cluster.Name, Kind: domain.OperationUpdate, TargetGeneration: rc.Cluster.Generation}
			if err := st.EnqueueOperation(t.Context(), op); err != nil {
				t.Fatal(err)
			}
			rc.Agents = rollingColdAgents{AgentDialer: agents, member: m.Name}
			registry, err := engine.NewRegistry(p)
			if err != nil {
				t.Fatal(err)
			}
			started := rc.Clock.Now()
			// Each manager runs one serialized observation. Stopping it before moving
			// the fixture clock keeps simulated time independent of worker scheduling.
			for elapsed := time.Duration(0); elapsed <= 11*time.Minute; elapsed += engine.ReadinessInterval {
				rc.Clock.(*testClock).at = started.Add(elapsed)
				if !neverStarts && elapsed >= 8*time.Minute {
					rc.Agents = agents
				}
				manager, err := reconciler.New(reconciler.Options{Store: st, Registry: registry, Context: *rc, ResyncInterval: time.Hour, OperationTimeouts: map[string]time.Duration{domain.OperationUpdate: 10 * time.Minute}})
				if err != nil {
					t.Fatal(err)
				}
				before, err := st.GetOperation(t.Context(), op.ID)
				if err != nil {
					t.Fatal(err)
				}
				// Managers are restarted between samples, so seed persisted
				// observed execution time explicitly; fake wall time must not
				// advance the production monotonic operation clock.
				before.Elapsed = elapsed
				if err := st.UpdateOperation(t.Context(), before); err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithCancel(t.Context())
				done := make(chan error, 1)
				go func() { done <- manager.Run(ctx) }()
				deadline := time.Now().Add(5 * time.Second)
				var actual *domain.Operation
				for {
					actual, err = st.GetOperation(t.Context(), op.ID)
					if err != nil {
						cancel()
						<-done
						t.Fatal(err)
					}
					if actual.Attempts > before.Attempts || actual.Phase == domain.PhaseFailed {
						break
					}
					if time.Now().After(deadline) {
						cancel()
						<-done
						t.Fatal("no reconciliation observation")
					}
					time.Sleep(time.Millisecond)
				}
				cancel()
				if err := <-done; err != nil {
					t.Fatal(err)
				}
				rc.Cluster, err = st.GetCluster(t.Context(), rc.Cluster.Ref())
				if err != nil {
					t.Fatal(err)
				}
				if actual.Phase == domain.PhaseCancelled {
					t.Fatalf("operation cancelled: %+v", actual)
				}
				if actual.Phase == domain.PhaseFailed {
					if !neverStarts {
						t.Fatalf("normal startup failed at %s: %+v", elapsed, actual)
					}
					if elapsed < 10*time.Minute || !strings.Contains(actual.Message, "deadline") || !strings.Contains(actual.Message, m.Name) {
						t.Fatalf("startup failed without bounded actionable diagnostic at %s: %+v", elapsed, actual)
					}
					return
				}
				if neverStarts && elapsed < 10*time.Minute && actual.Failures != 0 {
					t.Fatalf("expected startup consumed retry budget at %s: %+v", elapsed, actual)
				}
				if neverStarts && elapsed < 10*time.Minute && !strings.Contains(actual.Message, "rolling startup") {
					t.Fatalf("startup diagnostic lost: %+v", actual)
				}
				if actual.Phase == domain.PhaseSucceeded {
					if neverStarts || elapsed < 8*time.Minute || rc.Cluster.Status.ObservedGeneration != rc.Cluster.Generation || rc.Cluster.Status.Phase != domain.PhaseReady {
						t.Fatalf("unready operation succeeded at %s", elapsed)
					}
					return
				}
			}
			final, _ := st.GetCluster(t.Context(), rc.Cluster.Ref())
			actual, _ := st.GetOperation(t.Context(), op.ID)
			t.Fatalf("startup did not reach a terminal operation result: operation=%+v status=%+v internal=%s", actual, final.Status, final.Internal)
		})
	}
}
