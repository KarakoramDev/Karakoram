// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package dokploy

import (
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"testing"
	"time"
)

func markedProxyFake(t *testing.T, count int) (*fakeAPI, *Backend, *fakeApplication) {
	t.Helper()
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	spec := runtimeSpec("proxy", "")
	spec.MinContainers, spec.MaxContainers = count, count
	imported := importFake(t, b, spec)
	d := testDeployment(t, "")
	d.Runtime.Env = map[string]string{"KARAKORAM_PROXY_SERVING_PROBE": "1"}
	op, err := b.Deploy(t.Context(), "proxy", d)
	if err != nil {
		t.Fatal(err)
	}
	awaitFake(t, b, op)
	f.mu.Lock()
	a := f.apps[imported.Services["proxy"]]
	for _, task := range a.tasks {
		if task["state"] == "running" {
			task["health"] = "healthy"
		}
	}
	f.mu.Unlock()
	preserved, err := b.ProxyUpdatesPreserveServing(t.Context(), "proxy")
	if err != nil || !preserved {
		t.Fatalf("marked runtime not serving-capable: %v %v", preserved, err)
	}
	return f, b, a
}

func assertProxyNeverAllDown(t *testing.T, f *fakeAPI, a *fakeApplication) {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(a.healthyHistory) == 0 {
		t.Fatal("fake did not observe a health-gated rollout")
	}
	for step, healthy := range a.healthyHistory {
		if healthy < 1 {
			t.Fatalf("step %d stopped all healthy proxy tasks: %v", step, a.healthyHistory)
		}
	}
	if fakeCallsUnlocked(f, "application.stop") != 0 {
		t.Fatal("proxy update stopped the service")
	}
}
func fakeCallsUnlocked(f *fakeAPI, proc string) int { return f.calls[proc] }

func TestProxyUpdatesKeepHealthyServingThroughout(t *testing.T) {
	for _, action := range []string{"up", "down", "singleton-down", "resources"} {
		t.Run(action, func(t *testing.T) {
			count, target := 1, 3
			if action == "down" {
				count, target = 3, 2
			}
			if action == "singleton-down" {
				count, target = 2, 1
			}
			f, b, a := markedProxyFake(t, count)
			f.mu.Lock()
			a.taskCapacity = max(count, target) + 1
			f.mu.Unlock()
			var op infra.Op
			var err error
			if action == "resources" {
				op, err = b.SetResources(t.Context(), "proxy", infra.Resources{MinCPU: 2, MaxCPU: 2, MinRAM: 1, MaxRAM: 1})
			} else {
				op, err = b.SetContainerCount(t.Context(), "proxy", target, target)
			}
			if err != nil {
				t.Fatal(err)
			}
			state, err := b.OpStatus(t.Context(), op)
			if err != nil || state.Done || state.Failed {
				t.Fatalf("starting replacement admitted early: %+v %v", state, err)
			}
			awaitFake(t, b, op)
			assertProxyNeverAllDown(t, f, a)
			f.mu.Lock()
			defer f.mu.Unlock()
			for _, order := range a.reloadOrders {
				if order != "start-first" {
					t.Fatalf("proxy update used %s", order)
				}
			}
			for _, task := range a.tasks {
				if task["state"] == "running" && task["health"] != "healthy" {
					t.Fatalf("unhealthy running task admitted: %+v", task)
				}
			}
		})
	}
}

func TestProxyNeverHealthyReplacementPausesWithIncumbents(t *testing.T) {
	f, b, a := markedProxyFake(t, 1)
	f.mu.Lock()
	a.neverHealthyProxy = true
	f.mu.Unlock()
	op, err := b.SetResources(t.Context(), "proxy", infra.Resources{MinCPU: 2, MaxCPU: 2})
	if err != nil {
		t.Fatal(err)
	}
	var state infra.OpState
	for range 5 {
		state, err = b.OpStatus(t.Context(), op)
		if err != nil {
			t.Fatal(err)
		}
		if state.Failed {
			break
		}
	}
	if !state.Failed || state.Done {
		t.Fatalf("never-healthy rollout did not fail: %+v", state)
	}
	assertProxyNeverAllDown(t, f, a)
	if fakeMeta(t, f, "proxy").Pending == nil || !fakeMeta(t, f, "proxy").Pending.Failed {
		t.Fatal("failure receipt not durable")
	}
}

func TestProxyNoSurgeCapacityFailsWithoutStoppingIncumbent(t *testing.T) {
	f, b, a := markedProxyFake(t, 1)
	f.mu.Lock()
	a.taskCapacity = 1
	f.mu.Unlock()
	op, err := b.SetContainerCount(t.Context(), "proxy", 2, 2)
	if err != nil {
		t.Fatal(err)
	}
	o, _ := decodeOp(op)
	b.now = func() time.Time { return o.Issued.Add(taskStartupWindow + time.Second) }
	state, err := b.OpStatus(t.Context(), op)
	if err != nil || !state.Failed || state.Done {
		t.Fatalf("capacity failure: %+v %v", state, err)
	}
	assertProxyNeverAllDown(t, f, a)
}

func TestProxyLostReloadReplyAdoptsExactHealthyRollout(t *testing.T) {
	for _, action := range []string{"scale", "resources"} {
		t.Run(action, func(t *testing.T) {
			f, b, a := markedProxyFake(t, 1)
			f.mu.Lock()
			f.failureOnce["application.reload"] = fakeResponse{500, "INTERNAL_SERVER_ERROR", "lost reply"}
			f.failureEffect["application.reload"] = func() { f.beginProxyRollout(a) }
			f.mu.Unlock()
			submit := func(b *Backend) (infra.Op, error) {
				if action == "scale" {
					return b.SetContainerCount(t.Context(), "proxy", 2, 2)
				}
				return b.SetResources(t.Context(), "proxy", infra.Resources{MinCPU: 2, MaxCPU: 2})
			}
			if _, err := submit(b); err == nil {
				t.Fatal("lost reply not injected")
			}
			original := fakeMeta(t, f, "proxy").Pending
			if original == nil || !original.HealthGated {
				t.Fatal("missing pre-delivery serving receipt")
			}
			restarted := fakeBackend(t, f, nil)
			op, err := submit(restarted)
			if err != nil {
				t.Fatal(err)
			}
			recovered, _ := decodeOp(op)
			if !sameOperation(original, recovered) || fakeCalls(f, "application.reload") != 1 {
				t.Fatal("lost reply enqueued a duplicate rollout")
			}
			awaitFake(t, restarted, op)
			assertProxyNeverAllDown(t, f, a)
		})
	}
}

func TestLegacyProxyDoesNotClaimServingUpdates(t *testing.T) {
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	importFake(t, b, runtimeSpec("proxy", ""))
	deployFake(t, b, "proxy", "")
	preserved, err := b.ProxyUpdatesPreserveServing(t.Context(), "proxy")
	if err != nil || preserved {
		t.Fatalf("legacy runtime claimed serving updates: %v %v", preserved, err)
	}
	op, err := b.SetContainerCount(t.Context(), "proxy", 2, 2)
	if err != nil {
		t.Fatal(err)
	}
	awaitFake(t, b, op)
	if row := fakeAppSnapshot(t, f, "proxy"); row["healthCheckSwarm"] != nil {
		t.Fatal("legacy image received unsupported healthcheck")
	}
}

func TestProxyRolloutDoesNotCommitWithOldTaskStillRunning(t *testing.T) {
	f, b, a := markedProxyFake(t, 1)
	op, err := b.SetContainerCount(t.Context(), "proxy", 2, 2)
	if err != nil {
		t.Fatal(err)
	}
	// Advance the platform without committing the backend receipt, then expose a
	// still-draining old task even though the target's new tasks are all healthy.
	f.mu.Lock()
	for range 5 {
		f.advanceProxyRollout(a)
	}
	old := a.proxyRollout.old[0]
	old["state"] = "shutdown"
	old["currentState"] = "Running 1 second ago"
	a.holdTasks = true
	f.mu.Unlock()
	state, err := b.OpStatus(t.Context(), op)
	if err != nil || state.Done || state.Failed {
		t.Fatalf("old task still running committed: %+v %v", state, err)
	}
}

func TestProxyColdRedeployClearsServingGateAndLegacyCapability(t *testing.T) {
	f, b, _ := markedProxyFake(t, 1)
	op, err := b.SetContainerCount(t.Context(), "proxy", 2, 2)
	if err != nil {
		t.Fatal(err)
	}
	awaitFake(t, b, op)
	op, err = b.Deploy(t.Context(), "proxy", testDeployment(t, ""))
	if err != nil {
		t.Fatal(err)
	}
	if row := fakeAppSnapshot(t, f, "proxy"); row["healthCheckSwarm"] != nil {
		t.Fatal("cold deployment retained a native serving admission barrier")
	}
	awaitFake(t, b, op)
	preserved, err := b.ProxyUpdatesPreserveServing(t.Context(), "proxy")
	if err != nil || preserved {
		t.Fatalf("unmarked redeployment retained serving capability: %v %v", preserved, err)
	}
}

func TestProxyRollbackOrStaleSpecCannotCommit(t *testing.T) {
	for _, condition := range []string{"rollback", "stale-spec"} {
		t.Run(condition, func(t *testing.T) {
			f, b, a := markedProxyFake(t, 1)
			op, err := b.SetContainerCount(t.Context(), "proxy", 2, 2)
			if err != nil {
				t.Fatal(err)
			}
			f.mu.Lock()
			a.holdTasks = true
			service := f.services[fakeString(a.row, "appName")]
			if condition == "rollback" {
				service["UpdateStatus"] = map[string]any{"State": "rollback_completed"}
			} else {
				service["Spec"].(map[string]any)["TaskTemplate"].(map[string]any)["ForceUpdate"] = 1000
			}
			f.mu.Unlock()
			state, err := b.OpStatus(t.Context(), op)
			if err != nil || !state.Failed || state.Done {
				t.Fatalf("unsafe receipt committed: %+v %v", state, err)
			}
			assertProxyNeverAllDown(t, f, a)
		})
	}
}
