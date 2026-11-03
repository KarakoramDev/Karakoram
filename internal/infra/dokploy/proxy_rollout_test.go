// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package dokploy

import (
	"fmt"
	"net/http"
	"reflect"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/infra"
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

func proxyUpgradeDeployment(t *testing.T) infra.Deployment {
	t.Helper()
	d := testDeployment(t, "")
	d.Runtime.Env = map[string]string{"KARAKORAM_PROXY_SERVING_PROBE": "1", "KARAKORAM_TEST_BUILD": "next"}
	return d
}

func TestProxyAgentUpgradeDeployPreservesServing(t *testing.T) {
	for _, count := range []int{1, 3} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			f, b, a := markedProxyFake(t, count)
			op, err := b.Deploy(t.Context(), "proxy", proxyUpgradeDeployment(t))
			if err != nil {
				t.Fatal(err)
			}
			o, err := decodeOp(op)
			if err != nil || !o.HealthGated || o.ExpectedReplicas != count {
				t.Fatalf("missing serving deployment receipt: %+v %v", o, err)
			}
			row := fakeAppSnapshot(t, f, "proxy")
			for _, key := range []string{"updateConfigSwarm", "rollbackConfigSwarm"} {
				policy := row[key].(map[string]any)
				if fakeInt(policy, "Parallelism") != 1 || policy["Order"] != "start-first" || policy["FailureAction"] != "pause" || fakeInt(policy, "MaxFailureRatio") != 0 {
					t.Fatalf("unsafe %s: %+v", key, policy)
				}
			}
			probe := row["healthCheckSwarm"].(map[string]any)
			if !reflect.DeepEqual(probe["Test"], []any{"CMD", layout.AppDir + "/karakoram", "agent", "readiness", "--serving-proxy", "--data-dir=" + layout.DataDir("")}) {
				t.Fatalf("wrong serving healthcheck: %+v", probe)
			}
			awaitFake(t, b, op)
			assertProxyNeverAllDown(t, f, a)
		})
	}
}

func TestProxyAgentUpgradeDeployPausedOrRollbackFails(t *testing.T) {
	for _, condition := range []string{"paused", "rollback_started", "rollback_paused", "rollback_completed"} {
		t.Run(condition, func(t *testing.T) {
			f, b, a := markedProxyFake(t, 1)
			f.mu.Lock()
			a.neverHealthyProxy = true
			f.mu.Unlock()
			op, err := b.Deploy(t.Context(), "proxy", proxyUpgradeDeployment(t))
			if err != nil {
				t.Fatal(err)
			}
			// Finish the build without committing its backend receipt.
			f.mu.Lock()
			for range 2 {
				f.advanceDeployments(a)
			}
			a.holdTasks = true
			f.services[fakeString(a.row, "appName")]["UpdateStatus"] = map[string]any{"State": condition}
			f.mu.Unlock()
			state, err := b.OpStatus(t.Context(), op)
			if err != nil || !state.Failed || state.Done {
				t.Fatalf("unsafe update accepted: %+v %v", state, err)
			}
			if pending := fakeMeta(t, f, "proxy").Pending; pending == nil || !pending.Failed {
				t.Fatal("deployment failure was not durable")
			}
			assertProxyNeverAllDown(t, f, a)
		})
	}
}

func TestProxyAgentUpgradeLostUploadReplyAdoptsRollout(t *testing.T) {
	f, b, a := markedProxyFake(t, 2)
	loseCommittedReply(b, func(r *http.Request) bool {
		return r.URL.Path == "/api/application.dropDeployment"
	})
	if _, err := b.Deploy(t.Context(), "proxy", proxyUpgradeDeployment(t)); err == nil {
		t.Fatal("missing lost upload reply")
	}
	original := fakeMeta(t, f, "proxy").Pending
	restarted := fakeBackend(t, f, nil)
	op, err := restarted.Deploy(t.Context(), "proxy", proxyUpgradeDeployment(t))
	if err != nil {
		t.Fatal(err)
	}
	recovered, _ := decodeOp(op)
	if original == nil || !original.HealthGated || !sameOperation(original, recovered) || fakeCalls(f, "application.dropDeployment") != 2 {
		t.Fatal("lost reply duplicated the proxy deployment")
	}
	awaitFake(t, restarted, op)
	assertProxyNeverAllDown(t, f, a)
}

func TestLegacyProxyAgentUpgradeInstallsMarkerWithoutServingClaim(t *testing.T) {
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	importFake(t, b, runtimeSpec("proxy", ""))
	deployFake(t, b, "proxy", "")
	op, err := b.Deploy(t.Context(), "proxy", proxyUpgradeDeployment(t))
	if err != nil {
		t.Fatal(err)
	}
	o, _ := decodeOp(op)
	if o.HealthGated || fakeAppSnapshot(t, f, "proxy")["healthCheckSwarm"] != nil {
		t.Fatal("legacy upgrade claimed serving preservation")
	}
	awaitFake(t, b, op)
	preserved, err := b.ProxyUpdatesPreserveServing(t.Context(), "proxy")
	if err != nil || !preserved {
		t.Fatalf("successful agent upgrade did not enable later serving updates: %v %v", preserved, err)
	}
}

func TestProxyAgentUpgradeFrozenRedeployDoesNotChangePolicy(t *testing.T) {
	f, b, _ := markedProxyFake(t, 1)
	d := proxyUpgradeDeployment(t)
	serving := false
	d.ProxyPreserveServing = &serving
	op, err := b.Deploy(t.Context(), "proxy", d)
	if err != nil {
		t.Fatal(err)
	}
	o, _ := decodeOp(op)
	if o.HealthGated || fakeAppSnapshot(t, f, "proxy")["healthCheckSwarm"] != nil {
		t.Fatal("frozen redeploy choice silently became a serving rollout")
	}
	awaitFake(t, b, op)
}

func TestProxyAgentUpgradeServingRequestRejectsLegacyRuntime(t *testing.T) {
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	importFake(t, b, runtimeSpec("proxy", ""))
	deployFake(t, b, "proxy", "")
	d := proxyUpgradeDeployment(t)
	serving := true
	d.ProxyPreserveServing = &serving
	if _, err := b.Deploy(t.Context(), "proxy", d); err == nil {
		t.Fatal("unsupported serving request silently used ordinary redeploy")
	}
	if fakeCalls(f, "application.dropDeployment") != 1 {
		t.Fatal("unsupported serving request uploaded a build")
	}
}

func TestProxyDeploymentLookupNeverSubmits(t *testing.T) {
	for _, include := range []bool{false, true} {
		for _, outcome := range []string{"updating", "paused", "not-submitted"} {
			t.Run(fmt.Sprintf("%s/include=%v", outcome, include), func(t *testing.T) {
				f, b, a := markedProxyFake(t, 2)
				d := proxyUpgradeDeployment(t)
				if include {
					d.Runtime.Env = map[string]string{"KARAKORAM_PROXY_SERVING_PROBE": "1"}
				}
				d.RequestID = "agent-upgrade"
				serving := true
				d.ProxyPreserveServing = &serving
				if outcome != "not-submitted" {
					loseCommittedReply(b, func(r *http.Request) bool {
						return r.URL.Path == "/api/application.dropDeployment"
					})
					if _, err := b.Deploy(t.Context(), "proxy", d); err == nil {
						t.Fatal("missing lost upload reply")
					}
				}
				if outcome == "paused" {
					f.mu.Lock()
					for range 2 {
						f.advanceDeployments(a)
					}
					a.holdTasks = true
					f.services[fakeString(a.row, "appName")]["UpdateStatus"] = map[string]any{"State": "paused"}
					f.mu.Unlock()
				}
				uploads := fakeCalls(f, "application.dropDeployment")
				updates := fakeCalls(f, "application.update")
				restarted := fakeBackend(t, f, nil)
				op, err := restarted.LookupProxyDeployment(t.Context(), "proxy", d.RequestID)
				if err != nil {
					t.Fatal(err)
				}
				if outcome == "not-submitted" {
					if op != nil {
						t.Fatal("lookup invented a deployment receipt")
					}
				} else {
					if op == nil {
						t.Fatal("lookup lost the accepted deployment")
					}
					o, err := decodeOp(*op)
					if err != nil || o.RequestID != d.RequestID || !o.HealthGated {
						t.Fatalf("lookup did not recover exact rollout identity: %+v %v", o, err)
					}
					state, err := restarted.OpStatus(t.Context(), *op)
					if err != nil || state.Done || state.Failed != (outcome == "paused") {
						t.Fatalf("wrong recovered rollout status: %+v %v", state, err)
					}
				}
				// OpStatus may persist the failure, but lookup itself never writes.
				if fakeCalls(f, "application.dropDeployment") != uploads || fakeCalls(f, "application.reload") != 0 || fakeCalls(f, "application.stop") != 0 {
					t.Fatal("non-submitting lookup delivered a mutation")
				}
				if outcome != "paused" && fakeCalls(f, "application.update") != updates {
					t.Fatal("non-submitting lookup changed application policy")
				}
			})
		}
	}
}

func TestProxyDeploymentLookupRejectsMismatchedRequest(t *testing.T) {
	f, b, _ := markedProxyFake(t, 1)
	d := proxyUpgradeDeployment(t)
	d.RequestID = "original-upgrade"
	if _, err := b.Deploy(t.Context(), "proxy", d); err != nil {
		t.Fatal(err)
	}
	uploads := fakeCalls(f, "application.dropDeployment")
	updates := fakeCalls(f, "application.update")
	for _, request := range []string{"", "different-upgrade"} {
		if _, err := b.LookupProxyDeployment(t.Context(), "proxy", request); err == nil {
			t.Fatal("lookup adopted an unrelated pending deployment")
		}
	}
	if fakeCalls(f, "application.dropDeployment") != uploads || fakeCalls(f, "application.update") != updates {
		t.Fatal("mismatched lookup submitted or changed work")
	}
}
