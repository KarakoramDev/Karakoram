// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package dokploy

import (
	"fmt"
	"strings"
	"testing"

	model "github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres"
	"github.com/KarakoramDev/Karakoram/internal/engine/supabase"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

// Exercise the real provider receipt, both observation orders and reconstruction.
// More than one failed replacement must not weaken Swarm's terminal authority.
func TestOrdinaryProxyTerminalObservation(t *testing.T) {
	for _, action := range []string{"scale", "resources"} {
		for _, condition := range []string{"paused", "rollback_started", "rollback_paused", "rollback_completed", "stale-spec"} {
			for _, lookupFirst := range []bool{false, true} {
				for _, failedTasks := range []int{1, 3} {
					t.Run(fmt.Sprintf("%s/%s/lookup=%t/failed=%d", action, condition, lookupFirst, failedTasks), func(t *testing.T) {
						f, b, a := markedProxyFake(t, 2)
						var op infra.Op
						var err error
						if action == "scale" {
							op, err = b.SetContainerCount(t.Context(), "proxy", 3, 3)
						} else {
							op, err = b.SetResources(t.Context(), "proxy", infra.Resources{MinCPU: 2, MaxCPU: 2})
						}
						if err != nil {
							t.Fatal(err)
						}
						f.mu.Lock()
						a.holdTasks = true
						service := f.services[fakeString(a.row, "appName")]
						service["UpdateStatus"] = map[string]any{"State": condition}
						if condition == "stale-spec" {
							service["Spec"].(map[string]any)["TaskTemplate"].(map[string]any)["ForceUpdate"] = 1000
						}
						for i := range failedTasks {
							a.tasks = append(a.tasks, map[string]any{"containerId": fmt.Sprintf("failed-%d", i), "state": "failed", "currentState": "Failed 1 second ago"})
						}
						reloads := f.calls["application.reload"]
						f.mu.Unlock()
						consume := func(b *Backend) error {
							_, err := engine.ObserveProxyRollout(t.Context(), &engine.Context{Backend: b}, op, "proxy", 1, true)
							return err
						}
						if lookupFirst {
							_, err = b.GetService(t.Context(), "proxy")
						} else {
							err = consume(b)
						}
						if err == nil || !strings.Contains(err.Error(), "manual recovery required") || !strings.Contains(err.Error(), "PATCH proxy count/resources") {
							t.Fatalf("missing terminal recovery diagnostic: %v", err)
						}
						b = fakeBackend(t, f, nil)
						if err := consume(b); err == nil || !strings.Contains(err.Error(), "manual recovery required") {
							t.Fatalf("receipt lost terminal authority after restart: %v", err)
						}
						for range 2 {
							_, err := b.GetService(t.Context(), "proxy")
							if err == nil || !strings.Contains(err.Error(), "manual recovery required") {
								t.Fatalf("retained lookup lost terminal authority: %v", err)
							}
						}
						m := fakeMeta(t, f, "proxy")
						original, _ := decodeOp(op)
						if m.Pending == nil || !sameOperation(m.Pending, original) || !m.Pending.Failed || m.Pending.Uncertain || m.ProxyAdmissionFailure == nil {
							t.Fatalf("failure fence lost: %+v", m)
						}
						if fakeCalls(f, "application.reload") != reloads {
							t.Fatal("observation replayed rollout")
						}
						assertProxyNeverAllDown(t, f, a)
					})
				}
			}
		}
	}
}

// Feed ordinary engine checkpoints, not agent-upgrade operations, through each
// public provider's Sync. Supabase delegates PostgreSQL; PXC uses the MySQL core.
func TestOrdinaryProxyTerminalEngineParity(t *testing.T) {
	for _, name := range []string{"postgres", "supabase", "mysql", "pxc"} {
		for _, action := range []string{"scale", "resize"} {
			for _, lookupFirst := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/lookup=%t", name, action, lookupFirst), func(t *testing.T) {
					var p engine.Provider
					key := "mysql"
					switch name {
					case "postgres":
						p = postgres.New(postgres.Options{})
						key = "postgres"
					case "supabase":
						p = supabase.New(postgres.Options{})
						key = "postgres"
					default:
						p = mysql.New(mysql.Options{Provider: name})
					}
					rc, f, b := providerBudgetContext(t, p)
					rc.Artifact = budgetArtifact(renderTestArtifact(t))
					if name != "pxc" {
						rc.Cluster.Spec.Topology.Type = "single"
						rc.Cluster.Spec.Components.Engine.Replicas = 1
					}
					id := providerBudgetRuntime(t, b, "proxy", "", 2, true)
					oldResources := fakeMeta(t, f, "proxy").Resources
					var op infra.Op
					var err error
					if action == "scale" {
						op, err = b.SetContainerCount(t.Context(), "proxy", 3, 3)
					} else {
						op, err = b.SetResources(t.Context(), "proxy", infra.Resources{MinCPU: 2, MaxCPU: 2})
					}
					if err != nil {
						t.Fatal(err)
					}
					failedTarget := fakeMeta(t, f, "proxy")
					proxy := rc.Cluster.Spec.Components.Extra["proxy"]
					proxy.Replicas, proxy.Resources = failedTarget.Max, failedTarget.Resources
					rc.Cluster.Spec.Components.Extra["proxy"] = proxy
					f.mu.Lock()
					a := f.apps[id]
					a.holdTasks = true
					f.services[fakeString(a.row, "appName")]["UpdateStatus"] = map[string]any{"State": "paused"}
					f.mu.Unlock()
					if lookupFirst {
						_, _ = b.GetService(t.Context(), "proxy")
					}
					rc.Backend = fakeBackend(t, f, nil)
					rc.Cluster.Generation = 2
					rc.Cluster.Status.ObservedGeneration = 1
					state := map[string]any{"uid": "sample-identity", "configVersion": 1, "topology": rc.Cluster.Spec.Topology.Type, "formed": true,
						"formation": map[string]any{"state": "formed"},
						"members":   []map[string]any{{"name": "proxy", "role": "proxy", "stage": "joined", "serviceId": id, "replicas": 2, "configVersion": 1, "version": rc.Cluster.Spec.Version, "resources": oldResources}},
						"pending":   map[string]any{"action": action, "member": "proxy", "op": op, "replicas": failedTarget.Max, "resources": failedTarget.Resources},
					}
					if key == "mysql" {
						members := state["members"].([]map[string]any)
						for index := range rc.Cluster.Spec.Components.Engine.Replicas {
							dataName := fmt.Sprintf("samplem%d", index)
							dataID := providerBudgetRuntime(t, b, dataName, dataName+"v", 1, false)
							members = append(members, map[string]any{"name": dataName, "role": "data", "index": index, "stage": "joined", "serviceId": dataID, "volume": dataName + "v", "replicas": 1, "configVersion": 1, "version": rc.Cluster.Spec.Version})
						}
						state["members"] = members
						delete(state, "pending")
						stage := "scaled"
						if action == "resize" {
							stage = "resource-scaled"
						}
						state["rolling"] = map[string]any{"from": rc.Cluster.Spec.Version, "to": rc.Cluster.Spec.Version, "order": []string{"proxy"}, "stage": stage, "op": op, "targets": map[string]any{"proxy": proxy}}
					}
					providerBudgetState(t, rc, key, state)
					before := fakeCalls(f, "application.reload")
					result, err := p.Sync(t.Context(), rc)
					if err == nil {
						err = result.ConvergenceError
					}
					if err == nil || !strings.Contains(err.Error(), "manual recovery required") {
						t.Fatalf("ordinary %s failed to propagate terminal receipt: %v", name, err)
					}
					if rc.Cluster.Status.ObservedGeneration != 1 || fakeCalls(f, "application.reload") != before {
						t.Fatal("failed update committed generation or replayed")
					}
					if rc.Cluster.Status.Phase == model.PhaseReady {
						t.Fatal("failed rollout reported ready")
					}
				})
			}
		}
	}
}
