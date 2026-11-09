// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package dokploy

import (
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func TestProxyAdmissionFailureFenceSurvivesReceiptCleanup(t *testing.T) {
	for _, action := range []string{"scale", "resources", "deploy"} {
		t.Run(action, func(t *testing.T) {
			f, b, a := markedProxyFake(t, 1)
			f.mu.Lock()
			a.neverHealthyProxy = true
			f.mu.Unlock()
			var op infra.Op
			var err error
			switch action {
			case "scale":
				op, err = b.SetContainerCount(t.Context(), "proxy", 2, 2)
			case "resources":
				op, err = b.SetResources(t.Context(), "proxy", infra.Resources{MinCPU: 2, MaxCPU: 2, MinRAM: 1, MaxRAM: 1})
			case "deploy":
				d := testDeployment(t, "")
				d.Runtime.Env = map[string]string{"KARAKORAM_PROXY_SERVING_PROBE": "1"}
				op, err = b.Deploy(t.Context(), "proxy", d)
			}
			if err != nil {
				t.Fatal(err)
			}
			failed := false
			for range 20 {
				state, err := b.OpStatus(t.Context(), op)
				if err != nil {
					t.Fatal(err)
				}
				if state.Failed {
					failed = true
					break
				}
			}
			if !failed {
				t.Fatal("rollout did not fail")
			}
			metadata := fakeMeta(t, f, "proxy")
			if metadata.ProxyAdmissionFailure == nil {
				t.Fatal("failure admission authority was not persisted")
			}
			f.mu.Lock()
			rejected := a.proxyRollout.next[0]
			rejectedID := fakeString(rejected, "containerId")
			rejected["state"], rejected["currentState"] = "running", "Starting 1 second ago"
			a.holdTasks = true
			// Even losing the engine/submission receipt or clearing Swarm's paused
			// status must not erase the independently persisted failure authority.
			metadata.Pending = nil
			a.row["description"] = metadata.description()
			f.services[fakeString(a.row, "appName")]["UpdateStatus"] = map[string]any{"State": "completed"}
			f.mu.Unlock()
			for range 2 {
				b = fakeBackend(t, f, nil) // Reconstruct the CP/backend; only platform state survives.
				inventory, err := b.Containers(infra.WithFreshTaskInventory(t.Context()), "proxy")
				if err != nil {
					t.Fatal(err)
				}
				found := false
				for _, task := range inventory {
					if task.ID == rejectedID {
						found = true
						if task.ProxyServingAdmission {
							t.Fatal("rejected task admitted after receipt cleanup/restart")
						}
					}
				}
				if !found {
					t.Fatal("rejected Starting task disappeared from test inventory")
				}
			}
			f.mu.Lock()
			a.holdTasks = false
			a.neverHealthyProxy = false
			f.mu.Unlock()
			retry, err := b.SetResources(t.Context(), "proxy", infra.Resources{MinCPU: 3, MaxCPU: 3, MinRAM: 1, MaxRAM: 1})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := b.OpStatus(t.Context(), retry); err != nil {
				t.Fatal(err)
			}
			f.mu.Lock()
			a.holdTasks = true
			newID := fakeString(a.proxyRollout.next[0], "containerId")
			f.mu.Unlock()
			inventory, err := b.Containers(infra.WithFreshTaskInventory(t.Context()), "proxy")
			if err != nil {
				t.Fatal(err)
			}
			allowed := false
			for _, task := range inventory {
				if task.ID == rejectedID && task.ProxyServingAdmission {
					t.Fatal("accepted retry authorized its rejected predecessor")
				}
				if task.ID == newID {
					allowed = task.ProxyServingAdmission
				}
			}
			if !allowed {
				t.Fatal("accepted retry did not release admission for its new generation")
			}
			f.mu.Lock()
			a.holdTasks = false
			f.mu.Unlock()
			awaitFake(t, b, retry)
			if fakeMeta(t, f, "proxy").ProxyAdmissionFailure != nil {
				t.Fatal("successful retry did not retire its durable failure fence")
			}
		})
	}
}
