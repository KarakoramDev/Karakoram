// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package pbm

import (
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestPBMGoMemoryBudgetUsesSmallestAllocationAndActiveCache(t *testing.T) {
	for _, tc := range []struct {
		name                    string
		minRAM, maxRAM, cacheGB float64
		wantMiB                 uint64
	}{
		{"legacy-unknown", 0, 0, 0, 0},
		{"half-gib", 0.5, 0.5, 0, 0},
		{"exact-floor", 0.9375, 0.9375, 0, 320},
		{"below-floor", 0.9365234375, 0.9365234375, 0, 319},
		{"one-gib", 1, 1, 0, 384},
		{"two-gib", 2, 2, 0.75, 896},
		{"four-gib", 4, 4, 1.75, 1920},
		{"eight-gib", 8, 8, 3.75, 3968},
		{"active-cache-exhausts-minimum", 1, 8, 3.75, 0},
		{"active-cache-not-desired-cache", 1, 8, 0.25, 384},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := GoMemoryLimitBytes(domain.Resources{MinRAM: tc.minRAM, MaxRAM: tc.maxRAM}, tc.cacheGB)
			if got != tc.wantMiB*mebibyte {
				t.Fatalf("budget=%d MiB want %d", got/mebibyte, tc.wantMiB)
			}
		})
	}
}

func TestLogicalRestoreRejectsInsufficientBudgetBeforeNativeMutation(t *testing.T) {
	for _, component := range []string{"engine", "configServer"} {
		t.Run(component, func(t *testing.T) {
			env, st, agent, _ := setup(t)
			env.RC.Cluster.Spec.Components.Engine.Resources = domain.Resources{MinRAM: 8, MaxRAM: 8}
			env.RC.Cluster.Spec.Components.ConfigServer.Resources = domain.Resources{MinRAM: 8, MaxRAM: 8}
			// One applied member is below the floor despite a pending increase.
			env.Nodes[0].Resources = domain.Resources{MinRAM: 0.5, MaxRAM: 0.5}
			if component == "configServer" {
				env.Sharded = true
				env.Nodes[0].ReplSet = "cfg"
			}
			if err := st.CreateBackup(t.Context(), &domain.Backup{Name: "base", Cluster: "target", Storage: "storage", Type: "logical", PBMName: "snapshot", Phase: domain.PhaseSucceeded}); err != nil {
				t.Fatal(err)
			}
			if err := st.CreateRestore(t.Context(), &domain.Restore{Name: "recover", Cluster: "target", Source: domain.RestoreSource{Backup: "base"}, Phase: domain.PhasePending}); err != nil {
				t.Fatal(err)
			}
			op := &domain.Operation{ID: "recover-operation", Kind: domain.OperationRestore, Payload: json.RawMessage(`{"restore":"recover"}`)}
			res, err := RunRestore(t.Context(), env, op)
			if err == nil || !res.Done || !strings.Contains(err.Error(), "PBMRestoreMemoryBudgetTooSmall") || !strings.Contains(err.Error(), component) || !strings.Contains(err.Error(), "320 MiB") {
				t.Fatal(res, err)
			}
			requests := agent.recordedRequests()
			starts := agent.startCount()
			if len(requests) != 0 || starts != 0 {
				t.Fatal("insufficient budget reached native/configuration commands", requests, starts)
			}
			r, err := st.GetRestore(t.Context(), env.RC.Cluster.Ref(), "recover")
			if err != nil || r.Phase != domain.PhaseFailed {
				t.Fatal(r, err)
			}
		})
	}
}

func TestLogicalRestoreBudgetAcceptsOneGiBAndDoesNotReclassifyNamedWork(t *testing.T) {
	env, agent, _, op := restoreStartupSetup(t, false)
	if err := logicalRestoreMemoryError(env); err != nil {
		t.Fatal("one-GiB default budget unexpectedly rejected", err)
	}
	env.RC.Cluster.Spec.Components.Engine.Resources = domain.Resources{MinRAM: 0.5, MaxRAM: 0.5}
	env.Nodes[0].Resources = domain.Resources{MinRAM: 0.5, MaxRAM: 0.5}
	agent.restoreRunning = true
	res, err := RunRestore(t.Context(), env, op)
	starts := agent.startCount()
	if err != nil || res.Done || starts != 1 || !strings.Contains(res.Message, "PBMMemoryBudgetUnknown") {
		t.Fatal("named legacy work was reinitiated/rejected or hid unknown budget", res, err, starts)
	}
}

func TestLogicalRestoreGoBudgetFloorBoundary(t *testing.T) {
	env, _, _, _ := setup(t)
	for _, tc := range []struct {
		ram    float64
		reject bool
	}{{0.9365234375, true}, {0.9375, false}, {1, false}} {
		env.Nodes[0].Resources = domain.Resources{MinRAM: tc.ram, MaxRAM: tc.ram}
		if err := logicalRestoreMemoryError(env); (err != nil) != tc.reject {
			t.Fatalf("RAM=%g reject=%v: %v", tc.ram, tc.reject, err)
		}
	}
}

func TestPBMGoMemoryBudgetObservationRequiresAppliedMatchingLimit(t *testing.T) {
	env, _, _, _ := setup(t)
	env.RC.Cluster.Spec.Components.Engine.Resources = domain.Resources{MinRAM: 4, MaxRAM: 4}
	var limit atomic.Uint64
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(agentapi.Status{Processes: map[string]agentapi.ProcessStatus{"pbm-agent": {GoMemoryLimitBytes: limit.Load()}}})
	}))
	defer server.Close()
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	env.RC.Agents = testDialer{server.URL, ca}
	for _, tc := range []struct {
		limit uint64
		known bool
	}{{0, false}, {896 << 20, false}, {384 << 20, true}} {
		limit.Store(tc.limit)
		if got := observeGoMemoryBudget(t.Context(), env); got != tc.known {
			t.Fatalf("launch limit=%d known=%v want %v", tc.limit, got, tc.known)
		}
	}
	env.Sharded = true
	env.Nodes[0].ReplSet = "cfg"
	env.Nodes[1].Resources = domain.Resources{MinRAM: 8, MaxRAM: 8}
	env.Nodes[1].CacheGB = 3.75
	limit.Store(384 << 20)
	if observeGoMemoryBudget(t.Context(), env) {
		t.Fatal("minimum config-server target was incorrectly accepted for the larger shard")
	}
}

func TestPBMAppliedMinimumBudgetAcrossReplicaShardAndConfigMembers(t *testing.T) {
	node := func(name, set string, ram, cache float64) Node {
		return Node{Member: name, ReplSet: set, Resources: domain.Resources{MinRAM: ram, MaxRAM: ram}, CacheGB: cache}
	}
	for _, tc := range []struct {
		name                 string
		nodes                []Node
		limiting             string
		budgetMiB            uint64
		workers, collections int
		errorCode            string
	}{
		{"replica-outlier", []Node{node("large", "rs0", 4, 1.75), node("small", "rs0", 1, 0.25)}, "small", 384, 1, 1, ""},
		{"small-config-server", []Node{node("cfg", "cfg", 1, 0.25), node("shard", "rs0", 8, 3.75)}, "cfg", 384, 1, 1, ""},
		{"small-shard", []Node{node("cfg", "cfg", 8, 3.75), node("shard", "rs0", 0.5, 0.25)}, "shard", 0, 1, 1, "PBMRestoreMemoryBudgetTooSmall"},
		{"below-floor-config-server", []Node{node("cfg", "cfg", 0.5, 0.25), node("shard", "rs0", 8, 3.75)}, "cfg", 0, 1, 1, "PBMRestoreMemoryBudgetTooSmall"},
		{"different-active-caches", []Node{node("cfg", "cfg", 4, 1.75), node("shard", "rs0", 4, 0.25)}, "cfg", 1920, 5, 2, ""},
		{"unknown-applied-member", []Node{node("cfg", "cfg", 8, 3.75), node("unknown", "rs0", 0, 0)}, "unknown", 0, 1, 1, "PBMRestoreMemoryBudgetUnknown"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env, _, _, _ := setup(t)
			env.Sharded, env.Nodes = true, tc.nodes
			env.RC.Cluster.Spec.Components.Engine.Resources = domain.Resources{MinRAM: 32, MaxRAM: 32}
			env.RC.Cluster.Spec.Components.ConfigServer.Resources = domain.Resources{MinRAM: 32, MaxRAM: 32}
			budget, limiting := minimumGoMemoryBudget(env)
			if budget != tc.budgetMiB*mebibyte || limiting == nil || limiting.Member != tc.limiting {
				t.Fatal("wrong applied limiting member", budget, limiting)
			}
			workers, collections := logicalRestoreConcurrency(env)
			if workers != tc.workers || collections != tc.collections {
				t.Fatal("desired sizing inflated concurrency", workers, collections)
			}
			err := logicalRestoreMemoryError(env)
			if tc.errorCode == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tc.errorCode) || !strings.Contains(err.Error(), tc.limiting) {
				t.Fatal("minimum applied member did not gate admission", err)
			}
		})
	}
}
