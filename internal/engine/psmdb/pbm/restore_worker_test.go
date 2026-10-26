// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package pbm

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"gopkg.in/yaml.v3"
)

type workerFailureHooks struct {
	Hooks
	reason      string
	err         error
	calls       int
	unconfirmed bool
}

func (h *workerFailureHooks) ObserveRestoreWorkerFailure(context.Context, string, Env, map[string]RestoreWorkerIdentity) (string, bool, error) {
	h.calls++
	return h.reason, h.unconfirmed, h.err
}

func TestDeadRestoreWorkerFailsClosedWithoutReinitiation(t *testing.T) {
	env, agent, _, op := restoreStartupSetup(t, false)
	hooks := &workerFailureHooks{Hooks: env.Hooks, reason: "native restore worker replaced; heartbeat and native progress stale for 2m0s; inspect memory limits"}
	env.Hooks = hooks
	agent.restoreRunning = true
	result, err := RunRestore(t.Context(), env, op)
	if err == nil || !result.Done || !strings.Contains(err.Error(), "no automatic re-initiation") || !strings.Contains(err.Error(), "Operator action required") {
		t.Fatal(result, err)
	}
	outcome, err := ReadRestoreOutcome(result.Checkpoint)
	if err != nil || !outcome.ConsistencyUncertain || outcome.RecoveredSuccessfully || outcome.Failure == "" {
		t.Fatal(outcome, err)
	}
	r, err := env.RC.Store.GetRestore(t.Context(), env.RC.Cluster.Ref(), "clone")
	if err != nil || r.Phase != domain.PhaseFailed || r.CompletedAt == nil || !strings.Contains(r.Error, "worker replaced") {
		t.Fatal(r, err)
	}
	// Durable terminal checkpoint survives CP restart; no native command or
	// cleanup gets a chance to mutate the partially-restored target again.
	op.Checkpoint = result.Checkpoint
	raw, _ := json.Marshal(op)
	var resumed domain.Operation
	if err := json.Unmarshal(raw, &resumed); err != nil {
		t.Fatal(err)
	}
	_, _ = RunRestore(t.Context(), env, &resumed)
	starts := agent.startCount()
	if starts != 1 || hooks.calls != 1 {
		t.Fatal("terminal restore replayed or re-observed", starts, hooks.calls)
	}
}

func TestRestoreWorkerObservationDoesNotAbortHealthyUnknownOrTerminalNative(t *testing.T) {
	for _, scenario := range []string{"healthy", "unreachable", "native-done", "physical"} {
		t.Run(scenario, func(t *testing.T) {
			env, agent, _, op := restoreStartupSetup(t, scenario == "physical")
			hooks := &workerFailureHooks{Hooks: env.Hooks}
			env.Hooks = hooks
			agent.restoreRunning = scenario != "native-done"
			if scenario == "unreachable" {
				hooks.err = errors.New("agent unreachable")
			}
			if scenario == "native-done" || scenario == "physical" {
				hooks.reason = "dead worker"
			}
			res, err := RunRestore(t.Context(), env, op)
			if err != nil || res.Done {
				t.Fatal(res, err)
			}
			cp, decodeErr := loadCheckpoint(&domain.Operation{Checkpoint: res.Checkpoint})
			starts := agent.startCount()
			if decodeErr != nil || cp.Failure != "" || cp.RestoreConsistencyUncertain || starts != 1 {
				t.Fatal(cp, decodeErr, starts)
			}
			if scenario == "native-done" && (!cp.RestoreNativeSucceeded || hooks.calls != 0) {
				t.Fatal("positive native completion lost", cp, hooks.calls)
			}
			if scenario == "physical" && hooks.calls != 0 {
				t.Fatal("intentional physical worker restart classified as abort")
			}
		})
	}
}

func TestPBMLogicalRestoreConcurrencyUsesMemberMemoryBudget(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		ram, configRAM       float64
		sharded              bool
		workers, collections int
	}{
		{"legacy", 0, 0, false, 1, 1},
		{"one-gib", 1, 0, false, 1, 1},
		{"two-gib", 2, 0, false, 1, 2},
		{"four-gib", 4, 0, false, 5, 2},
		{"eight-gib", 8, 0, false, 10, 2},
		{"small-config-server", 8, 1, true, 1, 1},
		{"small-shard", 1, 8, true, 1, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env, _, _, _ := setup(t)
			env.Sharded = tc.sharded
			// Desired sizing is intentionally unrelated: pending increases
			// must never grant memory the running members do not have.
			env.RC.Cluster.Spec.Components.Engine.Resources = domain.Resources{MinRAM: 32, MaxRAM: 32}
			env.RC.Cluster.Spec.Components.ConfigServer.Resources = domain.Resources{MinRAM: 32, MaxRAM: 32}
			for i := range env.Nodes {
				env.Nodes[i].Resources = domain.Resources{MinRAM: tc.ram, MaxRAM: tc.ram}
				env.Nodes[i].CacheGB = max(0.25, 0.5*(tc.ram-0.5))
			}
			if tc.sharded {
				env.Nodes[0].ReplSet = "cfg"
				env.Nodes[0].Resources = domain.Resources{MinRAM: tc.configRAM, MaxRAM: tc.configRAM}
				env.Nodes[0].CacheGB = max(0.25, 0.5*(tc.configRAM-0.5))
			}
			raw, err := RenderConfig(env)
			if err != nil {
				t.Fatal(err)
			}
			var config struct {
				Restore map[string]int `yaml:"restore"`
			}
			if err := yaml.Unmarshal(raw, &config); err != nil {
				t.Fatal(err)
			}
			for key, want := range map[string]int{"batchSize": 500, "numInsertionWorkers": tc.workers, "numParallelCollections": tc.collections, "numDownloadWorkers": 1, "maxDownloadBufferMb": 64, "downloadChunkMb": 8} {
				if config.Restore[key] != want {
					t.Fatalf("%s=%d want %d", key, config.Restore[key], want)
				}
			}
		})
	}
}
