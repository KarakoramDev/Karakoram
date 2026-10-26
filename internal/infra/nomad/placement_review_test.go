// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package nomad

import (
	"net/http"
	"strings"
	"testing"
)

func TestBlockedPlacementDoesNotMaskTerminalRollout(t *testing.T) {
	for _, outcome := range []string{"failed", "cancelled", "canceled", "auto-revert"} {
		t.Run(outcome, func(t *testing.T) {
			f, b, op := blockedDeployment(t, false)
			f.setAutoTick(false)
			f.mu.Lock()
			job := f.jobs[fakeKey("default", "runtime")]
			if outcome == "auto-revert" {
				job.row["Version"] = fakeInt(job.row, "Version") + 1
				job.row["Meta"].(fakeRow)["karakoram.operation"] = "prior-stable-operation"
			} else {
				job.deployment["Status"] = outcome
			}
			f.mu.Unlock()
			state, err := b.OpStatus(t.Context(), op)
			if err != nil || !state.Failed || state.Done || state.Status == "BackendPlacementFailed" {
				t.Fatalf("placement masked terminal %s: %+v %v", outcome, state, err)
			}
			if outcome == "auto-revert" && !strings.Contains(state.Message, "automatically reverted") {
				t.Fatal(state.Message)
			}
		})
	}
}

func TestPlacementObservationIsolatesOverlappingClusterNames(t *testing.T) {
	f := newFakeNomad(t)
	b := fakeBackend(t, f, nil)
	importFake(t, b, runtimeSpec("demom0", ""), runtimeSpec("demo2m0", ""))
	deployFake(t, b, "demom0", "")
	for _, id := range []string{"node-a", "node-b", "node-c"} {
		f.setNodeEligible(id, false)
	}
	op, err := b.Deploy(t.Context(), "demo2m0", testDeployment(t, ""))
	if err != nil {
		t.Fatal(err)
	}
	for range 20 {
		state, err := b.OpStatus(t.Context(), op)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(state.Message, "placement blocked") {
			break
		}
	}
	failures, err := b.PlacementFailures(t.Context(), []string{"demom0"})
	if err != nil || len(failures) != 0 {
		t.Fatalf("demo inherited demo2 placement: %+v %v", failures, err)
	}
	f.mu.Lock()
	f.requestHook = func(w http.ResponseWriter, r *http.Request) (bool, func()) {
		if r.Method == http.MethodGet && (strings.Contains(r.URL.Path, "demo2m0") || strings.HasPrefix(r.URL.Path, "/v1/evaluation/")) {
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
			return true, nil
		}
		return false, nil
	}
	f.mu.Unlock()
	failures, err = b.PlacementFailures(t.Context(), []string{"demom0"})
	if err != nil || len(failures) != 0 {
		t.Fatalf("demo inherited demo2 observation failure: %+v %v", failures, err)
	}
	if _, err := b.PlacementFailures(t.Context(), []string{"demo2m0"}); err == nil {
		t.Fatal("unrelated observation failure was not exercised")
	}
}
