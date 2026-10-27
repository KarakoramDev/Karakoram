// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package nomad

import (
	"encoding/json"
	"errors"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func TestResourcePlanRejectsBeforeStopping(t *testing.T) {
	for _, reason := range []string{"cpu", "memory", "constraint", "unavailable", "invalid_json"} {
		t.Run(reason, func(t *testing.T) {
			f := newFakeNomad(t)
			b := fakeBackend(t, f, nil)
			importFake(t, b, volumeSpec("volume"), runtimeSpec("runtime", "volume"))
			deployFake(t, b, "runtime", "volume")
			before, err := b.require(t.Context(), "runtime")
			if err != nil {
				t.Fatal(err)
			}
			containers, err := b.Containers(t.Context(), "runtime")
			if err != nil || len(containers) != 1 {
				t.Fatalf("before containers: %+v %v", containers, err)
			}
			volume, err := b.require(t.Context(), "volume")
			if err != nil {
				t.Fatal(err)
			}
			plans := 0
			f.mu.Lock()
			f.requestHook = func(w http.ResponseWriter, r *http.Request) (bool, func()) {
				if r.URL.Path != "/v1/job/runtime/plan" {
					return false, nil
				}
				plans++
				var body map[string]any
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Error(err)
				}
				job := fakeMap(body["Job"])
				groups := fakeRows(job["TaskGroups"])
				if r.Method != http.MethodPost || len(groups) != 1 || job["Stop"] == true {
					t.Errorf("invalid plan: %+v", body)
				}
				if len(groups) == 1 {
					constraints := fakeRows(groups[0]["Constraints"])
					if len(constraints) != 1 || constraints[0]["RTarget"] != volume.Node {
						t.Errorf("plan lost volume pin: %+v", groups[0])
					}
					resources := fakeMap(fakeRows(groups[0]["Tasks"])[0]["Resources"])
					if fakeInt(resources, "MemoryMB") != 2048 {
						t.Errorf("plan lost proposed RAM: %+v", resources)
					}
				}
				if reason == "unavailable" {
					http.Error(w, "unavailable", 503)
					return true, nil
				}
				if reason == "invalid_json" {
					_, _ = w.Write([]byte("{"))
					return true, nil
				}
				failure := fakeRow{"DimensionExhausted": fakeRow{reason: 1}}
				if reason == "constraint" {
					failure = fakeRow{"ConstraintFiltered": fakeRow{"node eligibility": 1}}
				}
				fakeJSON(w, 200, fakeRow{"FailedTGAllocs": fakeRow{"member": failure}})
				return true, nil
			}
			f.mu.Unlock()
			calls := len(f.requests())
			for range 3 {
				op, err := b.SetResources(t.Context(), "runtime", infra.Resources{MinCPU: 2, MaxCPU: 2, MinRAM: 2, MaxRAM: 2})
				if err == nil || op.ID != "" {
					t.Fatalf("resize admitted: %+v %v", op, err)
				}
				if reason != "unavailable" && reason != "invalid_json" {
					var rejected *infra.ResourceUpdateRejectedError
					if !errors.As(err, &rejected) || !errors.Is(err, infra.ErrRejected) || !strings.Contains(err.Error(), volume.Node) || !strings.Contains(err.Error(), reason) {
						t.Fatalf("missing terminal placement reason: %v", err)
					}
				}
			}
			if plans != 3 {
				t.Fatalf("retry skipped plan: %d", plans)
			}
			for _, call := range f.requests()[calls:] {
				if call.Method != http.MethodGet {
					t.Fatalf("rejected resize mutated Nomad: %+v", call)
				}
			}
			after, err := b.require(t.Context(), "runtime")
			if err != nil || !reflect.DeepEqual(before.Resources, after.Resources) || !reflect.DeepEqual(before.Pending, after.Pending) {
				t.Fatalf("rejection changed record: %+v %v", after, err)
			}
			remaining, err := b.Containers(t.Context(), "runtime")
			if err != nil || len(remaining) != 1 || remaining[0].ID != containers[0].ID {
				t.Fatalf("running allocation changed: %+v %v", remaining, err)
			}
		})
	}
}

func TestResourcePlanAllowsFeasibleResize(t *testing.T) {
	f := newFakeNomad(t)
	b := fakeBackend(t, f, nil)
	importFake(t, b, volumeSpec("volume"), runtimeSpec("runtime", "volume"))
	deployFake(t, b, "runtime", "volume")
	op, err := b.SetResources(t.Context(), "runtime", infra.Resources{MinCPU: 2, MaxCPU: 2, MinRAM: 2, MaxRAM: 2})
	if err != nil {
		t.Fatal(err)
	}
	awaitFake(t, b, op)
	planned := false
	for _, call := range f.requests() {
		if call.Path == "/v1/job/runtime/plan" {
			planned = true
		}
	}
	if !planned {
		t.Fatal("destructive resource update did not plan")
	}
	if overlaps := f.writerOverlaps(); len(overlaps) != 0 {
		t.Fatalf("single-writer overlap: %v", overlaps)
	}
}

func TestResourcePlanRechecksBeforeRetryingStop(t *testing.T) {
	f := newFakeNomad(t)
	b := fakeBackend(t, f, nil)
	importFake(t, b, volumeSpec("volume"), runtimeSpec("runtime", "volume"))
	deployFake(t, b, "runtime", "volume")
	before, err := b.Containers(t.Context(), "runtime")
	if err != nil || len(before) != 1 {
		t.Fatalf("initial containers: %+v %v", before, err)
	}
	f.mu.Lock()
	f.requestHook = func(w http.ResponseWriter, r *http.Request) (bool, func()) {
		if r.Method == http.MethodPost && r.URL.Path == "/v1/jobs" {
			http.Error(w, "temporarily unavailable", http.StatusServiceUnavailable)
			return true, nil
		}
		return false, nil
	}
	f.mu.Unlock()
	if _, err := b.SetResources(t.Context(), "runtime", infra.Resources{MaxCPU: 2, MaxRAM: 2}); !errors.Is(err, infra.ErrUnavailable) {
		t.Fatalf("expected failed initial stop: %v", err)
	}
	r, err := b.require(t.Context(), "runtime")
	if err != nil || r.Pending == nil {
		t.Fatalf("missing pending retry: %+v %v", r, err)
	}
	// Expire the uncertain-delivery lease; the failed request did not reach
	// the scheduler, but the backend correctly quarantines its delivery.
	r.Pending.Lease = nil
	if err := b.save(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.requestHook = func(w http.ResponseWriter, req *http.Request) (bool, func()) {
		if req.URL.Path == "/v1/job/runtime/plan" {
			fakeJSON(w, 200, fakeRow{"FailedTGAllocs": fakeRow{"member": fakeRow{"DimensionExhausted": fakeRow{"cpu": 1}}}})
			return true, nil
		}
		return false, nil
	}
	f.mu.Unlock()
	state, err := b.OpStatus(t.Context(), encodeOp(*r.Pending))
	var rejected *infra.ResourceUpdateRejectedError
	if !errors.As(err, &rejected) || !state.Failed {
		t.Fatalf("infeasible stop retry not terminal: %+v %v", state, err)
	}
	for range 3 {
		state, err = b.OpStatus(t.Context(), encodeOp(*r.Pending))
		if !errors.As(err, &rejected) || !state.Failed {
			t.Fatalf("terminal retry fence lost: %+v %v", state, err)
		}
	}
	after, err := b.Containers(t.Context(), "runtime")
	if err != nil || len(after) != 1 || before[0].ID != after[0].ID {
		t.Fatalf("retry stopped original allocation: %+v %v", after, err)
	}
}

func TestResourcePlanUsesExactReplacementMarker(t *testing.T) {
	f := newFakeNomad(t)
	b := fakeBackend(t, f, nil)
	spec := runtimeSpec("runtime", "volume")
	spec.Resources = infra.Resources{MinCPU: 1, MaxCPU: 2, MinRAM: 1, MaxRAM: 1}
	importFake(t, b, volumeSpec("volume"), spec)
	deployFake(t, b, "runtime", "volume")
	current, err := b.require(t.Context(), "runtime")
	if err != nil {
		t.Fatal(err)
	}
	var planned map[string]any
	f.mu.Lock()
	f.requestHook = func(w http.ResponseWriter, r *http.Request) (bool, func()) {
		if r.URL.Path == "/v1/job/runtime/plan" {
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Error(err)
			}
			planned = fakeMap(body["Job"])
			fakeJSON(w, 200, fakeRow{"Annotations": fakeRow{"DesiredTGUpdates": fakeRow{"member": fakeRow{"DestructiveUpdate": 1}}}})
			return true, nil
		}
		return false, nil
	}
	f.mu.Unlock()
	// Only change a bound that does not change rendered CPU: this used to
	// plan the old job even though registration would replace the allocation.
	change := current.Resources
	change.MinCPU = change.MinCPU / 2
	op, err := b.SetResources(t.Context(), "runtime", change)
	if err != nil {
		t.Fatal(err)
	}
	pending, err := b.require(t.Context(), "runtime")
	if err != nil {
		t.Fatal(err)
	}
	rendered, err := b.renderJob(t.Context(), pending)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(rendered["Job"])
	if err != nil {
		t.Fatal(err)
	}
	var registered map[string]any
	if err := json.Unmarshal(encoded, &registered); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(planned, registered) {
		t.Fatalf("plan differs from exact registration: planned=%+v registered=%+v", planned, registered)
	}
	marker := fakeString(fakeMap(planned["Meta"]), "karakoram.operation")
	if marker == "" || marker == current.Pending.Issued.Format("2006-01-02T15:04:05.999999999Z07:00") {
		t.Fatal("plan retained predecessor operation marker")
	}
	task := fakeRows(fakeRows(planned["TaskGroups"])[0]["Tasks"])[0]
	if fakeString(fakeMap(task["Meta"]), "karakoram.operation") != marker {
		t.Fatal("plan lacks destructive task-spec marker")
	}
	awaitFake(t, b, op)
}

func TestResourcePlanRejectsNoopAndInPlaceProof(t *testing.T) {
	for _, kind := range []string{"noop", "inplace"} {
		t.Run(kind, func(t *testing.T) {
			f := newFakeNomad(t)
			b := fakeBackend(t, f, nil)
			importFake(t, b, volumeSpec("volume"), runtimeSpec("runtime", "volume"))
			deployFake(t, b, "runtime", "volume")
			f.mu.Lock()
			f.requestHook = func(w http.ResponseWriter, r *http.Request) (bool, func()) {
				if r.URL.Path != "/v1/job/runtime/plan" {
					return false, nil
				}
				inPlace := 0
				if kind == "inplace" {
					inPlace = 1
				}
				fakeJSON(w, 200, fakeRow{"Annotations": fakeRow{"DesiredTGUpdates": fakeRow{"member": fakeRow{"InPlaceUpdate": inPlace}}}})
				return true, nil
			}
			f.mu.Unlock()
			if _, err := b.SetResources(t.Context(), "runtime", infra.Resources{MaxRAM: 2}); !errors.Is(err, infra.ErrRejected) {
				t.Fatalf("non-placement plan admitted destructive stop: %v", err)
			}
		})
	}
}

func TestResourcePlanRejectsIneligiblePinnedNode(t *testing.T) {
	f := newFakeNomad(t)
	b := fakeBackend(t, f, nil)
	importFake(t, b, volumeSpec("volume"), runtimeSpec("runtime", "volume"))
	deployFake(t, b, "runtime", "volume")
	volume, err := b.require(t.Context(), "volume")
	if err != nil {
		t.Fatal(err)
	}
	f.setNodeEligible(volume.Node, false)
	if _, err := b.SetResources(t.Context(), "runtime", infra.Resources{MinCPU: .25}); !errors.Is(err, infra.ErrRejected) || !strings.Contains(err.Error(), volume.Node) {
		t.Fatalf("ineligible pinned node admitted: %v", err)
	}
}
