// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package nomad

import (
	"strings"
	"testing"
	"time"
)

func TestOperationSurfacesStalledStopWithoutReleasingFence(t *testing.T) {
	f := newFakeNomad(t)
	b := fakeBackend(t, f, nil)
	importFake(t, b, runtimeSpec("runtime", ""))
	deployFake(t, b, "runtime", "")
	f.setAutoTick(false)
	op, err := b.Stop(t.Context(), "runtime")
	if err != nil {
		t.Fatal(err)
	}
	at := time.Now().UTC()
	b.clock = func() time.Time { return at }
	f.mu.Lock()
	var allocationID string
	for _, a := range f.allocs {
		if a.row["ClientStatus"] != "running" {
			continue
		}
		allocationID = fakeString(a.row, "ID")
		a.row["DesiredStatus"] = "stop"
		a.row["Job"] = f.jobs[fakeKey("default", "runtime")].row
		for _, task := range fakeMap(a.row["TaskStates"]) {
			fakeMap(task)["Events"] = []any{fakeRow{"Type": "Killing", "Time": at.Add(-3 * time.Minute).UnixNano()}}
		}
	}
	f.mu.Unlock()
	state, err := b.OpStatus(t.Context(), op)
	if err != nil || state.Done || state.Failed || state.Status != "BackendStopStalled" || !strings.Contains(state.Message, allocationID) || !strings.Contains(state.Message, "exec") {
		t.Fatalf("stalled stop remained silent or released fence: %+v %v", state, err)
	}
	r, err := b.require(t.Context(), "runtime")
	if err != nil || r.Pending == nil || r.Pending.Phase != "stopping" {
		t.Fatalf("diagnostic changed durable lifecycle phase: %+v %v", r, err)
	}
	f.setAutoTick(true)
	awaitFake(t, b, op)
}

func TestStopObservationRequiresCurrentKillingBeyondJobGrace(t *testing.T) {
	for _, mode := range []string{"expired", "within-grace", "future", "missing-event", "restarted", "terminal", "desired-run", "foreign-job"} {
		t.Run(mode, func(t *testing.T) {
			f := newFakeNomad(t)
			b := fakeBackend(t, f, nil)
			importFake(t, b, runtimeSpec("runtime", ""))
			deployFake(t, b, "runtime", "")
			f.setAutoTick(false)
			at := time.Now().UTC()
			b.clock = func() time.Time { return at }
			f.mu.Lock()
			for _, a := range f.allocs {
				if a.row["ClientStatus"] != "running" {
					continue
				}
				a.row["DesiredStatus"] = "stop"
				a.row["Job"] = f.jobs[fakeKey("default", "runtime")].row
				age := 3 * time.Minute
				if mode == "within-grace" {
					age = 130 * time.Second
				}
				if mode == "future" {
					age = -time.Minute
				}
				events := []any{fakeRow{"Type": "Killing", "Time": at.Add(-age).UnixNano()}}
				if mode == "missing-event" {
					events = nil
				}
				if mode == "restarted" {
					events = append(events, fakeRow{"Type": "Started", "Time": at.Add(-time.Second).UnixNano()})
				}
				for _, task := range fakeMap(a.row["TaskStates"]) {
					fakeMap(task)["Events"] = events
				}
				if mode == "terminal" {
					a.row["ClientStatus"] = "complete"
				}
				if mode == "desired-run" {
					a.row["DesiredStatus"] = "run"
				}
			}
			if mode == "foreign-job" {
				f.jobs[fakeKey("default", "runtime")].row["Meta"].(fakeRow)["karakoram.id"] = "foreign"
			}
			f.mu.Unlock()
			failures, err := b.RuntimeStopFailures(t.Context(), []string{"runtime"})
			if mode == "foreign-job" {
				if err == nil {
					t.Fatal("foreign job provided owned stop diagnosis")
				}
				return
			}
			want := 0
			if mode == "expired" {
				want = 1
			}
			if err != nil || len(failures) != want {
				t.Fatalf("mode=%s failures=%+v err=%v", mode, failures, err)
			}
			if want > 0 && (failures[0].Node == "" || !strings.Contains(failures[0].Message, "-i=false -t=false")) {
				t.Fatalf("missing scoped operator hint: %+v", failures[0])
			}
		})
	}
}
