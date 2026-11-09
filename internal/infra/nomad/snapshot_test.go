// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package nomad

import (
	"context"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func steadyNomadContext(t *testing.T) context.Context {
	t.Helper()
	return infra.WithSteadyObservation(infra.ObservationContext(t.Context()))
}

func TestSnapshotSteadyObservationsAndFreshReads(t *testing.T) {
	f := newFakeNomad(t)
	b := fakeBackend(t, f, nil)
	importFake(t, b, runtimeSpec("runtime", ""))
	deployFake(t, b, "runtime", "")
	f.setAutoTick(false)
	observe := func(ctx context.Context) {
		t.Helper()
		if _, err := b.ProjectInfo(ctx); err != nil {
			t.Fatal(err)
		}
		if _, err := b.ListServices(ctx, "runtime"); err != nil {
			t.Fatal(err)
		}
		if _, err := b.Containers(ctx, "runtime"); err != nil {
			t.Fatal(err)
		}
		if _, err := b.PlacementFailures(ctx, []string{"runtime"}); err != nil {
			t.Fatal(err)
		}
		if _, err := b.RuntimeStopFailures(ctx, []string{"runtime"}); err != nil {
			t.Fatal(err)
		}
	}
	ctx := steadyNomadContext(t)
	if _, hit := b.PlatformSnapshot().Lookup(ctx, "listing-baseline"); hit {
		t.Fatal("listing warm-up unexpectedly served an entry")
	}
	observe(infra.ObservationContext(t.Context()))
	observe(ctx)
	before := len(f.requests())
	observe(ctx)
	if got := len(f.requests()) - before; got != 0 {
		t.Fatalf("warm steady observation made %d requests", got)
	}
	for name, fresh := range map[string]context.Context{
		"lifecycle":            infra.WithoutObservation(ctx),
		"fresh inventory":      infra.WithFreshTaskInventory(ctx),
		"ordinary observation": infra.ObservationContext(t.Context()),
	} {
		t.Run(name, func(t *testing.T) {
			before := len(f.requests())
			if _, err := b.Containers(fresh, "runtime"); err != nil {
				t.Fatal(err)
			}
			if len(f.requests()) == before {
				t.Fatal("fresh read was served from snapshot")
			}
		})
	}
}

func TestSnapshotMutationInvalidatesAcrossClients(t *testing.T) {
	for _, method := range []string{http.MethodPut, http.MethodPost, http.MethodDelete} {
		t.Run(method, func(t *testing.T) {
			f := newFakeNomad(t)
			b := fakeBackend(t, f, nil)
			second := fakeBackend(t, f, nil)
			ctx := steadyNomadContext(t)
			if _, err := b.ProjectInfo(ctx); err != nil {
				t.Fatal(err)
			}
			before := len(f.requests())
			if _, err := second.ProjectInfo(ctx); err != nil {
				t.Fatal(err)
			}
			if len(f.requests()) != before {
				t.Fatal("same target did not share snapshot")
			}
			// Even a rejected variables write makes the previous inventory doubtful.
			_, _, _ = second.c.request(ctx, method, "/v1/var/karakoram/test", nil, []byte(`{}`), jsonResponseLimit, 0, false)
			before = len(f.requests())
			if _, err := b.ProjectInfo(ctx); err != nil {
				t.Fatal(err)
			}
			if len(f.requests()) == before {
				t.Fatal("mutation did not invalidate sibling client")
			}
		})
	}
}

func TestSnapshotListingDriftAndIdleStability(t *testing.T) {
	f := newFakeNomad(t)
	b := fakeBackend(t, f, map[string]string{"region": "region-a"})
	importFake(t, b, runtimeSpec("runtime", ""))
	deployFake(t, b, "runtime", "")
	f.setAutoTick(false)
	now := time.Now().Add(time.Second)
	restore := infra.SetSnapshotClockForTest(func() time.Time { return now })
	defer restore()
	ctx := steadyNomadContext(t)
	if _, err := b.ProjectInfo(ctx); err != nil {
		t.Fatal(err)
	}
	generation := b.PlatformSnapshot().DriftGeneration()
	f.mu.Lock()
	for _, job := range f.jobs {
		job.row["ModifyIndex"] = 99999
		job.row["SubmitTime"] = 99999
	}
	for _, a := range f.allocs {
		a.row["ModifyIndex"] = 99999
		a.row["ModifyTime"] = 99999
		a.row["TaskStates"] = fakeRow{"member": fakeRow{"Events": []any{fakeRow{"Type": "Started", "Time": 99999}}}}
	}
	f.mu.Unlock()
	now = now.Add(infra.SnapshotListingInterval + time.Second)
	before := len(f.requests())
	if _, err := b.ProjectInfo(ctx); err != nil {
		t.Fatal(err)
	}
	calls := f.requests()[before:]
	if len(calls) != 2 || calls[0].Path != "/v1/jobs" || calls[0].Query.Get("meta") != "true" || calls[1].Path != "/v1/allocations" {
		t.Fatalf("idle listing requests: %+v", calls)
	}
	for _, call := range calls {
		if call.Query.Get("region") != "region-a" || call.Query.Get("namespace") != "default" {
			t.Fatalf("listing lost target scope: %+v", call)
		}
	}
	if b.PlatformSnapshot().DriftGeneration() != generation {
		t.Fatal("idle scheduler churn invalidated snapshot")
	}
	f.mu.Lock()
	for _, a := range f.allocs {
		a.row["ClientStatus"] = "failed"
	}
	f.mu.Unlock()
	now = now.Add(infra.SnapshotListingInterval + time.Second)
	before = len(f.requests())
	if _, err := b.ProjectInfo(ctx); err != nil {
		t.Fatal(err)
	}
	if len(f.requests())-before != 3 || b.PlatformSnapshot().DriftGeneration() <= generation {
		t.Fatal("allocation drift did not invalidate cached project read")
	}
}

func TestSnapshotTargetIsolation(t *testing.T) {
	f := newFakeNomad(t)
	base := newClient(f.URL, "region-a", "default", fakeNomadToken, nil)
	base.http = f.Client()
	same := newClient(f.URL, "region-a", "default", fakeNomadToken, nil)
	if base.snapshot != same.snapshot {
		t.Fatal("same target did not share snapshot")
	}
	for _, c := range []*client{
		newClient(f.URL, "region-b", "default", fakeNomadToken, nil),
		newClient(f.URL, "region-a", "other", fakeNomadToken, nil),
		newClient(newFakeNomad(t).URL, "region-a", "default", fakeNomadToken, nil),
	} {
		if base.snapshot == c.snapshot {
			t.Fatal("distinct origin, region or namespace shared snapshot")
		}
	}
}

func TestSnapshotExcludesBlockingAndLogs(t *testing.T) {
	f := newFakeNomad(t)
	b := fakeBackend(t, f, nil)
	ctx := infra.WithRequestCaller(steadyNomadContext(t), infra.CallerAPI)
	reads := func() uint64 {
		for _, provider := range infra.PlatformCounters() {
			if provider.Provider == "nomad" {
				for _, request := range provider.Requests {
					if request.Caller == infra.CallerAPI {
						return request.Reads
					}
				}
			}
		}
		return 0
	}
	before := reads()
	for _, q := range []url.Values{{"index": {"0"}}, {"wait": {"0s"}}} {
		for range 2 {
			if _, err := b.c.get(ctx, "/v1/jobs", q, nil); err != nil {
				t.Fatal(err)
			}
		}
	}
	if len(f.requests()) != 4 {
		t.Fatalf("blocking queries were cached: %+v", f.requests())
	}
	f.setAutoTick(false)
	f.mu.Lock()
	f.allocs["logs"] = &fakeAlloc{
		row:  fakeRow{"ID": "logs", "Namespace": "default"},
		logs: map[string]string{"member/stdout": "hello"},
	}
	f.mu.Unlock()
	for range 2 {
		data, err := b.c.getRaw(ctx, "/v1/client/fs/logs/logs", url.Values{"plain": {"true"}, "task": {"member"}, "type": {"stdout"}}, 1024)
		if err != nil || string(data) != "hello" {
			t.Fatalf("log read: %q, %v", data, err)
		}
	}
	if len(f.requests()) != 6 {
		t.Fatal("log queries were cached")
	}
	if got := reads() - before; got != 6 {
		t.Fatalf("blocking/log network GET counter=%d, want 6", got)
	}
	t.Log("blocking/log GETs counted=6, cached=0")
}

func TestSnapshotListingFingerprintFields(t *testing.T) {
	for _, field := range []string{"ID", "Status", "Stop", "Version", "JobModifyIndex", "Meta", "allocation ID", "JobID", "ClientStatus", "DesiredStatus", "JobVersion", "NodeID"} {
		t.Run(field, func(t *testing.T) {
			f := newFakeNomad(t)
			b := fakeBackend(t, f, nil)
			importFake(t, b, runtimeSpec("runtime", ""))
			deployFake(t, b, "runtime", "")
			f.setAutoTick(false)
			before, err := b.c.snapshotListing(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			f.mu.Lock()
			switch field {
			case "ID", "Status", "Stop", "Version", "JobModifyIndex", "Meta":
				for _, job := range f.jobs {
					switch field {
					case "Stop":
						job.row[field] = true
					case "Version", "JobModifyIndex":
						job.row[field] = 99999
					case "Meta":
						job.row[field] = fakeRow{"KARAKORAM_OWNER": "changed"}
					default:
						job.row[field] = "changed"
					}
				}
			default:
				for _, a := range f.allocs {
					if field == "JobVersion" {
						a.row[field] = 99999
					} else if field == "allocation ID" {
						a.row["ID"] = "changed"
					} else {
						a.row[field] = "changed"
					}
				}
			}
			f.mu.Unlock()
			after, err := b.c.snapshotListing(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			if before == after {
				t.Fatal("stable field drift was omitted from fingerprint")
			}
		})
	}
}

func TestSnapshotListingFailureFallsBack(t *testing.T) {
	f := newFakeNomad(t)
	b := fakeBackend(t, f, nil)
	now := time.Now().Add(time.Second)
	restore := infra.SetSnapshotClockForTest(func() time.Time { return now })
	defer restore()
	ctx := steadyNomadContext(t)
	if _, err := b.ProjectInfo(ctx); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.requestHook = func(w http.ResponseWriter, r *http.Request) (bool, func()) {
		if r.URL.Path == "/v1/allocations" {
			http.Error(w, "listing unavailable", http.StatusServiceUnavailable)
			return true, nil
		}
		return false, nil
	}
	f.mu.Unlock()
	now = now.Add(infra.SnapshotListingInterval + time.Second)
	for range 2 {
		before := len(f.requests())
		if _, err := b.ProjectInfo(ctx); err != nil {
			t.Fatal(err)
		}
		calls := f.requests()[before:]
		if len(calls) == 0 || calls[len(calls)-1].Path != "/v1/namespace/default" {
			t.Fatal("failed listing allowed a cached project read")
		}
	}
	f.mu.Lock()
	f.requestHook = nil
	f.mu.Unlock()
	now = now.Add(infra.SnapshotListingInterval + time.Second)
	if _, err := b.ProjectInfo(ctx); err != nil {
		t.Fatal(err)
	}
	before := len(f.requests())
	if _, err := b.ProjectInfo(ctx); err != nil {
		t.Fatal(err)
	}
	if len(f.requests()) != before {
		t.Fatal("snapshot did not resume after listing recovered")
	}
}
