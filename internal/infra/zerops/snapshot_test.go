// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package zerops

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type snapshotCountingTransport struct {
	base        http.RoundTripper
	requests    atomic.Int64
	failListing atomic.Bool
}

func (c *snapshotCountingTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	c.requests.Add(1)
	if c.failListing.Load() && strings.HasSuffix(r.URL.Path, "/service-stack") {
		return &http.Response{StatusCode: http.StatusServiceUnavailable, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{}`)), Request: r}, nil
	}
	return c.base.RoundTrip(r)
}

func snapshotBackend(t *testing.T) (*Backend, *fakeAPI, *snapshotCountingTransport) {
	t.Helper()
	server, fake := newFakeAPIWithState(t)
	counter := &snapshotCountingTransport{base: http.DefaultTransport}
	b, err := New(Options{BaseURL: server.URL, Token: "token", ProjectID: "project", HTTPClient: &http.Client{Transport: counter}, MaxRetries: -1})
	if err != nil {
		t.Fatal(err)
	}
	fake.services["service"] = &ServiceStack{ID: "service", ProjectID: "project", Name: "member", Status: "ACTIVE", Base: "alpine@3.21"}
	return b, fake, counter
}

func snapshotWarmListing(t *testing.T, b *Backend, steady context.Context) {
	t.Helper()
	if _, hit := b.PlatformSnapshot().Lookup(steady, "listing-baseline"); hit {
		t.Fatal("listing warm-up unexpectedly served an entry")
	}
}

func TestPlatformSnapshotSteadyAndFresh(t *testing.T) {
	b, _, counter := snapshotBackend(t)
	ctx := context.Background()
	steady := infra.WithSteadyObservation(infra.ObservationContext(ctx))
	get := func(ctx context.Context) {
		t.Helper()
		if _, err := b.client.GetService(ctx, "service"); err != nil {
			t.Fatal(err)
		}
	}
	snapshotWarmListing(t, b, steady)
	get(ctx)
	get(steady)
	counter.requests.Store(0)
	get(steady)
	if n := counter.requests.Load(); n != 0 {
		t.Fatalf("warm steady requests = %d", n)
	}
	for _, fresh := range []context.Context{ctx, infra.ObservationContext(ctx), infra.WithoutObservation(steady), infra.WithFreshTaskInventory(steady)} {
		counter.requests.Store(0)
		get(fresh)
		if n := counter.requests.Load(); n != 1 {
			t.Fatalf("fresh requests = %d, want 1", n)
		}
	}
	if _, err := b.client.Lifecycle(steady, "service", "stop"); err != nil {
		t.Fatal(err)
	}
	counter.requests.Store(0)
	s, err := b.client.GetService(steady, "service")
	if err != nil || s.Status == "ACTIVE" || counter.requests.Load() == 0 {
		t.Fatalf("mutation served stale state: status=%s requests=%d err=%v", s.Status, counter.requests.Load(), err)
	}
}

func TestPlatformSnapshotDriftAndPagination(t *testing.T) {
	now := time.Now().Add(time.Minute)
	restore := infra.SetSnapshotClockForTest(func() time.Time { return now })
	defer restore()
	b, fake, counter := snapshotBackend(t)
	for i := range 204 {
		id := fmt.Sprintf("extra%03d", i)
		fake.services[id] = &ServiceStack{ID: id, Name: id, Status: "ACTIVE"}
	}
	ctx := context.Background()
	steady := infra.WithSteadyObservation(infra.ObservationContext(ctx))
	counter.requests.Store(0)
	snapshotWarmListing(t, b, steady)
	if n := counter.requests.Load(); n != 3 {
		t.Fatalf("205-stack listing requests = %d, want 3", n)
	}
	if _, err := b.client.GetService(ctx, "service"); err != nil {
		t.Fatal(err)
	}
	counter.requests.Store(0)
	if _, err := b.client.GetService(steady, "service"); err != nil {
		t.Fatal(err)
	}
	if n := counter.requests.Load(); n != 0 {
		t.Fatalf("warm detail requests = %d, want 0", n)
	}
	fake.mu.Lock()
	fake.services["service"].Name = "renamed"
	fake.mu.Unlock()
	now = now.Add(infra.SnapshotListingInterval + time.Second)
	counter.requests.Store(0)
	s, err := b.client.GetService(steady, "service")
	if err != nil || s.Name != "renamed" || b.PlatformSnapshot().DriftGeneration() == 0 {
		t.Fatalf("drift not observed: %+v, err=%v", s, err)
	}
	if n := counter.requests.Load(); n != 4 {
		t.Fatalf("drift requests = %d, want 3 listing + 1 detail", n)
	}
}

func TestPlatformSnapshotTargetSharing(t *testing.T) {
	b, _, counter := snapshotBackend(t)
	makeBackend := func(base, project string) *Backend {
		t.Helper()
		x, err := NewFactory(Options{BaseURL: base, HTTPClient: &http.Client{Transport: counter}}).New(infra.Target{ID: project, Token: "token"})
		if err != nil {
			t.Fatal(err)
		}
		return x.(*Backend)
	}
	same := makeBackend(b.client.base+"/", "project")
	otherProject := makeBackend(b.client.base, "different")
	otherBase := makeBackend(b.client.base+"/api", "project")
	if same.PlatformSnapshot() != b.PlatformSnapshot() || otherProject.PlatformSnapshot() == b.PlatformSnapshot() || otherBase.PlatformSnapshot() == b.PlatformSnapshot() {
		t.Fatal("snapshot must be shared only by normalized base URL and project")
	}
	ctx := context.Background()
	steady := infra.WithSteadyObservation(infra.ObservationContext(ctx))
	snapshotWarmListing(t, b, steady)
	if _, err := b.client.GetService(ctx, "service"); err != nil {
		t.Fatal(err)
	}
	if _, err := b.client.GetService(steady, "service"); err != nil {
		t.Fatal(err)
	}
	counter.requests.Store(0)
	if _, err := same.client.GetService(steady, "service"); err != nil {
		t.Fatal(err)
	}
	if counter.requests.Load() != 0 {
		t.Fatal("sibling backend did not share validated GET")
	}
}

func TestServiceStackFingerprintStable(t *testing.T) {
	var p page[ServiceStack]
	if err := json.Unmarshal(fixture(t, "list"), &p); err != nil {
		t.Fatal(err)
	}
	fingerprint := func(list []ServiceStack) string {
		t.Helper()
		value, err := serviceStackFingerprint(list)
		if err != nil {
			t.Fatal(err)
		}
		return value
	}
	original := fingerprint(p.List)
	var raw map[string]any
	if err := json.Unmarshal(fixture(t, "list"), &raw); err != nil {
		t.Fatal(err)
	}
	for _, entry := range raw["list"].([]any) {
		s := entry.(map[string]any)
		s["created"], s["lastUpdate"] = "changed", "changed"
		s["project"] = map[string]any{"lastUpdate": "changed"}
	}
	data, err := json.Marshal(raw)
	if err != nil {
		t.Fatal(err)
	}
	var changed page[ServiceStack]
	if err := json.Unmarshal(data, &changed); err != nil {
		t.Fatal(err)
	}
	slices.Reverse(changed.List)
	if fingerprint(changed.List) != original {
		t.Fatal("timestamps, nested project telemetry or listing order changed fingerprint")
	}
	for _, mutate := range []func(*ServiceStack){
		func(s *ServiceStack) { s.Name += "changed" },
		func(s *ServiceStack) { s.Status = "STOPPED" },
		func(s *ServiceStack) { s.Mode = "changed" },
		func(s *ServiceStack) { s.Ports = []ServicePort{{Port: 1234}} },
		func(s *ServiceStack) {
			s.CurrentAutoscaling = map[string]json.RawMessage{"horizontalAutoscaling": json.RawMessage(`{"minContainerCount":9}`)}
		},
	} {
		list := slices.Clone(p.List)
		mutate(&list[0])
		if fingerprint(list) == original {
			t.Fatal("stable field change was not detected")
		}
	}
}

func TestServiceStackFingerprintCanonicalOrdering(t *testing.T) {
	a := ServiceStack{ID: "service", Ports: []ServicePort{{Port: 2}, {Port: 1}}, CurrentAutoscaling: map[string]json.RawMessage{"verticalAutoscaling": json.RawMessage(`{"minResource":{"cpuCoreCount":1,"memoryGBytes":2},"cpuMode":"SHARED"}`)}}
	b := a
	b.Ports = []ServicePort{{Port: 1}, {Port: 2}}
	b.CurrentAutoscaling = map[string]json.RawMessage{"verticalAutoscaling": json.RawMessage(`{"cpuMode":"SHARED","minResource":{"memoryGBytes":2,"cpuCoreCount":1}}`)}
	first, err := serviceStackFingerprint([]ServiceStack{a})
	if err != nil {
		t.Fatal(err)
	}
	second, err := serviceStackFingerprint([]ServiceStack{b})
	if err != nil {
		t.Fatal(err)
	}
	if first != second || a.Ports[0].Port != 2 {
		t.Fatal("fingerprint depends on object/port ordering or altered caller inventory")
	}
}

func TestPlatformSnapshotListingFailureFallsBack(t *testing.T) {
	now := time.Now().Add(time.Minute)
	restore := infra.SetSnapshotClockForTest(func() time.Time { return now })
	defer restore()
	b, fake, counter := snapshotBackend(t)
	ctx := context.Background()
	steady := infra.WithSteadyObservation(infra.ObservationContext(ctx))
	snapshotWarmListing(t, b, steady)
	for _, readCtx := range []context.Context{ctx, steady} {
		if _, err := b.client.GetService(readCtx, "service"); err != nil {
			t.Fatal(err)
		}
	}
	fake.mu.Lock()
	fake.services["service"].Name = "changed"
	fake.mu.Unlock()
	now = now.Add(infra.SnapshotListingInterval + time.Second)
	counter.failListing.Store(true)
	counter.requests.Store(0)
	s, err := b.client.GetService(steady, "service")
	if err != nil || s.Name != "changed" || counter.requests.Load() != 2 {
		t.Fatalf("failed listing did not force live detail: %+v requests=%d err=%v", s, counter.requests.Load(), err)
	}
	counter.failListing.Store(false)
	// Failed checks are rate-limited; move beyond the shared retry window.
	now = now.Add(infra.SnapshotListingInterval)
	if _, err := b.client.GetService(steady, "service"); err != nil {
		t.Fatal(err)
	}
	counter.requests.Store(0)
	if _, err := b.client.GetService(steady, "service"); err != nil {
		t.Fatal(err)
	}
	if counter.requests.Load() != 0 {
		t.Fatal("snapshot did not recover after successful listing")
	}
}

func TestPlatformSnapshotSignedURLsNeverCached(t *testing.T) {
	b, _, counter := snapshotBackend(t)
	steady := infra.WithRequestCaller(infra.WithSteadyObservation(infra.ObservationContext(context.Background())), infra.CallerAPI)
	reads := func() uint64 {
		for _, provider := range infra.PlatformCounters() {
			if provider.Provider == "zerops" {
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
	external := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{ "items": [] }`)
	}))
	defer external.Close()
	for _, target := range []string{external.URL + "/signed-log", b.client.base + "/signed-log?serviceStackId=service"} {
		counter.requests.Store(0)
		for range 2 {
			var out any
			if err := b.client.request(steady, http.MethodGet, target, func() io.Reader { return nil }, 0, "", &out, false, false); err != nil {
				t.Fatal(err)
			}
		}
		if n := counter.requests.Load(); n != 2 {
			t.Fatalf("signed URL requests = %d, want 2", n)
		}
	}
	if got := reads() - before; got != 4 {
		t.Fatalf("signed-log network GET counter=%d, want 4", got)
	}
	t.Log("signed-log GETs counted=4, cached=0")
}

func TestFakeContainersDistinctAddresses(t *testing.T) {
	b, fake, _ := snapshotBackend(t)
	fake.services["second"] = &ServiceStack{ID: "second", Name: "second", Status: "ACTIVE"}
	fake.services["service"].CurrentAutoscaling = map[string]json.RawMessage{"horizontalAutoscaling": json.RawMessage(`{"minContainerCount":2}`)}
	seen4, seen6 := map[string]bool{}, map[string]bool{}
	for _, id := range []string{"service", "second"} {
		containers, err := getPages[RuntimeContainer](context.Background(), b.client, "/service-stack/"+id+"/container")
		if err != nil {
			t.Fatal(err)
		}
		for _, c := range containers {
			if seen4[c.IPv4] || seen6[c.IPv6] {
				t.Fatalf("duplicate generated address: %+v", c)
			}
			seen4[c.IPv4], seen6[c.IPv6] = true, true
		}
	}
	if len(seen4) != 3 {
		t.Fatalf("containers = %d, want 3", len(seen4))
	}
}

func TestSnapshotCaptureBounded(t *testing.T) {
	var capture snapshotCapture
	chunk := []byte(strings.Repeat(" ", 32769))
	for !capture.overflow {
		if n, err := capture.Write(chunk); err != nil || n != len(chunk) {
			t.Fatalf("capture write = %d, %v", n, err)
		}
		if len(capture.data) > infra.SnapshotMaxBody || cap(capture.data) > infra.SnapshotMaxBody {
			t.Fatalf("capture exceeded limit: len=%d cap=%d", len(capture.data), cap(capture.data))
		}
	}
	if len(capture.data) != 0 || cap(capture.data) != 0 {
		t.Fatal("overflow retained an uncacheable response")
	}
	if n, err := capture.Write(chunk); err != nil || n != len(chunk) || len(capture.data) != 0 {
		t.Fatal("overflow must continue consuming without capturing")
	}
	var exact snapshotCapture
	if _, err := exact.Write(make([]byte, infra.SnapshotMaxBody)); err != nil || exact.overflow || len(exact.data) != infra.SnapshotMaxBody {
		t.Fatal("exact-limit response must remain cacheable")
	}
}

func TestPlatformSnapshotOversizedTailNotCached(t *testing.T) {
	var details atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/project/project/service-stack" {
			fakePage(w, r, []ServiceStack{{ID: "service", Name: "member"}})
			return
		}
		details.Add(1)
		if _, err := io.WriteString(w, `{"id":"service","name":"member"}`); err != nil {
			return
		}
		// Stream a whitespace tail rather than constructing an oversized body.
		chunk := strings.Repeat(" ", 32<<10)
		for range infra.SnapshotMaxBody/len(chunk) + 1 {
			if _, err := io.WriteString(w, chunk); err != nil {
				return
			}
		}
	}))
	defer server.Close()
	client, err := NewClient(Options{BaseURL: server.URL, Token: "token", ProjectID: "project", MaxRetries: -1})
	if err != nil {
		t.Fatal(err)
	}
	fresh := context.Background()
	steady := infra.WithSteadyObservation(infra.ObservationContext(fresh))
	for _, ctx := range []context.Context{fresh, steady, steady} {
		s, err := client.GetService(ctx, "service")
		if err != nil || s.ID != "service" || s.Name != "member" {
			t.Fatalf("valid JSON with oversized tail must still decode: %+v, %v", s, err)
		}
	}
	if n := details.Load(); n != 3 {
		t.Fatalf("oversized responses were cached: detail requests=%d, want 3", n)
	}
}
