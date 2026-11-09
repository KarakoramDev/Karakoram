// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package dokploy

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func snapshotSteadyContext(t *testing.T) context.Context {
	t.Helper()
	return infra.WithSteadyObservation(infra.ObservationContext(t.Context()))
}

func snapshotReadNodes(t *testing.T, b *Backend, ctx context.Context) {
	t.Helper()
	var nodes []node
	if err := b.c.get(ctx, "cluster.getNodes", nil, &nodes); err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 3 {
		t.Fatalf("cached body lost nodes: got %d", len(nodes))
	}
}

func TestPlatformSnapshotSteadyAndFreshContexts(t *testing.T) {
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	steady := snapshotSteadyContext(t)
	snapshotReadNodes(t, b, steady)
	before := fakeCalls(f, "cluster.getNodes")
	listing := fakeCalls(f, "environment.one")
	snapshotReadNodes(t, b, steady)
	if fakeCalls(f, "cluster.getNodes") != before || fakeCalls(f, "environment.one") != listing {
		t.Fatal("warm steady read reached the platform")
	}
	for _, ctx := range []context.Context{t.Context(), infra.WithoutObservation(steady), infra.WithFreshTaskInventory(steady)} {
		snapshotReadNodes(t, b, ctx)
		before++
		if got := fakeCalls(f, "cluster.getNodes"); got != before {
			t.Fatalf("fresh context served snapshot: got %d requests, want %d", got, before)
		}
	}
}

func TestSteadyInspectionCannotAuthorizePinnedDial(t *testing.T) {
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	importFake(t, b, runtimeSpec("runtime", ""))
	deployFake(t, b, "runtime", "")
	app := fakeAppSnapshot(t, f, "runtime")
	name := fakeString(app, "appName")
	// Establish the listing baseline and warm the platform inspection before
	// attaching the pass memo whose two freshness classes we exercise.
	warm := snapshotSteadyContext(t)
	if _, err := b.inspectService(warm, name); err != nil {
		t.Fatal(err)
	}
	before := fakeCalls(f, "docker.getConfig")
	steady := infra.WithObservationCache(warm, new(sync.Map))
	if _, err := b.inspectService(steady, name); err != nil {
		t.Fatal(err)
	}
	if got := fakeCalls(f, "docker.getConfig"); got != before {
		t.Fatalf("steady inspection missed warm snapshot: got %d, want %d", got, before)
	}
	fresh := infra.WithFreshTaskInventory(steady)
	if _, err := b.Containers(fresh, "runtime"); err != nil {
		t.Fatal(err)
	}
	if got := fakeCalls(f, "docker.getConfig"); got != before+1 {
		t.Fatalf("dial authorization reused steady inspection: got %d, want %d", got, before+1)
	}
	// Fresh evidence is reusable by both classes within the read-only pass.
	for _, ctx := range []context.Context{fresh, steady} {
		if _, err := b.inspectService(ctx, name); err != nil {
			t.Fatal(err)
		}
	}
	if got := fakeCalls(f, "docker.getConfig"); got != before+1 {
		t.Fatalf("fresh inspection was not shared: got %d, want %d", got, before+1)
	}
}

func TestPlatformSnapshotSharedTargetAndMutation(t *testing.T) {
	f := newFakeAPI(t)
	first, second := fakeBackend(t, f, nil), fakeBackend(t, f, nil)
	if first.PlatformSnapshot() != second.PlatformSnapshot() {
		t.Fatal("same target does not share snapshot")
	}
	steady := snapshotSteadyContext(t)
	snapshotReadNodes(t, first, steady)
	before := fakeCalls(f, "cluster.getNodes")
	snapshotReadNodes(t, second, steady)
	if fakeCalls(f, "cluster.getNodes") != before {
		t.Fatal("second backend missed shared snapshot")
	}
	if err := second.c.post(t.Context(), "network.create", map[string]any{"name": "snapshot-network"}, nil); err != nil {
		t.Fatal(err)
	}
	snapshotReadNodes(t, first, steady)
	if fakeCalls(f, "cluster.getNodes") != before+1 {
		t.Fatal("sibling mutation did not invalidate snapshot")
	}
	// Failed mutations are just as uncertain as successful ones.
	if err := second.c.post(t.Context(), "missing.procedure", map[string]any{}, nil); err == nil {
		t.Fatal("expected rejected mutation")
	}
	snapshotReadNodes(t, first, steady)
	if fakeCalls(f, "cluster.getNodes") != before+2 {
		t.Fatal("failed sibling mutation did not invalidate snapshot")
	}
}

func TestPlatformSnapshotEnvironmentIsolation(t *testing.T) {
	f := newFakeAPI(t)
	first := fakeBackend(t, f, nil)
	f.mu.Lock()
	f.environments["other"] = map[string]any{"environmentId": "other", "projectId": "project", "name": "other"}
	f.mu.Unlock()
	factory := newFactory(func(key string) string {
		if key == "KARAKORAM_DOKPLOY_ALLOWED_URLS" {
			return f.server.URL
		}
		return ""
	})
	otherBackend, err := factory.New(infra.Target{Kind: "dokploy", ID: "other", Token: fakeAPIKey, Config: map[string]string{"url": f.server.URL}})
	if err != nil {
		t.Fatal(err)
	}
	other := otherBackend.(*Backend)
	if first.PlatformSnapshot() == other.PlatformSnapshot() {
		t.Fatal("different environments share snapshot")
	}
	steady := snapshotSteadyContext(t)
	snapshotReadNodes(t, first, steady)
	before := fakeCalls(f, "cluster.getNodes")
	snapshotReadNodes(t, other, steady)
	if fakeCalls(f, "cluster.getNodes") != before+1 {
		t.Fatal("different environment served first environment's snapshot")
	}
	if err := other.c.post(t.Context(), "network.create", map[string]any{"name": "other-network"}, nil); err != nil {
		t.Fatal(err)
	}
	snapshotReadNodes(t, first, steady)
	if fakeCalls(f, "cluster.getNodes") != before+1 {
		t.Fatal("different environment invalidated first environment")
	}
}

func TestPlatformSnapshotListingDriftAndIdleStability(t *testing.T) {
	now := time.Now().Add(time.Second)
	restore := infra.SetSnapshotClockForTest(func() time.Time { return now })
	defer restore()
	f := newFakeAPI(t)
	f.mu.Lock()
	f.apps["app"] = &fakeApplication{row: map[string]any{"applicationId": "app", "name": "member", "environmentId": "environment", "applicationStatus": "done", "description": "owner", "serverId": "node-a", "createdAt": "first"}}
	f.mu.Unlock()
	b := fakeBackend(t, f, nil)
	steady := snapshotSteadyContext(t)
	snapshotReadNodes(t, b, steady)
	before := fakeCalls(f, "cluster.getNodes")
	listing := fakeCalls(f, "environment.one")
	generation := b.PlatformSnapshot().DriftGeneration()
	f.mu.Lock()
	f.apps["app"].row["createdAt"] = "later"
	f.mu.Unlock()
	now = now.Add(infra.SnapshotListingInterval + time.Second)
	snapshotReadNodes(t, b, steady)
	if fakeCalls(f, "cluster.getNodes") != before || fakeCalls(f, "environment.one") != listing+1 {
		t.Fatal("idle listing was not one fresh GET with a stable fingerprint")
	}
	if b.PlatformSnapshot().DriftGeneration() != generation {
		t.Fatal("volatile timestamp changed listing fingerprint")
	}
	f.mu.Lock()
	f.apps["app"].row["applicationStatus"] = "error"
	f.mu.Unlock()
	now = now.Add(infra.SnapshotListingInterval + time.Second)
	snapshotReadNodes(t, b, steady)
	if fakeCalls(f, "cluster.getNodes") != before+1 || fakeCalls(f, "environment.one") != listing+2 {
		t.Fatal("status drift did not invalidate snapshot after one fresh listing")
	}
	if b.PlatformSnapshot().DriftGeneration() <= generation {
		t.Fatal("status drift did not advance drift generation")
	}
}

func TestPlatformSnapshotHitBypassesReadAdmission(t *testing.T) {
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	steady := snapshotSteadyContext(t)
	snapshotReadNodes(t, b, steady)
	for range 8 {
		if err := b.c.observations.Acquire(t.Context()); err != nil {
			t.Fatal(err)
		}
		defer b.c.observations.Release()
	}
	ctx, cancel := context.WithTimeout(steady, 100*time.Millisecond)
	defer cancel()
	snapshotReadNodes(t, b, ctx)
}

func TestPlatformSnapshotOptionalAppNameAndOrdering(t *testing.T) {
	now := time.Now().Add(time.Second)
	restore := infra.SetSnapshotClockForTest(func() time.Time { return now })
	defer restore()
	var version atomic.Int32
	var reads atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/environment.one" {
			reads.Add(1)
			_, _ = w.Write([]byte(`{}`))
			return
		}
		switch version.Load() {
		case 0:
			_, _ = w.Write([]byte(`{"applications":[{"applicationId":"a","appName":"original"},{"applicationId":"b"}]}`))
		case 1:
			_, _ = w.Write([]byte(`{"applications":[{"applicationId":"b"},{"applicationId":"a","appName":"original"}]}`))
		default:
			_, _ = w.Write([]byte(`{"applications":[{"applicationId":"a","appName":"changed"},{"applicationId":"b"}]}`))
		}
	}))
	defer server.Close()
	c := newClient(server.URL, "test")
	c.setSnapshot("environment")
	steady := snapshotSteadyContext(t)
	read := func() {
		t.Helper()
		if err := c.get(steady, "application.one", nil, nil); err != nil {
			t.Fatal(err)
		}
	}
	read()
	version.Store(1)
	now = now.Add(infra.SnapshotListingInterval + time.Second)
	read()
	if reads.Load() != 1 {
		t.Fatal("listing order changed fingerprint")
	}
	version.Store(2)
	now = now.Add(infra.SnapshotListingInterval + time.Second)
	read()
	if reads.Load() != 2 {
		t.Fatal("optional appName drift did not invalidate")
	}
}
