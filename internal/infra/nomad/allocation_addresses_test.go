// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package nomad

import (
	"fmt"
	"maps"
	"runtime"
	"strings"
	"testing"
	"time"
	"weak"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func TestAllocationAddressCacheBoundAndValidation(t *testing.T) {
	var cache allocationAddressCache
	key := allocationAddressKey{target: "nomad|https://nomad.example.com|region-a|default", id: "allocation"}
	for _, addresses := range []allocationAddresses{{}, {Address: "invalid"}, {AddressIPv6: "fe80::1%eth0"}} {
		cache.store(key, addresses)
		if _, ok := cache.lookup(key); ok {
			t.Fatal("empty or invalid addresses were retained")
		}
	}
	want := allocationAddresses{Address: "192.0.2.8", AddressIPv6: "2001:db8::8"}
	cache.store(key, want)
	cache.store(key, allocationAddresses{Address: "192.0.2.9"})
	if got, ok := cache.lookup(key); !ok || got != want {
		t.Fatalf("immutable addresses changed: %+v, %v", got, ok)
	}
	other := key
	other.target = "nomad|https://nomad.example.com|region-b|default"
	if _, ok := cache.lookup(other); ok {
		t.Fatal("allocation IDs leaked across targets")
	}
	for i := range allocationAddressCacheLimit {
		cache.store(allocationAddressKey{target: key.target, id: fmt.Sprint(i)}, want)
	}
	if len(cache.entries) != allocationAddressCacheLimit {
		t.Fatalf("unbounded address cache: %d", len(cache.entries))
	}
	if _, ok := cache.lookup(key); ok {
		t.Fatal("oldest allocation was not evicted")
	}
	if got, ok := cache.lookup(allocationAddressKey{target: key.target, id: fmt.Sprint(allocationAddressCacheLimit - 1)}); !ok || got != want {
		t.Fatal("newest allocation was not retained")
	}
}

func TestAllocationAddressesSharedWithoutCachingLiveness(t *testing.T) {
	f := newFakeNomad(t)
	b := fakeBackend(t, f, nil)
	importFake(t, b, runtimeSpec("runtime", ""))
	deployFake(t, b, "runtime", "")
	f.setAutoTick(false)
	f.mu.Lock()
	f.projectAllocations = true
	f.mu.Unlock()
	before := len(f.requests())
	containers, err := b.Containers(t.Context(), "runtime")
	if err != nil || len(containers) != 1 || containers[0].IPv4 == "" {
		t.Fatalf("initial hydration: %+v, %v", containers, err)
	}
	want := containers[0]
	details := 0
	for _, call := range f.requests()[before:] {
		if strings.HasPrefix(call.Path, "/v1/allocation/") {
			details++
		}
	}
	if details != 1 {
		t.Fatalf("initial hydration made %d detail reads", details)
	}
	second := fakeBackend(t, f, nil)
	ctx := infra.WithFreshTaskInventory(nomadObservationAttempt(t))
	before = len(f.requests())
	for range 2 {
		containers, err := second.Containers(ctx, "runtime")
		if err != nil || len(containers) != 1 || containers[0] != want {
			t.Fatalf("cached addresses: %+v, %v", containers, err)
		}
	}
	listings := 0
	for _, call := range f.requests()[before:] {
		if strings.HasPrefix(call.Path, "/v1/allocation/") {
			t.Fatal("immutable addresses were hydrated again")
		}
		if call.Path == "/v1/job/runtime/allocations" {
			listings++
		}
	}
	if listings != 2 {
		t.Fatalf("fresh authorization made %d listings, want 2", listings)
	}
	f.mu.Lock()
	for _, a := range f.allocs {
		a.row["ClientStatus"] = "complete"
	}
	f.mu.Unlock()
	containers, err = second.Containers(infra.WithoutObservation(ctx), "runtime")
	if err != nil || len(containers) != 0 {
		t.Fatalf("address cache hid terminal allocation: %+v, %v", containers, err)
	}
	f.mu.Lock()
	for id, a := range f.allocs {
		row := maps.Clone(a.row)
		row["ID"], row["ClientStatus"] = "replacement-"+id, "running"
		row["NetworkStatus"] = fakeRow{"Address": "198.51.100.42"}
		f.allocs["replacement-"+id] = &fakeAlloc{row: row}
		break
	}
	f.mu.Unlock()
	containers, err = b.Containers(t.Context(), "runtime")
	if err != nil || len(containers) != 1 || containers[0].IPv4 != "198.51.100.42" || containers[0].ID == want.ID {
		t.Fatalf("replacement reused predecessor addresses: %+v, %v", containers, err)
	}
}

func warmUnreferencedAllocationAddresses(t *testing.T) (weak.Pointer[infra.TargetSnapshot], weak.Pointer[client], allocationAddressKey) {
	t.Helper()
	f := newFakeNomad(t)
	b := fakeBackend(t, f, nil)
	importFake(t, b, runtimeSpec("runtime", ""))
	deployFake(t, b, "runtime", "")
	f.setAutoTick(false)
	f.mu.Lock()
	f.projectAllocations = true
	f.mu.Unlock()
	before := len(f.requests())
	containers, err := b.Containers(t.Context(), "runtime")
	if err != nil || len(containers) != 1 || containers[0].IPv4 == "" {
		t.Fatalf("initial hydration: %+v, %v", containers, err)
	}
	hydrated := false
	for _, request := range f.requests()[before:] {
		if request.Path == "/v1/allocation/"+containers[0].ID {
			hydrated = true
		}
	}
	if !hydrated {
		t.Fatal("fixture did not hydrate allocation addresses")
	}
	key := allocationAddressKey{target: b.c.snapshotKey, id: containers[0].ID}
	if _, ok := allocationAddressInventory.lookup(key); !ok {
		t.Fatal("hydrated addresses were not cached")
	}
	b.c.http.CloseIdleConnections()
	return weak.Make(b.c.snapshot), weak.Make(b.c), key
}

func TestAllocationAddressCacheDoesNotRetainRemovedTarget(t *testing.T) {
	target, client, key := warmUnreferencedAllocationAddresses(t)
	deadline := time.Now().Add(5 * time.Second)
	for {
		runtime.GC()
		if target.Value() == nil && client.Value() == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("allocation address cache retained removed snapshot or listing client")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, ok := allocationAddressInventory.lookup(key); !ok {
		t.Fatal("test did not retain cached addresses while reclaiming the target")
	}
}
