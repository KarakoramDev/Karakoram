// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package namespaces

import (
	"context"
	"errors"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type instanceInventoryBackend struct {
	*projectBackend
	inventories map[string][]infra.Container
	err         error
}

func (b *instanceInventoryBackend) Containers(ctx context.Context, member string) ([]infra.Container, error) {
	if !infra.FreshTaskInventory(ctx) {
		return nil, errors.New("authorization did not require fresh task inventory")
	}
	return b.inventories[member], b.err
}

func TestInstanceAuthorizationUsesNamespaceOwnedLiveInventory(t *testing.T) {
	r, backend, _ := setupRegistry(t)
	n := createTestNamespace(t, r)
	inventory := &instanceInventoryBackend{projectBackend: backend, inventories: map[string][]infra.Container{
		"demopx":     {{ID: "a", Hostname: "proxy.1.taskid", IPv4: "192.0.2.10", IPv6: "2001:db8::10", Status: infra.ContainerRunning}, {ID: "b", Hostname: "stopped.taskid", IPv4: "192.0.2.11", Status: infra.StatusStopped}},
		"otherpx":    {{ID: "other", Hostname: "other.taskid", IPv4: "192.0.2.12", Status: infra.ContainerActive}},
		"startingpx": {{ID: "new", Hostname: "starting.1.new", IPv4: "192.0.2.13", Status: infra.ContainerStarting}},
	}}
	r.mu.Lock()
	r.backends[n.Name] = inventory
	r.mu.Unlock()
	for _, instance := range []string{"proxy.1.taskid", "192.0.2.10", "2001:db8::10"} {
		if err := r.authorizeInstance(t.Context(), n.Name, "demopx", instance); err != nil {
			t.Fatalf("owned live instance %s refused: %v", instance, err)
		}
	}
	for _, instance := range []string{"192.0.2.11", "stopped.taskid", "other.taskid", "192.0.2.12", "192.0.2.99", "demopx", ""} {
		if err := r.authorizeInstance(t.Context(), n.Name, "demopx", instance); err == nil {
			t.Fatalf("unowned/stopped instance accepted: %s", instance)
		}
	}
	if err := r.authorizeInstance(t.Context(), n.Name, "startingpx", "starting.1.new"); err != nil {
		t.Fatalf("owned starting task refused: %v", err)
	}
	if err := r.authorizeInstance(t.Context(), n.Name, "demopx", "starting.1.new"); err == nil {
		t.Fatal("foreign starting task accepted")
	}
	// Cross-namespace lookup must not consult the first namespace's backend.
	if err := r.authorizeInstance(t.Context(), "not-enrolled", "demopx", "192.0.2.10"); err == nil {
		t.Fatal("cross-namespace instance accepted")
	}
	inventory.inventories["demopx"] = nil
	if err := r.authorizeInstance(t.Context(), n.Name, "demopx", "192.0.2.10"); err == nil {
		t.Fatal("disappeared instance authorized from cached inventory")
	}
	inventory.err = errors.New("inventory unavailable")
	if err := r.authorizeInstance(t.Context(), n.Name, "demopx", "proxy.1.taskid"); !errors.Is(err, inventory.err) {
		t.Fatal("inventory failure must fail closed", err)
	}
}
