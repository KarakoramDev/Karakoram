// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package supabase

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type componentInventoryBackend struct {
	*testBackend
	member    string
	instances []infra.Container
	err       error
}

func (b *componentInventoryBackend) Containers(ctx context.Context, name string) ([]infra.Container, error) {
	if name == b.member {
		return b.instances, b.err
	}
	return b.testBackend.Containers(ctx, name)
}

func TestComponentReadinessRequiresEveryInstance(t *testing.T) {
	for _, failure := range []string{"unhealthy", "rest-database-unavailable", "native-error", "stale-config", "unreachable", "missing", "desired-increase", "extra-live-unhealthy", "inventory-error", "shared-address", "no-address"} {
		t.Run(failure, func(t *testing.T) {
			p, rc, backend := setupProvider(t)
			convergeProvider(t, p, rc)
			state, err := loadState(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			component := "auth"
			if failure == "rest-database-unavailable" {
				component = "rest"
			}
			m := state.Members[component]
			desired := rc.Cluster.Spec.Components.Extra[component]
			desired.Replicas = 2
			m.Replicas = 2
			if failure == "desired-increase" {
				m.Replicas = 1
			}
			if failure == "extra-live-unhealthy" {
				m.Replicas, desired.Replicas = 1, 1
			}
			rc.Cluster.Spec.Components.Extra[component] = desired
			if err := saveState(rc.Cluster, state); err != nil {
				t.Fatal(err)
			}
			a := newGatewayInstanceFixture(t, m.Name, m.ConfigVersion, nil)
			b := newGatewayInstanceFixture(t, m.Name, m.ConfigVersion, a.server.TLS.Clone())
			inventory := &componentInventoryBackend{testBackend: backend, member: m.Name, instances: []infra.Container{
				{ID: "allocation-a", Hostname: "gateway-a", Status: infra.ContainerRunning},
				{ID: "allocation-b", Hostname: "gateway-b", Status: infra.ContainerActive},
			}}
			rc.Backend = inventory
			rc.Agents = gatewayInstanceAgents{AgentDialer: rc.Agents, member: m.Name, instances: map[string]*gatewayInstanceFixture{"gateway-a": a, "gateway-b": b}}
			b.mu.Lock()
			switch failure {
			case "unhealthy", "extra-live-unhealthy":
				b.ready, b.message = false, "component health endpoint is unavailable"
			case "rest-database-unavailable":
				b.ready, b.message = false, "REST database/schema cache serving probe returned HTTP 503; inspect PostgREST and its PostgreSQL primary connectivity"
			case "native-error":
				b.unobservable = true
			case "stale-config":
				b.version--
			case "unreachable":
				inventory.instances[1].Hostname = "unreachable"
			case "missing", "desired-increase":
				inventory.instances = inventory.instances[:1]
			case "inventory-error":
				inventory.err = errors.New("inventory unavailable")
			case "shared-address":
				inventory.instances[1].Hostname = m.Name
			case "no-address":
				inventory.instances[1].Hostname = ""
			}
			b.mu.Unlock()
			status, err := p.Status(t.Context(), rc)
			if err != nil {
				t.Fatal(err)
			}
			if status.Phase == domain.PhaseReady {
				t.Fatal("healthy logical endpoint hid failed sibling")
			}
			found := false
			for _, member := range status.Members {
				if member.Name != m.Name {
					continue
				}
				found = true
				if member.Health || member.Agent.Message == "" {
					t.Fatalf("missing component diagnosis: %+v", member)
				}
				if (failure == "unhealthy" || failure == "rest-database-unavailable") && (!strings.Contains(member.Agent.Message, "allocation-b") || !strings.Contains(member.Agent.Message, b.message)) {
					t.Fatalf("native failure lost: %s", member.Agent.Message)
				}
			}
			if !found {
				t.Fatal("missing component member")
			}
			componentCondition := false
			for _, condition := range status.Conditions {
				if condition.Type == "ComponentsReady" && condition.Status == "False" && condition.Message != "" {
					componentCondition = true
				}
			}
			if !componentCondition {
				t.Fatal("missing current component condition")
			}
			b.mu.Lock()
			b.ready, b.message, b.version = true, "", m.ConfigVersion
			b.unobservable = false
			b.mu.Unlock()
			inventory.err = nil
			inventory.instances = []infra.Container{{ID: "allocation-a", Hostname: "gateway-a", Status: infra.ContainerRunning}, {ID: "allocation-b", Hostname: "gateway-b", Status: infra.ContainerActive}}
			if failure == "desired-increase" {
				// Model the infrastructure scale acknowledgement as well as
				// the newly visible instance's application acknowledgement.
				m.Replicas = desired.Replicas
				if err := saveState(rc.Cluster, state); err != nil {
					t.Fatal(err)
				}
			}
			status, err = p.Status(t.Context(), rc)
			if err != nil || status.Phase != domain.PhaseReady {
				t.Fatalf("healthy instances failed recovery: %s %v", status.Phase, err)
			}
		})
	}
}

func TestComponentBootstrapRequiresAllAppliedInstances(t *testing.T) {
	p, rc, backend := setupProvider(t)
	convergeProvider(t, p, rc)
	state, err := loadState(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	m := state.Members["auth"]
	m.Stage, m.Replicas = engine.ComponentDeployed, 2
	desired := rc.Cluster.Spec.Components.Extra["auth"]
	desired.Replicas = 2
	rc.Cluster.Spec.Components.Extra["auth"] = desired
	if err := saveState(rc.Cluster, state); err != nil {
		t.Fatal(err)
	}
	a := newGatewayInstanceFixture(t, m.Name, m.ConfigVersion, nil)
	b := newGatewayInstanceFixture(t, m.Name, m.ConfigVersion-1, a.server.TLS.Clone())
	rc.Backend = &componentInventoryBackend{testBackend: backend, member: m.Name, instances: []infra.Container{
		{ID: "allocation-a", Hostname: "gateway-a", Status: infra.ContainerRunning},
		{ID: "allocation-b", Hostname: "gateway-b", Status: infra.ContainerRunning},
	}}
	rc.Agents = gatewayInstanceAgents{AgentDialer: rc.Agents, member: m.Name, instances: map[string]*gatewayInstanceFixture{"gateway-a": a, "gateway-b": b}}
	if _, err := p.syncComponents(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	state, err = loadState(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	if state.Members["auth"].Stage == engine.ComponentJoined {
		t.Fatal("bootstrap joined on one instance's config acknowledgement")
	}
}
