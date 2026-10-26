// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package supabase

import (
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func TestSweepHungNativePreservesHealthyPeers(t *testing.T) {
	for _, component := range []string{"auth", "gateway"} {
		t.Run(component, func(t *testing.T) {
			p, rc, backend := setupProvider(t)
			convergeProvider(t, p, rc)
			s, err := loadState(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			baseline, err := p.Status(t.Context(), rc)
			if err != nil {
				t.Fatal(err)
			}
			m := s.Members[component]
			a := newGatewayInstanceFixture(t, m.Name, m.ConfigVersion, nil)
			a.hung = true
			rc.Backend = &componentInventoryBackend{testBackend: backend, member: m.Name, instances: []infra.Container{{ID: "hung-allocation", Hostname: "gateway-a", Status: infra.ContainerRunning}}}
			rc.Agents = gatewayInstanceAgents{AgentDialer: rc.Agents, member: m.Name, instances: map[string]*gatewayInstanceFixture{"gateway-a": a}}
			rc.Clock = engine.RealClock{}
			status, err := engine.ObserveStatus(t.Context(), p, rc)
			if err != nil || status.ObservedAt.IsZero() || status.Phase == domain.PhaseReady {
				t.Fatalf("hung native sweep lost snapshot or retained Ready: %+v %v", status, err)
			}
			if len(status.Members) != len(baseline.Members) {
				t.Fatalf("native timeout removed members: got %d want %d", len(status.Members), len(baseline.Members))
			}
			found := false
			for _, member := range status.Members {
				if !member.Agent.Healthy {
					t.Fatalf("native timeout disconnected reachable agent: %+v", member)
				}
				if member.Name == m.Name {
					found = true
					if member.Health || !strings.Contains(member.Agent.Message, "hung-allocation") || !strings.Contains(member.Agent.Message, "restore observations") {
						t.Fatalf("missing native diagnosis: %+v", member)
					}
				} else if !member.Health {
					t.Fatalf("healthy peer lost native health: %+v", member)
				}
			}
			if !found {
				t.Fatal("hung member absent")
			}
			for _, c := range status.Conditions {
				if c.Type == "Ready" && c.Status == "True" || c.Type == "ComponentsReady" && c.Status == "True" {
					t.Fatal("false readiness", c)
				}
			}
		})
	}
}
