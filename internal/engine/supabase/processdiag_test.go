// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package supabase

import (
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func TestComponentProcessDiagnosticsEveryInstance(t *testing.T) {
	for _, state := range []string{"paused", "error", "running", "native-error"} {
		t.Run(state, func(t *testing.T) {
			p, rc, backend := setupProvider(t)
			convergeProvider(t, p, rc)
			s, err := loadState(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			m := s.Members["auth"]
			desired := rc.Cluster.Spec.Components.Extra["auth"]
			desired.Replicas, m.Replicas = 2, 2
			rc.Cluster.Spec.Components.Extra["auth"] = desired
			if err := saveState(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			a := newGatewayInstanceFixture(t, m.Name, m.ConfigVersion, nil)
			b := newGatewayInstanceFixture(t, m.Name, m.ConfigVersion, a.server.TLS.Clone())
			rc.Backend = &componentInventoryBackend{testBackend: backend, member: m.Name, instances: []infra.Container{
				{ID: "allocation-a", Hostname: "gateway-a", Status: infra.ContainerRunning},
				{ID: "allocation-b", Hostname: "gateway-b", Status: infra.ContainerActive},
			}}
			rc.Agents = gatewayInstanceAgents{AgentDialer: rc.Agents, member: m.Name, instances: map[string]*gatewayInstanceFixture{"gateway-a": a, "gateway-b": b}}
			b.mu.Lock()
			process := agentapi.ProcessStatus{State: state}
			if state == "error" {
				process.Error = "supervised child failed"
			}
			b.processes = map[string]agentapi.ProcessStatus{"auth": process}
			if state == "native-error" {
				b.unobservable = true
			}
			b.mu.Unlock()
			status, err := p.Status(t.Context(), rc)
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, member := range status.Members {
				if member.Name != m.Name {
					continue
				}
				found = true
				if !member.Agent.Healthy || member.Role != "auth" || member.Agent.ConfigVersion != m.ConfigVersion {
					t.Fatalf("lost agent connectivity or role: %+v", member)
				}
				if state == "running" {
					if !member.Health || status.Phase != domain.PhaseReady {
						t.Fatalf("healthy no-op failed: %+v phase=%s", member, status.Phase)
					}
				} else {
					if member.Health || status.Phase == domain.PhaseReady {
						t.Fatalf("failed component hidden: %+v", member)
					}
					if state != "native-error" && member.State != state {
						t.Fatalf("lost process state: %+v", member)
					}
				}
			}
			if !found {
				t.Fatal("component missing")
			}
			diagnosed := false
			for _, condition := range status.Conditions {
				if condition.Type == "Process/"+m.Name+"/auth/allocation-b" {
					diagnosed = true
					reason := "ProcessError"
					if state == "paused" {
						reason = "ProcessPaused"
						if !strings.Contains(condition.Message, "SIGCONT") {
							t.Fatal("missing safe recovery guidance")
						}
					}
					if condition.Reason != reason || condition.Status != "False" {
						t.Fatalf("wrong process condition: %+v", condition)
					}
				}
				if state != "running" && condition.Type == "Ready" && condition.Status == "True" {
					t.Fatal("failed child reported Ready")
				}
			}
			if diagnosed != (state == "paused" || state == "error") {
				t.Fatalf("unexpected process diagnostics: %+v", status.Conditions)
			}
			observed, err := componentStatus(t.Context(), rc, m)
			if err != nil || observed.Ready != (state == "running") {
				t.Fatalf("reconcile readiness lost diagnostics: %+v %v", observed, err)
			}
		})
	}
}
