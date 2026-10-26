// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/infra/fake"
)

func TestCreateHAWithDeploymentAdmission(t *testing.T) {
	for _, backend := range []string{"zerops", "dokploy"} {
		for _, members := range []int{3, 5} {
			for _, cap := range []int{1, 2} {
				t.Run(fmt.Sprintf("%s/members%d/cap%d", backend, members, cap), func(t *testing.T) {
					p, rc, b := setup(t, "ha")
					rc.Cluster.Spec.Components.Engine.Replicas = members
					peers := make([]string, members)
					for i := range members {
						peers[i] = fmt.Sprintf("demop%d", i)
					}
					admission := &fake.Admission{Backend: b, Limit: cap, Peers: peers, LegacyNative: os.Getenv("KARAKORAM_TEST_LEGACY_NATIVE_READINESS") == "1"}
					rc.Backend = admission
					for range 200 {
						admission.Advance()
						if _, err := p.Sync(t.Context(), rc); err != nil {
							t.Fatal(err)
						}
						status, err := p.Status(t.Context(), rc)
						if err != nil {
							t.Fatal(err)
						}
						rc.Cluster.Status = status
						if status.Phase == domain.PhaseReady {
							if len(admission.Submitted) < members || admission.Peak > cap || len(status.DeploymentFailures) != 0 {
								t.Fatalf("invalid completion: submitted=%d peak=%d failures=%v", len(admission.Submitted), admission.Peak, status.DeploymentFailures)
							}
							if !admission.Ready() {
								t.Fatalf("model not natively converged: probes=%v", admission.ProbeErrors)
							}
							return
						}
					}
					t.Fatalf("admission bootstrap did not converge: commands=%v probes=%v", admission.Commands, admission.ProbeErrors)
				})
			}
		}
	}
}

func TestProxyDeploymentRetainsNativeReadinessWithHealthyDependencies(t *testing.T) {
	p, rc, _ := setup(t, "ha")
	rc.Cluster.Status = domain.ClusterStatus{ObservedAt: time.Now(), ObservedGeneration: 1, Members: []domain.MemberStatus{{Name: "demop0", Health: true, Agent: domain.AgentStatus{Healthy: true}}}}
	m := &member{Name: "demopx", Role: "proxy", Topology: "ha"}
	spec, err := p.runtimeSpec(rc, m)
	if err != nil {
		t.Fatal(err)
	}
	if spec.Readiness.Args[2] != "--engine=postgres" || m.NativeReadiness == nil || !*m.NativeReadiness {
		t.Fatal("proxy lost native overlap readiness", spec.Readiness)
	}
	rc.Cluster.Status = domain.ClusterStatus{}
	retry, err := p.runtimeSpec(rc, m)
	if err != nil || fmt.Sprint(retry.Readiness.Args) != fmt.Sprint(spec.Readiness.Args) {
		t.Fatal("retry changed probe", retry.Readiness, err)
	}
}

func TestApplyFailureIsVisibleInControlPlaneMemberStatus(t *testing.T) {
	for _, name := range []string{"demop0", "demopx"} {
		t.Run(name, func(t *testing.T) {
			p, rc, backend := setup(t, "ha")
			s := converge(t, p, rc)
			diagnostic := "engine configuration apply failed while dependencies unavailable"
			backend.genericOverrides = map[string]agentapi.Status{name: {ConfigVersion: s.ConfigVersion, Error: diagnostic}}
			status, err := p.Status(t.Context(), rc)
			if err != nil {
				t.Fatal(err)
			}
			if status.Phase == domain.PhaseReady {
				t.Fatal("Apply failure reported Ready")
			}
			for _, member := range status.Members {
				if member.Name == name {
					if member.Health || !member.Agent.Healthy || !strings.Contains(member.Agent.Message, diagnostic) {
						t.Fatal("Apply failure missing from CP member status", member)
					}
					return
				}
			}
			t.Fatal("member absent from CP status", name)
		})
	}
}

func TestSuccessfulApplyStatusRetainsClusterHealthAfterSupersededReply(t *testing.T) {
	p, rc, backend := setup(t, "ha")
	s := converge(t, p, rc)
	// The agent regression applies N, refuses N-1, and proves that its wire
	// status retains N without an active Error. Consume that status contract
	// here so a stale-reply diagnostic cannot flap CP member/cluster health.
	backend.genericOverrides = map[string]agentapi.Status{"demop0": {ConfigVersion: s.ConfigVersion}}
	status, err := p.Status(t.Context(), rc)
	if err != nil || status.Phase != domain.PhaseReady {
		t.Fatal("successful newer apply lost cluster health", status, err)
	}
	for _, member := range status.Members {
		if member.Name == "demop0" {
			if !member.Health || !member.Agent.Healthy || member.Agent.Message != "" {
				t.Fatal("successful newer apply lost member health", member)
			}
			return
		}
	}
	t.Fatal("newer applied member missing")
}
