// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"fmt"
	"os"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/infra/fake"
)

func TestCreateClusterWithDeploymentAdmission(t *testing.T) {
	for _, backend := range []string{"zerops", "dokploy"} {
		for _, members := range []int{3, 5} {
			for _, cap := range []int{1, 2} {
				t.Run(fmt.Sprintf("%s/members%d/cap%d", backend, members, cap), func(t *testing.T) {
					p, rc, b, _ := coreSetup(t, members)
					peers := make([]string, members)
					for i := range members {
						peers[i] = NodeHostname("demo", i)
					}
					admission := &fake.Admission{Backend: b, Limit: cap, Peers: peers, LegacyNative: os.Getenv("KARAKORAM_TEST_LEGACY_NATIVE_READINESS") == "1"}
					rc.Backend = admission
					for range 300 {
						admission.Advance()
						if _, err := p.Sync(t.Context(), rc); err != nil {
							t.Fatal(err)
						}
						s, err := load(rc.Cluster)
						if err != nil {
							t.Fatal(err)
						}
						if s.Phase == "ready" && s.Pending == nil && s.Rolling == nil {
							if len(admission.Submitted) < members || admission.Peak > cap || len(rc.Cluster.Status.DeploymentFailures) != 0 {
								t.Fatalf("invalid completion: submitted=%d peak=%d failures=%v", len(admission.Submitted), admission.Peak, rc.Cluster.Status.DeploymentFailures)
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

func TestFrontendDeploymentRetainsNativeReadinessWithHealthyDependencies(t *testing.T) {
	p, rc, _, _ := coreSetup(t, 3)
	rc.Cluster.Status = domain.ClusterStatus{ObservedAt: rc.Clock.Now(), ObservedGeneration: 1, Members: []domain.MemberStatus{{Name: "demon0", Health: true, Agent: domain.AgentStatus{Healthy: true}}}}
	m := &member{Name: "demokb", Kind: kindKibana}
	spec, err := p.deploymentSpec(rc, m, rc.Cluster.Spec.Version)
	if err != nil {
		t.Fatal(err)
	}
	if spec.Readiness.Args[2] != "--engine=elasticsearch" || m.NativeReadiness == nil || !*m.NativeReadiness {
		t.Fatal("frontend lost native overlap readiness", spec.Readiness)
	}
	rc.Cluster.Status = domain.ClusterStatus{}
	retry, err := p.deploymentSpec(rc, m, rc.Cluster.Spec.Version)
	if err != nil || fmt.Sprint(retry.Readiness.Args) != fmt.Sprint(spec.Readiness.Args) {
		t.Fatal("retry changed probe", retry.Readiness, err)
	}
}
