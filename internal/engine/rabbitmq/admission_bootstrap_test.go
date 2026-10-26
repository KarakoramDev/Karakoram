// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package rabbitmq

import (
	"fmt"
	"os"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/infra/fake"
)

func TestCreateClusterWithDeploymentAdmission(t *testing.T) {
	for _, backend := range []string{"zerops", "dokploy"} {
		for _, members := range []int{3, 5} {
			for _, cap := range []int{1, 2} {
				t.Run(fmt.Sprintf("%s/members%d/cap%d", backend, members, cap), func(t *testing.T) {
					p, rc, b, _ := setup(t)
					rc.Cluster.Spec.Components.Engine.Replicas = members
					peers := make([]string, members)
					for i := range members {
						peers[i] = fmt.Sprintf("demom%d", i)
					}
					admission := &fake.Admission{Backend: b, Limit: cap, Peers: peers, LegacyNative: os.Getenv("KARAKORAM_TEST_LEGACY_NATIVE_READINESS") == "1"}
					rc.Backend = admission
					for range 300 {
						admission.Advance()
						if _, err := p.Sync(t.Context(), rc); err != nil {
							t.Fatal(err)
						}
						s := state(t, rc)
						if s.Phase == "ready" && s.Pending == nil && s.Bootstrapped {
							if len(admission.Submitted) != members || admission.Peak > cap || len(rc.Cluster.Status.DeploymentFailures) != 0 {
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
