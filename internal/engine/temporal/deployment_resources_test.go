// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package temporal

import (
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine/enginetest"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func TestExhaustedComponentRearmAppliesResourcesBeforeDeployment(t *testing.T) {
	for _, topology := range []string{"combined", "split"} {
		t.Run(topology, func(t *testing.T) {
			p, rc, _, _ := fixture(t, topology)
			enginetest.AssertDeploymentResourceRearm(t, rc, func() error {
				_, err := p.Sync(t.Context(), rc)
				return err
			}, func() infra.Resources {
				s, err := loadState(rc.Cluster)
				if err != nil {
					t.Fatal(err)
				}
				for name, member := range s.Members {
					if _, failed := rc.Cluster.Status.DeploymentFailures[member.Name]; !failed {
						continue
					}
					desired := rc.Cluster.Spec.Components.Extra[name]
					desired.Resources.MinRAM *= 2
					desired.Resources.MaxRAM *= 2
					rc.Cluster.Spec.Components.Extra[name] = desired
					return desired.Resources
				}
				t.Fatal("missing exhausted component")
				return infra.Resources{}
			})
		})
	}
}

func TestRuntimeRecoveryPreservesAcceptedResourcesInClosedWindow(t *testing.T) {
	for _, topology := range []string{"combined", "split"} {
		t.Run(topology, func(t *testing.T) {
			p, rc, _, _ := fixture(t, topology)
			converge(t, p, rc)
			before, err := loadState(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			component := "server"
			if topology == "split" {
				component = "history"
			}
			m := before.Members[component]
			accepted := m.Resources
			desired := rc.Cluster.Spec.Components.Extra[component]
			desired.Resources.MaxCPU++
			rc.Cluster.Spec.Components.Extra[component] = desired
			rc.Cluster.Generation++
			if _, err := rc.Backend.DeleteServiceByID(t.Context(), m.Name, m.ServiceID); err != nil {
				t.Fatal(err)
			}
			enginetest.AssertRuntimeRecoveryPreservesResources(t, rc, m.Name, accepted, accepted, func() error {
				_, err := p.Sync(t.Context(), rc)
				return err
			}, func() (infra.Resources, bool, error) {
				s, err := loadState(rc.Cluster)
				if err != nil {
					return infra.Resources{}, false, err
				}
				current := s.Members[component]
				return current.Resources, current.Stage == "joined" && s.Pending == nil, nil
			})
		})
	}
}
