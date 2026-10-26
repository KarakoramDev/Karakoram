// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package supabase

import (
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestComponentScaleToZeroAndBackAcknowledgesGeneration(t *testing.T) {
	for _, component := range []string{"realtime", "rest", "studio"} {
		t.Run(component, func(t *testing.T) {
			p, rc, backend := setupProvider(t)
			desired := rc.Cluster.Spec.Components.Extra[component]
			desired.Replicas = 1
			rc.Cluster.Spec.Components.Extra[component] = desired
			convergeProvider(t, p, rc)
			for _, replicas := range []int{0, 1} {
				rc.Cluster.Generation++
				desired.Replicas = replicas
				rc.Cluster.Spec.Components.Extra[component] = desired
				convergeProvider(t, p, rc)
				state, err := loadState(rc.Cluster)
				if err != nil {
					t.Fatal(err)
				}
				member := state.Members[component]
				if replicas == 0 {
					if member != nil || backend.services[rc.Cluster.Name+codes[component]] != nil {
						t.Fatal("disabled component retained its service or lifecycle")
					}
				} else if member == nil || member.Replicas != 1 {
					t.Fatal("re-enabled component did not rejoin")
				}
				if rc.Cluster.Status.Phase != domain.PhaseReady || rc.Cluster.Status.ObservedGeneration != rc.Cluster.Generation {
					t.Fatal("component scale did not acknowledge Ready generation", rc.Cluster.Status)
				}
			}
		})
	}
}
