// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package elastic

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestMaintenanceHTTPSPatchDoesNotMutateRuntime(t *testing.T) {
	for _, kind := range []string{kindKibana, kindAPM} {
		for _, initialPublic := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/%t", kind, initialPublic), func(t *testing.T) {
				p, rc, f, _ := coreSetup(t, 1)
				desired := rc.Cluster.Spec.Components.Extra[kind]
				desired.Replicas = 1
				rc.Cluster.Spec.Components.Extra[kind] = desired
				rc.Cluster.Spec.EngineOptions, _ = json.Marshal(map[string]any{kind: map[string]any{"public": initialPublic}})
				s := coreConverge(t, p, rc, f)
				name := s.members(kind)[0].Name
				rc.Cluster.Generation = 2
				rc.Cluster.Status.ObservedGeneration = 1
				rc.Cluster.Spec.Maintenance = &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: fmt.Sprintf("0 0 1 %d *", int(time.Now().Month())%12+1), Duration: "1h"}}}
				rc.Cluster.Spec.EngineOptions, _ = json.Marshal(map[string]any{kind: map[string]any{"public": !initialPublic}})
				before := f.mutations
				for range 3 {
					if _, err := p.revokeComponentExposure(t.Context(), rc, s); err != nil {
						t.Fatal(err)
					}
					if _, err := p.expose(t.Context(), rc, s); err != nil {
						t.Fatal(err)
					}
				}
				if f.mutations != before || s.Pending != nil || s.find(name).Subdomain != initialPublic {
					t.Fatal("closed window updated ingress/runtime")
				}
				rc.Cluster.MaintenanceOverrideGeneration = 2
				if _, err := p.expose(t.Context(), rc, s); err != nil {
					t.Fatal(err)
				}
				if s.Pending == nil || f.mutations != before+1 {
					t.Fatal("override did not submit route mutation")
				}
			})
		}
	}
}
