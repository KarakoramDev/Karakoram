// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package rabbitmq

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestMaintenanceHTTPSPatchDoesNotMutateRuntime(t *testing.T) {
	for _, initialPublic := range []bool{false, true} {
		t.Run(fmt.Sprint(initialPublic), func(t *testing.T) {
			p, rc, f, _ := setup(t)
			rc.Cluster.Spec.EngineOptions, _ = json.Marshal(map[string]any{"managementUI": map[string]any{"public": initialPublic}})
			ready(t, p, rc)
			s := state(t, rc)
			rc.Cluster.Generation = 2
			rc.Cluster.Status.ObservedGeneration = 1
			rc.Cluster.Spec.Maintenance = &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: fmt.Sprintf("0 0 1 %d *", int(time.Now().Month())%12+1), Duration: "1h"}}}
			// Configuration is already accepted here: test the route mutation's own
			// gate independently of the application's separate rolling-config gate.
			for _, m := range s.runningMembers() {
				m.EngineOptions, _ = json.Marshal(map[string]any{"managementUI": map[string]any{"public": !initialPublic}})
			}
			before := f.mutations
			for range 3 {
				if _, err := p.syncExposure(t.Context(), rc, s); err != nil {
					t.Fatal(err)
				}
			}
			if f.mutations != before || s.Pending != nil {
				t.Fatal("closed window updated ingress/runtime")
			}
			rc.Cluster.MaintenanceOverrideGeneration = 2
			if _, err := p.syncExposure(t.Context(), rc, s); err != nil {
				t.Fatal(err)
			}
			if f.mutations != before+1 || s.Pending == nil {
				t.Fatal("override did not submit route mutation")
			}
		})
	}
}
