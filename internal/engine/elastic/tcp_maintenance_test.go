// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package elastic

import (
	"fmt"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"testing"
	"time"
)

func TestMaintenanceTCPChangesDoNotMutateRuntime(t *testing.T) {
	for _, public := range []bool{false, true} {
		t.Run(fmt.Sprint(public), func(t *testing.T) {
			p, rc, b, _ := coreSetup(t, 1)
			rc.Cluster.Spec.Exposure.Public = public
			s := coreConverge(t, p, rc, b)
			rc.Cluster.Generation = 2
			rc.Cluster.Status.ObservedGeneration = 1
			rc.Cluster.Spec.Exposure.Public = !public
			rc.Cluster.Spec.Maintenance = &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: fmt.Sprintf("0 0 1 %d *", int(time.Now().Month())%12+1), Duration: "1h"}}}
			before := b.mutations
			for range 3 {
				if busy, err := p.expose(t.Context(), rc, s); err != nil || !busy || b.mutations != before || s.Pending != nil {
					t.Fatal("closed window changed TCP runtime", busy, err)
				}
			}
			rc.Cluster.MaintenanceOverrideGeneration = 2
			if busy, err := p.expose(t.Context(), rc, s); err != nil || !busy || b.mutations != before+1 {
				t.Fatal("override did not change route", busy, err)
			}
		})
	}
}
