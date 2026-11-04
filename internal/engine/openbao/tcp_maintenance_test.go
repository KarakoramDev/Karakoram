// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package openbao

import (
	"fmt"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"reflect"
	"testing"
	"time"
)

func TestMaintenanceTCPChangesDoNotMutateRuntime(t *testing.T) {
	for _, public := range []bool{false, true} {
		t.Run(fmt.Sprint(public), func(t *testing.T) {
			p, rc, b, _ := setup(t)
			rc.Cluster.Spec.Exposure.Public = public
			ready(t, p, rc)
			s := state(t, rc)
			before, err := b.ListPublicPorts(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			rc.Cluster.Generation = 2
			rc.Cluster.Status.ObservedGeneration = 1
			rc.Cluster.Spec.Exposure.Public = !public
			rc.Cluster.Spec.Maintenance = &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: fmt.Sprintf("0 0 1 %d *", int(time.Now().Month())%12+1), Duration: "1h"}}}
			for range 3 {
				if busy, err := p.reconcileExposure(t.Context(), rc, s); err != nil || !busy {
					t.Fatal(busy, err)
				}
			}
			after, err := b.ListPublicPorts(t.Context())
			if err != nil || !reflect.DeepEqual(before, after) || s.Pending != nil {
				t.Fatal("closed window changed TCP runtime", err)
			}
			rc.Cluster.MaintenanceOverrideGeneration = 2
			if busy, err := p.reconcileExposure(t.Context(), rc, s); err != nil || !busy {
				t.Fatal(busy, err)
			}
			after, err = b.ListPublicPorts(t.Context())
			if err != nil || reflect.DeepEqual(before, after) {
				t.Fatal("override did not change routing", err)
			}
		})
	}
}
