// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package neo4j

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
			converge(t, p, rc)
			before, err := b.ListPublicPorts(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			rc.Cluster.Generation = 2
			rc.Cluster.Status.ObservedGeneration = 1
			rc.Cluster.Spec.Exposure.Public = !public
			rc.Cluster.Spec.Maintenance = &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: fmt.Sprintf("0 0 1 %d *", int(time.Now().Month())%12+1), Duration: "1h"}}}
			for range 3 {
				if _, err := p.Sync(t.Context(), rc); err != nil {
					t.Fatal(err)
				}
			}
			after, err := b.ListPublicPorts(t.Context())
			if err != nil || !reflect.DeepEqual(before, after) || rc.Cluster.Status.ObservedGeneration != 1 {
				t.Fatal("closed window changed routes or acknowledged generation", err)
			}
			rc.Cluster.MaintenanceOverrideGeneration = 2
			converge(t, p, rc)
			after, err = b.ListPublicPorts(t.Context())
			if err != nil || reflect.DeepEqual(before, after) {
				t.Fatal("override did not change routing", err)
			}
		})
	}
}
