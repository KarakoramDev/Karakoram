// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package weaviate

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
			f := coreSetup(t, 1)
			s := coreConverge(t, f)
			f.rc.Cluster.Spec.Exposure.Public = public
			for range 4 {
				if _, err := f.p.reconcileExposure(t.Context(), f.rc, s); err != nil {
					t.Fatal(err)
				}
			}
			before, err := f.backend.ListPublicPorts(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			f.rc.Cluster.Generation = 2
			f.rc.Cluster.Status.ObservedGeneration = 1
			f.rc.Cluster.Spec.Exposure.Public = !public
			f.rc.Cluster.Spec.Maintenance = &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: fmt.Sprintf("0 0 1 %d *", int(time.Now().Month())%12+1), Duration: "1h"}}}
			for range 3 {
				if busy, err := f.p.reconcileExposure(t.Context(), f.rc, s); err != nil || !busy {
					t.Fatal(busy, err)
				}
			}
			after, err := f.backend.ListPublicPorts(t.Context())
			if err != nil || !reflect.DeepEqual(before, after) || s.Pending != nil {
				t.Fatal("closed window changed TCP runtime", err)
			}
			f.rc.Cluster.MaintenanceOverrideGeneration = 2
			if busy, err := f.p.reconcileExposure(t.Context(), f.rc, s); err != nil || !busy {
				t.Fatal(busy, err)
			}
			after, err = f.backend.ListPublicPorts(t.Context())
			if err != nil || reflect.DeepEqual(before, after) {
				t.Fatal("override did not change routing", err)
			}
		})
	}
}
