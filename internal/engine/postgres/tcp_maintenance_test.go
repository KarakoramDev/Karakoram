// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"fmt"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"reflect"
	"testing"
	"time"
)

func TestMaintenanceTCPChangesDoNotMutateRuntime(t *testing.T) {
	for _, change := range []string{"enable", "disable", "allowlist"} {
		t.Run(change, func(t *testing.T) {
			p, rc, base := setup(t, "single")
			b := &exposureBackend{fakeBackend: base, routes: map[string]infra.PublicPort{}}
			rc.Backend = b
			rc.Cluster.Spec.Exposure = domain.Exposure{Public: change != "enable", IPType: "IPV4", Allowlist: []string{"192.0.2.0/24"}}
			s := converge(t, p, rc)
			before := map[string]infra.PublicPort{}
			for k, v := range b.routes {
				before[k] = v
			}
			rc.Cluster.Generation = 2
			rc.Cluster.Status.ObservedGeneration = 1
			rc.Cluster.Spec.Maintenance = &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: fmt.Sprintf("0 0 1 %d *", int(time.Now().Month())%12+1), Duration: "1h"}}}
			switch change {
			case "enable":
				rc.Cluster.Spec.Exposure.Public = true
			case "disable":
				rc.Cluster.Spec.Exposure.Public = false
			case "allowlist":
				rc.Cluster.Spec.Exposure.Allowlist = []string{"192.0.2.0/25"}
			}
			for range 3 {
				if _, err := p.Sync(t.Context(), rc); err != nil {
					t.Fatal(err)
				}
			}
			if !reflect.DeepEqual(before, b.routes) || rc.Cluster.Status.ObservedGeneration != 1 {
				t.Fatal("closed window changed routes or acknowledged generation")
			}
			rc.Cluster.MaintenanceOverrideGeneration = 2
			if _, err := p.restrictExposure(t.Context(), rc, s); err != nil {
				t.Fatal(err)
			}
			if deferred, err := p.reconcileExposure(t.Context(), rc, s); err != nil || deferred {
				t.Fatal(deferred, err)
			}
			if reflect.DeepEqual(before, b.routes) {
				t.Fatal("override did not apply desired route")
			}
		})
	}
}
