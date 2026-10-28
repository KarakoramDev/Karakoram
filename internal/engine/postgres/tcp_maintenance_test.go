// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"context"
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

type zeropsFamilyExposureBackend struct{ *exposureBackend }

func (b *zeropsFamilyExposureBackend) ListPublicPorts(ctx context.Context) ([]infra.PublicPortRoute, error) {
	routes, err := b.exposureBackend.ListPublicPorts(ctx)
	for i := range routes {
		switch routes[i].IPType {
		case "IPV4":
			routes[i].IPType = "IP_V4"
		case "IPV6":
			routes[i].IPType = "IP_V6"
		}
	}
	return routes, err
}

func TestMaintenanceZeropsRouteSpellingDoesNotDeferGeneration(t *testing.T) {
	for _, family := range []string{"IPV4", "IPV6"} {
		t.Run(family, func(t *testing.T) {
			p, rc, base := setup(t, "single")
			b := &zeropsFamilyExposureBackend{exposureBackend: &exposureBackend{fakeBackend: base, routes: map[string]infra.PublicPort{}}}
			rc.Backend = b
			rc.Cluster.Spec.Exposure = domain.Exposure{Public: true, IPType: family}
			converge(t, p, rc)
			rc.Cluster.Generation = 2
			rc.Cluster.Status.ObservedGeneration = 1
			rc.Cluster.Spec.Maintenance = &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: fmt.Sprintf("0 0 1 %d *", int(time.Now().Month())%12+1), Duration: "1h"}}}
			// Complete normal configuration-publication checkpoints before
			// testing the desired generation's final routing barrier.
			for range 20 {
				if _, err := p.Sync(t.Context(), rc); err != nil {
					t.Fatal(err)
				}
				if rc.Cluster.Status.ObservedGeneration == 2 {
					break
				}
			}
			if rc.Cluster.Status.ObservedGeneration != 2 {
				t.Fatal("unchanged Zerops route blocked generation acknowledgement", rc.SyncWait, rc.Cluster.Status.Conditions)
			}
			for _, condition := range rc.Cluster.Status.Conditions {
				if condition.Type == "MaintenanceWindow" && condition.Reason == "WaitingForWindow" {
					t.Fatal("unchanged Zerops route classified as a new disruption")
				}
			}
		})
	}
}
