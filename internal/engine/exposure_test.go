// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package engine

import (
	"context"
	"errors"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"testing"
	"time"
)

type windowRouteBackend struct {
	infra.Backend
	routes []infra.PublicPortRoute
	err    error
}

func (b windowRouteBackend) ListPublicPorts(context.Context) ([]infra.PublicPortRoute, error) {
	return b.routes, b.err
}

func TestPublicTCPDeferredOnlyForActualChanges(t *testing.T) {
	port := infra.PublicPort{Port: 5432, TargetPort: 5432, Protocol: "tcp", IPType: "IPV4"}
	for _, scenario := range []string{"create", "enable", "disable", "noop-public", "noop-private", "open", "override", "error"} {
		t.Run(scenario, func(t *testing.T) {
			rc := &Context{Clock: fixedRouteClock{}, Cluster: &domain.Cluster{Generation: 2, Status: domain.ClusterStatus{ObservedGeneration: 1}, Spec: domain.ClusterSpec{Maintenance: &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: "0 2 * * *", Duration: "1h"}}}}}}
			b := windowRouteBackend{}
			desired := &port
			want := false
			switch scenario {
			case "create":
				rc.Cluster.Status.ObservedGeneration = 0
				b.err = errors.New("must not read inventory while provisioning")
			case "enable":
				want = true
			case "disable":
				desired = nil
				b.routes = []infra.PublicPortRoute{{Hostname: "member", PublicPort: port}}
				want = true
			case "noop-public":
				b.routes = []infra.PublicPortRoute{{Hostname: "member", PublicPort: port}}
			case "noop-private":
				desired = nil
			case "open":
				rc.Cluster.Spec.Maintenance.Windows[0].Schedule = "* * * * *"
				b.err = errors.New("must not read inventory while open")
			case "override":
				rc.Cluster.MaintenanceOverrideGeneration = 2
				b.err = errors.New("must not read inventory with override")
			case "error":
				b.err = errors.New("inventory unavailable")
			}
			rc.Backend = b
			deferred, err := PublicTCPDeferred(t.Context(), rc, "member", desired)
			if deferred != want || (err != nil) != (scenario == "error") {
				t.Fatal(deferred, err)
			}
		})
	}
}

type fixedRouteClock struct{}

func (fixedRouteClock) Now() time.Time { return time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC) }

func TestPublicTCPDeferredZeropsIPFamilySpellings(t *testing.T) {
	for _, desiredFamily := range []string{"IPV4", "IP_V4", "IPV6", "IP_V6"} {
		for _, observedFamily := range []string{"ipv4", "IP_V4", "ipv6", "IP_V6"} {
			t.Run(desiredFamily+"/"+observedFamily, func(t *testing.T) {
				desired := infra.PublicPort{Port: 5432, TargetPort: 5432, Protocol: "tcp", IPType: desiredFamily}
				observed := desired
				observed.IPType = observedFamily
				rc := &Context{
					Clock: fixedRouteClock{},
					Cluster: &domain.Cluster{
						Generation: 2,
						Status:     domain.ClusterStatus{ObservedGeneration: 1},
						Spec:       domain.ClusterSpec{Maintenance: &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: "0 2 * * *", Duration: "1h"}}}},
					},
					Backend: windowRouteBackend{routes: []infra.PublicPortRoute{{Hostname: "member", PublicPort: observed}}},
				}
				deferred, err := PublicTCPDeferred(t.Context(), rc, "member", &desired)
				wantDeferred := desiredFamily[len(desiredFamily)-1] != observedFamily[len(observedFamily)-1]
				if err != nil || deferred != wantDeferred {
					t.Fatal("IP family spelling changed route admission", deferred, err)
				}
			})
		}
	}
}
