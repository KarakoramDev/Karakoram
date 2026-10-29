// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"fmt"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"testing"
	"time"
)

type maintenanceRouteBackend struct {
	*fakeBackend
	routes    []infra.PublicPortRoute
	mutations int
}

func (b *maintenanceRouteBackend) Capabilities() infra.Capabilities {
	c := b.fakeBackend.Capabilities()
	c.TCPAllowlist = true
	return c
}

func (b *maintenanceRouteBackend) ProjectInfo(context.Context) (infra.Project, error) {
	return infra.Project{PublicIPv4: "192.0.2.1", PublicIPv6: "2001:db8::1"}, nil
}

func (b *maintenanceRouteBackend) ListPublicPorts(context.Context) ([]infra.PublicPortRoute, error) {
	return b.routes, nil
}
func (b *maintenanceRouteBackend) EnsurePublicPort(_ context.Context, name string, port infra.PublicPort) error {
	b.mutations++
	b.routes = []infra.PublicPortRoute{{Hostname: name, PublicPort: port}}
	return nil
}
func (b *maintenanceRouteBackend) DeletePublicPorts(context.Context, string) error {
	b.mutations++
	b.routes = nil
	return nil
}

func TestMaintenanceTCPChangesDoNotMutateRuntime(t *testing.T) {
	for _, provider := range []string{"mysql", "pxc"} {
		for _, public := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/%t", provider, public), func(t *testing.T) {
				p, rc, base := setup(t, "single")
				if provider == "pxc" {
					p, rc, base = setupPXC(t)
				}
				converge(t, p, rc, base)
				b := &maintenanceRouteBackend{fakeBackend: base}
				rc.Backend = b
				s, _ := load(rc.Cluster)
				rc.Cluster.Spec.Exposure = domain.Exposure{Public: public, IPType: "IPV4"}
				if _, err := p.reconcileExposure(t.Context(), rc, s, true); err != nil {
					t.Fatal(err)
				}
				before := b.mutations
				rc.Cluster.Generation = 2
				rc.Cluster.Status.ObservedGeneration = 1
				rc.Cluster.Spec.Exposure.Public = !public
				rc.Cluster.Spec.Maintenance = &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: fmt.Sprintf("0 0 1 %d *", int(time.Now().Month())%12+1), Duration: "1h"}}}
				for range 3 {
					deferred, err := p.reconcileExposure(t.Context(), rc, s, true)
					if err != nil || !deferred || b.mutations != before {
						t.Fatal("closed window changed TCP runtime", deferred, err)
					}
				}
				rc.Cluster.MaintenanceOverrideGeneration = 2
				if deferred, err := p.reconcileExposure(t.Context(), rc, s, true); err != nil || deferred || b.mutations == before {
					t.Fatal("override did not apply route", deferred, err)
				}
			})
		}
	}
}
