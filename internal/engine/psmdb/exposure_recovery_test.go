// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package psmdb

import (
	"context"
	"reflect"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type admissionInfra struct {
	*fakeInfra
	t      *testing.T
	rc     *engine.Context
	routes map[string]infra.PublicPort
}

func (b *admissionInfra) EnsurePublicPort(ctx context.Context, name string, route infra.PublicPort) error {
	b.t.Helper()
	s, err := load(b.rc.Cluster)
	if err != nil || s.ExposureAdmission.Admitted == nil {
		b.t.Fatal("route mutation without persisted admission", err)
	}
	admitted := s.ExposureAdmission.Admitted
	if !admitted.Public || !reflect.DeepEqual(route.Allowlist, admitted.Allowlist) || route.IPType != admitted.IPType {
		b.t.Fatal("route differs from persisted admission", route, admitted)
	}
	b.routes[name] = route
	return b.fakeInfra.EnsurePublicPort(ctx, name, route)
}

func TestExposureRecoveryAdmission(t *testing.T) {
	for _, recovery := range []string{"resume", "runtime-delete"} {
		for _, policy := range []string{"private", "public", "private-to-public", "allowlist"} {
			t.Run(recovery+"/"+policy, func(t *testing.T) {
				p, rc, _ := writableReadinessFixture(t)
				b := &admissionInfra{fakeInfra: rc.Backend.(*fakeInfra), t: t, rc: rc, routes: make(map[string]infra.PublicPort)}
				rc.Backend = b
				admitted := domain.Exposure{Public: policy == "public" || policy == "allowlist"}
				if policy == "allowlist" {
					admitted.Allowlist = []string{"192.0.2.0/24"}
				}
				rc.Cluster.Spec.Exposure = admitted
				rc.Cluster.Generation = 1
				rc.Cluster.Status.ObservedGeneration = 1
				s, err := load(rc.Cluster)
				if err != nil {
					t.Fatal(err)
				}
				// Exercise migration from legacy application evidence before its loss.
				s.ExposureAdmission = engine.ExposureAdmission{}
				for i, m := range s.Members {
					m.PublicHash = hash(admitted)
					if admitted.Public {
						m.PublicPort = 27017 + i
						b.routes[m.Name] = infra.PublicPort{Port: m.PublicPort, TargetPort: 27017, Protocol: "tcp", IPType: admitted.IPType, Allowlist: admitted.Allowlist}
					}
				}
				if err := save(rc.Cluster, s); err != nil {
					t.Fatal(err)
				}
				if _, err := p.Sync(t.Context(), rc); err != nil {
					t.Fatal(err)
				}
				s, err = load(rc.Cluster)
				if err != nil || s.ExposureAdmission.Admitted == nil {
					t.Fatal("legacy policy not migrated", err)
				}
				rc.Cluster.Generation = 2
				closeFleetWindow(rc)
				deferred := policy == "private-to-public" || policy == "allowlist"
				if policy == "private-to-public" {
					rc.Cluster.Spec.Exposure.Public = true
				}
				if policy == "allowlist" {
					rc.Cluster.Spec.Exposure.Allowlist = []string{"198.51.100.0/24"}
				}
				volumes := make(map[string]string)
				for _, m := range s.Members {
					volumes[m.Name] = m.VolumeID
				}
				if recovery == "resume" {
					s.Suspended = true
					clear(b.routes)
				} else {
					delete(b.services, s.Members[0].Name)
					delete(b.routes, s.Members[0].Name)
				}
				if err := save(rc.Cluster, s); err != nil {
					t.Fatal(err)
				}
				// Reconstruct provider at every persisted checkpoint, including recovery.
				for range 100 {
					p = New(p.options)
					if _, err := p.Sync(t.Context(), rc); err != nil {
						t.Fatal(err)
					}
					if !deferred && rc.Cluster.Status.ObservedGeneration == 2 {
						break
					}
				}
				s, err = load(rc.Cluster)
				if err != nil {
					t.Fatal(err)
				}
				for _, m := range s.Members {
					if m.VolumeID != volumes[m.Name] {
						t.Fatal("recovery changed retained volume", m.Name)
					}
					if admitted.Public {
						route, ok := b.routes[m.Name]
						if !ok || !reflect.DeepEqual(route.Allowlist, admitted.Allowlist) {
							t.Fatal("admitted route not restored", m.Name, route)
						}
					}
				}
				if !admitted.Public && len(b.routes) != 0 {
					t.Fatal("unadmitted public exposure published", b.routes)
				}
				if deferred {
					if rc.Cluster.Status.ObservedGeneration != 1 {
						t.Fatal("deferred generation acknowledged")
					}
					rc.Cluster.MaintenanceOverrideGeneration = 2
					for range 30 {
						p = New(p.options)
						if _, err := p.Sync(t.Context(), rc); err != nil {
							t.Fatal(err)
						}
						if rc.Cluster.Status.ObservedGeneration == 2 {
							break
						}
					}
					for _, m := range s.Members {
						route, ok := b.routes[m.Name]
						if !ok || !reflect.DeepEqual(route.Allowlist, rc.Cluster.Spec.Exposure.Allowlist) {
							t.Fatal("override did not publish desired policy", m.Name, route)
						}
					}
				}
				if rc.Cluster.Status.ObservedGeneration != 2 {
					t.Fatal("recovery did not acknowledge generation", s.Phase)
				}
			})
		}
	}
}
