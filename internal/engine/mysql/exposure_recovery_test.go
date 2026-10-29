// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

func TestExposureAdmissionSurvivesRecovery(t *testing.T) {
	private := domain.Exposure{}
	public := domain.Exposure{Public: true, IPType: "IPV4", Allowlist: []string{"192.0.2.0/24"}}
	for _, provider := range []string{"mysql", "pxc"} {
		for _, recovery := range []string{"resume", "runtime"} {
			for _, tc := range []struct {
				name              string
				admitted, desired domain.Exposure
				deferred          bool
			}{
				{"private", private, private, false},
				{"public", public, public, false},
				{"publish", private, public, true},
				{"withdraw", public, private, true},
				{"allowlist", public, domain.Exposure{Public: true, IPType: "IPV4", Allowlist: []string{"198.51.100.0/24"}}, true},
				{"family", public, domain.Exposure{Public: true, IPType: "IPV6", Allowlist: public.Allowlist}, true},
			} {
				t.Run(fmt.Sprintf("%s/%s/%s", provider, recovery, tc.name), func(t *testing.T) {
					p, rc, base := setup(t, "single")
					if provider == "pxc" {
						p, rc, base = setupPXC(t)
					}
					b := &maintenanceRouteBackend{fakeBackend: base}
					rc.Backend = b
					rc.Cluster.Spec.Exposure = tc.admitted
					converge(t, p, rc, base)
					previous := rc.Cluster.Generation
					volumes := map[string]string{}
					s, err := load(rc.Cluster)
					if err != nil {
						t.Fatal(err)
					}
					for _, m := range s.data() {
						volumes[m.Name] = m.VolumeID
					}
					rc.Cluster.Spec.Maintenance = &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: fmt.Sprintf("0 0 1 %d *", int(time.Now().Month())%12+1), Duration: "1h"}}}
					rc.Cluster.Generation++
					rc.Cluster.Spec.Exposure = tc.desired
					if recovery == "resume" {
						suspendGroup(t, p, rc, base)
					} else {
						// Reconstruct at the same receipt boundary used by runtime deletion.
						target := s.find(connectionMember(rc, s))
						if err := b.DeletePublicPorts(t.Context(), target.Name); err != nil {
							t.Fatal(err)
						}
						op, err := b.DeleteServiceByID(t.Context(), target.Name, target.ServiceID)
						if err != nil {
							t.Fatal(err)
						}
						p.track(s, target, "delete-runtime", op)
						if err := save(rc.Cluster, s); err != nil {
							t.Fatal(err)
						}
					}
					for range 120 {
						p = New(Options{Provider: provider})
						if _, err := p.Sync(t.Context(), rc); err != nil {
							t.Fatal(err)
						}
						base.apply(t, p, rc)
					}
					s, err = load(rc.Cluster)
					if err != nil {
						t.Fatal(err)
					}
					for _, m := range s.data() {
						if m.VolumeID != volumes[m.Name] {
							t.Fatal("recovery replaced volume", m.Name)
						}
					}
					assertRoutes := func(want domain.Exposure) {
						t.Helper()
						if !want.Public {
							if len(b.routes) != 0 {
								t.Fatal("private policy published routes", b.routes)
							}
							return
						}
						if len(b.routes) != 1 || b.routes[0].Hostname != connectionMember(rc, s) || b.routes[0].IPType != want.IPType || !reflect.DeepEqual(b.routes[0].Allowlist, want.Allowlist) {
							t.Fatal("wrong restored policy", b.routes, want)
						}
					}
					assertRoutes(tc.admitted)
					if tc.deferred {
						if rc.Cluster.Status.ObservedGeneration != previous {
							t.Fatal("deferred generation acknowledged", rc.Cluster.Status.ObservedGeneration)
						}
						rc.Cluster.MaintenanceOverrideGeneration = rc.Cluster.Generation
						converge(t, New(Options{Provider: provider}), rc, base)
						assertRoutes(tc.desired)
					} else if rc.Cluster.Status.ObservedGeneration != rc.Cluster.Generation {
						t.Fatal("recovery generation not acknowledged")
					}
					status, err := p.Status(t.Context(), rc)
					if err != nil || status.Phase != domain.PhaseReady {
						t.Fatal("recovery not ready", status.Phase, err)
					}
				})
			}
		}
	}
}

func TestLegacyExposureAdmissionBeforeReceiptCleanup(t *testing.T) {
	for _, known := range []bool{false, true} {
		t.Run(fmt.Sprint(known), func(t *testing.T) {
			p, rc, base := setup(t, "single")
			b := &maintenanceRouteBackend{fakeBackend: base}
			rc.Backend = b
			rc.Cluster.Spec.Exposure = domain.Exposure{Public: true, IPType: "IPV4"}
			converge(t, p, rc, base)
			s, err := load(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			s.ExposureAdmission = engine.ExposureAdmission{}
			if !known {
				for _, m := range s.Members {
					m.ExposureHash = ""
				}
			}
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			rc.Cluster.Generation++
			rc.Cluster.Spec.Maintenance = &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: fmt.Sprintf("0 0 1 %d *", int(time.Now().Month())%12+1), Duration: "1h"}}}
			suspendGroup(t, p, rc, base)
			for range 80 {
				p = New(Options{})
				if _, err := p.Sync(t.Context(), rc); err != nil {
					t.Fatal(err)
				}
				base.apply(t, p, rc)
			}
			if known {
				if len(b.routes) != 1 || rc.Cluster.Status.ObservedGeneration != rc.Cluster.Generation {
					t.Fatal("known legacy receipt not restored")
				}
			} else if len(b.routes) != 0 || rc.Cluster.Status.ObservedGeneration == rc.Cluster.Generation {
				t.Fatal("unknown legacy receipt admitted public exposure")
			}
		})
	}
}
