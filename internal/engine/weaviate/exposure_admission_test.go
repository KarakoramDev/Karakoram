// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package weaviate

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

func reconstructExposureProvider(t *testing.T, f *coreFixture) {
	t.Helper()
	if err := f.rc.Store.(*store.Store).UpdateClusterState(t.Context(), f.rc.Cluster); err != nil {
		t.Fatal(err)
	}
	persisted, err := f.rc.Store.GetCluster(t.Context(), f.rc.Cluster.Ref())
	if err != nil {
		t.Fatal(err)
	}
	f.rc.Cluster.Internal = persisted.Internal
	f.p = New(Options{Store: f.rc.Store})
}

func TestExposureAdmissionSurvivesRecovery(t *testing.T) {
	for _, recovery := range []string{"resume", "missing-runtime"} {
		for _, change := range []string{"private", "public", "private-to-public", "public-to-private", "allowlist", "ip-family"} {
			t.Run(recovery+"/"+change, func(t *testing.T) {
				f := coreSetup(t, 1)
				admitted := domain.Exposure{Public: change != "private" && change != "private-to-public", IPType: "IPV4"}
				if change == "allowlist" {
					admitted.Allowlist = []string{"192.0.2.0/24"}
				}
				f.rc.Cluster.Spec.Exposure = admitted
				before := coreConverge(t, f)
				m := before.Members[0]
				observed := f.rc.Cluster.Status.ObservedGeneration
				f.rc.Cluster.Generation++
				f.rc.Cluster.Spec.Maintenance = &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: fmt.Sprintf("0 0 1 %d *", int(time.Now().Month())%12+1), Duration: "1h"}}}
				switch change {
				case "private-to-public":
					f.rc.Cluster.Spec.Exposure.Public = true
				case "public-to-private":
					f.rc.Cluster.Spec.Exposure.Public = false
				case "allowlist":
					f.rc.Cluster.Spec.Exposure.Allowlist = []string{"198.51.100.0/24"}
				case "ip-family":
					f.rc.Cluster.Spec.Exposure.IPType = "IPV6"
				}
				deferred := change != "private" && change != "public"
				if recovery == "resume" {
					suspended := false
					for range 100 {
						done, err := f.p.Cleanup(t.Context(), f.rc, domain.DeleteOptions{KeepVolumes: true, KeepBackups: true})
						if err != nil {
							t.Fatal(err)
						}
						reconstructExposureProvider(t, f)
						if done {
							suspended = true
							break
						}
					}
					if !suspended {
						t.Fatal("suspension did not finish")
					}
				} else {
					if err := f.backend.DeletePublicPorts(t.Context(), m.Name); err != nil {
						t.Fatal(err)
					}
					if _, err := f.backend.DeleteServiceByID(t.Context(), m.Name, m.ServiceID); err != nil {
						t.Fatal(err)
					}
				}
				reconstructExposureProvider(t, f)
				for range 100 {
					if _, err := f.p.Sync(t.Context(), f.rc); err != nil {
						t.Fatal(err)
					}
					reconstructExposureProvider(t, f)
				}
				s, err := load(f.rc.Cluster)
				if err != nil {
					t.Fatal(err)
				}
				runtime, err := f.backend.GetService(t.Context(), m.Name)
				if err != nil || runtime == nil || s.Pending != nil || s.Members[0].VolumeID != m.VolumeID {
					t.Fatal("recovery did not preserve healthy runtime/volume", err)
				}
				routes, err := f.backend.ListPublicPorts(t.Context())
				if err != nil {
					t.Fatal(err)
				}
				wantRoutes := 0
				if admitted.Public {
					wantRoutes = 2
				}
				if len(routes) != wantRoutes {
					t.Fatalf("restored %d routes, want %d", len(routes), wantRoutes)
				}
				for _, route := range routes {
					if route.IPType != admitted.IPType || !reflect.DeepEqual(route.Allowlist, admitted.Allowlist) {
						t.Fatal("recovery published deferred policy", route)
					}
				}
				if !reflect.DeepEqual(s.ExposureAdmission.Admitted, &admitted) {
					t.Fatal("admission receipt changed during recovery")
				}
				if deferred {
					if f.rc.Cluster.Status.ObservedGeneration != observed {
						t.Fatal("deferred generation acknowledged")
					}
					f.rc.Cluster.MaintenanceOverrideGeneration = f.rc.Cluster.Generation
					coreConverge(t, f)
				} else if f.rc.Cluster.Status.ObservedGeneration != f.rc.Cluster.Generation || s.Phase != "ready" {
					t.Fatal("unchanged exposure prevented resume/heal acknowledgement")
				}
				if f.rc.Cluster.Status.ObservedGeneration != f.rc.Cluster.Generation {
					t.Fatal("override did not release acknowledgement barrier")
				}
				routes, err = f.backend.ListPublicPorts(t.Context())
				if err != nil {
					t.Fatal(err)
				}
				wantRoutes = 0
				if f.rc.Cluster.Spec.Exposure.Public {
					wantRoutes = 2
				}
				if len(routes) != wantRoutes {
					t.Fatal("admitted desired routes not applied")
				}
				for _, route := range routes {
					if route.IPType != f.rc.Cluster.Spec.Exposure.IPType || !reflect.DeepEqual(route.Allowlist, f.rc.Cluster.Spec.Exposure.Allowlist) {
						t.Fatal("override did not publish desired policy")
					}
				}
			})
		}
	}
}

func TestLegacyExposureAdmissionBeforeCleanup(t *testing.T) {
	f := coreSetup(t, 1)
	f.rc.Cluster.Spec.Exposure = domain.Exposure{Public: true, IPType: "IPV4", Allowlist: []string{"192.0.2.0/24"}}
	s := coreConverge(t, f)
	applied := f.rc.Cluster.Spec.Exposure
	s.ExposureAdmission.Admitted = nil
	if err := s.save(f.rc.Cluster); err != nil {
		t.Fatal(err)
	}
	f.rc.Cluster.Generation++
	f.rc.Cluster.Spec.Exposure.Allowlist = []string{"198.51.100.0/24"}
	if _, err := f.p.Cleanup(t.Context(), f.rc, domain.DeleteOptions{KeepVolumes: true, KeepBackups: true}); err != nil {
		t.Fatal(err)
	}
	reconstructExposureProvider(t, f)
	s, err := load(f.rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(s.ExposureAdmission.Admitted, &applied) {
		t.Fatal("legacy cleanup lost applied policy")
	}
}

func TestLegacyUnknownExposureDefaultsPrivate(t *testing.T) {
	f := coreSetup(t, 1)
	s := coreConverge(t, f)
	s.ExposureAdmission.Admitted = nil
	f.rc.Cluster.Generation++
	f.rc.Cluster.Spec.Exposure.Public = true
	f.rc.Cluster.Spec.Maintenance = &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: fmt.Sprintf("0 0 1 %d *", int(time.Now().Month())%12+1), Duration: "1h"}}}
	if err := s.save(f.rc.Cluster); err != nil {
		t.Fatal(err)
	}
	reconstructExposureProvider(t, f)
	for range 4 {
		if _, err := f.p.Sync(t.Context(), f.rc); err != nil {
			t.Fatal(err)
		}
	}
	s, err := load(f.rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	routes, err := f.backend.ListPublicPorts(t.Context())
	if err != nil || len(routes) != 0 || s.ExposureAdmission.Admitted == nil || s.ExposureAdmission.Admitted.Public || f.rc.Cluster.Status.ObservedGeneration == f.rc.Cluster.Generation {
		t.Fatal("unknown legacy policy published or acknowledged desired exposure", err)
	}
}

type unrestrictedRouteBackend struct{ infra.Backend }

func (b unrestrictedRouteBackend) Capabilities() infra.Capabilities {
	c := b.Backend.Capabilities()
	c.TCPAllowlist = false
	return c
}

func TestLegacyUnrestrictedExposureResumesOutsideWindow(t *testing.T) {
	for _, family := range []string{"IPV4", "IPV6"} {
		t.Run(family, func(t *testing.T) {
			f := coreSetup(t, 1)
			f.rc.Backend = unrestrictedRouteBackend{f.rc.Backend}
			cidr := "0.0.0.0/0"
			if family == "IPV6" {
				cidr = "::/0"
			}
			admitted := domain.Exposure{Public: true, IPType: family, Allowlist: []string{cidr}, AllowAllAddresses: true}
			f.rc.Cluster.Spec.Exposure = admitted
			s := coreConverge(t, f)
			s.ExposureAdmission.Admitted = nil
			if err := s.save(f.rc.Cluster); err != nil {
				t.Fatal(err)
			}
			f.rc.Cluster.Generation++
			f.rc.Cluster.Spec.Maintenance = &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: fmt.Sprintf("0 0 1 %d *", int(time.Now().Month())%12+1), Duration: "1h"}}}
			stopped := false
			for range 100 {
				done, err := f.p.Cleanup(t.Context(), f.rc, domain.DeleteOptions{KeepVolumes: true, KeepBackups: true})
				if err != nil {
					t.Fatal(err)
				}
				reconstructExposureProvider(t, f)
				if done {
					stopped = true
					break
				}
			}
			if !stopped {
				t.Fatal("suspension did not finish")
			}
			s = coreConverge(t, f)
			if !reflect.DeepEqual(s.ExposureAdmission.Admitted, &admitted) || f.rc.Cluster.Status.ObservedGeneration != f.rc.Cluster.Generation {
				t.Fatal("legacy unrestricted admission lost acknowledgement or family", s.ExposureAdmission.Admitted)
			}
			routes, err := f.rc.Backend.ListPublicPorts(t.Context())
			if err != nil || len(routes) != 2 {
				t.Fatal("unrestricted routes not restored", routes, err)
			}
			for _, route := range routes {
				if route.IPType != family || !reflect.DeepEqual(route.Allowlist, admitted.Allowlist) {
					t.Fatal("restored wrong unrestricted route", route)
				}
			}
		})
	}
}
