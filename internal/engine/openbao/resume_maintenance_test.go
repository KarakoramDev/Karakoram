// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package openbao

import (
	"fmt"
	"reflect"

	"github.com/KarakoramDev/Karakoram/internal/engine"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestResumeRestoresExposureOutsideMaintenanceWindow(t *testing.T) {
	for _, public := range []bool{false, true} {
		t.Run(fmt.Sprint(public), func(t *testing.T) {
			p, rc, backend, agents := setup(t)
			rc.Cluster.Spec.Exposure.Public = public
			ready(t, p, rc)
			volumes := map[string]string{}
			for _, m := range state(t, rc).Members {
				volumes[m.Name] = m.VolumeID
			}
			rc.Cluster.Spec.Maintenance = &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: fmt.Sprintf("0 0 1 %d *", int(time.Now().Month())%12+1), Duration: "1h"}}}
			if rc.WaitForWindow().RequeueAfter == 0 {
				t.Fatal("fixture maintenance window is open")
			}
			rc.Cluster.Generation++
			stopped := false
			for range 100 {
				done, err := p.Cleanup(t.Context(), rc, domain.DeleteOptions{KeepVolumes: true, KeepBackups: true})
				if err != nil {
					t.Fatal(err)
				}
				if done {
					stopped = true
					break
				}
			}
			if !stopped {
				t.Fatal("suspension did not finish")
			}
			rc.Cluster.Status.ObservedGeneration = rc.Cluster.Generation
			rc.Cluster.Generation++
			// Recreate the persisted live incident boundary: native members are
			// healthy, but the recreated runtimes have no applied exposure hash.
			syncUntil(t, p, rc, func(s *internalState) bool {
				if s.Suspended || s.Unsealing != "" || s.Phase != "initializing" {
					return false
				}
				for _, m := range s.Members {
					if m.Stage != "ready" || !m.Healthy || m.Sealed || m.ResumeDeploy != nil || m.PublicHash != "" {
						return false
					}
				}
				return true
			})
			p = New(p.options)
			agents.p = p
			ready(t, p, rc)
			status, err := p.Status(t.Context(), rc)
			if err != nil || status.Phase != domain.PhaseReady || status.ObservedGeneration != rc.Cluster.Generation {
				t.Fatalf("resume did not acknowledge generation: %+v %v", status, err)
			}
			for _, m := range state(t, rc).Members {
				if m.VolumeID != volumes[m.Name] || backend.services[m.Volume] == nil || m.PublicHash == "" || m.Sealed || !m.Healthy {
					t.Fatalf("resume lost data identity or exposure: %+v", m)
				}
				if agents.member(m.Name).initCalls != map[bool]int{true: 1, false: 0}[m.Name == "demom0"] {
					t.Fatal("resume initialized existing data")
				}
			}
			routes, err := backend.ListPublicPorts(t.Context())
			if err != nil || public && len(routes) != len(volumes) || !public && len(routes) != 0 {
				t.Fatalf("resume did not restore requested routes: %+v %v", routes, err)
			}
			if rc.WaitForWindow().RequeueAfter == 0 {
				t.Fatal("resume unexpectedly required opening the maintenance window")
			}
		})
	}
}

func TestRecoveryRestoresOnlyAdmittedExposure(t *testing.T) {
	for _, recovery := range []string{"resume", "missing-runtime", "empty-hash"} {
		for _, change := range []string{"enable", "allowlist", "family"} {
			t.Run(recovery+"/"+change, func(t *testing.T) {
				p, rc, backend, agents := setup(t)
				rc.Cluster.Spec.Exposure = domain.Exposure{Public: change != "enable", IPType: "IPV4", Allowlist: []string{"192.0.2.0/24"}}
				ready(t, p, rc)
				applied := rc.Cluster.Spec.Exposure
				observed := rc.Cluster.Status.ObservedGeneration
				rc.Cluster.Spec.Maintenance = &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: fmt.Sprintf("0 0 1 %d *", int(time.Now().Month())%12+1), Duration: "1h"}}}
				rc.Cluster.Generation++
				switch change {
				case "enable":
					rc.Cluster.Spec.Exposure.Public = true
				case "allowlist":
					rc.Cluster.Spec.Exposure.Allowlist = []string{"0.0.0.0/0"}
				case "family":
					rc.Cluster.Spec.Exposure.IPType = "IPV6"
				}
				// Establish the deferred desired-generation boundary first.
				if _, err := p.Sync(t.Context(), rc); err != nil {
					t.Fatal(err)
				}
				switch recovery {
				case "resume":
					stopped := false
					for range 100 {
						done, err := p.Cleanup(t.Context(), rc, domain.DeleteOptions{KeepVolumes: true})
						if err != nil {
							t.Fatal(err)
						}
						if done {
							stopped = true
							break
						}
					}
					if !stopped {
						t.Fatal("suspend did not finish")
					}
					rc.Cluster.Generation++
				case "missing-runtime":
					for _, m := range state(t, rc).Members {
						delete(backend.services, m.Name)
						if err := backend.DeletePublicPorts(t.Context(), m.Name); err != nil {
							t.Fatal(err)
						}
					}
				case "empty-hash":
					s := state(t, rc)
					for _, m := range s.Members {
						m.PublicHash = ""
						if err := backend.DeletePublicPorts(t.Context(), m.Name); err != nil {
							t.Fatal(err)
						}
					}
					if err := s.save(rc.Cluster); err != nil {
						t.Fatal(err)
					}
				}
				p = New(p.options)
				agents.p = p
				syncUntil(t, p, rc, func(s *internalState) bool {
					for _, m := range s.Members {
						if backend.services[m.Name] == nil || m.Stage != "ready" || !m.Healthy || m.Sealed || m.PublicHash == "" || m.ResumeDeploy != nil {
							return false
						}
					}
					return true
				})
				for range 3 {
					if _, err := p.Sync(t.Context(), rc); err != nil {
						t.Fatal(err)
					}
				}
				s := state(t, rc)
				if rc.Cluster.Status.ObservedGeneration != observed || !reflect.DeepEqual(*s.ExposureAdmission.Admitted, applied) {
					t.Fatal("recovery admitted or acknowledged deferred desired exposure")
				}
				assertAdmittedRoutes(t, rc, backend, applied, len(s.Members))
				rc.Cluster.MaintenanceOverrideGeneration = rc.Cluster.Generation
				ready(t, p, rc)
				assertAdmittedRoutes(t, rc, backend, rc.Cluster.Spec.Exposure, len(s.Members))
			})
		}
	}
}

func assertAdmittedRoutes(t *testing.T, rc *engine.Context, backend *fakeInfra, exposure domain.Exposure, members int) {
	t.Helper()
	routes, err := backend.ListPublicPorts(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if !exposure.Public {
		if len(routes) != 0 {
			t.Fatal("recovery published deferred public access", routes)
		}
		return
	}
	if len(routes) != members {
		t.Fatal("recovery failed to restore admitted routes", routes)
	}
	for _, route := range routes {
		if route.IPType != exposure.IPType || !reflect.DeepEqual(route.Allowlist, exposure.Allowlist) {
			t.Fatal("recovery applied unadmitted route policy", route)
		}
	}
}

func TestLegacyExposureAdmissionMigratesBeforeResume(t *testing.T) {
	p, rc, _, _ := setup(t)
	rc.Cluster.Spec.Exposure.Public = true
	ready(t, p, rc)
	s := state(t, rc)
	s.ExposureAdmission = engine.ExposureAdmission{}
	if err := s.save(rc.Cluster); err != nil {
		t.Fatal(err)
	}
	rc.Cluster.Spec.Exposure.Public = false
	rc.Cluster.Generation++
	s = state(t, rc)
	if s.ExposureAdmission.Admitted == nil || !s.ExposureAdmission.Admitted.Public {
		t.Fatal("legacy applied hash lost its public admission")
	}
}

func TestLegacyEmptyExposureReceiptRecoversConservatively(t *testing.T) {
	for _, public := range []bool{false, true} {
		t.Run(fmt.Sprint(public), func(t *testing.T) {
			p, rc, backend, _ := setup(t)
			rc.Cluster.Spec.Exposure.Public = public
			ready(t, p, rc)
			observed := rc.Cluster.Status.ObservedGeneration
			s := state(t, rc)
			s.ExposureAdmission = engine.ExposureAdmission{}
			for _, m := range s.Members {
				m.PublicHash = ""
				if err := backend.DeletePublicPorts(t.Context(), m.Name); err != nil {
					t.Fatal(err)
				}
			}
			if err := s.save(rc.Cluster); err != nil {
				t.Fatal(err)
			}
			rc.Cluster.Generation++
			rc.Cluster.Spec.Maintenance = &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: fmt.Sprintf("0 0 1 %d *", int(time.Now().Month())%12+1), Duration: "1h"}}}
			for range 8 {
				if _, err := p.Sync(t.Context(), rc); err != nil {
					t.Fatal(err)
				}
			}
			assertAdmittedRoutes(t, rc, backend, domain.Exposure{}, len(s.Members))
			if public {
				if rc.Cluster.Status.ObservedGeneration != observed {
					t.Fatal("unproven legacy public exposure was acknowledged")
				}
				rc.Cluster.MaintenanceOverrideGeneration = rc.Cluster.Generation
			}
			ready(t, p, rc)
		})
	}
}
