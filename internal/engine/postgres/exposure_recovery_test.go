// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func TestExposureAdmissionSurvivesRecovery(t *testing.T) {
	t.Setenv("KARAKORAM_ARTIFACT_MIRROR", "https://artifacts.example.invalid")
	for _, profile := range []string{"postgres", "timescaledb", "citus"} {
		for _, recovery := range []string{"resume", "missing-runtime"} {
			for _, change := range []string{"unchanged-public", "unchanged-private", "enable", "allowlist", "family"} {
				t.Run(profile+"/"+recovery+"/"+change, func(t *testing.T) {
					p, rc, base := setup(t, "single")
					if profile == "timescaledb" {
						rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"profiles":["timescaledb"]}`)
					}
					if profile == "citus" {
						p, rc, base, _ = setupDistributed(t, 1)
					}
					b := &exposureBackend{fakeBackend: base, routes: map[string]infra.PublicPort{}}
					rc.Backend = b
					admitted := domain.Exposure{Public: change != "enable" && change != "unchanged-private", IPType: "IPV4", Allowlist: []string{"192.0.2.0/24"}}
					rc.Cluster.Spec.Exposure = admitted
					s := converge(t, p, rc)
					volumes := map[string]string{}
					for _, m := range s.data() {
						volumes[m.Name] = m.VolumeID
					}
					target := connectionMember(rc, s)
					rc.Cluster.Spec.Maintenance = &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: fmt.Sprintf("0 0 1 %d *", int(time.Now().Month())%12+1), Duration: "1h"}}}
					rc.Cluster.Generation++
					switch change {
					case "enable":
						rc.Cluster.Spec.Exposure.Public = true
					case "allowlist":
						rc.Cluster.Spec.Exposure.Allowlist = []string{"198.51.100.0/24"}
					case "family":
						rc.Cluster.Spec.Exposure.IPType = "IPV6"
					}
					deferred := change == "enable" || change == "allowlist" || change == "family"
					if deferred {
						// Persist the deferred policy before losing its runtime application.
						if _, err := p.Sync(t.Context(), rc); err != nil {
							t.Fatal(err)
						}
					}
					if recovery == "resume" {
						done := false
						for range 100 {
							var err error
							done, err = p.Cleanup(t.Context(), rc, domain.DeleteOptions{KeepVolumes: true, KeepBackups: true})
							if err != nil {
								t.Fatal(err)
							}
							if done {
								break
							}
						}
						if !done {
							t.Fatal("suspension did not finish")
						}
					} else {
						delete(base.services, target)
						delete(b.routes, target)
					}
					// Rebuild the provider from the persisted journal, not the old state pointer.
					artifact := p.citusArtifact
					p = New(p.options)
					p.citusArtifact = artifact
					for range 150 {
						if _, err := p.Sync(t.Context(), rc); err != nil {
							t.Fatal(err)
						}
						if !deferred && rc.Cluster.Status.ObservedGeneration == rc.Cluster.Generation {
							break
						}
					}
					route, public := b.routes[target]
					if public != admitted.Public || public && (route.IPType != admitted.IPType || !reflect.DeepEqual(route.Allowlist, admitted.Allowlist)) {
						t.Fatalf("recovery did not restore only admitted policy: route=%+v public=%v admitted=%+v", route, public, admitted)
					}
					if (rc.Cluster.Status.ObservedGeneration == rc.Cluster.Generation) == deferred {
						t.Fatalf("generation barrier incorrect: generation=%d observed=%d wait=%s", rc.Cluster.Generation, rc.Cluster.Status.ObservedGeneration, rc.SyncWait)
					}
					s, err := load(rc.Cluster)
					if err != nil {
						t.Fatal(err)
					}
					for _, m := range s.data() {
						if m.VolumeID != volumes[m.Name] {
							t.Fatal("retained volume identity changed")
						}
					}
					if deferred {
						rc.Cluster.MaintenanceOverrideGeneration = rc.Cluster.Generation
						p = New(p.options)
						p.citusArtifact = artifact
						for range 150 {
							if _, err := p.Sync(t.Context(), rc); err != nil {
								t.Fatal(err)
							}
							if rc.Cluster.Status.ObservedGeneration == rc.Cluster.Generation {
								break
							}
						}
						if rc.Cluster.Status.ObservedGeneration != rc.Cluster.Generation {
							t.Fatal("override did not release generation", rc.SyncWait)
						}
						route, public = b.routes[target]
						desired := rc.Cluster.Spec.Exposure
						if public != desired.Public || public && (route.IPType != desired.IPType || !reflect.DeepEqual(route.Allowlist, desired.Allowlist)) {
							t.Fatal("override did not publish desired policy", route)
						}
					}
				})
			}
		}
	}
}

func TestLegacyExposureAdmissionSeedsRetainedRoutes(t *testing.T) {
	p, rc, base := setup(t, "single")
	b := &exposureBackend{fakeBackend: base, routes: map[string]infra.PublicPort{}}
	rc.Backend = b
	rc.Cluster.Spec.Exposure = domain.Exposure{Public: true, IPType: "IPV4", Allowlist: []string{"192.0.2.0/24"}}
	s := converge(t, p, rc)
	s.ExposureAdmission.Admitted = nil
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	rc.Cluster.Generation++
	rc.Cluster.Spec.Exposure.Allowlist = []string{"198.51.100.0/24"}
	rc.Cluster.Spec.Maintenance = &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: fmt.Sprintf("0 0 1 %d *", int(time.Now().Month())%12+1), Duration: "1h"}}}
	if _, err := p.Sync(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	s, err := load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	if s.ExposureAdmission.Admitted == nil || !reflect.DeepEqual(s.ExposureAdmission.Admitted.Allowlist, []string{"192.0.2.0/24"}) {
		t.Fatal("legacy route policy not retained")
	}
}

type unrestrictedRouteBackend struct{ *exposureBackend }

func (b unrestrictedRouteBackend) Capabilities() infra.Capabilities {
	c := b.exposureBackend.Capabilities()
	c.TCPAllowlist = false
	return c
}

func TestLegacyUnrestrictedExposureResumesOutsideWindow(t *testing.T) {
	for _, family := range []string{"IPV4", "IPV6"} {
		t.Run(family, func(t *testing.T) {
			p, rc, base := setup(t, "single")
			b := &exposureBackend{fakeBackend: base, routes: map[string]infra.PublicPort{}}
			rc.Backend = unrestrictedRouteBackend{b}
			cidr := "0.0.0.0/0"
			if family == "IPV6" {
				cidr = "::/0"
			}
			admitted := domain.Exposure{Public: true, IPType: family, Allowlist: []string{cidr}, AllowAllAddresses: true}
			rc.Cluster.Spec.Exposure = admitted
			s := converge(t, p, rc)
			s.ExposureAdmission.Admitted = nil
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			rc.Cluster.Generation++
			rc.Cluster.Spec.Maintenance = &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: fmt.Sprintf("0 0 1 %d *", int(time.Now().Month())%12+1), Duration: "1h"}}}
			// Backend family aliases must retain the same acknowledged policy.
			for name, route := range b.routes {
				if family == "IPV4" {
					route.IPType = "IP_V4"
				} else {
					route.IPType = "IP_V6"
				}
				b.routes[name] = route
			}
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
			p = New(p.options)
			// Native Ready may precede exposure reapplication on resume.
			// Drive the full generation/route barrier, not only that phase.
			for range 150 {
				if _, err := p.Sync(t.Context(), rc); err != nil {
					t.Fatal(err)
				}
				if rc.Cluster.Status.ObservedGeneration == rc.Cluster.Generation && len(b.routes) == 1 {
					break
				}
			}
			var err error
			s, err = load(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(s.ExposureAdmission.Admitted, &admitted) || rc.Cluster.Status.ObservedGeneration != rc.Cluster.Generation {
				t.Fatal("legacy unrestricted admission lost acknowledgement or family", s.ExposureAdmission.Admitted)
			}
			if len(b.routes) != 1 {
				t.Fatal("unrestricted route not restored", b.routes)
			}
			for _, route := range b.routes {
				if route.IPType != family || !reflect.DeepEqual(route.Allowlist, admitted.Allowlist) {
					t.Fatal("restored wrong unrestricted route", route)
				}
			}
		})
	}
}
