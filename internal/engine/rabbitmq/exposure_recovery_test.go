// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package rabbitmq

import (
	"cmp"
	"encoding/json"
	"reflect"
	"slices"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func TestRecoveryRestoresAdmittedExposureOutsideWindow(t *testing.T) {
	for _, recovery := range []string{"resume", "missing-runtime"} {
		for _, policy := range []string{"private", "public", "private-to-public", "allowlist", "ip-family", "management"} {
			t.Run(recovery+"/"+policy, func(t *testing.T) {
				p, rc, b, agents := setup(t)
				rc.Cluster.Spec.Exposure = domain.Exposure{Public: policy == "public" || policy == "allowlist" || policy == "ip-family", IPType: "IPV4", Allowlist: []string{"192.0.2.0/24"}}
				if policy == "management" {
					rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"managementUI":{"public":true}}`)
				}
				ready(t, p, rc)
				admitted := rc.Cluster.Spec.Exposure
				before, err := b.ListPublicPorts(t.Context())
				if err != nil {
					t.Fatal(err)
				}
				volumes := map[string]string{}
				for _, m := range state(t, rc).Members {
					volumes[m.Volume] = m.VolumeID
				}
				closedFleetWindow(rc)
				observed := rc.Cluster.Status.ObservedGeneration
				deferred := policy == "private-to-public" || policy == "allowlist" || policy == "ip-family"
				switch policy {
				case "private-to-public":
					rc.Cluster.Spec.Exposure.Public = true
				case "allowlist":
					rc.Cluster.Spec.Exposure.Allowlist = []string{"198.51.100.0/24"}
				case "ip-family":
					rc.Cluster.Spec.Exposure.IPType = "IPV6"
				}
				rc.Cluster.Generation++
				if recovery == "resume" {
					cleanupCluster(t, p, rc, domain.DeleteOptions{KeepVolumes: true, KeepBackups: true})
					rc.Cluster.Generation++
				} else {
					for _, m := range state(t, rc).Members {
						// Keep the native majority available while one runtime heals.
						if m.Index != 0 {
							continue
						}
						delete(b.services, m.Name)
						if err := b.DeletePublicPorts(t.Context(), m.Name); err != nil {
							t.Fatal(err)
						}
						b.subdomains[m.Name] = false
					}
				}
				// Every step reconstructs the provider and serializes the cluster, as a
				// control-plane replacement would; admission cannot live in provider memory.
				step := func() {
					t.Helper()
					rc.Cluster = cloneCluster(t, rc.Cluster)
					p = New(p.options)
					agents.p = p
					if _, err := p.Sync(t.Context(), rc); err != nil {
						t.Fatalf("sync: %v\n%s", err, rc.Cluster.Internal)
					}
				}
				restored := false
				for range 250 {
					step()
					s := state(t, rc)
					receipts := s.Pending == nil && len(s.runningMembers()) == len(volumes) && (s.Phase == "exposure" || s.Phase == "ready")
					for _, m := range s.Members {
						target := 5671
						if m.TLSMode == "disabled" {
							target = 5672
						}
						receipts = receipts && b.services[m.Name] != nil
						receipts = receipts && m.PublicHash == exposureHash(admitted, target)
						if policy == "management" {
							receipts = receipts && m.ManagementPublic && b.subdomains[m.Name]
						}
					}
					if receipts && (deferred || rc.Cluster.Status.ObservedGeneration == rc.Cluster.Generation && s.Phase == "ready") {
						restored = true
						break
					}
				}
				if !restored {
					t.Fatalf("recovery stalled: %s", rc.Cluster.Internal)
				}
				after, err := b.ListPublicPorts(t.Context())
				slices.SortFunc(after, func(a, b infra.PublicPortRoute) int { return cmp.Compare(a.Hostname, b.Hostname) })
				if err != nil || !reflect.DeepEqual(before, after) {
					t.Fatalf("admitted routes not restored: before=%+v after=%+v err=%v", before, after, err)
				}
				for volume, id := range volumes {
					if b.services[volume] == nil || b.services[volume].ID != id {
						t.Fatalf("retained volume identity changed: %s", volume)
					}
				}
				if allowed, _ := rc.DisruptionAllowed(); allowed {
					t.Fatal("window unexpectedly open")
				}
				if deferred {
					for range 5 {
						step()
					}
					if rc.Cluster.Status.ObservedGeneration != observed {
						t.Fatal("deferred desired generation acknowledged")
					}
					rc.Cluster.MaintenanceOverrideGeneration = rc.Cluster.Generation
					for range 250 {
						step()
						if rc.Cluster.Status.ObservedGeneration == rc.Cluster.Generation {
							break
						}
					}
					if rc.Cluster.Status.ObservedGeneration != rc.Cluster.Generation {
						t.Fatal("override did not acknowledge desired generation")
					}
					routes, err := b.ListPublicPorts(t.Context())
					if err != nil || len(routes) != len(volumes) {
						t.Fatalf("desired routes missing: %+v %v", routes, err)
					}
					for _, route := range routes {
						if route.IPType != rc.Cluster.Spec.Exposure.IPType || !reflect.DeepEqual(route.Allowlist, rc.Cluster.Spec.Exposure.Allowlist) {
							t.Fatalf("override did not apply desired policy: %+v", route)
						}
					}
				}
			})
		}
	}
}

func TestLegacyExposureAdmissionBeforeReceiptLoss(t *testing.T) {
	for _, acknowledged := range []bool{false, true} {
		p, rc, _, _ := setup(t)
		rc.Cluster.Spec.Exposure.Public = true
		ready(t, p, rc)
		s := state(t, rc)
		s.ExposureAdmission.Admitted = nil
		if !acknowledged {
			rc.Cluster.Generation++
		}
		if err := s.save(rc.Cluster); err != nil {
			t.Fatal(err)
		}
		s = state(t, rc)
		if s.ExposureAdmission.Admitted == nil || !s.ExposureAdmission.Admitted.Public {
			t.Fatal("retained applied/acknowledged policy was not seeded")
		}
	}
}

func TestLegacyUnknownExposureRecoversPrivateUntilAdmission(t *testing.T) {
	p, rc, b, _ := setup(t)
	rc.Cluster.Spec.Exposure.Public = true
	ready(t, p, rc)
	s := state(t, rc)
	s.ExposureAdmission.Admitted = nil
	for _, m := range s.Members {
		m.PublicHash = ""
	}
	if err := s.save(rc.Cluster); err != nil {
		t.Fatal(err)
	}
	rc.Cluster.Generation++
	closedFleetWindow(rc)
	p = New(p.options)
	s = state(t, rc)
	for range len(s.Members) + 1 {
		busy, err := p.syncExposure(t.Context(), rc, s)
		if err != nil || !busy {
			t.Fatal("unknown policy must remain deferred", busy, err)
		}
	}
	if s.ExposureAdmission.Admitted == nil || s.ExposureAdmission.Admitted.Public || len(b.publicPorts) != 0 {
		t.Fatal("unknown legacy baseline republished desired public access")
	}
}

func TestManagementRecoveryRestoresOnlyAdmittedExposure(t *testing.T) {
	p, rc, b, agents := setup(t)
	rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"managementUI":{"public":true}}`)
	ready(t, p, rc)
	rc.Cluster.Generation++
	closedFleetWindow(rc)
	s := state(t, rc)
	for _, m := range s.Members {
		// Simulate completed runtime recovery with a deferred public-to-private
		// management edit already in the runtime's retained configuration.
		m.EngineOptions = json.RawMessage(`{"managementUI":{"public":false}}`)
		m.ManagementPublic = false
		b.subdomains[m.Name] = false
	}
	if err := s.save(rc.Cluster); err != nil {
		t.Fatal(err)
	}
	for range 20 {
		p = New(p.options)
		agents.p = p
		s = state(t, rc)
		if s.Pending != nil {
			if _, err := p.observePending(t.Context(), rc, s); err != nil {
				t.Fatal(err)
			}
		} else if busy, err := p.syncExposure(t.Context(), rc, s); err != nil || !busy {
			t.Fatal("deferred management policy lost its generation barrier", busy, err)
		}
		if err := s.save(rc.Cluster); err != nil {
			t.Fatal(err)
		}
	}
	for _, m := range s.Members {
		if !m.ManagementPublic || !b.subdomains[m.Name] {
			t.Fatal("admitted public management route was not restored")
		}
	}
	rc.Cluster.MaintenanceOverrideGeneration = rc.Cluster.Generation
	for range 20 {
		if s.Pending != nil {
			if _, err := p.observePending(t.Context(), rc, s); err != nil {
				t.Fatal(err)
			}
		} else if _, err := p.syncExposure(t.Context(), rc, s); err != nil {
			t.Fatal(err)
		}
	}
	for _, m := range s.Members {
		if m.ManagementPublic || b.subdomains[m.Name] {
			t.Fatal("override did not admit private management exposure")
		}
	}
}
