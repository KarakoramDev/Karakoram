// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package neo4j

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestExposureAdmissionSurvivesRecovery(t *testing.T) {
	for _, recovery := range []string{"resume", "missing-runtime"} {
		for _, policy := range []string{"private", "public", "private-to-public", "allowlist", "ip-family", "legacy-public"} {
			t.Run(recovery+"/"+policy, func(t *testing.T) {
				p, rc, b, _ := setup(t)
				admitted := domain.Exposure{Public: policy != "private" && policy != "private-to-public"}
				if admitted.Public {
					admitted.IPType = "IPV4"
					admitted.Allowlist = []string{"192.0.2.0/24"}
				}
				rc.Cluster.Spec.Exposure = admitted
				converge(t, p, rc)
				before, err := load(rc.Cluster)
				if err != nil {
					t.Fatal(err)
				}
				name, volume := before.Member.Name, before.Member.VolumeID
				if policy == "legacy-public" {
					before.ExposureAdmission.Admitted = nil
					if err = save(rc.Cluster, before); err != nil {
						t.Fatal(err)
					}
				}
				rc.Cluster.Generation++
				rc.Cluster.Spec.Maintenance = &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: fmt.Sprintf("0 0 1 %d *", int(time.Now().Month())%12+1), Duration: "1h"}}}
				desired := admitted
				deferred := false
				switch policy {
				case "private-to-public":
					desired.Public = true
					desired.IPType = "IPV4"
					deferred = true
				case "allowlist":
					desired.Allowlist = []string{"198.51.100.0/24"}
					deferred = true
				case "ip-family":
					desired.IPType = "IPV6"
					deferred = true
				}
				rc.Cluster.Spec.Exposure = desired
				if recovery == "resume" {
					done := false
					for range 50 {
						p = New(p.options)
						done, err = p.Cleanup(t.Context(), rc, domain.DeleteOptions{KeepVolumes: true})
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
					if err = b.DeletePublicPorts(t.Context(), name); err != nil {
						t.Fatal(err)
					}
					if _, err = b.DeleteService(t.Context(), name); err != nil {
						t.Fatal(err)
					}
				}
				// Reconstruct on every pass: only the serialized journal may preserve admission.
				for range 50 {
					p = New(p.options)
					if _, err = p.Sync(t.Context(), rc); err != nil {
						t.Fatal(err)
					}
				}
				after, err := load(rc.Cluster)
				if err != nil {
					t.Fatal(err)
				}
				if after.Member.Stage != "ready" || after.Member.VolumeID != volume || after.Member.ServiceID == "" {
					t.Fatal("accepted runtime did not recover with its retained volume")
				}
				routes, err := b.ListPublicPorts(t.Context())
				if err != nil {
					t.Fatal(err)
				}
				if admitted.Public {
					if len(routes) != 1 || routes[0].Hostname != name || routes[0].Port != 7687 || routes[0].IPType != admitted.IPType || !reflect.DeepEqual(routes[0].Allowlist, admitted.Allowlist) {
						t.Fatalf("recovery did not restore admitted exposure: %+v", routes)
					}
				} else if len(routes) != 0 {
					t.Fatalf("recovery published unadmitted access: %+v", routes)
				}
				if after.ExposureAdmission.Admitted == nil || !reflect.DeepEqual(*after.ExposureAdmission.Admitted, admitted) {
					t.Fatal("persisted admission changed during closed-window recovery")
				}
				wantGeneration := rc.Cluster.Generation
				if deferred {
					wantGeneration--
				}
				if rc.Cluster.Status.ObservedGeneration != wantGeneration {
					t.Fatalf("observed generation = %d, want %d", rc.Cluster.Status.ObservedGeneration, wantGeneration)
				}
				if !deferred {
					return
				}
				rc.Cluster.MaintenanceOverrideGeneration = rc.Cluster.Generation
				for range 5 {
					p = New(p.options)
					if _, err = p.Sync(t.Context(), rc); err != nil {
						t.Fatal(err)
					}
				}
				routes, err = b.ListPublicPorts(t.Context())
				if err != nil {
					t.Fatal(err)
				}
				if len(routes) != 1 || routes[0].IPType != desired.IPType || !reflect.DeepEqual(routes[0].Allowlist, desired.Allowlist) || rc.Cluster.Status.ObservedGeneration != rc.Cluster.Generation {
					t.Fatal("override did not publish and acknowledge desired exposure", routes)
				}
			})
		}
	}
}
