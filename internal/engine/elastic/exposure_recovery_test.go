// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package elastic

import (
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

func TestExposureAdmissionAcrossRecovery(t *testing.T) {
	for _, recovery := range []string{"resume", "missing-runtime"} {
		for _, scenario := range []string{"private", "public", "private-to-public", "public-to-private", "allowlist"} {
			t.Run(recovery+"/"+scenario, func(t *testing.T) {
				p, rc, b, _ := coreSetup(t, 1)
				initialPublic := scenario == "public" || scenario == "public-to-private" || scenario == "allowlist"
				rc.Cluster.Spec.Exposure = domain.Exposure{Public: initialPublic, IPType: "IPV4", Allowlist: []string{"192.0.2.0/24"}}
				setComponents := func(public bool) {
					for _, kind := range []string{kindKibana, kindAPM} {
						c := rc.Cluster.Spec.Components.Extra[kind]
						c.Replicas = 1
						rc.Cluster.Spec.Components.Extra[kind] = c
					}
					rc.Cluster.Spec.EngineOptions, _ = json.Marshal(map[string]any{"kibana": map[string]any{"public": public}, "apm": map[string]any{"public": public}})
				}
				setComponents(initialPublic)
				s := coreConverge(t, p, rc, b)
				volumeID := s.esNodes()[0].VolumeID
				admitted := rc.Cluster.Spec.Exposure
				observed := rc.Cluster.Status.ObservedGeneration
				rc.Cluster.Generation++
				rc.Cluster.Spec.Maintenance = &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: fmt.Sprintf("0 0 1 %d *", int(time.Now().Month())%12+1), Duration: "1h"}}}
				deferred := scenario == "private-to-public" || scenario == "public-to-private" || scenario == "allowlist"
				if scenario == "private-to-public" || scenario == "public-to-private" {
					rc.Cluster.Spec.Exposure.Public = !initialPublic
					setComponents(!initialPublic)
				}
				if scenario == "allowlist" {
					rc.Cluster.Spec.Exposure.Allowlist = []string{"198.51.100.0/24"}
				}
				if rc.WaitForWindow().RequeueAfter <= 0 {
					t.Fatal("fixture window is open")
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
					for _, m := range s.Members {
						delete(b.services, m.Name)
						delete(b.nodes, m.Name)
					}
					b.public = nil
				}
				b.calls = nil
				// Reconstruct both provider and serialized cluster at the recovery boundary.
				reconstruct := func() {
					p = New(ProviderOptions{Store: rc.Store})
					c := cloneCluster(t, rc.Cluster)
					// Internal is deliberately excluded from the public JSON API.
					c.Internal = slices.Clone(rc.Cluster.Internal)
					rc.Cluster = c
				}
				reconstruct()
				for range 120 {
					if _, err := p.Sync(t.Context(), rc); err != nil {
						t.Fatal(err)
					}
					var err error
					s, err = load(rc.Cluster)
					if err != nil {
						t.Fatal(err)
					}
					if !deferred && s.Phase == "ready" {
						break
					}
					reconstruct()
				}
				if s.Pending != nil || s.esNodes()[0].Stage != stageJoined {
					t.Fatal("recovery has not finished")
				}
				if s.esNodes()[0].VolumeID != volumeID {
					t.Fatal("recovery replaced retained data")
				}
				if initialPublic {
					if len(b.public) != 1 || !reflect.DeepEqual(b.public[0].Allowlist, admitted.Allowlist) || b.public[0].IPType != admitted.IPType {
						t.Fatal("admitted TCP policy not restored", b.public)
					}
				} else if len(b.public) != 0 {
					t.Fatal("unadmitted TCP policy published")
				}
				for _, kind := range []string{kindKibana, kindAPM} {
					m := s.members(kind)[0]
					if m.Subdomain != initialPublic {
						t.Fatal("unadmitted HTTPS policy", kind, m.Subdomain)
					}
					if initialPublic && !slices.Contains(b.calls, "subdomain:true:"+m.Name) {
						t.Fatal("HTTPS route was not restored", b.calls)
					}
				}
				if deferred {
					if rc.Cluster.Status.ObservedGeneration != observed {
						t.Fatal("deferred generation acknowledged")
					}
					rc.Cluster.MaintenanceOverrideGeneration = rc.Cluster.Generation
					p = New(ProviderOptions{Store: rc.Store})
					s = coreConverge(t, p, rc, b)
					if s.KibanaExposure.Admitted.Public != rc.Cluster.Spec.Exposure.Public || s.APMExposure.Admitted.Public != rc.Cluster.Spec.Exposure.Public {
						t.Fatal("override did not admit component policy")
					}
					if rc.Cluster.Spec.Exposure.Public {
						if len(b.public) != 1 || !reflect.DeepEqual(b.public[0].Allowlist, rc.Cluster.Spec.Exposure.Allowlist) {
							t.Fatal("override did not apply desired TCP policy", b.public)
						}
					} else if len(b.public) != 0 {
						t.Fatal("override did not withdraw TCP")
					}
				}
				if rc.Cluster.Status.ObservedGeneration != rc.Cluster.Generation || s.Phase != "ready" {
					t.Fatal("recovered generation cannot complete")
				}
			})
		}
	}
}

func TestLegacyExposureAdmissionBeforeReceiptLoss(t *testing.T) {
	p, rc, b, _ := coreSetup(t, 1)
	rc.Cluster.Spec.Exposure.Public = true
	s := coreConverge(t, p, rc, b)
	s.NodeExposure = engine.ExposureAdmission{}
	rc.Cluster.Generation++
	rc.Cluster.Spec.Exposure.Public = false
	seedExposure(rc, s)
	if s.NodeExposure.Admitted == nil || !s.NodeExposure.Admitted.Public {
		t.Fatal("legacy applied public policy was not retained")
	}
	s.NodeExposure = engine.ExposureAdmission{}
	s.esNodes()[0].ExposureHash = ""
	rc.Cluster.Spec.Exposure.Public = true
	rc.Cluster.Spec.Maintenance = &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: fmt.Sprintf("0 0 1 %d *", int(time.Now().Month())%12+1), Duration: "1h"}}}
	effective, err := resolveExposure(rc, s)
	if err != nil || effective.node.Public || !effective.deferred {
		t.Fatal("unknown legacy policy must stay private", effective, err)
	}
}
