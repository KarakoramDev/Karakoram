// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package supabase

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

func TestGatewayRecoveryRetainsExposureAdmission(t *testing.T) {
	for _, recovery := range []string{"runtime-delete", "resume", "legacy-runtime-delete", "legacy-resume"} {
		for _, initialPublic := range []bool{false, true} {
			for _, change := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/public=%t/change=%t", recovery, initialPublic, change), func(t *testing.T) {
					p, rc, backend := setupProvider(t)
					public := &publicBackend{testBackend: backend}
					rc.Backend = public
					setPublic := func(enabled bool) {
						rc.Cluster.Spec.EngineOptions = json.RawMessage(fmt.Sprintf(`{"gateway":{"publicHttps":%t}}`, enabled))
					}
					setPublic(initialPublic)
					convergeProvider(t, p, rc)
					if recovery == "legacy-runtime-delete" || recovery == "legacy-resume" {
						state, err := loadState(rc.Cluster)
						if err != nil {
							t.Fatal(err)
						}
						state.GatewayAdmission = engine.ExposureAdmission{}
						state.GatewayURL = ""
						if err := saveState(rc.Cluster, state); err != nil {
							t.Fatal(err)
						}
					}
					rc.Cluster.Spec.Maintenance = &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: fmt.Sprintf("0 0 1 %d *", int(time.Now().Month())%12+1), Duration: "1h"}}}
					if rc.WaitForWindow().RequeueAfter == 0 {
						t.Fatal("test requires closed maintenance window")
					}
					if recovery == "resume" || recovery == "legacy-resume" {
						done := false
						for range 150 {
							var err error
							done, err = p.Cleanup(t.Context(), rc, domain.DeleteOptions{KeepVolumes: true})
							if err != nil {
								t.Fatal(err)
							}
							if done {
								break
							}
						}
						if !done {
							t.Fatal("suspension cleanup did not finish")
						}
					} else {
						delete(backend.services, "demogw")
						delete(backend.deployed, "demogw")
					}
					public.enabled = false
					rc.Cluster.Generation++
					observed := rc.Cluster.Status.ObservedGeneration
					if change {
						setPublic(!initialPublic)
					}
					// Both admission and runtime recovery must survive provider reconstruction.
					p = New(p.options)
					if _, err := p.Sync(t.Context(), rc); err != nil {
						t.Fatal(err)
					}
					p = New(p.options)
					if !change {
						convergeProvider(t, p, rc)
					} else {
						for range 150 {
							if _, err := p.Sync(t.Context(), rc); err != nil {
								t.Fatal(err)
							}
						}
					}
					state, err := loadState(rc.Cluster)
					if err != nil {
						t.Fatal(err)
					}
					gateway := state.Members["gateway"]
					if gateway == nil || gateway.Stage != engine.ComponentJoined || gateway.Public != initialPublic || public.enabled != initialPublic {
						t.Fatalf("admitted exposure not restored: gateway=%+v public=%t", gateway, public.enabled)
					}
					if change {
						if rc.Cluster.Status.ObservedGeneration != observed {
							t.Fatal("deferred desired generation acknowledged")
						}
						rc.Cluster.MaintenanceOverrideGeneration = rc.Cluster.Generation
						p = New(p.options)
						convergeProvider(t, p, rc)
						if public.enabled == initialPublic {
							t.Fatal("override did not admit desired exposure")
						}
					}
					if rc.Cluster.Status.ObservedGeneration != rc.Cluster.Generation {
						t.Fatal("recovered generation not acknowledged")
					}
				})
			}
		}
	}
}

func TestGatewayUnknownLegacyExposureStaysPrivate(t *testing.T) {
	_, rc, _ := setupProvider(t)
	rc.Cluster.Generation = 2
	rc.Cluster.Status.ObservedGeneration = 1
	rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"gateway":{"publicHttps":true}}`)
	rc.Cluster.Spec.Maintenance = &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: fmt.Sprintf("0 0 1 %d *", int(time.Now().Month())%12+1), Duration: "1h"}}}
	state := &componentState{Members: map[string]*componentMember{}}
	public, deferred, err := resolveGatewayExposure(t.Context(), rc, state)
	if err != nil || public || !deferred {
		t.Fatalf("unknown legacy baseline was published: public=%t deferred=%t err=%v", public, deferred, err)
	}
	state, err = loadState(rc.Cluster)
	if err != nil || state.GatewayAdmission.Admitted == nil || state.GatewayAdmission.Admitted.Public {
		t.Fatal("private legacy admission was not persisted", err)
	}
}
