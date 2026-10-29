// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package engine

import (
	"context"
	"errors"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"testing"
	"time"

	"encoding/json"
	"fmt"
	"reflect"
)

type windowRouteBackend struct {
	infra.Backend
	routes []infra.PublicPortRoute
	err    error
}

func (b windowRouteBackend) ListPublicPorts(context.Context) ([]infra.PublicPortRoute, error) {
	return b.routes, b.err
}

func TestPublicTCPDeferredOnlyForActualChanges(t *testing.T) {
	port := infra.PublicPort{Port: 5432, TargetPort: 5432, Protocol: "tcp", IPType: "IPV4"}
	for _, scenario := range []string{"create", "enable", "disable", "noop-public", "noop-private", "open", "override", "error"} {
		t.Run(scenario, func(t *testing.T) {
			rc := &Context{Clock: fixedRouteClock{}, Cluster: &domain.Cluster{Generation: 2, Status: domain.ClusterStatus{ObservedGeneration: 1}, Spec: domain.ClusterSpec{Maintenance: &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: "0 2 * * *", Duration: "1h"}}}}}}
			b := windowRouteBackend{}
			desired := &port
			want := false
			switch scenario {
			case "create":
				rc.Cluster.Status.ObservedGeneration = 0
				b.err = errors.New("must not read inventory while provisioning")
			case "enable":
				want = true
			case "disable":
				desired = nil
				b.routes = []infra.PublicPortRoute{{Hostname: "member", PublicPort: port}}
				want = true
			case "noop-public":
				b.routes = []infra.PublicPortRoute{{Hostname: "member", PublicPort: port}}
			case "noop-private":
				desired = nil
			case "open":
				rc.Cluster.Spec.Maintenance.Windows[0].Schedule = "* * * * *"
				b.err = errors.New("must not read inventory while open")
			case "override":
				rc.Cluster.MaintenanceOverrideGeneration = 2
				b.err = errors.New("must not read inventory with override")
			case "error":
				b.err = errors.New("inventory unavailable")
			}
			rc.Backend = b
			deferred, err := PublicTCPDeferred(t.Context(), rc, "member", desired)
			if deferred != want || (err != nil) != (scenario == "error") {
				t.Fatal(deferred, err)
			}
		})
	}
}

type fixedRouteClock struct{}

func (fixedRouteClock) Now() time.Time { return time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC) }

func TestPublicTCPDeferredZeropsIPFamilySpellings(t *testing.T) {
	for _, desiredFamily := range []string{"IPV4", "IP_V4", "IPV6", "IP_V6"} {
		for _, observedFamily := range []string{"ipv4", "IP_V4", "ipv6", "IP_V6"} {
			t.Run(desiredFamily+"/"+observedFamily, func(t *testing.T) {
				desired := infra.PublicPort{Port: 5432, TargetPort: 5432, Protocol: "tcp", IPType: desiredFamily}
				observed := desired
				observed.IPType = observedFamily
				rc := &Context{
					Clock: fixedRouteClock{},
					Cluster: &domain.Cluster{
						Generation: 2,
						Status:     domain.ClusterStatus{ObservedGeneration: 1},
						Spec:       domain.ClusterSpec{Maintenance: &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: "0 2 * * *", Duration: "1h"}}}},
					},
					Backend: windowRouteBackend{routes: []infra.PublicPortRoute{{Hostname: "member", PublicPort: observed}}},
				}
				deferred, err := PublicTCPDeferred(t.Context(), rc, "member", &desired)
				wantDeferred := desiredFamily[len(desiredFamily)-1] != observedFamily[len(observedFamily)-1]
				if err != nil || deferred != wantDeferred {
					t.Fatal("IP family spelling changed route admission", deferred, err)
				}
			})
		}
	}
}

func TestExposureAdmissionSurvivesLostApplicationState(t *testing.T) {
	for _, change := range []string{"enable", "allowlist", "family"} {
		t.Run(change, func(t *testing.T) {
			rc := &Context{Clock: fixedRouteClock{}, Cluster: &domain.Cluster{Generation: 1}}
			applied := domain.Exposure{Public: change != "enable", IPType: "IPV4", Allowlist: []string{"192.0.2.0/24"}}
			var admission ExposureAdmission
			if effective, deferred := admission.Resolve(rc, applied); deferred || !reflect.DeepEqual(effective, applied) {
				t.Fatal("initial provisioning did not admit exposure", effective, deferred)
			}
			rc.Cluster.Status.ObservedGeneration = 1
			rc.Cluster.Generation++
			rc.Cluster.Spec.Maintenance = &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: "0 2 * * *", Duration: "1h"}}}
			desired := applied
			switch change {
			case "enable":
				desired.Public = true
			case "allowlist":
				desired.Allowlist = []string{"0.0.0.0/0"}
			case "family":
				desired.IPType = "IPV6"
			}
			// Runtime hashes/routes are deliberately absent. Only the durable
			// policy receipt may authorize their restoration after CP restart.
			raw, err := json.Marshal(admission)
			if err != nil {
				t.Fatal(err)
			}
			admission = ExposureAdmission{}
			if err := json.Unmarshal(raw, &admission); err != nil {
				t.Fatal(err)
			}
			if effective, deferred := admission.Resolve(rc, desired); !deferred || !reflect.DeepEqual(effective, applied) {
				t.Fatal("lost runtime receipt admitted desired exposure", effective, deferred)
			}
			rc.Cluster.MaintenanceOverrideGeneration = rc.Cluster.Generation
			if effective, deferred := admission.Resolve(rc, desired); deferred || !reflect.DeepEqual(effective, desired) {
				t.Fatal("override did not admit desired exposure", effective, deferred)
			}
		})
	}
}

func TestExposureAdmissionLegacyBaseline(t *testing.T) {
	for _, known := range []bool{false, true} {
		t.Run(fmt.Sprint(known), func(t *testing.T) {
			rc := &Context{Clock: fixedRouteClock{}, Cluster: &domain.Cluster{Generation: 2, Status: domain.ClusterStatus{ObservedGeneration: 1}, Spec: domain.ClusterSpec{Maintenance: &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: "0 2 * * *", Duration: "1h"}}}}}}
			prior := domain.Exposure{Public: true, IPType: "IPV4", Allowlist: []string{"192.0.2.0/24"}}
			var admission ExposureAdmission
			if known {
				admission.Seed(prior)
			}
			effective, deferred := admission.Resolve(rc, prior)
			if deferred == known || effective.Public != known {
				t.Fatal("legacy admission guessed unrecorded access", effective, deferred)
			}
		})
	}
}

func TestExposureAdmissionProvisioningChangeRemainsExempt(t *testing.T) {
	rc := &Context{Clock: fixedRouteClock{}, Cluster: &domain.Cluster{Generation: 2, Spec: domain.ClusterSpec{Maintenance: &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: "0 2 * * *", Duration: "1h"}}}}}}
	var admission ExposureAdmission
	admission.Seed(domain.Exposure{})
	effective, deferred := admission.Resolve(rc, domain.Exposure{Public: true, IPType: "IPV4"})
	if deferred || !effective.Public {
		t.Fatal("initial provisioning inherited desired-update maintenance gate", effective, deferred)
	}
}
