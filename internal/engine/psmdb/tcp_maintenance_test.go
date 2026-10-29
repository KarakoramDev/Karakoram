// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package psmdb

import (
	"fmt"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"testing"
	"time"
)

func TestMaintenanceTCPChangesDoNotMutateRuntime(t *testing.T) {
	p, rc, _ := writableReadinessFixture(t)
	b := rc.Backend.(*fakeInfra)
	s, err := load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	public := rc.Cluster.Spec.Exposure
	public.Public = true
	s.ExposureAdmission = engine.ExposureAdmission{}
	s.ExposureAdmission.Seed(public)
	for i, m := range s.Members {
		m.PublicPort, m.PublicHash = 27017+i, hash(public)
		b.publicPorts = append(b.publicPorts, infra.PublicPortRoute{Hostname: m.Name, PublicPort: infra.PublicPort{Port: m.PublicPort, TargetPort: 27017, Protocol: "tcp"}})
	}
	rc.Cluster.Spec.Exposure.Public = false
	rc.Cluster.Generation = 2
	rc.Cluster.Status.ObservedGeneration = 1
	rc.Cluster.Spec.Maintenance = &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: fmt.Sprintf("0 0 1 %d *", int(time.Now().Month())%12+1), Duration: "1h"}}}
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
	}
	if len(b.unexposed) != 0 || len(b.exposed) != 0 || rc.Cluster.Status.ObservedGeneration != 1 {
		t.Fatal("closed window updated public runtime")
	}
	rc.Cluster.MaintenanceOverrideGeneration = 2
	if deferred, err := restrictExposure(t.Context(), rc, s); err != nil || deferred || len(b.unexposed) != len(s.Members) {
		t.Fatal("override did not withdraw routes", deferred, err)
	}
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	b.publicPorts = nil
	rc.Cluster.Spec.Exposure.Public = true
	rc.Cluster.Generation = 3
	rc.Cluster.Status.ObservedGeneration = 2
	rc.Cluster.MaintenanceOverrideGeneration = 0
	for range 8 {
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
	}
	if len(b.exposed) != 0 || rc.Cluster.Status.ObservedGeneration != 2 {
		t.Fatal("closed window published a TCP route")
	}
	rc.Cluster.MaintenanceOverrideGeneration = 3
	for range 12 {
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
		if len(b.exposed) == len(s.Members) {
			break
		}
	}
	if len(b.exposed) != len(s.Members) {
		t.Fatal("override did not publish routes", b.exposed)
	}
}
