// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package keycloak

import (
	"fmt"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"testing"
	"time"
)

func TestMaintenanceResourceChange(t *testing.T) {
	for _, release := range []string{"open", "override"} {
		t.Run(release, func(t *testing.T) {
			p, rc, bindings, _ := setup(t)
			converge(t, p, rc)
			old := bindings.backend.service.Resources
			rc.Cluster.Generation = 2
			rc.Cluster.Spec.Maintenance = &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: fmt.Sprintf("0 0 1 %d *", int(time.Now().Month())%12+1), Duration: "1h"}}}
			rc.Cluster.Spec.Components.Engine.Resources.MinRAM++
			rc.Cluster.Spec.Components.Engine.Resources.MaxRAM++
			converge(t, p, rc)
			s, _ := load(rc.Cluster)
			if s.Resources != old || bindings.backend.service.Resources != old || s.Pending != nil {
				t.Fatal("closed window resized runtime")
			}
			found := false
			for _, c := range rc.Cluster.Status.Conditions {
				found = found || c.Type == "MaintenanceWindow" && c.Status == "False" && c.Reason == "WaitingForWindow"
			}
			if !found {
				t.Fatal("missing maintenance wait")
			}
			if release == "override" {
				rc.Cluster.MaintenanceOverrideGeneration = rc.Cluster.Generation
			} else {
				rc.Cluster.Spec.Maintenance.Windows[0].Schedule = "* * * * *"
			}
			converge(t, p, rc)
			s, _ = load(rc.Cluster)
			if s.Resources != rc.Cluster.Spec.Components.Engine.Resources {
				t.Fatal("resource change did not proceed")
			}
		})
	}
}

func TestMaintenanceBindingRotationExempt(t *testing.T) {
	p, rc, bindings, _ := setup(t)
	converge(t, p, rc)
	s, _ := load(rc.Cluster)
	oldVersion := s.ConfigVersion
	rc.Cluster.Spec.Maintenance = &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: fmt.Sprintf("0 0 1 %d *", int(time.Now().Month())%12+1), Duration: "1h"}}}
	bindings.conn.CredentialGeneration++
	bindings.password = "rotated-test-password"
	converge(t, p, rc)
	s, _ = load(rc.Cluster)
	if s.ConfigVersion <= oldVersion {
		t.Fatal("credential rotation blocked by window")
	}
	rc.Cluster.Spec.EngineOptions = []byte(`{"database":{"cluster":"pg"},"hostname":"https://identity.example.test"}`)
	oldVersion = s.ConfigVersion
	converge(t, p, rc)
	s, _ = load(rc.Cluster)
	if s.ConfigVersion != oldVersion {
		t.Fatal("application configuration inherited credential exemption")
	}
}
