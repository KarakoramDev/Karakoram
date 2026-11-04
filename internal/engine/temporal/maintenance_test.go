// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package temporal

import (
	"fmt"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"testing"
	"time"
)

func TestMaintenanceResourceChange(t *testing.T) {
	for _, release := range []string{"open", "override"} {
		t.Run(release, func(t *testing.T) {
			p, rc, _, _ := fixture(t, "combined")
			converge(t, p, rc)
			s, _ := loadState(rc.Cluster)
			old := s.Members["server"].Resources
			rc.Cluster.Generation++
			rc.Cluster.Spec.Maintenance = &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: fmt.Sprintf("0 0 1 %d *", int(time.Now().Month())%12+1), Duration: "1h"}}}
			d := rc.Cluster.Spec.Components.Extra["server"]
			d.Resources.MinRAM++
			d.Resources.MaxRAM++
			rc.Cluster.Spec.Components.Extra["server"] = d
			for range 8 {
				if _, err := p.Sync(t.Context(), rc); err != nil {
					t.Fatal(err)
				}
			}
			s, _ = loadState(rc.Cluster)
			svc, _ := rc.Backend.GetService(t.Context(), s.Members["server"].Name)
			if s.Members["server"].Resources != old || svc.Resources != old || s.Pending != nil {
				t.Fatal("closed window disrupted component")
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
			s, _ = loadState(rc.Cluster)
			if s.Members["server"].Resources != d.Resources {
				t.Fatal("resource change did not proceed")
			}
		})
	}
}

func TestMaintenanceBindingRotationExempt(t *testing.T) {
	p, rc, bindings, _ := fixture(t, "combined")
	converge(t, p, rc)
	rc.Cluster.Spec.Maintenance = &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: fmt.Sprintf("0 0 1 %d *", int(time.Now().Month())%12+1), Duration: "1h"}}}
	bindings.conn.CredentialGeneration++
	// Health may remain Ready between role publications; wait for the
	// credential-generation barrier, not only the native health helper.
	for range 20 {
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
		current, _ := loadState(rc.Cluster)
		if current.CredentialGeneration == bindings.conn.CredentialGeneration {
			break
		}
	}
	s, _ := loadState(rc.Cluster)
	if s.CredentialGeneration != bindings.conn.CredentialGeneration {
		t.Fatal("binding rotation blocked by window")
	}
	// A concurrent resource change does not inherit the binding exemption.
	bindings.conn.CredentialGeneration++
	d := rc.Cluster.Spec.Components.Extra["server"]
	old := d.Resources
	d.Resources.MinRAM++
	d.Resources.MaxRAM++
	rc.Cluster.Spec.Components.Extra["server"] = d
	for range 10 {
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
	}
	s, _ = loadState(rc.Cluster)
	if s.Members["server"].Resources != old {
		t.Fatal("resource mutation inherited credential exemption")
	}
}
