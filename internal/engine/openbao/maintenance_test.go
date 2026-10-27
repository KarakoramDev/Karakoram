// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package openbao

import (
	"fmt"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"testing"
	"time"
)

func TestMaintenanceResourceChange(t *testing.T) {
	for _, release := range []string{"open", "override"} {
		t.Run(release, func(t *testing.T) {
			p, rc, b, a := setup(t)
			ready(t, p, rc)
			old := ordered(state(t, rc))[0].Resources
			rc.Cluster.Generation++
			rc.Cluster.Spec.Maintenance = &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: fmt.Sprintf("0 0 1 %d *", int(time.Now().Month())%12+1), Duration: "1h"}}}
			rc.Cluster.Spec.Components.Engine.Resources.MinRAM++
			rc.Cluster.Spec.Components.Engine.Resources.MaxRAM++
			b.log.clear()
			for range 8 {
				if _, err := p.Sync(t.Context(), rc); err != nil {
					t.Fatal(err)
				}
			}
			for _, m := range state(t, rc).Members {
				if m.Resources != old || a.member(m.Name).sealed {
					t.Fatal("closed window disrupted member")
				}
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
			ready(t, p, rc)
			for _, m := range state(t, rc).Members {
				if m.Resources != runtimeResources(rc.Cluster.Spec.Components.Engine.Resources) {
					t.Fatal("resource change did not proceed")
				}
			}
		})
	}
}
