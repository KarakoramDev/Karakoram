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
			b.log.clear()
			// A CPU-only PATCH reaches the gate on its first evaluation.
			rc.Cluster.Spec.Components.Engine.Resources.MaxCPU++
			result, err := p.Sync(t.Context(), rc)
			if err != nil || result.ConvergenceError != nil || result.RequeueAfter <= 0 {
				t.Fatal("first resource evaluation did not defer successfully", result, err)
			}
			waiting := false
			for _, condition := range rc.Cluster.Status.Conditions {
				waiting = waiting || condition.Type == "MaintenanceWindow" && condition.Reason == "WaitingForWindow"
			}
			if !waiting {
				t.Fatal("first CPU evaluation did not publish maintenance wait")
			}
			for _, member := range state(t, rc).Members {
				if member.Change != nil {
					t.Fatal("closed window persisted an unadmitted resource plan")
				}
			}
			status, err := p.Status(t.Context(), rc)
			if err != nil || status.Phase != domain.PhaseReady || status.ObservedGeneration >= rc.Cluster.Generation {
				t.Fatalf("deferred CPU change lost serving phase or advanced generation: %+v %v", status, err)
			}
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
			if _, err := p.Sync(t.Context(), rc); err != nil {
				t.Fatal(err)
			}
			status, err = p.Status(t.Context(), rc)
			if err != nil || status.Phase != domain.PhaseUpdating || status.ObservedGeneration >= rc.Cluster.Generation {
				t.Fatalf("admitted CPU roll lacks updating phase: %+v %v", status, err)
			}
			syncUntil(t, p, rc, func(s *internalState) bool {
				for _, m := range s.Members {
					if m.Change != nil && m.Change.Started {
						return true
					}
				}
				return false
			})
			status, err = p.Status(t.Context(), rc)
			if err != nil || status.Phase != domain.PhaseUpdating || status.ObservedGeneration >= rc.Cluster.Generation {
				t.Fatalf("started CPU roll lacks updating phase: %+v %v", status, err)
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
