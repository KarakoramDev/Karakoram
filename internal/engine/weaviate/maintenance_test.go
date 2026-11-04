// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package weaviate

import (
	"context"
	"errors"
	"fmt"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"testing"
	"time"
)

func TestMaintenanceResourceChange(t *testing.T) {
	for _, release := range []string{"open", "override"} {
		t.Run(release, func(t *testing.T) {
			f := coreSetup(t, 1)
			before := coreConverge(t, f)
			old := before.Members[0].Resources
			hash := before.Members[0].ConfigHash
			f.rc.Cluster.Generation++
			f.rc.Cluster.Spec.Maintenance = &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: fmt.Sprintf("0 0 1 %d *", int(time.Now().Month())%12+1), Duration: "1h"}}}
			f.rc.Cluster.Spec.Components.Engine.Resources.MinRAM++
			f.rc.Cluster.Spec.Components.Engine.Resources.MaxRAM++
			for range 8 {
				if _, err := f.p.Sync(t.Context(), f.rc); err != nil {
					t.Fatal(err)
				}
			}
			s, _ := load(f.rc.Cluster)
			svc, _ := f.backend.GetService(t.Context(), s.Members[0].Name)
			if s.Members[0].Resources != old || svc.Resources != runtimeResources(old) || s.Members[0].ConfigHash != hash {
				t.Fatal("closed window activated disruptive configuration")
			}
			found := false
			for _, c := range f.rc.Cluster.Status.Conditions {
				found = found || c.Type == "MaintenanceWindow" && c.Status == "False" && c.Reason == "WaitingForWindow"
			}
			if !found {
				t.Fatal("missing maintenance wait")
			}
			if release == "override" {
				f.rc.Cluster.MaintenanceOverrideGeneration = f.rc.Cluster.Generation
			} else {
				f.rc.Cluster.Spec.Maintenance.Windows[0].Schedule = "* * * * *"
			}
			s = coreConverge(t, f)
			if s.Members[0].Resources != f.rc.Cluster.Spec.Components.Engine.Resources {
				t.Fatal("resource change did not proceed")
			}
		})
	}
}

type rejectingResourcePlanner struct {
	infra.Backend
	calls int
}

func (b *rejectingResourcePlanner) PlanResources(context.Context, string, infra.Resources) error {
	b.calls++
	return &infra.ResourceUpdateRejectedError{Message: "capacity unavailable"}
}
func TestResourcePreflightPreservesServingMember(t *testing.T) {
	f := coreSetup(t, 1)
	before := coreConverge(t, f)
	old := before.Members[0].Resources
	hash := before.Members[0].ConfigHash
	f.rc.Cluster.Spec.Components.Engine.Resources.MinRAM++
	f.rc.Cluster.Spec.Components.Engine.Resources.MaxRAM++
	planner := &rejectingResourcePlanner{Backend: f.backend}
	f.rc.Backend = planner
	rejected := false
	for range 8 {
		_, err := f.p.Sync(t.Context(), f.rc)
		if errors.Is(err, infra.ErrRejected) {
			rejected = true
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	s, _ := load(f.rc.Cluster)
	if !rejected || planner.calls == 0 || s.Members[0].Resources != old || s.Members[0].ConfigHash != hash || s.Rolling != nil {
		t.Fatal("preflight changed serving configuration")
	}
	f.rc.Cluster.Spec.Components.Engine.Resources = old
	if _, err := f.p.Sync(t.Context(), f.rc); err != nil {
		t.Fatal("revert blocked", err)
	}
}
