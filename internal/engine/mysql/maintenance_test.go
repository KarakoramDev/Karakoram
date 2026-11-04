// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

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
	for _, provider := range []string{"mysql", "pxc"} {
		for _, release := range []string{"open", "override"} {
			t.Run(provider+"/"+release, func(t *testing.T) {
				p, rc, b := setup(t, "group")
				if provider == "pxc" {
					p, rc, b = setupPXC(t)
				}
				converge(t, p, rc, b)
				before, _ := load(rc.Cluster)
				old := before.data()[0].Resources
				rc.Cluster.Generation++
				rc.Cluster.Spec.Maintenance = &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: fmt.Sprintf("0 0 1 %d *", int(time.Now().Month())%12+1), Duration: "1h"}}}
				rc.Cluster.Spec.Components.Engine.Resources.MinRAM++
				rc.Cluster.Spec.Components.Engine.Resources.MaxRAM++
				for range 8 {
					if _, err := p.Sync(t.Context(), rc); err != nil {
						t.Fatal(err)
					}
					b.apply(t, p, rc)
				}
				s, _ := load(rc.Cluster)
				if s.Rolling != nil {
					t.Fatal("closed window created roll")
				}
				for _, m := range s.data() {
					if m.Resources != old || m.Offline || b.services[m.Name].Resources != old {
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
				day2Ticks(t, rc, b, func(s *state) bool {
					for _, m := range s.data() {
						if m.Resources != rc.Cluster.Spec.Components.Engine.Resources || m.Offline {
							return false
						}
					}
					return s.Rolling == nil
				})
			})
		}
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
	p, rc, b := setup(t, "single")
	converge(t, p, rc, b)
	s, _ := load(rc.Cluster)
	m := s.data()[0]
	old := m.Resources
	rc.Cluster.Spec.Components.Engine.Resources.MinRAM++
	rc.Cluster.Spec.Components.Engine.Resources.MaxRAM++
	planner := &rejectingResourcePlanner{Backend: b}
	rc.Backend = planner
	rejected := false
	for range 8 {
		_, err := p.Sync(t.Context(), rc)
		if errors.Is(err, infra.ErrRejected) {
			rejected = true
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		b.apply(t, p, rc)
	}
	s, _ = load(rc.Cluster)
	m = s.data()[0]
	if !rejected || planner.calls == 0 || m.Offline || m.Resources != old || s.Rolling != nil {
		t.Fatal("preflight did not preserve current serving state")
	}
	rc.Cluster.Spec.Components.Engine.Resources = old
	if _, err := p.Sync(t.Context(), rc); err != nil {
		t.Fatal("revert blocked", err)
	}
}
