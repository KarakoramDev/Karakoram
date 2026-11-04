// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package neo4j

import (
	"encoding/json"
	"fmt"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine/neo4j/neo4jspec"
	"testing"
	"time"
)

func TestMaintenanceResourceChange(t *testing.T) {
	for _, release := range []string{"open", "override"} {
		t.Run(release, func(t *testing.T) {
			p, rc, b, _ := setup(t)
			converge(t, p, rc)
			before, _ := load(rc.Cluster)
			old := before.Member.Resources
			activeConfig, err := p.MemberConfig(t.Context(), rc, before.Member.Name)
			if err != nil {
				t.Fatal(err)
			}
			rc.Cluster.Generation = 2
			rc.Cluster.Spec.Maintenance = &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: fmt.Sprintf("0 0 1 %d *", int(time.Now().Month())%12+1), Duration: "1h"}}}
			rc.Cluster.Spec.Components.Engine.Resources.MinRAM++
			rc.Cluster.Spec.Components.Engine.Resources.MaxRAM++
			for range 5 {
				if _, err := p.Sync(t.Context(), rc); err != nil {
					t.Fatal(err)
				}
			}
			s, _ := load(rc.Cluster)
			svc, _ := b.GetService(t.Context(), s.Member.Name)
			if s.Member.Resources != old || svc.Resources != old || s.Member.Pending != nil {
				t.Fatal("closed window disrupted member")
			}
			pendingConfig, err := p.MemberConfig(t.Context(), rc, s.Member.Name)
			if err != nil || string(pendingConfig.EngineSpec) != string(activeConfig.EngineSpec) {
				t.Fatal("closed resource request leaked through autonomous config pull", err)
			}
			found := false
			for _, c := range rc.Cluster.Status.Conditions {
				found = found || c.Type == "MaintenanceWindow" && c.Status == "False" && c.Reason == "WaitingForWindow"
			}
			if !found {
				t.Fatal("missing maintenance wait", rc.Cluster.Status.Conditions)
			}
			if release == "override" {
				rc.Cluster.MaintenanceOverrideGeneration = rc.Cluster.Generation
			} else {
				rc.Cluster.Spec.Maintenance.Windows[0].Schedule = "* * * * *"
			}
			converge(t, p, rc)
			s, _ = load(rc.Cluster)
			if s.Member.Resources != rc.Cluster.Spec.Components.Engine.Resources {
				t.Fatal("resource change did not proceed")
			}
		})
	}
}

func TestMaintenanceConfigurationPull(t *testing.T) {
	p, rc, _, _ := setup(t)
	converge(t, p, rc)
	s, _ := load(rc.Cluster)
	before, err := p.MemberConfig(t.Context(), rc, s.Member.Name)
	if err != nil {
		t.Fatal(err)
	}
	rc.Cluster.Spec.Maintenance = &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: fmt.Sprintf("0 0 1 %d *", int(time.Now().Month())%12+1), Duration: "1h"}}}
	rc.Cluster.Generation++
	rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"settings":{"db.transaction.timeout":"60s"}}`)
	if _, err = p.Sync(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	after, err := p.MemberConfig(t.Context(), rc, s.Member.Name)
	if err != nil || string(before.EngineSpec) != string(after.EngineSpec) {
		t.Fatal("unapproved settings leaked through pull", err)
	}
	rc.Cluster.MaintenanceOverrideGeneration = rc.Cluster.Generation
	converge(t, p, rc)
	after, err = p.MemberConfig(t.Context(), rc, s.Member.Name)
	if err != nil {
		t.Fatal(err)
	}
	var spec neo4jspec.Spec
	if err = json.Unmarshal(after.EngineSpec, &spec); err != nil || spec.Settings["db.transaction.timeout"] != "60s" {
		t.Fatal("approved settings not published", err)
	}
}
