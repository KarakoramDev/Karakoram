// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"encoding/json"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"testing"
)

func TestMaintenanceCitusGroupScaleIn(t *testing.T) {
	for _, release := range []string{"open", "override"} {
		t.Run(release, func(t *testing.T) {
			p, rc, _, fake := setupDistributed(t, 1)
			rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"citus":{"workerGroups":2}}`)
			converge(t, p, rc)
			closeRolloutWindow(rc)
			rc.Cluster.Generation++
			rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"citus":{"workerGroups":1}}`)
			for range 4 {
				if _, err := p.Sync(t.Context(), rc); err != nil {
					t.Fatal(err)
				}
			}
			s, _ := load(rc.Cluster)
			if s.CitusScale != nil || len(s.group(2).data()) == 0 {
				t.Fatal("closed window admitted group retirement")
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
				rc.Cluster.Spec.Maintenance = &domain.MaintenancePolicy{}
			}
			admitted := false
			for range 25 {
				if _, err := p.Sync(t.Context(), rc); err != nil {
					t.Fatal(err)
				}
				s, _ = load(rc.Cluster)
				if s.CitusScale != nil && s.CitusScale.Started {
					admitted = true
					break
				}
			}
			if !admitted {
				t.Fatal("group drain did not proceed", fake.calls)
			}
			// Window closure after drain must not strand the retired group.
			closeRolloutWindow(rc)
			rc.Cluster.MaintenanceOverrideGeneration = 0
			for range 80 {
				if _, err := p.Sync(t.Context(), rc); err != nil {
					t.Fatal(err)
				}
				s, _ = load(rc.Cluster)
				if s.CitusScale == nil && len(s.group(2).data()) == 0 {
					return
				}
			}
			t.Fatal("window closure interrupted group retirement")
		})
	}
}
