// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package weaviate

import "testing"

func TestRetirementDoesNotReprovisionDeletedRuntime(t *testing.T) {
	for _, stage := range []string{"planned", "imported", "deployed", "ready"} {
		t.Run(stage, func(t *testing.T) {
			f := coreSetup(t, 4)
			s := coreConverge(t, f)
			m := s.Members[3]
			m.Removing, m.Stage, m.CleanupStep = true, stage, "runtime-deleted"
			s.ScaleDown = &scaleDown{Member: m.Name, Stage: "volume", Target: 3}
			f.rc.Cluster.Spec.Components.Engine.Replicas = 3
			if _, err := f.backend.DeleteServiceByID(t.Context(), m.Name, m.ServiceID); err != nil {
				t.Fatal(err)
			}
			m.ServiceID = ""
			if err := s.save(f.rc.Cluster); err != nil {
				t.Fatal(err)
			}
			before := len(f.backend.Calls())
			if _, err := f.p.Sync(t.Context(), f.rc); err != nil {
				t.Fatal(err)
			}
			current, err := load(f.rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			if current.Pending == nil || current.Pending.Action != "delete-volume" || current.ScaleDown.Stage != "forget" {
				t.Fatal("ordinary recovery displaced retirement ownership")
			}
			for _, call := range f.backend.Calls()[before:] {
				if call == "ImportServices" || call == "Deploy" {
					t.Fatal("retirement recreated a runtime", call)
				}
			}
			if _, err := f.p.Sync(t.Context(), f.rc); err != nil {
				t.Fatal("retirement volume deletion was blocked", err)
			}
			if runtime, err := f.backend.GetService(t.Context(), m.Name); err != nil || runtime != nil {
				t.Fatal("retired runtime reappeared", err)
			}
		})
	}
}

func TestRuntimeRecoverySkipsRetiringMember(t *testing.T) {
	f := coreSetup(t, 4)
	s := coreConverge(t, f)
	m := s.Members[3]
	m.Removing = true
	if _, err := f.backend.DeleteServiceByID(t.Context(), m.Name, m.ServiceID); err != nil {
		t.Fatal(err)
	}
	before := len(f.backend.Calls())
	if busy, err := f.p.reconcileRuntime(t.Context(), f.rc, s); err != nil || busy {
		t.Fatal("retiring member was treated as a lost serving runtime", err)
	}
	for _, call := range f.backend.Calls()[before:] {
		if call == "ImportServices" || call == "Deploy" {
			t.Fatal("runtime recovery recreated retiring identity", call)
		}
	}
}
