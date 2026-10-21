// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"encoding/json"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestDistributedGroupAllocation(t *testing.T) {
	c := &domain.Cluster{Name: "formation"}
	c.Spec.Components.Engine.Replicas = 3
	c.Spec.EngineOptions = json.RawMessage(`{"citus":{"workerGroups":2}}`)
	s := &state{Topology: "distributed", ConfigVersion: 1}
	s.allocate(c)
	if len(s.data()) != 9 {
		t.Fatalf("members = %d", len(s.data()))
	}
	for _, name := range []string{"formationp0", "formationp2", "formationw1p0", "formationw1p2", "formationw2p0", "formationw2p2"} {
		if s.find(name) == nil {
			t.Fatalf("missing %s", name)
		}
	}
	if len(s.group(1).data()) != 3 || len(s.memberGroup("formationw2p1").data()) != 3 {
		t.Fatal("group view leaked membership")
	}
	replacement := s.newGroupMember(c, 1)
	if replacement.Name != "formationw1p3" || replacement.Group != 1 || replacement.EtcdState != "" {
		t.Fatalf("replacement = %+v", replacement)
	}
	if s.newGroupMember(c, 0).Name != "formationp3" {
		t.Fatal("worker allocation consumed coordinator index")
	}
	if err := save(c, s); err != nil {
		t.Fatal(err)
	}
	loaded, err := load(c)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.newGroupMember(c, 1).Name != "formationw1p4" {
		t.Fatal("group index did not survive checkpoint")
	}
}
