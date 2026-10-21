// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package store

import (
	"slices"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestPlacementEventsTransitionsAndRestartDeduplication(t *testing.T) {
	s, path := testStore(t)
	c := &domain.Cluster{Name: "placement"}
	if err := s.CreateCluster(t.Context(), c); err != nil {
		t.Fatal(err)
	}
	save := func(groups map[string][]string) {
		t.Helper()
		c.Status.Placement = domain.Placement{Source: "backend", Groups: groups}
		if err := s.UpdateClusterState(t.Context(), c); err != nil {
			t.Fatal(err)
		}
	}
	save(map[string][]string{"unknown": {"a", "b"}})
	save(map[string][]string{"n1": {"a", "b"}})
	save(map[string][]string{"n1": {"b", "a"}})
	reopened, err := Open(path, s.sealer)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	persisted, err := reopened.GetCluster(t.Context(), c.Ref())
	if err != nil {
		t.Fatal(err)
	}
	if err = reopened.UpdateClusterState(t.Context(), persisted); err != nil {
		t.Fatal(err)
	}
	// A different co-located set is a new warning, not a separated transition.
	save(map[string][]string{"n2": {"a", "b"}})
	save(map[string][]string{"n1": {"a"}, "n2": {"b"}})
	save(map[string][]string{"n1": {"a"}, "n2": {"b"}})
	save(map[string][]string{"best-effort:unknown": {"a", "b"}})
	save(map[string][]string{"n1": {"a", "b"}})
	events, err := reopened.ListEvents(t.Context(), EventFilter{Cluster: c.Name})
	if err != nil {
		t.Fatal(err)
	}
	var reasons []string
	for _, event := range events {
		if event.Reason != "MembersCoLocated" && event.Reason != "MembersSeparated" {
			continue
		}
		reasons = append(reasons, event.Reason)
		want := "Warning"
		if event.Reason == "MembersSeparated" {
			want = "Normal"
		}
		if event.Type != want {
			t.Fatal(event)
		}
	}
	if !slices.Equal(reasons, []string{"MembersCoLocated", "MembersCoLocated", "MembersSeparated", "MembersCoLocated"}) {
		t.Fatal(reasons)
	}
}
