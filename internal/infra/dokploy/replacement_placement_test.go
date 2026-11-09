// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package dokploy

import (
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"testing"
)

func TestReplacementPlacementWithAsymmetricHolders(t *testing.T) {
	for _, mode := range []string{"without-hint", "replacement", "down", "drained", "avoid"} {
		t.Run(mode, func(t *testing.T) {
			f := newFakeAPI(t)
			b := fakeBackend(t, f, nil)
			nodes, err := b.nodes(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			// Manager: GR m0 + Neo4j; worker one: GR m2 + PG;
			// worker two: GR m1. Without a hint, total holders break the group tie.
			holders := []metadata{
				{Kind: infra.KindVolume, Hostname: "grm0v", Node: "node-a", SpreadGroup: "gr"},
				{Kind: infra.KindVolume, Hostname: "neo4jv", Node: "node-a", SpreadGroup: "neo4j"},
				{Kind: infra.KindVolume, Hostname: "grm2v", Node: "node-b", SpreadGroup: "gr"},
				{Kind: infra.KindVolume, Hostname: "pgv", Node: "node-b", SpreadGroup: "pg"},
				{Kind: infra.KindVolume, Hostname: "grm1v", Node: "node-c", SpreadGroup: "gr"},
			}
			replaces, want := "grm0v", "node-a"
			var avoid []string
			switch mode {
			case "without-hint":
				replaces, want = "", "node-c"
			case "down":
				nodes[0].Status.State, want = "down", "node-c"
			case "drained":
				nodes[0].Spec.Availability, want = "drain", "node-c"
			case "avoid":
				avoid, want = []string{"node-a"}, "node-c"
			}
			got, err := chooseNode(nodes, holders, "gr", replaces, avoid...)
			if err != nil || got != want {
				t.Fatalf("placement = %q, %v; want %q", got, err, want)
			}
		})
	}
}
