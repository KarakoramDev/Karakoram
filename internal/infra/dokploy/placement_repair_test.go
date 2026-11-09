// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package dokploy

import (
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func TestEligiblePlacementGroups(t *testing.T) {
	for _, mode := range []string{"all", "labelled", "labelled-down", "drained", "down"} {
		t.Run(mode, func(t *testing.T) {
			f := newFakeAPI(t)
			b := fakeBackend(t, f, nil)
			want := []string{"node-a", "node-b", "node-c"}
			switch mode {
			case "labelled", "labelled-down":
				for i := range f.nodes {
					f.nodes[i]["Spec"].(map[string]any)["Labels"] = map[string]any{}
				}
				f.nodes[0]["Spec"].(map[string]any)["Labels"] = map[string]any{"karakoram.data": "true"}
				want = []string{"node-a"}
				if mode == "labelled-down" {
					f.nodes[0]["Status"].(map[string]any)["State"] = "down"
					want = nil
				}
			case "drained":
				f.nodes[0]["Spec"].(map[string]any)["Availability"] = "drain"
				want = []string{"node-b", "node-c"}
			case "down":
				f.nodes[0]["Status"].(map[string]any)["State"] = "down"
				want = []string{"node-b", "node-c"}
			}
			inventory, err := b.PlacementSnapshot(t.Context())
			if err != nil || !slices.Equal(inventory.Eligible, want) {
				t.Fatalf("inventory=%v err=%v want=%v", inventory, err, want)
			}
		})
	}
}

func TestReplacementRepairC6(t *testing.T) {
	for _, member := range []string{"m1", "m3"} {
		for _, mode := range []string{"baseline", "manager-extra", "worker-extra", "lexical-worker-first", "drained", "down", "explicit-avoid", "initial-create", "initial-manager-extra", "initial-lexical"} {
			t.Run(member+"/"+mode, func(t *testing.T) {
				f := newFakeAPI(t)
				b := fakeBackend(t, f, nil)
				names := []string{"node-a", "node-b", "node-c"}
				if mode == "lexical-worker-first" || mode == "initial-lexical" {
					names[0] = "z-manager"
				}
				for i := range f.nodes {
					f.nodes[i]["Description"].(map[string]any)["Hostname"] = names[i]
				}
				seed := func(name, group string, node int) {
					t.Helper()
					for i := range f.nodes {
						f.nodes[i]["Spec"].(map[string]any)["Labels"] = map[string]any{"karakoram.data": fmt.Sprint(i == node)}
					}
					spec := volumeSpec(name)
					spec.SpreadGroup = group
					importFake(t, b, spec)
				}
				// Retained m0 is not a surviving member, but its holder still affects ranking.
				seed("m0", "gr", 0)
				seed("neo4j", "neo4j", 0)
				seed("m2", "gr", 1)
				seed("pg", "pg", 1)
				seed("m1", "gr", 2)
				seed("m3", "gr", 2)
				if mode == "manager-extra" || mode == "initial-manager-extra" {
					seed("extra", "other", 0)
				}
				if mode == "worker-extra" {
					seed("extra", "other", 1)
				}
				for i := range f.nodes {
					f.nodes[i]["Spec"].(map[string]any)["Labels"] = map[string]any{"karakoram.data": "true"}
				}
				if mode == "drained" {
					f.nodes[0]["Spec"].(map[string]any)["Availability"] = "drain"
				}
				if mode == "down" {
					f.nodes[0]["Status"].(map[string]any)["State"] = "down"
				}
				replacement := volumeSpec("replacement")
				replacement.SpreadGroup = "gr"
				replacement.SpreadReplaces = member
				replacement.SpreadAvoid = []string{names[1], names[2]}
				if mode == "explicit-avoid" {
					replacement.SpreadAvoid = append(replacement.SpreadAvoid, names[0])
				}
				if mode == "initial-create" || mode == "initial-manager-extra" || mode == "initial-lexical" {
					replacement.SpreadReplaces, replacement.SpreadAvoid = "", nil
				}
				op, err := b.ImportServices(t.Context(), []infra.ServiceSpec{replacement, runtimeSpec("replacement-runtime", "replacement")})
				if mode == "drained" || mode == "down" || mode == "explicit-avoid" {
					if !errors.Is(err, infra.ErrPlacementUnsatisfiable) {
						t.Fatalf("expected strict refusal, got %v", err)
					}
					if s, err := b.GetService(t.Context(), "replacement"); err != nil || s != nil {
						t.Fatalf("allocated rejected replacement: %+v %v", s, err)
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				awaitFake(t, b, op)
				want := names[0]
				if mode == "initial-manager-extra" || mode == "initial-lexical" {
					want = names[1]
				}
				if got := fakeMeta(t, f, "replacement").Node; got != want {
					t.Fatalf("replacement node=%s want=%s", got, want)
				}
				constraints := fakeAppSnapshot(t, f, "replacement-runtime")["placementSwarm"].(map[string]any)["Constraints"].([]any)
				if len(constraints) != 1 || constraints[0] != "node.hostname=="+want {
					t.Fatalf("runtime pin=%v", constraints)
				}
			})
		}
	}
}
