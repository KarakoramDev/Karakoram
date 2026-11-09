// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package nomad

import (
	"errors"
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func TestEligiblePlacementGroups(t *testing.T) {
	for _, mode := range []string{"all", "name-fallback", "drained", "down", "ineligible", "pool", "datacenter"} {
		t.Run(mode, func(t *testing.T) {
			f := newFakeNomad(t)
			b := fakeBackend(t, f, nil)
			n := f.nodes["node-a"]
			want := []string{"node-a", "node-b", "node-c"}
			switch mode {
			case "name-fallback":
				n.Name = ""
			case "drained":
				n.Drain = true
			case "down":
				n.Status = "down"
			case "ineligible":
				n.SchedulingEligibility = "ineligible"
			case "pool":
				n.NodePool = "other"
			case "datacenter":
				b.target.Config["datacenters"] = n.Datacenter
				n.Datacenter = "other"
			}
			if mode != "all" && mode != "name-fallback" {
				want = want[1:]
			}
			f.nodes[n.ID] = n
			inventory, err := b.PlacementSnapshot(t.Context())
			if err != nil || !slices.Equal(inventory.Eligible, want) {
				t.Fatalf("inventory=%v err=%v want=%v", inventory, err, want)
			}
		})
	}
}

func TestReplacementRepairC6(t *testing.T) {
	for _, member := range []string{"m1", "m3"} {
		for _, mode := range []string{"baseline", "manager-extra", "lexical-worker-first", "down", "drained", "explicit-avoid", "capacity"} {
			t.Run(member+"/"+mode, func(t *testing.T) {
				f := newFakeNomad(t)
				b := fakeBackend(t, f, nil)
				if mode == "lexical-worker-first" {
					n := f.nodes["node-a"]
					n.Name = "z-manager"
					f.nodes[n.ID] = n
				}
				if mode == "down" {
					n := f.nodes["node-a"]
					n.Status = "down"
					f.nodes[n.ID] = n
				}
				if mode == "drained" {
					n := f.nodes["node-a"]
					n.Drain = true
					f.nodes[n.ID] = n
				}
				records := []*record{
					{Hostname: "m0", Kind: infra.KindVolume, Node: "node-a", SpreadGroup: "gr"},
					{Hostname: "neo4j", Kind: infra.KindVolume, Node: "node-a", SpreadGroup: "neo4j"},
					{Hostname: "m2", Kind: infra.KindVolume, Node: "node-b", SpreadGroup: "gr"},
					{Hostname: "pg", Kind: infra.KindVolume, Node: "node-b", SpreadGroup: "pg"},
					{Hostname: "m1", Kind: infra.KindVolume, Node: "node-c", SpreadGroup: "gr"},
					{Hostname: "m3", Kind: infra.KindVolume, Node: "node-c", SpreadGroup: "gr"},
				}
				if mode == "manager-extra" {
					records = append(records, &record{Hostname: "extra", Kind: infra.KindVolume, Node: "node-a", SpreadGroup: "other"})
				}
				if mode == "capacity" {
					f.requestHook = func(w http.ResponseWriter, r *http.Request) (bool, func()) {
						if r.URL.Path == "/v1/node/node-a/allocations" {
							fakeJSON(w, 200, []fakeRow{{"ClientStatus": "running", "AllocatedResources": fakeRow{"Tasks": fakeRow{"other": fakeRow{"Memory": fakeRow{"MemoryMB": 1000000}}}}}})
							return true, nil
						}
						// Avoided survivors must never become capacity fallback candidates.
						if strings.HasPrefix(r.URL.Path, "/v1/node/node-b") || strings.HasPrefix(r.URL.Path, "/v1/node/node-c") {
							t.Errorf("queried avoided node capacity: %s", r.URL.Path)
						}
						return false, nil
					}
				}
				spec := volumeSpec("replacement")
				spec.SpreadGroup, spec.SpreadReplaces = "gr", member
				spec.SpreadAvoid = []string{"node-b", "node-c"}
				if mode == "explicit-avoid" {
					spec.SpreadAvoid = append(spec.SpreadAvoid, "node-a")
				}
				pending := []volumePlacement{}
				got, err := b.chooseVolumeNode(t.Context(), records, spec, infra.Resources{MinCPU: 0.1, MaxCPU: 0.1, MinRAM: 1, MaxRAM: 1}, "", &pending)
				unsatisfiable := mode == "down" || mode == "drained" || mode == "explicit-avoid" || mode == "capacity"
				if unsatisfiable {
					if got != "" || !errors.Is(err, infra.ErrPlacementUnsatisfiable) {
						t.Fatalf("node=%q err=%v; want refusal", got, err)
					}
				} else if err != nil || got != "node-a" {
					t.Fatalf("node=%q err=%v; want node-a", got, err)
				}
			})
		}
	}
}
