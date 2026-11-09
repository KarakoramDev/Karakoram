// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package nomad

import (
	"errors"
	"slices"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func TestPlacementAliasesPreservePublicGroups(t *testing.T) {
	for _, mode := range []string{"mixed", "all-ids", "unknown-name", "ambiguous-name"} {
		t.Run(mode, func(t *testing.T) {
			f := newFakeNomad(t)
			b := fakeBackend(t, f, nil)
			for _, member := range []string{"m0", "m1", "m2"} {
				importFake(t, b, runtimeSpec(member, ""))
				deployFake(t, b, member, "")
			}
			f.setAutoTick(false)
			ids := []string{"00000000-0000-4000-8000-000000000001", "00000000-0000-4000-8000-000000000002", "00000000-0000-4000-8000-000000000003"}
			names := []string{"host-a", "host-b", "host-c"}
			if mode == "ambiguous-name" {
				names[1] = names[0]
			}
			f.mu.Lock()
			for i, key := range []string{"node-a", "node-b", "node-c"} {
				n := f.nodes[key]
				delete(f.nodes, key)
				n.ID, n.Name = ids[i], names[i]
				// Ineligible admission does not stop an incumbent or erase its aliases.
				if i == 1 {
					n.SchedulingEligibility = "ineligible"
				}
				f.nodes[n.ID] = n
			}
			for _, allocation := range f.allocs {
				member := fakeString(allocation.row, "JobID")
				index := 0
				if member == "m2" {
					index = 1
				}
				allocation.row["NodeID"], allocation.row["NodeName"] = ids[index], names[index]
				if member == "m1" || mode == "all-ids" {
					delete(allocation.row, "NodeName")
				}
				if mode == "unknown-name" && member == "m0" {
					allocation.row["NodeName"] = "unresolved-incumbent"
				}
			}
			f.mu.Unlock()
			status := domain.ClusterStatus{Phase: domain.PhaseReady}
			for _, member := range []string{"m0", "m1", "m2"} {
				containers, err := b.Containers(t.Context(), member)
				if err != nil || len(containers) != 1 {
					t.Fatalf("containers %s: %+v %v", member, containers, err)
				}
				// The public projection remains the allocation name, with ID fallback.
				want := names[0]
				if member == "m2" {
					want = names[1]
				}
				if member == "m1" || mode == "all-ids" {
					want = ids[0]
					if member == "m2" {
						want = ids[1]
					}
				}
				if mode == "unknown-name" && member == "m0" {
					want = "unresolved-incumbent"
				}
				if containers[0].PlacementGroup != want {
					t.Fatalf("public placement changed: %s=%q want %q", member, containers[0].PlacementGroup, want)
				}
				status.Members = append(status.Members, domain.MemberStatus{Name: member, PlacementGroup: containers[0].PlacementGroup})
			}
			rc := &engine.Context{Backend: b, Cluster: &domain.Cluster{Name: "demo"}}
			if err := engine.ObserveFailureDomains(t.Context(), rc, &status, []string{"m0", "m1", "m2"}); err != nil {
				t.Fatal(err)
			}
			condition := status.Conditions[len(status.Conditions)-1]
			if mode == "unknown-name" || mode == "ambiguous-name" {
				if condition.Status != "Unknown" || condition.Reason != "PlacementUnknown" {
					t.Fatalf("unresolved identity certified domains: %+v", condition)
				}
				if _, err := engine.ReplacementPlacementAvoid(t.Context(), rc, []string{"m0", "m2"}, nil); !errors.Is(err, engine.ErrUnavailable) {
					t.Fatalf("unknown identity admitted replacement: %v", err)
				}
				return
			}
			if condition.Status != "False" || condition.Reason != "SharedNode" || status.Phase != domain.PhaseDegraded {
				t.Fatalf("mixed aliases hid physical co-location: %+v", status)
			}
			avoid, err := engine.ReplacementPlacementAvoid(t.Context(), rc, []string{"m0", "m2"}, nil)
			if err != nil || !slices.Equal(avoid, ids[:2]) {
				t.Fatalf("avoidance not canonical: %v, %v", avoid, err)
			}
			if _, err := engine.ReplacementPlacementAvoid(t.Context(), rc, []string{"m0", "m2"}, []string{"host-c"}); !errors.Is(err, infra.ErrPlacementUnsatisfiable) {
				t.Fatalf("explicit alias weakened exclusion: %v", err)
			}
		})
	}
}
