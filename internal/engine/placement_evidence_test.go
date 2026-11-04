// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package engine

import (
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestPlacementEvidence(t *testing.T) {
	for _, tc := range []struct {
		name          string
		groups        []string
		authoritative bool
		source        string
		guarantee     bool
		colocated     int
	}{
		{"backend separated", []string{"n1", "n2"}, true, "backend", true, 0},
		{"backend colocated", []string{"n1", "n1"}, true, "backend", false, 1},
		{"backend partial", []string{"n1", ""}, true, "backend", false, 0},
		{"backend unavailable", []string{"", "unknown"}, true, "unavailable", false, 0},
		{"backend empty", nil, true, "unavailable", false, 0},
		{"fingerprints separated", []string{"best-effort:a", "best-effort:b"}, false, "best-effort", false, 0},
		{"fingerprints colocated", []string{"best-effort:a", "best-effort:a"}, false, "best-effort", false, 1},
		{"fingerprints partial", []string{"best-effort:a", ""}, false, "best-effort", false, 0},
		{"fingerprints unknown", []string{"best-effort:unknown", "best-effort:unknown"}, false, "unavailable", false, 0},
		{"no evidence", []string{"", ""}, false, "unavailable", false, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status := domain.ClusterStatus{}
			for i, group := range tc.groups {
				status.Members = append(status.Members, domain.MemberStatus{Name: string(rune('a' + i)), PlacementGroup: group})
			}
			ObservePlacement(&status, tc.authoritative)
			placement := status.Placement
			if placement.Source != tc.source || placement.GuaranteedAntiAffinity != tc.guarantee || len(domain.CoLocatedPlacementGroups(placement)) != tc.colocated {
				t.Fatalf("placement = %+v", placement)
			}
		})
	}
}
