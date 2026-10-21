// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package domain

import (
	"slices"
	"strings"
)

// KnownPlacementGroup excludes diagnostic placeholders, never physical hosts.
func KnownPlacementGroup(group string) bool {
	switch strings.ToLower(group) {
	case "", "unknown", "unavailable", "pending", "best-effort:unknown":
		return false
	}
	return true
}

// CoLocatedPlacementGroups is a stable deduplication key independent of map and
// member order. Unknown observations never establish co-location.
func CoLocatedPlacementGroups(placement Placement) []string {
	var groups []string
	for group, members := range placement.Groups {
		if KnownPlacementGroup(group) && len(members) > 1 {
			groups = append(groups, group)
		}
	}
	slices.Sort(groups)
	return groups
}
