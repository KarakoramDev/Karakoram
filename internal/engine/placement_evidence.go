// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package engine

import (
	"fmt"
	"slices"
	"strings"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

// DescribePlacement classifies groups from one evidence source. Callers must use
// backend groups exclusively when Placement is supported, otherwise fingerprints.
func DescribePlacement(groups map[string][]string, authoritative bool) domain.Placement {
	out := domain.Placement{Source: "unavailable", Groups: groups}
	if out.Groups == nil {
		out.Groups = map[string][]string{}
	}
	known, unknown, distinct := 0, 0, true
	keys := make([]string, 0, len(groups))
	for group, members := range groups {
		if !domain.KnownPlacementGroup(group) {
			unknown += len(members)
			continue
		}
		known += len(members)
		distinct = distinct && len(members) <= 1
		keys = append(keys, group)
	}
	slices.Sort(keys)
	var warnings []string
	if known > 0 {
		if authoritative {
			out.Source = "backend"
		} else {
			out.Source = "best-effort"
		}
	}
	out.GuaranteedAntiAffinity = authoritative && known > 0 && unknown == 0 && distinct
	if !authoritative {
		warnings = append(warnings, "The infrastructure backend does not guarantee anti-affinity; host fingerprints are best-effort, not proof of host-failure tolerance")
	}
	for _, group := range keys {
		members := groups[group]
		if len(members) < 2 {
			continue
		}
		members = slices.Clone(members)
		slices.Sort(members)
		verb := "are"
		if !authoritative {
			verb = "appear"
		}
		warnings = append(warnings, fmt.Sprintf("members %s %s co-located", strings.Join(members, ", "), verb))
	}
	if unknown > 0 || known == 0 {
		if authoritative {
			warnings = append(warnings, "backend placement is unknown for some members")
		} else {
			warnings = append(warnings, "placement unknown until members report host fingerprints")
		}
	}
	out.Warning = strings.Join(warnings, "; ")
	return out
}

// ObservePlacement rebuilds grouping from the same member evidence returned to
// clients, including unknown members so partial evidence cannot certify HA.
func ObservePlacement(status *domain.ClusterStatus, authoritative bool) {
	groups := make(map[string][]string)
	for _, member := range status.Members {
		group := member.PlacementGroup
		if !domain.KnownPlacementGroup(group) {
			group = "unknown"
		}
		groups[group] = append(groups[group], member.Name)
	}
	status.Placement = DescribePlacement(groups, authoritative)
}
