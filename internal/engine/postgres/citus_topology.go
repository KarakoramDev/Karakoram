// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"context"
	"fmt"
	"strings"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
)

// citusTopology compares Patroni's observed writers with its SQL registration.
// Ordinary reconciliation must never repair pg_dist_node behind Patroni's back.
func citusTopology(ctx context.Context, rc *engine.Context, s *state, observations map[string]pgspec.StatusResponse) domain.Condition {
	condition := domain.Condition{Type: "CitusTopology", Status: "False", Reason: "MetadataDrift", LastTransitionTime: now(rc)}
	leaders := map[int]string{}
	var issues []string
	for _, group := range s.groups() {
		count := 0
		for _, m := range group.data() {
			st, ok := observations[m.Name]
			if !m.Removing && ok && st.Running && st.Role == "primary" && st.Leader == m.Name {
				count++
				leaders[group.id] = m.Name
			}
		}
		if count != 1 {
			issues = append(issues, fmt.Sprintf("group %d has %d observed leaders", group.id, count))
		}
	}
	if len(issues) != 0 {
		condition.Message = strings.Join(issues, "; ")
		return condition
	}
	coordinator := leaders[0]
	if coordinator == "" {
		condition.Message = "Coordinator primary is not observed"
		return condition
	}
	var catalog pgspec.CitusStatusResponse
	ctx, cancel := context.WithTimeout(ctx, engine.ObservationTimeout)
	defer cancel()
	if err := pgAction(ctx, rc, coordinator, pgspec.ActionCitusStatus, struct{}{}, &catalog); err != nil {
		condition.Status, condition.Reason = "Unknown", "ObservationFailed"
		condition.Message = "Cannot observe coordinator pg_dist_node: " + err.Error()
		return condition
	}
	if !catalog.IsCoordinator {
		issues = append(issues, "observed coordinator does not identify as coordinator")
	}
	registered := map[int]int{}
	for _, node := range catalog.Nodes {
		if node.Role != "primary" {
			continue
		}
		registered[node.GroupID]++
		if leader, ok := leaders[node.GroupID]; !ok || node.Host != leader || node.Port != 5432 || !node.Active {
			issues = append(issues, fmt.Sprintf("pg_dist_node group %d primary %s:%d does not match active observed leader %s", node.GroupID, node.Host, node.Port, leader))
		}
	}
	for _, group := range s.groups() {
		if registered[group.id] != 1 {
			issues = append(issues, fmt.Sprintf("group %d has %d registered primaries", group.id, registered[group.id]))
		}
	}
	if len(issues) != 0 {
		condition.Message = strings.Join(issues, "; ")
		return condition
	}
	condition.Status, condition.Reason = "True", "MetadataMatches"
	condition.Message = "Every group has one primary matching Patroni-managed pg_dist_node"
	return condition
}
