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
	backup, err := loadBackupState(rc.Cluster)
	if err != nil {
		condition.Status, condition.Reason = "Unknown", "ObservationFailed"
		condition.Message = "Cannot observe Citus restore ownership"
		return condition
	}
	if backup.ManagedRolesHeld || backup.RestoreOperation != "" || s.RestoreRecovery != nil && restoreOperation(s.RestoreRecovery) {
		// Clearing the desired hold is not proof that every agent reloaded it.
		// The owning restore performs its explicit SQL barrier only after all
		// release reloads acknowledge success; ordinary Status must not race it.
		condition.Reason = "RestoreInProgress"
		condition.Message = "Restore owns the formation; waiting for its all-member access release and SQL barrier"
		return condition
	}
	if err := citusConnectivity(ctx, rc, leaders); err != nil {
		condition.Reason = "ConnectivityFailed"
		condition.Message = "Coordinator cannot execute SQL on every group primary: " + err.Error()
		return condition
	}
	condition.Status, condition.Reason = "True", "MetadataMatches"
	condition.Message = "Every group primary matches Patroni metadata and accepts coordinator SQL"
	return condition
}

// Metadata alone does not prove the outbound credential used by Citus works.
// This read-only probe must run only after restore client fences are released.
func citusConnectivity(ctx context.Context, rc *engine.Context, leaders map[int]string) error {
	if leaders[0] == "" {
		return engine.Invalid("coordinator primary is not observed")
	}
	var status pgspec.CitusStatusResponse
	if err := pgAction(ctx, rc, leaders[0], pgspec.ActionCitusConnectivity, struct{}{}, &status); err != nil {
		return err
	}
	if !status.IsCoordinator || len(status.Nodes) != len(leaders) {
		return engine.Invalid("Citus connectivity observation omits group primaries")
	}
	seen := map[int]bool{}
	for _, node := range status.Nodes {
		if seen[node.GroupID] || !node.Active || node.Role != "primary" || node.Port != pgspec.PortPostgres || leaders[node.GroupID] == "" || node.Host != leaders[node.GroupID] {
			return engine.Invalid("Citus SQL connectivity failed for group %d", node.GroupID)
		}
		seen[node.GroupID] = true
	}
	return nil
}
