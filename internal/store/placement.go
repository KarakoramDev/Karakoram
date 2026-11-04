// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"slices"
	"strings"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

// Persist the observation and event in the same transaction. The previous stored
// group set is the deduplication journal, including across process restarts and
// operation checkpoint writes.
func recordPlacementTransition(ctx context.Context, tx *sql.Tx, c *domain.Cluster) error {
	var data []byte
	if err := tx.QueryRowContext(ctx, "SELECT status FROM clusters WHERE namespace=? AND name=?", domain.NormalizedNamespace(c.Namespace), c.Name).Scan(&data); err != nil {
		return typed(err)
	}
	var previous domain.ClusterStatus
	if err := json.Unmarshal(data, &previous); err != nil {
		return err
	}
	before := domain.CoLocatedPlacementGroups(previous.Placement)
	after := domain.CoLocatedPlacementGroups(c.Status.Placement)
	if slices.Equal(before, after) {
		return nil
	}
	event := domain.Event{Namespace: c.Namespace, Cluster: c.Name, Resource: "clusters", Target: c.Name, Actor: Actor(ctx)}
	if len(after) > 0 {
		event.Type, event.Reason = "Warning", "MembersCoLocated"
		event.Message = "Members are observed co-located in placement groups: " + strings.Join(after, ", ")
		if c.Status.Placement.Source == "best-effort" {
			event.Message += "; host fingerprints are best-effort, not proof of host-failure tolerance"
		}
	} else {
		event.Type, event.Reason = "Normal", "MembersSeparated"
		event.Message = "Member co-location is no longer observed; unknown placement does not establish host separation"
	}
	return appendEvent(ctx, tx, &event)
}
