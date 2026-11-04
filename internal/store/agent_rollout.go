// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package store

import (
	"context"
	"encoding/json"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

// EnqueueAgentRollout atomically fences automatic retries and installation-wide
// concurrency, including rollouts for clusters no longer in the inventory.
func (s *Store) EnqueueAgentRollout(ctx context.Context, op *domain.Operation, limit int) (bool, error) {
	if err := prepareOperation(ctx, op); err != nil {
		return false, err
	}
	tx, err := s.writer.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	var existing int
	if err = tx.QueryRowContext(ctx, "SELECT count(*) FROM records WHERE kind='operation' AND (key=? OR (scope=? AND phase IN ('Pending','Running') AND json_extract(data,'$.kind')=?))", op.ID, domain.ClusterRef{Namespace: op.Namespace, Name: op.Cluster}.Key(), domain.OperationUpgradeAgents).Scan(&existing); err != nil {
		return false, err
	}
	if existing > 0 {
		return false, nil
	}
	var active int
	if err = tx.QueryRowContext(ctx, "SELECT count(DISTINCT scope) FROM records WHERE kind='operation' AND phase IN ('Pending','Running') AND json_extract(data,'$.kind')=?", domain.OperationUpgradeAgents).Scan(&active); err != nil {
		return false, err
	}
	if active >= limit {
		return false, nil
	}
	if err = admitClusterOperation(ctx, tx, op); err != nil {
		return false, err
	}
	if err = put(ctx, tx, "operation", domain.ClusterRef{Namespace: op.Namespace, Name: op.Cluster}.Key(), op.ID, op, "", "", string(op.Phase), true); err != nil {
		return false, err
	}
	return true, tx.Commit()
}

// AppendDeduplicatedEvent commits a durable dedupe marker and event together.
// The marker survives event retention and controller/control-plane restarts.
func (s *Store) AppendDeduplicatedEvent(ctx context.Context, key string, event *domain.Event) (bool, error) {
	tx, err := s.writer.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	var count int
	if err = tx.QueryRowContext(ctx, "SELECT count(*) FROM records WHERE kind='setting' AND scope='' AND key=?", key).Scan(&count); err != nil {
		return false, err
	}
	if count > 0 {
		return false, nil
	}
	if err = appendEvent(ctx, tx, event); err != nil {
		return false, err
	}
	if err = put(ctx, tx, "setting", "", key, json.RawMessage(`true`), "", "", "", true); err != nil {
		return false, err
	}
	return true, tx.Commit()
}
