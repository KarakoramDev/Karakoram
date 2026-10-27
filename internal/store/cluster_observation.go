// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package store

import (
	"context"
	"encoding/json"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

// GetClusterObservation returns an isolated cluster and its opaque stored status
// checkpoint. The checkpoint is for equality comparison only, never logging or
// public projection: decoded status may redact diagnostics or omit unknown fields.
func (s *Store) GetClusterObservation(ctx context.Context, ref domain.ClusterRef) (*domain.Cluster, []byte, error) {
	if err := ref.Validate(); err != nil {
		return nil, nil, err
	}
	var storedStatus []byte
	c, err := scanClusterStatus(s.reader.QueryRowContext(ctx, "SELECT "+clusterColumns+" FROM clusters WHERE namespace=? AND name=?", domain.NormalizedNamespace(ref.Namespace), ref.Name), &storedStatus)
	return c, storedStatus, err
}

// UpdateClusterObservation publishes availability only if the inspected lifecycle
// checkpoint still owns the cluster. It never writes provider Internal, advances
// generation, or races an imperative operation's checkpoints. A lost fence is a
// discarded sample, not permission to retry publication against a newer state.
// previousStatus is the opaque checkpoint returned by GetClusterObservation, not
// a re-encoding of provider input. Comparing the full checkpoint also fences writes
// made at identical wall timestamps (including after a clock correction).
func (s *Store) UpdateClusterObservation(ctx context.Context, before *domain.Cluster, previousStatus []byte, status domain.ClusterStatus) (bool, error) {
	if err := before.Ref().Validate(); err != nil {
		return false, err
	}
	status.ObservedGeneration = before.Status.ObservedGeneration
	data, err := json.Marshal(status)
	if err != nil {
		return false, err
	}
	internal := before.Internal
	if len(internal) == 0 {
		internal = json.RawMessage(`{}`)
	}
	result, err := s.writer.ExecContext(ctx, `UPDATE clusters SET status=?,updated=?
WHERE namespace=? AND name=? AND created=? AND generation=? AND deleting=0
AND updated=? AND internal=? AND status=?
AND NOT EXISTS (SELECT 1 FROM records WHERE kind='operation' AND scope=? AND phase='Running')`,
		data, time.Now().UTC().UnixNano(), domain.NormalizedNamespace(before.Namespace), before.Name,
		before.CreatedAt.UnixNano(), before.Generation, before.UpdatedAt.UnixNano(), []byte(internal), previousStatus, before.Ref().Key())
	if err != nil {
		return false, err
	}
	n, err := result.RowsAffected()
	return n == 1, err
}
