// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package store

import (
	"context"
	"database/sql"
	"strconv"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

type clusterSnapshotKey struct{}

type clusterSnapshot struct {
	ref        domain.ClusterRef
	created    int64
	generation int64
}

// clusterIncarnation is an opaque equality token from the persisted cluster
// identity already used by snapshot admission, never a chronological comparison.
func clusterIncarnation(created int64) string { return strconv.FormatInt(created, 10) }

// WithClusterSnapshot fences a mutation against the cluster incarnation and
// desired generation that its caller inspected and validated.
func WithClusterSnapshot(ctx context.Context, c *domain.Cluster) context.Context {
	return context.WithValue(ctx, clusterSnapshotKey{}, clusterSnapshot{c.Ref(), c.CreatedAt.UnixNano(), c.Generation})
}

func checkClusterSnapshot(ctx context.Context, tx *sql.Tx, c *domain.Cluster) error {
	expected, ok := ctx.Value(clusterSnapshotKey{}).(clusterSnapshot)
	if ok && (expected.ref.Key() != c.Ref().Key() || expected.created != c.CreatedAt.UnixNano() || expected.generation != c.Generation) {
		return domain.Conflict("cluster", c.Name, domain.ReasonConflict, ErrConflict, "Cluster %s changed since it was inspected; reload it and retry", c.Name)
	}
	return nil
}

func admitClusterOperation(ctx context.Context, tx *sql.Tx, op *domain.Operation) error {
	ref := domain.ClusterRef{Namespace: op.Namespace, Name: op.Cluster}
	c, err := scanCluster(tx.QueryRowContext(ctx, "SELECT "+clusterColumns+" FROM clusters WHERE namespace=? AND name=?", domain.NormalizedNamespace(ref.Namespace), ref.Name))
	if err != nil {
		return err
	}
	if err = checkClusterSnapshot(ctx, tx, c); err != nil {
		return err
	}
	if op.Kind != domain.OperationRestore && op.Kind != "initial-restore" && op.Kind != domain.OperationBackup && op.Kind != domain.OperationDeleteBackup {
		_, initial, err := initialDataSourceOperation(ctx, tx, ref)
		if err != nil {
			return err
		}
		if initial != nil {
			return initialRestoreConflict(initial)
		}
	}
	op.ClusterIncarnation = clusterIncarnation(c.CreatedAt.UnixNano())
	suspensionSafe := op.Kind == domain.OperationDeleteBackup
	if c.Deleting {
		return domain.Conflict("cluster", c.Name, domain.ReasonInvalidState, ErrConflict, "Cluster %s is being deleted; no further actions are accepted", c.Name)
	}
	if c.Status.Phase == domain.PhaseSuspended && !suspensionSafe {
		return domain.Conflict("cluster", c.Name, domain.ReasonInvalidState, ErrConflict, "Cluster %s is suspended; resume it before requesting this action", c.Name)
	}
	return nil
}

func checkSpecSnapshot(ctx context.Context, tx *sql.Tx, ref domain.ClusterRef) error {
	c, err := scanCluster(tx.QueryRowContext(ctx, "SELECT "+clusterColumns+" FROM clusters WHERE namespace=? AND name=?", domain.NormalizedNamespace(ref.Namespace), ref.Name))
	if err != nil {
		return err
	}
	if err = checkClusterSnapshot(ctx, tx, c); err != nil {
		return err
	}
	if c.Deleting {
		return domain.Conflict("cluster", c.Name, domain.ReasonInvalidState, ErrConflict, "Cluster %s is deleting; desired state cannot be edited", c.Name)
	}
	return nil
}
