// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package store

import (
	"context"
	"database/sql"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

// Previews run the same admission checks as the corresponding writes inside a
// read-only transaction: a preview fails exactly when the identical request
// would be refused at admission, and never mutates state.

// CheckCreateCluster checks current admission, including the initial data
// source's backup dependency fence, without reserving the name.
func (s *Store) CheckCreateCluster(ctx context.Context, c *domain.Cluster) error {
	tx, err := s.writer.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	ref := c.Ref()
	if err = namespaceForInsert(ctx, tx, ref.Namespace); err != nil {
		return err
	}
	if d := c.Spec.DataSource; d != nil {
		if err = admitRestoreBackup(ctx, tx, ref, domain.ClusterRef{Namespace: d.Namespace, Name: d.Cluster}, d.Backup, d.PITRTime != nil); err != nil {
			return err
		}
	}
	var exists int
	err = tx.QueryRowContext(ctx, "SELECT 1 FROM clusters WHERE namespace=? AND name=?", domain.NormalizedNamespace(ref.Namespace), ref.Name).Scan(&exists)
	if err == sql.ErrNoRows {
		return nil
	}
	if err != nil {
		return typed(err)
	}
	return ErrConflict
}

// CheckClusterSpec uses the same snapshot, deletion and initial-restore
// amendment admission as spec writes. The caller's operation context selects
// the operation kind; credentials reports supplied secret values.
func (s *Store) CheckClusterSpec(ctx context.Context, ref domain.ClusterRef, spec domain.ClusterSpec, credentials bool) error {
	tx, err := s.writer.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = checkSpecSnapshot(ctx, tx, ref); err != nil {
		return err
	}
	_, spec, err = preserveRetryEpoch(ctx, tx, ref, spec)
	if err != nil {
		return err
	}
	_, err = admitInitialSpecAmendment(ctx, tx, ref, spec, spec, credentials)
	return err
}

// CheckRestore checks operation, initial-restore and source-backup dependency
// admission plus record uniqueness without enqueueing.
func (s *Store) CheckRestore(ctx context.Context, restore *domain.Restore) error {
	tx, err := s.writer.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	target := domain.ClusterRef{Namespace: restore.Namespace, Name: restore.Cluster}
	op := domain.Operation{Namespace: domain.NormalizedNamespace(restore.Namespace), Cluster: restore.Cluster, Kind: domain.OperationRestore}
	if err = admitClusterOperation(ctx, tx, &op); err != nil {
		return err
	}
	if c, initial, err := initialDataSourceOperation(ctx, tx, target); err != nil {
		return err
	} else if initial != nil {
		if !matchesInitialRestore(c, restore) {
			return initialRestoreConflict(initial)
		}
		if _, err := admitInitialNativeContinuation(ctx, tx, c, initial); err != nil {
			return err
		}
	}
	if err = admitRestoreBackup(ctx, tx, target, domain.ClusterRef{Namespace: restore.SourceNamespace, Name: restore.Source.SourceCluster}, restore.Source.Backup, restore.Source.PITRTime != nil); err != nil {
		return err
	}
	if err = guardRestoreBindings(ctx, tx, target); err != nil {
		return err
	}
	var exists int
	err = tx.QueryRowContext(ctx, "SELECT 1 FROM records WHERE kind='restore' AND scope=? AND key=?", target.Key(), restore.Name).Scan(&exists)
	if err == sql.ErrNoRows {
		return nil
	}
	if err != nil {
		return typed(err)
	}
	return ErrConflict
}
