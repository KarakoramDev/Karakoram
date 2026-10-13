// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
)

func prepareOperation(op *domain.Operation) error {
	if op.ID == "" {
		id, err := secrets.RandomString(24)
		if err != nil {
			return err
		}
		op.ID = id
	}
	now := time.Now().UTC()
	if op.CreatedAt.IsZero() {
		op.CreatedAt = now
	}
	op.UpdatedAt = now
	if op.Phase == "" {
		op.Phase = domain.PhasePending
	}
	return nil
}

// EnqueueOperation durably queues operation.
func (s *Store) EnqueueOperation(ctx context.Context, op *domain.Operation) error {
	if err := prepareOperation(op); err != nil {
		return err
	}
	return put(ctx, s.writer, "operation", op.Cluster, op.ID, op, "", "", string(op.Phase), true)
}

// GetOperation loads operation.
func (s *Store) GetOperation(ctx context.Context, id string) (*domain.Operation, error) {
	var data []byte
	err := s.reader.QueryRowContext(ctx, "SELECT data FROM records WHERE kind='operation' AND key=?", id).Scan(&data)
	if err != nil {
		return nil, typed(err)
	}
	var op domain.Operation
	if err = json.Unmarshal(data, &op); err != nil {
		return nil, err
	}
	return &op, nil
}

// ListOperations returns ordered operations.
func (s *Store) ListOperations(ctx context.Context, cluster string, phases ...domain.Phase) ([]domain.Operation, error) {
	ops, err := list[domain.Operation](ctx, s, "operation", cluster)
	if err != nil {
		return nil, err
	}
	if len(phases) == 0 {
		return ops, nil
	}
	out := make([]domain.Operation, 0)
	for _, op := range ops {
		for _, phase := range phases {
			if op.Phase == phase {
				out = append(out, op)
				break
			}
		}
	}
	return out, nil
}

// UpdateOperation persists changes to operation.
func (s *Store) UpdateOperation(ctx context.Context, op *domain.Operation) error {
	op.UpdatedAt = time.Now().UTC()
	return s.update(ctx, "operation", op.Cluster, op.ID, op, "", "", string(op.Phase))
}

// UpdateOperationAndClusterState commits the checkpoint together with the
// provider state it references, without overwriting concurrent desired-state
// edits or deletion intent. Neither record is created if it has been removed.
func (s *Store) UpdateOperationAndClusterState(ctx context.Context, op *domain.Operation, c *domain.Cluster) error {
	if op.Cluster != c.Name {
		return ErrConflict
	}
	tx, err := s.writer.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	op.UpdatedAt = time.Now().UTC()
	data, err := json.Marshal(op)
	if err != nil {
		return err
	}
	r, err := tx.ExecContext(ctx, "UPDATE records SET data=?,phase=?,updated=? WHERE kind='operation' AND scope=? AND key=? AND phase='Running'", data, string(op.Phase), op.UpdatedAt.UnixNano(), op.Cluster, op.ID)
	if err = affected(r, err); err != nil {
		return err
	}
	if err = updateClusterState(ctx, tx, c); err != nil {
		return err
	}
	return tx.Commit()
}

// retireOperations fails only unfinished work, retaining its history and
// checkpoint. It runs in the transaction establishing deletion intent/removal.
func retireOperations(ctx context.Context, tx *sql.Tx, cluster, message string) error {
	rows, err := tx.QueryContext(ctx, "SELECT data FROM records WHERE kind='operation' AND scope=? AND phase IN ('Pending','Running')", cluster)
	if err != nil {
		return err
	}
	var ops []domain.Operation
	for rows.Next() {
		var data []byte
		var op domain.Operation
		if err = rows.Scan(&data); err != nil {
			rows.Close()
			return err
		}
		if err = json.Unmarshal(data, &op); err != nil {
			rows.Close()
			return err
		}
		ops = append(ops, op)
	}
	err = rows.Err()
	if closeErr := rows.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	for i := range ops {
		op := &ops[i]
		op.Phase = domain.PhaseFailed
		op.Message = message
		op.CompletedAt = &now
		op.UpdatedAt = now
		if err = put(ctx, tx, "operation", cluster, op.ID, op, "", "", string(op.Phase), false); err != nil {
			return err
		}
	}
	return nil
}

// ClaimOperation resumes Running work before claiming the oldest Pending action.
// Per-cluster ownership is enforced by the reconciler.
func (s *Store) ClaimOperation(ctx context.Context, cluster string) (*domain.Operation, error) {
	return s.claimOperation(ctx, cluster, true)
}

// ClaimRunningOperation resumes active work without starting a Pending action.
// Reconciliation uses it before ordinary Sync, which active work may exclude.
func (s *Store) ClaimRunningOperation(ctx context.Context, cluster string) (*domain.Operation, error) {
	return s.claimOperation(ctx, cluster, false)
}

func (s *Store) claimOperation(ctx context.Context, cluster string, includePending bool) (*domain.Operation, error) {
	tx, err := s.writer.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	var data []byte
	err = tx.QueryRowContext(ctx, "SELECT data FROM records WHERE kind='operation' AND scope=? AND phase IN ('Pending','Running') AND (? OR phase='Running') ORDER BY CASE phase WHEN 'Running' THEN 0 ELSE 1 END,created,key LIMIT 1", cluster, includePending).Scan(&data)
	if err != nil {
		return nil, typed(err)
	}
	var op domain.Operation
	if err = json.Unmarshal(data, &op); err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	op.Phase = domain.PhaseRunning
	op.Attempts++
	op.UpdatedAt = now
	if op.StartedAt == nil {
		op.StartedAt = &now
	}
	if err = put(ctx, tx, "operation", cluster, op.ID, &op, "", "", string(op.Phase), false); err != nil {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return &op, nil
}

// EnqueueBackup creates its inventory record and operation in one transaction.
func (s *Store) EnqueueBackup(ctx context.Context, backup *domain.Backup, op *domain.Operation) error {
	if err := prepareOperation(op); err != nil {
		return err
	}
	if backup.CreatedAt.IsZero() {
		backup.CreatedAt = time.Now().UTC()
	}
	if backup.Phase == "" {
		backup.Phase = domain.PhasePending
	}
	tx, err := s.writer.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = put(ctx, tx, "backup", backup.Cluster, backup.Name, backup, "", "", string(backup.Phase), true); err != nil {
		return err
	}
	if err = put(ctx, tx, "operation", op.Cluster, op.ID, op, "", "", string(op.Phase), true); err != nil {
		return err
	}
	return tx.Commit()
}

// EnqueueRestore creates a restore record and its operation in one transaction.
func (s *Store) EnqueueRestore(ctx context.Context, restore *domain.Restore, op *domain.Operation) error {
	if err := prepareOperation(op); err != nil {
		return err
	}
	if restore.CreatedAt.IsZero() {
		restore.CreatedAt = time.Now().UTC()
	}
	if restore.Phase == "" {
		restore.Phase = domain.PhasePending
	}
	tx, err := s.writer.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = put(ctx, tx, "restore", restore.Cluster, restore.Name, restore, "", "", string(restore.Phase), true); err != nil {
		return err
	}
	if err = put(ctx, tx, "operation", op.Cluster, op.ID, op, "", "", string(op.Phase), true); err != nil {
		return err
	}
	return tx.Commit()
}

// EnqueueBackupDeletion marks inventory and enqueues deletion atomically.
func (s *Store) EnqueueBackupDeletion(ctx context.Context, backup *domain.Backup, op *domain.Operation) error {
	if err := prepareOperation(op); err != nil {
		return err
	}
	tx, err := s.writer.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	backup.Phase = domain.PhaseDeleting
	data, err := json.Marshal(backup)
	if err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, "UPDATE records SET data=?,phase='Deleting',updated=? WHERE kind='backup' AND scope=? AND key=? AND phase='Succeeded'", data, time.Now().UTC().UnixNano(), backup.Cluster, backup.Name)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrConflict
	}
	if err := put(ctx, tx, "operation", op.Cluster, op.ID, op, "", "", string(op.Phase), true); err != nil {
		return err
	}
	return tx.Commit()
}
