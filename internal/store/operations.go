// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/redact"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
)

func prepareOperation(ctx context.Context, op *domain.Operation) error {
	op.Namespace = domain.NormalizedNamespace(op.Namespace)
	if err := (domain.ClusterRef{Namespace: op.Namespace, Name: op.Cluster}).Validate(); err != nil {
		return err
	}
	if op.Target.Kind == "" {
		op.Target = domain.OperationTarget{Kind: "cluster", Name: op.Cluster}
		var payload map[string]string
		_ = json.Unmarshal(op.Payload, &payload)
		if name := payload["backup"]; name != "" {
			op.Target = domain.OperationTarget{Kind: "backup", Name: name}
		} else if name := payload["restore"]; name != "" {
			op.Target = domain.OperationTarget{Kind: "restore", Name: name}
		} else if name := payload["member"]; name != "" {
			op.Target = domain.OperationTarget{Kind: "member", Name: name}
		} else if name := payload["user"]; name != "" {
			op.Target = domain.OperationTarget{Kind: "system-user", Name: name}
		}
	}
	if op.Actor == "" {
		op.Actor = Actor(ctx)
	}
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
	if op.Message == "" && op.Phase == domain.PhasePending {
		op.Message = "Queued " + op.Kind + " for " + op.Target.Kind + " " + op.Target.Name
	}
	return nil
}

// EnqueueOperation durably queues operation.
func (s *Store) EnqueueOperation(ctx context.Context, op *domain.Operation) error {
	if err := prepareOperation(ctx, op); err != nil {
		return err
	}
	tx, err := s.writer.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = admitClusterOperation(ctx, tx, op); err != nil {
		return err
	}
	if err = put(ctx, tx, "operation", domain.ClusterRef{Namespace: op.Namespace, Name: op.Cluster}.Key(), op.ID, op, "", "", string(op.Phase), true); err != nil {
		return err
	}
	return tx.Commit()
}

// GetOperation loads operation.
func (s *Store) GetOperation(ctx context.Context, id string) (*domain.Operation, error) {
	var data []byte
	err := s.reader.QueryRowContext(ctx, "SELECT data FROM records WHERE kind='operation' AND key=?", id).Scan(&data)
	if err != nil {
		return nil, typed(err)
	}
	var op domain.Operation
	if err = json.Unmarshal(redact.DiagnosticsJSON(data), &op); err != nil {
		return nil, err
	}
	return &op, nil
}

// ListOperations returns ordered operations.
func (s *Store) ListOperations(ctx context.Context, ref domain.ClusterRef, phases ...domain.Phase) ([]domain.Operation, error) {
	if err := ref.Validate(); err != nil {
		return nil, err
	}
	if len(phases) == 0 {
		return listClusterRecords[domain.Operation](ctx, s, "operation", ref)
	}
	filter, scopeArgs := clusterRecordFilter(ref)
	args := make([]any, 0, len(scopeArgs)+len(phases))
	args = append(args, scopeArgs...)
	var query strings.Builder
	query.Grow(128 + len(filter) + 2*len(phases))
	query.WriteString("SELECT data FROM records WHERE kind='operation'")
	query.WriteString(filter)
	query.WriteString(" AND phase IN (")
	for i, phase := range phases {
		if i != 0 {
			query.WriteByte(',')
		}
		query.WriteByte('?')
		args = append(args, string(phase))
	}
	query.WriteString(") ORDER BY created,key")
	return listQuery[domain.Operation](ctx, s, query.String(), args...)
}

// UpdateOperation persists changes to operation.
func (s *Store) UpdateOperation(ctx context.Context, op *domain.Operation) error {
	if err := (domain.ClusterRef{Namespace: op.Namespace, Name: op.Cluster}).Validate(); err != nil {
		return err
	}
	op.UpdatedAt = time.Now().UTC()
	tx, err := s.writer.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var raw []byte
	if err = tx.QueryRowContext(ctx, "SELECT data FROM records WHERE kind='operation' AND key=?", op.ID).Scan(&raw); err != nil {
		return typed(err)
	}
	var current domain.Operation
	if err = json.Unmarshal(raw, &current); err != nil {
		return err
	}
	if current.Terminal() {
		return nil
	} // Never overwrite retirement or a concurrent cancellation.
	if current.Kind == domain.OperationCreate && current.TargetGeneration != op.TargetGeneration {
		return domain.Conflict("operation", op.ID, domain.ReasonConflict, ErrConflict, "Initial create operation %s was amended to generation %d; reload it before recording convergence", op.ID, current.TargetGeneration)
	}
	op.CancelRequested = op.CancelRequested || current.CancelRequested
	if err = put(ctx, tx, "operation", domain.ClusterRef{Namespace: op.Namespace, Name: op.Cluster}.Key(), op.ID, op, "", "", string(op.Phase), false); err != nil {
		return err
	}
	if err = finalizeOperationRecord(ctx, tx, op); err != nil {
		return err
	}
	return tx.Commit()
}

// UpdateOperationAndClusterState commits the checkpoint together with the
// provider state it references, without overwriting concurrent desired-state
// edits or deletion intent. Neither record is created if it has been removed.
func (s *Store) UpdateOperationAndClusterState(ctx context.Context, op *domain.Operation, c *domain.Cluster) error {
	if err := c.Ref().Validate(); err != nil {
		return err
	}
	if op.Cluster != c.Name || domain.NormalizedNamespace(op.Namespace) != domain.NormalizedNamespace(c.Namespace) {
		return ErrConflict
	}
	tx, err := s.writer.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	op.UpdatedAt = time.Now().UTC()
	var raw []byte
	if err = tx.QueryRowContext(ctx, "SELECT data FROM records WHERE kind='operation' AND key=?", op.ID).Scan(&raw); err != nil {
		return typed(err)
	}
	var current domain.Operation
	if err = json.Unmarshal(raw, &current); err != nil {
		return err
	}
	if current.Kind == domain.OperationCreate && current.TargetGeneration != op.TargetGeneration {
		return domain.Conflict("operation", op.ID, domain.ReasonConflict, ErrConflict, "Initial create operation %s was amended to generation %d; reload it before recording convergence", op.ID, current.TargetGeneration)
	}
	op.CancelRequested = op.CancelRequested || current.CancelRequested
	data, err := json.Marshal(op)
	if err != nil {
		return err
	}
	r, err := tx.ExecContext(ctx, "UPDATE records SET data=?,phase=?,updated=? WHERE kind='operation' AND scope=? AND key=? AND phase='Running'", data, string(op.Phase), op.UpdatedAt.UnixNano(), c.Ref().Key(), op.ID)
	if err = affected(r, err); err != nil {
		return err
	}
	if err = updateClusterState(ctx, tx, c); err != nil {
		return err
	}
	if err = finalizeOperationRecord(ctx, tx, op); err != nil {
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
		if _, tracking := ctx.Value(operationContextKey{}).(*domain.Operation); tracking && domain.IsConvergenceOperation(op.Kind) {
			continue // The accepting mutation names the superseding operation.
		}
		op.Phase = domain.PhaseFailed
		op.Message = message
		op.CompletedAt = &now
		op.UpdatedAt = now
		if err = put(ctx, tx, "operation", cluster, op.ID, op, "", "", string(op.Phase), false); err != nil {
			return err
		}
		if err = finalizeOperationRecord(ctx, tx, op); err != nil {
			return err
		}
	}
	return nil
}

// ClaimOperation resumes Running work before claiming the oldest Pending action.
// Per-cluster ownership is enforced by the reconciler.
func (s *Store) ClaimOperation(ctx context.Context, ref domain.ClusterRef) (*domain.Operation, error) {
	if err := ref.Validate(); err != nil {
		return nil, err
	}
	return s.claimOperation(ctx, ref.Key(), true, "")
}

// ClaimRunningOperation resumes active work without starting a Pending action.
// Reconciliation uses it before ordinary Sync, which active work may exclude.
func (s *Store) ClaimRunningOperation(ctx context.Context, ref domain.ClusterRef) (*domain.Operation, error) {
	if err := ref.Validate(); err != nil {
		return nil, err
	}
	return s.claimOperation(ctx, ref.Key(), false, "")
}

// ClaimBackupDeletion permits retained-object cleanup without starting unrelated
// runtime work while a cluster is suspended.
func (s *Store) ClaimBackupDeletion(ctx context.Context, ref domain.ClusterRef) (*domain.Operation, error) {
	if err := ref.Validate(); err != nil {
		return nil, err
	}
	return s.claimOperation(ctx, ref.Key(), true, domain.OperationDeleteBackup)
}

func (s *Store) claimOperation(ctx context.Context, cluster string, includePending bool, kind string) (*domain.Operation, error) {
	tx, err := s.writer.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	selected, wait, err := maintenanceCandidate(ctx, tx, cluster, includePending, kind)
	if err != nil {
		return nil, err
	}
	if selected == nil {
		if err = tx.Commit(); err != nil {
			return nil, err
		}
		if wait != nil {
			return nil, wait
		}
		return nil, ErrNotFound
	}
	op := *selected
	if (op.Kind == domain.OperationRestore || op.Kind == "initial-restore") && op.Phase != domain.PhaseRunning {
		if err = claimInitialDataSource(ctx, tx, domain.ClusterRef{Namespace: op.Namespace, Name: op.Cluster}); err != nil {
			return nil, err
		}
	}
	// Running work already owns the native lifecycle, including compensation.
	// Do not re-admit it through an initial create tracker that may have failed
	// while cleanup was in progress. Its Running row itself fences amendments.
	if op.Phase == domain.PhaseRunning {
		if _, ok := ctx.Value(clusterSnapshotKey{}).(clusterSnapshot); ok {
			c, err := scanCluster(tx.QueryRowContext(ctx, "SELECT "+clusterColumns+" FROM clusters WHERE namespace=? AND name=?", domain.NormalizedNamespace(op.Namespace), op.Cluster))
			if err != nil {
				return nil, err
			}
			if err = checkClusterSnapshot(ctx, tx, c); err != nil {
				return nil, err
			}
		}
	}
	now := maintenanceTime(ctx)
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
	return s.enqueueBackup(ctx, backup, op, nil)
}

func (s *Store) enqueueBackup(ctx context.Context, backup *domain.Backup, op *domain.Operation, advance *scheduleAdvance) error {
	if err := (domain.ClusterRef{Namespace: backup.Namespace, Name: backup.Cluster}).Validate(); err != nil {
		return err
	}
	if err := prepareOperation(ctx, op); err != nil {
		return err
	}
	backup.OperationID = op.ID
	op.Target = domain.OperationTarget{Kind: "backup", Name: backup.Name}
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
	if backup.Cluster != op.Cluster || domain.NormalizedNamespace(backup.Namespace) != op.Namespace {
		return ErrConflict
	}
	if err = admitClusterOperation(ctx, tx, op); err != nil {
		return err
	}
	if err = advanceSchedule(ctx, tx, domain.ClusterRef{Namespace: op.Namespace, Name: op.Cluster}, advance); err != nil {
		return err
	}
	if err = put(ctx, tx, "backup", domain.ClusterRef{Namespace: backup.Namespace, Name: backup.Cluster}.Key(), backup.Name, backup, "", "", string(backup.Phase), true); err != nil {
		return err
	}
	if err = put(ctx, tx, "operation", domain.ClusterRef{Namespace: op.Namespace, Name: op.Cluster}.Key(), op.ID, op, "", "", string(op.Phase), true); err != nil {
		return err
	}
	return tx.Commit()
}

// EnqueueRestore creates a restore record and its operation in one transaction.
func (s *Store) EnqueueRestore(ctx context.Context, restore *domain.Restore, op *domain.Operation) error {
	return s.EnqueueRestoreWithSecrets(ctx, restore, op, nil)
}

// EnqueueRestoreWithSecrets atomically stages write-only restore inputs with
// the restore record and operation. Inputs are encrypted before the transaction.
func (s *Store) EnqueueRestoreWithSecrets(ctx context.Context, restore *domain.Restore, op *domain.Operation, values map[string][]byte) error {
	if err := (domain.ClusterRef{Namespace: restore.Namespace, Name: restore.Cluster}).Validate(); err != nil {
		return err
	}
	if err := prepareOperation(ctx, op); err != nil {
		return err
	}
	restore.OperationID = op.ID
	op.Target = domain.OperationTarget{Kind: "restore", Name: restore.Name}
	if restore.CreatedAt.IsZero() {
		restore.CreatedAt = time.Now().UTC()
	}
	if restore.Phase == "" {
		restore.Phase = domain.PhasePending
	}
	ref := domain.ClusterRef{Namespace: restore.Namespace, Name: restore.Cluster}
	sealed, err := s.sealClusterValues(ref.Key(), values)
	if err != nil {
		return err
	}
	tx, err := s.writer.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if restore.Cluster != op.Cluster || domain.NormalizedNamespace(restore.Namespace) != op.Namespace {
		return ErrConflict
	}
	if err = admitClusterOperation(ctx, tx, op); err != nil {
		return err
	}
	if c, initial, err := initialDataSourceOperation(ctx, tx, domain.ClusterRef{Namespace: op.Namespace, Name: op.Cluster}); err != nil {
		return err
	} else if initial != nil {
		if !matchesInitialRestore(c, restore) {
			return initialRestoreConflict(initial)
		}
		if _, err := admitInitialNativeContinuation(ctx, tx, c, initial); err != nil {
			return err
		}
	}
	if err = admitRestoreBackup(ctx, tx, domain.ClusterRef{Namespace: restore.Namespace, Name: restore.Cluster}, domain.ClusterRef{Namespace: restore.SourceNamespace, Name: restore.Source.SourceCluster}, restore.Source.Backup, restore.Source.PITRTime != nil); err != nil {
		return err
	}
	if err = guardRestoreBindings(ctx, tx, domain.ClusterRef{Namespace: restore.Namespace, Name: restore.Cluster}); err != nil {
		return err
	}
	if err = put(ctx, tx, "restore", domain.ClusterRef{Namespace: restore.Namespace, Name: restore.Cluster}.Key(), restore.Name, restore, "", "", string(restore.Phase), true); err != nil {
		return err
	}
	if err = put(ctx, tx, "operation", domain.ClusterRef{Namespace: op.Namespace, Name: op.Cluster}.Key(), op.ID, op, "", "", string(op.Phase), true); err != nil {
		return err
	}
	if err = writeClusterValues(ctx, tx, ref.Key(), sealed); err != nil {
		return err
	}
	return tx.Commit()
}

// EnqueueBackupDeletion marks inventory and enqueues deletion atomically.
func (s *Store) EnqueueBackupDeletion(ctx context.Context, backup *domain.Backup, op *domain.Operation) error {
	if err := (domain.ClusterRef{Namespace: backup.Namespace, Name: backup.Cluster}).Validate(); err != nil {
		return err
	}
	if err := prepareOperation(ctx, op); err != nil {
		return err
	}
	tx, err := s.writer.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	ref := domain.ClusterRef{Namespace: backup.Namespace, Name: backup.Cluster}
	if backup.Cluster != op.Cluster || domain.NormalizedNamespace(backup.Namespace) != op.Namespace {
		return ErrConflict
	}
	if _, fenced := ctx.Value(clusterSnapshotKey{}).(clusterSnapshot); fenced {
		current, readErr := scanCluster(tx.QueryRowContext(ctx, "SELECT "+clusterColumns+" FROM clusters WHERE namespace=? AND name=?", domain.NormalizedNamespace(ref.Namespace), ref.Name))
		if readErr != nil {
			return readErr
		}
		if err = checkClusterSnapshot(ctx, tx, current); err != nil {
			return err
		}
	}
	var raw []byte
	if err = tx.QueryRowContext(ctx, "SELECT data FROM records WHERE kind='backup' AND scope=? AND key=?", ref.Key(), backup.Name).Scan(&raw); err != nil {
		return typed(err)
	}
	if err = json.Unmarshal(raw, backup); err != nil {
		return err
	}
	if err = refuseBackupDependents(ctx, tx, ref, backup.Name, false, false); err != nil {
		if conflict, ok := err.(*domain.ResourceError); ok {
			conflict.State = string(backup.Phase)
		}
		return err
	}
	op.Target = domain.OperationTarget{Kind: "backup", Name: backup.Name}
	if backup.PBMName == "" && backup.Location == "" && backup.Postgres == nil && (backup.Phase == domain.PhaseFailed || backup.Phase == domain.PhasePending) {
		var active int
		if err = tx.QueryRowContext(ctx, "SELECT count(*) FROM records WHERE kind='operation' AND scope=? AND phase IN ('Pending','Running') AND json_extract(data,'$.kind')='backup' AND json_extract(data,'$.target.kind')='backup' AND json_extract(data,'$.target.name')=?", ref.Key(), backup.Name).Scan(&active); err != nil {
			return err
		}
		if active != 0 {
			return &domain.ResourceError{Resource: "backup", Name: backup.Name, Reason: domain.ReasonInvalidState, State: string(backup.Phase), Err: ErrConflict, Message: fmt.Sprintf("Backup %s still has active work; cancel its operation before deleting it", backup.Name)}
		}
		r, err := tx.ExecContext(ctx, "DELETE FROM records WHERE kind='backup' AND scope=? AND key=?", ref.Key(), backup.Name)
		if err = affected(r, err); err != nil {
			return err
		}
		if backup.RecoveryIdentity != "" {
			if _, err = tx.ExecContext(ctx, "DELETE FROM records WHERE kind='secret' AND scope=? AND key='users/userAdmin'", domain.ClusterRef{Namespace: backup.Namespace, Name: backup.RecoveryIdentity}.Key()); err != nil {
				return err
			}
		}
		stamp := time.Now().UTC()
		op.Phase, op.Message, op.StartedAt, op.CompletedAt = domain.PhaseSucceeded, "Removed never-started backup record "+backup.Name, &stamp, &stamp
		if err = put(ctx, tx, "operation", ref.Key(), op.ID, op, "", "", string(op.Phase), true); err != nil {
			return err
		}
		return tx.Commit()
	}
	previousPhase := backup.Phase
	backup.Phase = domain.PhaseDeleting
	data, err := json.Marshal(backup)
	if err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, "UPDATE records SET data=?,phase='Deleting',updated=? WHERE kind='backup' AND scope=? AND key=? AND phase IN ('Succeeded','Failed')", data, time.Now().UTC().UnixNano(), ref.Key(), backup.Name)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return &domain.ResourceError{Resource: "backup", Name: backup.Name, Reason: domain.ReasonInvalidState, State: string(previousPhase), Err: ErrConflict, Message: fmt.Sprintf("backup %q cannot be deleted in state %s", backup.Name, previousPhase)}
	}
	if err := put(ctx, tx, "operation", domain.ClusterRef{Namespace: op.Namespace, Name: op.Cluster}.Key(), op.ID, op, "", "", string(op.Phase), true); err != nil {
		return err
	}
	return tx.Commit()
}
