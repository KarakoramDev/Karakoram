// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package store

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

type operationContextKey struct{}

// WithOperation attaches a desired-state tracking record to the mutation's transaction.
// Only cluster mutations consume it; inventory operations have their own atomic enqueue.
func WithOperation(ctx context.Context, op *domain.Operation) context.Context {
	return context.WithValue(ctx, operationContextKey{}, op)
}

func acceptClusterOperation(ctx context.Context, tx *sql.Tx, ref domain.ClusterRef) error {
	if initial, ok := ctx.Value(initialAmendmentKey{}).(*domain.Operation); ok {
		if err := tx.QueryRowContext(ctx, "SELECT generation FROM clusters WHERE namespace=? AND name=?", domain.NormalizedNamespace(ref.Namespace), ref.Name).Scan(&initial.TargetGeneration); err != nil {
			return err
		}
		initial.UpdatedAt = time.Now().UTC()
		initial.Message = "Initial restore amended before native claim: placement/resource/replica settings updated"
		if err := put(ctx, tx, "operation", ref.Key(), initial.ID, initial, "", "", string(initial.Phase), false); err != nil {
			return err
		}
		if requested, ok := ctx.Value(operationContextKey{}).(*domain.Operation); ok && requested != nil {
			*requested = *initial
		}
		return appendEvent(ctx, tx, &domain.Event{Namespace: domain.NormalizedNamespace(ref.Namespace), Cluster: ref.Name, Operation: initial.ID, Type: "Normal", Reason: "InitialRestoreAmended", Message: initial.Message})
	}
	op, _ := ctx.Value(operationContextKey{}).(*domain.Operation)
	if op == nil {
		return nil
	}
	if op.Kind != domain.OperationCreate && op.Kind != domain.OperationDelete {
		_, initial, err := initialDataSourceOperation(ctx, tx, ref)
		if err != nil {
			return err
		}
		if initial != nil {
			return initialRestoreConflict(initial)
		}
	}
	op.Namespace, op.Cluster = domain.NormalizedNamespace(ref.Namespace), ref.Name
	op.Target = domain.OperationTarget{Kind: "cluster", Name: ref.Name}
	if err := prepareOperation(ctx, op); err != nil {
		return err
	}
	var created int64
	if err := tx.QueryRowContext(ctx, "SELECT generation,created FROM clusters WHERE namespace=? AND name=?", op.Namespace, ref.Name).Scan(&op.TargetGeneration, &created); err != nil {
		return typed(err)
	}
	op.ClusterIncarnation = clusterIncarnation(created)
	rows, err := tx.QueryContext(ctx, "SELECT data FROM records WHERE kind='operation' AND scope=? AND phase IN ('Pending','Running')", ref.Key())
	if err != nil {
		return err
	}
	var previous []domain.Operation
	for rows.Next() {
		var data []byte
		var old domain.Operation
		if err = rows.Scan(&data); err == nil {
			err = json.Unmarshal(data, &old)
		}
		if err != nil {
			rows.Close()
			return err
		}
		if domain.IsConvergenceOperation(old.Kind) {
			previous = append(previous, old)
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for i := range previous {
		old := &previous[i]
		old.Phase, old.Message = domain.PhaseCancelled, fmt.Sprintf("Superseded by operation %s (%s)", op.ID, op.Kind)
		old.CompletedAt, old.UpdatedAt = &op.CreatedAt, op.CreatedAt
		if err = put(ctx, tx, "operation", ref.Key(), old.ID, old, "", "", string(old.Phase), false); err != nil {
			return err
		}
	}
	return put(ctx, tx, "operation", ref.Key(), op.ID, op, "", "", string(op.Phase), true)
}

func finalizeOperationRecord(ctx context.Context, tx *sql.Tx, op *domain.Operation) error {
	if op.Phase != domain.PhaseFailed && op.Phase != domain.PhaseCancelled {
		return nil
	}
	kind, field := "", ""
	switch op.Kind {
	case domain.OperationBackup, domain.OperationDeleteBackup:
		kind, field = "backup", "backup"
	case domain.OperationRestore:
		kind, field = "restore", "restore"
	default:
		return nil
	}
	name := op.Target.Name
	if op.Target.Kind != kind || name == "" {
		if len(op.Payload) == 0 {
			return nil
		}
		var payload map[string]string
		if err := json.Unmarshal(op.Payload, &payload); err != nil {
			return err
		}
		name = payload[field]
	}
	var raw []byte
	err := tx.QueryRowContext(ctx, "SELECT data FROM records WHERE kind=? AND scope=? AND key=?", kind, domain.ClusterRef{Namespace: op.Namespace, Name: op.Cluster}.Key(), name).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if kind == "backup" {
		var b domain.Backup
		if err = json.Unmarshal(raw, &b); err != nil {
			return err
		}
		if op.Kind == domain.OperationDeleteBackup {
			if b.Phase != domain.PhaseDeleting {
				return nil
			}
			// Keep the immutable remote identity for an idempotent retry. A
			// failed deletion must not become an apparently successful backup.
			b.Phase, b.Error = domain.PhaseFailed, "Backup deletion did not complete: "+op.Message
			return put(ctx, tx, kind, domain.ClusterRef{Namespace: op.Namespace, Name: op.Cluster}.Key(), name, &b, "", "", string(b.Phase), false)
		}
		if b.Phase == domain.PhaseSucceeded {
			return nil
		}
		b.Phase, b.Error, b.CompletedAt = domain.PhaseFailed, op.Message, op.CompletedAt
		return put(ctx, tx, kind, domain.ClusterRef{Namespace: op.Namespace, Name: op.Cluster}.Key(), name, &b, "", "", string(b.Phase), false)
	}
	var r domain.Restore
	if err = json.Unmarshal(raw, &r); err != nil {
		return err
	}
	if r.Phase == domain.PhaseSucceeded {
		return nil
	}
	r.Phase, r.Error, r.CompletedAt = domain.PhaseFailed, op.Message, op.CompletedAt
	return put(ctx, tx, kind, domain.ClusterRef{Namespace: op.Namespace, Name: op.Cluster}.Key(), name, &r, "", "", string(r.Phase), false)
}

// RequestOperationCancellation uses compare-and-swap: a concurrent completion
// cannot be overwritten, and a provider checkpoint cannot lose the request.
func (s *Store) RequestOperationCancellation(ctx context.Context, id string, expected ...*domain.Operation) (*domain.Operation, error) {
	tx, err := s.writer.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	var data []byte
	if err = tx.QueryRowContext(ctx, "SELECT data FROM records WHERE kind='operation' AND key=?", id).Scan(&data); err != nil {
		return nil, typed(err)
	}
	var op domain.Operation
	if err = json.Unmarshal(data, &op); err != nil {
		return nil, err
	}
	if len(expected) != 0 && (op.Phase != expected[0].Phase || !bytes.Equal(op.Checkpoint, expected[0].Checkpoint)) {
		return nil, domain.Conflict("operation", id, domain.ReasonInvalidState, ErrConflict, "Operation %s advanced to another checkpoint; inspect it before retrying cancellation", id)
	}
	if op.Terminal() {
		return nil, domain.Conflict("operation", id, domain.ReasonInvalidState, ErrConflict, "Operation %s is %s and cannot be cancelled", id, op.Phase)
	}
	op.CancelRequested = true
	op.UpdatedAt = time.Now().UTC()
	if op.Phase == domain.PhasePending {
		op.Phase, op.Message = domain.PhaseCancelled, "Cancelled before execution; accepted desired state is not rolled back"
		op.CompletedAt = &op.UpdatedAt
		if err = finalizeOperationRecord(ctx, tx, &op); err != nil {
			return nil, err
		}
	}
	if err = put(ctx, tx, "operation", domain.ClusterRef{Namespace: op.Namespace, Name: op.Cluster}.Key(), id, &op, "", "", string(op.Phase), false); err != nil {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return &op, nil
}

// completeDeletionOperations runs atomically with suspension/removal.
func completeDeletionOperations(ctx context.Context, tx *sql.Tx, ref domain.ClusterRef, suspended bool, message string) error {
	var created, generation int64
	if err := tx.QueryRowContext(ctx, "SELECT created,generation FROM clusters WHERE namespace=? AND name=?", domain.NormalizedNamespace(ref.Namespace), ref.Name).Scan(&created, &generation); err != nil {
		return typed(err)
	}
	incarnation := clusterIncarnation(created)
	rows, err := tx.QueryContext(ctx, "SELECT data FROM records WHERE kind='operation' AND scope=? AND phase IN ('Pending','Running','Failed')", ref.Key())
	if err != nil {
		return err
	}
	var ops []domain.Operation
	for rows.Next() {
		var data []byte
		var op domain.Operation
		if err = rows.Scan(&data); err == nil {
			err = json.Unmarshal(data, &op)
		}
		if err != nil {
			rows.Close()
			return err
		}
		matchingGeneration := op.TargetGeneration == generation || (!suspended && op.TargetGeneration < generation)
		matchingIncarnation := op.ClusterIncarnation == incarnation
		// Actual removal also proves older, unidentified legacy targets absent;
		// suspension alone cannot establish which incarnation they belonged to.
		if op.Kind == domain.OperationDelete && (matchingIncarnation || (op.ClusterIncarnation == "" && !suspended)) && matchingGeneration {
			ops = append(ops, op)
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	for i := range ops {
		op := &ops[i]
		op.Phase, op.Message, op.CompletedAt, op.UpdatedAt = domain.PhaseSucceeded, message, &now, now
		if op.StartedAt == nil {
			op.StartedAt = &now
		}
		if err = put(ctx, tx, "operation", ref.Key(), op.ID, op, "", "", string(op.Phase), false); err != nil {
			return err
		}
	}
	return nil
}
