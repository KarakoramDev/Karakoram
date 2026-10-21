// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

func restoreOperation(op *domain.Operation) bool {
	return op.Kind == domain.OperationRestore || op.Kind == "initial-restore"
}

func (*Core) InitialDataSourceRecoveryPending(c *domain.Cluster) (bool, error) {
	s, err := load(c)
	return err == nil && c.Spec.DataSource != nil && s.RestoreRecovery != nil, err
}

// A destructive restore cannot be rolled back by releasing process holds. On
// terminal queue failure, ordinary reconciliation takes over the exact frozen
// operation and its receipt namespace, with no further retry/deadline eviction.
func handoffRestoreRecovery(rc *engine.Context, op *domain.Operation) (engine.OpResult, error) {
	s, err := load(rc.Cluster)
	if err != nil {
		return engine.OpResult{}, err
	}
	var cp backupCheckpoint
	if len(op.Checkpoint) > 0 {
		if err = json.Unmarshal(op.Checkpoint, &cp); err != nil {
			return engine.OpResult{}, err
		}
	}
	checkpoint := op.Checkpoint
	if cp.Step == "" {
		bs, err := loadBackupState(rc.Cluster)
		if err != nil {
			return engine.OpResult{}, err
		}
		if bs.RestoreOperation != op.ID || len(bs.RestoreCheckpoint) == 0 {
			// Rejected source/PITR preflight has not installed even the first
			// safety hold. There is no recovery work to own or queue to fence.
			return engine.OpResult{Done: true}, nil
		}
		checkpoint = bs.RestoreCheckpoint
	}
	if s.RestoreRecovery != nil && s.RestoreRecovery.ID != op.ID {
		return engine.OpResult{}, engine.ErrConflict
	}
	if s.RestoreRecovery == nil {
		copy := *op
		copy.Payload = append(json.RawMessage(nil), op.Payload...)
		copy.Checkpoint = append(json.RawMessage(nil), checkpoint...)
		s.RestoreRecovery = &copy
	}
	if err = save(rc.Cluster, s); err != nil {
		return engine.OpResult{}, err
	}
	return engine.OpResult{Done: true, Step: "restore-recovery", Checkpoint: s.RestoreRecovery.Checkpoint}, nil
}

func (p *Core) reconcileRestoreRecovery(ctx context.Context, rc *engine.Context, s *state) (bool, error) {
	op := s.RestoreRecovery
	if op == nil {
		return false, nil
	}
	result, runErr := p.runBackupOperation(ctx, rc, op)
	// Restore steps persist backup lineage and member state themselves. Keep those
	// writes instead of overwriting them with Sync's entry snapshot on return.
	current, err := load(rc.Cluster)
	if err != nil {
		return true, err
	}
	*s = *current
	if len(result.Checkpoint) > 0 {
		op.Checkpoint = result.Checkpoint
		op.Step = result.Step
	}
	s.RestoreRecovery = op
	if runErr != nil {
		if op.Kind == domain.OperationBackup {
			op.Message = fmt.Sprintf("Coordinated backup abort %s: %v; formation remains fenced until original native receipts are terminal", op.ID, runErr)
			return true, fmt.Errorf("%s", op.Message)
		}
		op.Message = fmt.Sprintf("Restore recovery %s at %s: %v; restore the unavailable dependency to resume the same backup and generation", op.ID, result.Step, runErr)
		return true, fmt.Errorf("%s", op.Message)
	}
	if result.Done {
		s.RestoreRecovery = nil
	}
	return true, nil
}
