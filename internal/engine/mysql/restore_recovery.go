// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"encoding/json"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
)

func secretsHash(key string) string { return secrets.Hash(key)[:24] }
func restoreOperation(op *domain.Operation) bool {
	return op.Kind == domain.OperationRestore || op.Kind == "initial-restore"
}
func (*Core) CanCancelOperation(_ context.Context, op *domain.Operation) error {
	if op.Kind == domain.OperationReplaceMember {
		return nil
	}
	if restoreOperation(op) {
		return engine.Invalid("destructive MySQL restore must finish under recovery ownership")
	}
	return engine.ErrNotSupported
}

// CancelOperation is also invoked for terminal queue failure. The saved source,
// group UUID and receipt remain owned by reconciliation rather than un-fencing
// partially restored data or choosing another source after a crash.
func (p *Core) CancelOperation(_ context.Context, rc *engine.Context, op *domain.Operation) (engine.OpResult, error) {
	if op.Kind == domain.OperationReplaceMember {
		return cancelReplacement(rc, op)
	}
	if !restoreOperation(op) {
		return engine.OpResult{}, engine.ErrNotSupported
	}
	bs, err := loadBackupState(rc.Cluster)
	if err != nil {
		return engine.OpResult{}, err
	}
	if bs.RestoreOperation == nil {
		return engine.OpResult{Done: true}, nil
	}
	if bs.RestoreOperation.ID != op.ID {
		return engine.OpResult{}, engine.ErrConflict
	}
	var cp backupCheckpoint
	if err := json.Unmarshal(bs.RestoreOperation.Checkpoint, &cp); err != nil {
		return engine.OpResult{}, err
	}
	// Failure before the first fence has no data recovery ownership to retain.
	// Releasing it permits a new request using a fresh, provable full baseline.
	untouched := cp.Step == "preflight" || cp.Step == "preflight-poll" || cp.Step == "fence" || cp.Step == "fence-recheck" || cp.Step == "fence-recheck-poll"
	if untouched && op.FailureCleanupStartedAt != nil {
		s, err := load(rc.Cluster)
		if err != nil {
			return engine.OpResult{}, err
		}
		for _, m := range s.data() {
			untouched = untouched && !m.Offline
		}
		if untouched {
			bs.RestoreOperation = nil
			if err := saveBackupState(rc.Cluster, bs); err != nil {
				return engine.OpResult{}, err
			}
			return engine.OpResult{Done: true, Step: "restore-preflight-aborted"}, nil
		}
	}
	return engine.OpResult{Done: true, Step: "restore-recovery", Checkpoint: bs.RestoreOperation.Checkpoint}, nil
}
func (p *Core) reconcileRestoreRecovery(ctx context.Context, rc *engine.Context, s *state) (bool, error) {
	bs, err := loadBackupState(rc.Cluster)
	if err != nil {
		return true, err
	}
	if bs.RestoreOperation == nil {
		return false, nil
	}
	_, err = p.runBackupOperation(ctx, rc, bs.RestoreOperation)
	current, e := load(rc.Cluster)
	if e != nil {
		return true, e
	}
	*s = *current
	return true, err
}
func (p *Core) syncInitialRestore(ctx context.Context, rc *engine.Context, s *state) (bool, error) {
	if rc.Cluster.Spec.DataSource == nil {
		return false, nil
	}
	bs, err := loadBackupState(rc.Cluster)
	if err != nil {
		return true, err
	}
	if bs.CloneComplete {
		return false, nil
	}
	for _, m := range s.data() {
		if m.Stage != "deployed" && m.Stage != "joined" {
			return false, nil
		}
	}
	op := &domain.Operation{ID: "initial-restore-" + s.UID, Namespace: rc.Cluster.Ref().Namespace, Cluster: rc.Cluster.Name, Kind: "initial-restore", Payload: json.RawMessage(`{}`)}
	_, err = p.runBackupOperation(ctx, rc, op)
	current, e := load(rc.Cluster)
	if e != nil {
		return true, e
	}
	*s = *current
	return true, err
}

// Sync is excluded during queued operations. Their checkpoints alone own holds;
// after terminal failure the durable backup state transfers ownership to Sync.
func (*Core) SyncDuringOperation(op *domain.Operation) bool {
	if op.Kind != domain.OperationReplaceMember {
		return false
	}
	var cp replacementState
	return json.Unmarshal(op.Checkpoint, &cp) == nil && cp.Stage == "join"
}
func (*Core) SupportsFailureCleanup(op *domain.Operation) bool {
	return restoreOperation(op) || op.Kind == domain.OperationReplaceMember
}
