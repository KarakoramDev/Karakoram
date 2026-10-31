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
func (p *Core) CancelOperation(ctx context.Context, rc *engine.Context, op *domain.Operation) (engine.OpResult, error) {
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
		// A source-resolution failure precedes handoff and owns no controls.
		// Complete only that new request's failure cleanup; the qualified old
		// blocked owner and its exact checkpoint remain untouched.
		if op.Kind == domain.OperationRestore && op.FailureCleanupStartedAt != nil {
			owner, err := terminalBlockedRestore(ctx, rc, bs)
			if err != nil {
				return engine.OpResult{}, err
			}
			if owner != nil {
				var unowned backupCheckpoint
				if len(op.Checkpoint) > 0 {
					if err := json.Unmarshal(op.Checkpoint, &unowned); err != nil {
						return engine.OpResult{}, err
					}
				}
				queued, err := rc.Store.GetOperation(ctx, op.ID)
				if err != nil {
					return engine.OpResult{}, err
				}
				matching := queued.Kind == domain.OperationRestore && queued.Cluster == rc.Cluster.Name &&
					domain.NormalizedNamespace(queued.Namespace) == domain.NormalizedNamespace(rc.Cluster.Namespace) &&
					(queued.Phase == domain.PhaseRunning || queued.Phase == domain.PhaseFailed || queued.Phase == domain.PhaseCancelled)
				untouched := unowned.Step == "" && unowned.Member == "" && unowned.Request == nil && unowned.Generation == 0 &&
					!unowned.Preflighted && unowned.PlanDigest == "" && unowned.RecheckID == "" &&
					(unowned.StopBoundary == "" || unowned.StopBoundary == restoreStopUntouched) &&
					len(unowned.RecoveredUsers) == 0 && unowned.GTIDCertification == nil && unowned.PreviousBlockedRestore == nil
				if matching && untouched {
					return engine.OpResult{Done: true, Step: "restore-preflight-aborted"}, nil
				}
			}
		}
		return engine.OpResult{}, engine.ErrConflict
	}
	var cp backupCheckpoint
	if err := json.Unmarshal(bs.RestoreOperation.Checkpoint, &cp); err != nil {
		return engine.OpResult{}, err
	}
	s, err := load(rc.Cluster)
	if err != nil {
		return engine.OpResult{}, err
	}
	certification := cp.GTIDCertification
	owned := s.RestoreGTIDFence == op.ID || certification != nil && (certification.FencesOwned || certification.CleanupPending || certification.RecoveryCleanupPending || certification.LegacyCleanupPending)
	if owned {
		// A negotiation checkpoint owns no controls. Before actual certification
		// starts, retain the native restore checkpoint and its recovery ownership.
		certifying := certification != nil && certification.Phase != "negotiated" && (cp.Step == "certify" || cp.Step == "certify-cleanup" || cp.Step == "certify-blocked")
		if certifying && !certification.Certified && !certification.Legacy {
			s.RestoreGTIDBlocked = "PXC restore GTID certification interrupted; preserve evidence and repair from the restored seed under operator control before a new full backup; never reset or inject GTIDs"
			if err = save(rc.Cluster, s); err != nil {
				return engine.OpResult{}, err
			}
			if err = completeRestoreGTIDCleanup(ctx, rc, s, op, &cp); err != nil {
				return engine.OpResult{}, err
			}
		} else if err = completeRestoreGTIDRecoveryCleanup(ctx, rc, s, op, &cp); err != nil {
			return engine.OpResult{}, err
		}
		// persistRestore replaces the saved operation checkpoint.
		bs, err = loadBackupState(rc.Cluster)
		if err != nil {
			return engine.OpResult{}, err
		}
	}
	// An untouched failed remedy must restore its prior blocked owner, not
	// acknowledge unresolved certification. Ordinary preflight owners can be
	// released; the durable certification latch still gates all other work.
	untouched := cp.Step == "preflight" || cp.Step == "preflight-poll" || cp.Step == "fence" || cp.Step == "fence-recheck" || cp.Step == "fence-recheck-poll"
	if untouched && op.FailureCleanupStartedAt != nil {
		s, err := load(rc.Cluster)
		if err != nil {
			return engine.OpResult{}, err
		}
		switch cp.StopBoundary {
		case restoreStopUntouched:
			// Existing Offline/stop holds may belong to the prior failed
			// certification. This restore has not acquired its own stop.
		case restoreStopStarted:
			untouched = false
		case "":
			for _, m := range s.data() {
				untouched = untouched && !m.Offline
			}
		default:
			return engine.OpResult{}, engine.Invalid("restore checkpoint has an unknown stop boundary")
		}
		if untouched {
			if cp.PreviousBlockedRestore != nil {
				previous := bs
				previous.RestoreOperation = cp.PreviousBlockedRestore
				owner, err := terminalBlockedRestore(ctx, rc, previous)
				if err != nil {
					return engine.OpResult{}, err
				}
				if owner == nil {
					return engine.OpResult{}, engine.ErrConflict
				}
				bs.RestoreOperation = owner
			} else {
				bs.RestoreOperation = nil
			}
			invalidateMySQLReadObservation(ctx, rc)
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

// Terminal hold release is independent of journal availability, audit delivery
// and fresh history comparison. Run it before ordinary Sync observations.
func (p *Core) reconcilePendingRestoreGTIDCleanup(ctx context.Context, rc *engine.Context, s *state) (bool, error) {
	if rc.Cluster.Spec.Provider != "pxc" {
		return false, nil
	}
	bs, err := loadBackupState(rc.Cluster)
	if err != nil {
		return true, err
	}
	if bs.RestoreOperation == nil {
		return false, nil
	}
	var cp backupCheckpoint
	if err = json.Unmarshal(bs.RestoreOperation.Checkpoint, &cp); err != nil {
		return true, err
	}
	pending := cp.Step == "certify-cleanup" || cp.GTIDCertification != nil && (cp.GTIDCertification.LegacyCleanupPending || cp.GTIDCertification.RecoveryCleanupPending) || cp.Step == "certify-blocked" && (cp.GTIDCertification == nil || cp.GTIDCertification.CleanupPending || !cp.GTIDCertification.CleanupComplete)
	if !pending {
		return false, nil
	}
	_, err = p.runRestore(ctx, rc, bs.RestoreOperation, cp)
	current, loadErr := load(rc.Cluster)
	if loadErr != nil {
		return true, loadErr
	}
	*s = *current
	return true, err
}
