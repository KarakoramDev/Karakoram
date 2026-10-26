// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package pbm

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/redact"
)

// BackupFailureCleanupTimeout bounds read-only recovery after initiation retries
// are exhausted. Observed execution time survives restart; offline time and wall
// clock corrections cannot turn an active native backup into a presumed failure.
const BackupFailureCleanupTimeout = 2 * time.Minute

// PrepareBackupFailureCleanup runs before runtime/storage resolution so missing
// dependencies cannot keep the operation Running for the generic six-hour window.
func PrepareBackupFailureCleanup(ctx context.Context, rc *engine.Context, op *domain.Operation) (engine.OpResult, error) {
	cp, err := loadCheckpoint(op)
	if err != nil {
		return engine.OpResult{}, err
	}
	if cp.Step == "done" {
		return result(cp, false), nil
	}
	if cp.BackupCleanupStartedElapsed == nil {
		elapsed := op.FailureCleanupElapsed
		cp.BackupCleanupStartedElapsed = &elapsed
	}
	if op.FailureCleanupElapsed-*cp.BackupCleanupStartedElapsed >= BackupFailureCleanupTimeout {
		return failBackupObservation(ctx, Env{RC: rc}, op, cp, "native outcome could not be confirmed within the two-minute failure-cleanup observation budget")
	}
	return result(cp, false), nil
}

func failBackupObservation(ctx context.Context, env Env, op *domain.Operation, cp checkpoint, reason string) (engine.OpResult, error) {
	name, err := payloadName(op, "backup")
	if err != nil {
		return result(cp, false), err
	}
	b, err := env.RC.Store.GetBackup(ctx, env.RC.Cluster.Ref(), name)
	if err != nil {
		return result(cp, false), err
	}
	cp.Failure = redact.NativeError(fmt.Sprintf("%s; %s; native backup %q, candidate unmanaged artifacts %v. Operator action required: inspect PBM status/list/describe-backup and native workers, reconcile the matching snapshot before retrying or deleting artifacts; no native cancel, deletion, restart, or configuration change was issued by failure cleanup", op.LastError, reason, cp.PBMName, cp.BackupCleanupCandidates))
	// A proven successful snapshot remains a usable, managed backup even if later
	// recovery dependencies fail. Never demote positive native completion evidence.
	if b.Phase != domain.PhaseSucceeded {
		stamp := now(env)
		b.Phase, b.Error, b.CompletedAt = domain.PhaseFailed, cp.Failure, &stamp
		if err = env.RC.Store.UpdateBackup(ctx, b); err != nil {
			return result(cp, false), err
		}
	}
	cp.Step = "done"
	res := result(cp, true)
	res.Message = cp.Failure
	return res, errors.New(cp.Failure)
}

// runBackupFailureCleanup never replays an uncertain start receipt. A failed
// CLI invocation does not prove that PBM rejected its already-submitted command.
// The per-cluster imperative queue remains owned throughout attribution/polling.
func runBackupFailureCleanup(ctx context.Context, env Env, op *domain.Operation) (engine.OpResult, error) {
	prepared, err := PrepareBackupFailureCleanup(ctx, env.RC, op)
	if err != nil || prepared.Done {
		return prepared, err
	}
	copyOp := *op
	copyOp.Checkpoint = prepared.Checkpoint
	cp, err := loadCheckpoint(&copyOp)
	if err != nil {
		return prepared, err
	}
	name, err := payloadName(op, "backup")
	if err != nil {
		return result(cp, false), err
	}
	b, err := env.RC.Store.GetBackup(ctx, env.RC.Cluster.Ref(), name)
	if err != nil {
		return result(cp, false), err
	}
	if cp.PBMName == "" {
		cp.PBMName = b.PBMName
	}
	if cp.Step == "done" {
		res := result(cp, true)
		res.Recovered = b.Phase == domain.PhaseSucceeded && cp.Failure == ""
		res.Message = cp.Failure
		return res, nil
	}
	if cp.PBMName == "" {
		// The window only identifies artifacts to report. Compatibility and
		// serialization cannot prove that an external PBM backup is ours.
		if cp.Step != "start" || op.StartedAt == nil || op.FailureCleanupStartedAt == nil {
			return failBackupObservation(ctx, env, op, cp, "no attributable native initiation window")
		}
		inv, e := inventory(ctx, env)
		if e != nil {
			return result(cp, false), e
		}
		cp.BackupCleanupCandidates = nil
		for _, s := range inv.Snapshots {
			started, e := time.Parse(time.RFC3339, s.Name)
			if s.Start != nil {
				started = *s.Start
				e = nil
			}
			if e != nil || started.Before(op.StartedAt.Truncate(time.Second)) || started.After(*op.FailureCleanupStartedAt) {
				continue
			}
			cp.BackupCleanupCandidates = append(cp.BackupCleanupCandidates, s.Name)
		}
		sort.Strings(cp.BackupCleanupCandidates)
		return failBackupObservation(ctx, env, op, cp, "no operation-specific native identity was returned by our initiation receipt; inventory candidates cannot establish ownership")
	}
	out, err := command(ctx, env, "", "", "describe-backup", nil, cp.PBMName, "--out=json")
	if err != nil {
		return result(cp, false), err
	}
	var observed snapshot
	if err = decodeLast(out, &observed); err != nil {
		return result(cp, false), err
	}
	if observed.Name != cp.PBMName || observed.Type != b.Type || len(observed.Namespaces) != 0 {
		return failBackupObservation(ctx, env, op, cp, "native description does not match the receipt-attributed name, backup type, or full-cluster namespace scope")
	}
	applySnapshot(b, observed, now(env))
	if err = env.RC.Store.UpdateBackup(ctx, b); err != nil {
		return result(cp, false), err
	}
	if b.Phase != domain.PhaseSucceeded && b.Phase != domain.PhaseFailed {
		return result(cp, false), nil
	}
	cp.Step = "done"
	cp.Failure = b.Error
	res := result(cp, true)
	res.Recovered = b.Phase == domain.PhaseSucceeded
	if !res.Recovered {
		res.Message = cp.Failure
	}
	return res, nil
}
