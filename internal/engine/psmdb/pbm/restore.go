// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package pbm

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/redact"
)

const restoreMetadataGrace = time.Minute

func restoreMetadataPending(err error) bool {
	var failure *commandFailure
	if !errors.As(err, &failure) || failure.command != "describe-restore" || failure.exitCode != 1 {
		return false
	}
	// PBM can print warnings before its final error. Classify only the last
	// nonempty stderr line, never stdout or a substring of a different error.
	line := strings.TrimSpace(failure.stderr)
	if index := strings.LastIndexByte(line, '\n'); index >= 0 {
		line = strings.TrimSpace(line[index+1:])
	}
	return line == "Error: get restore meta: not found"
}

// RunRestore resumes logical, PITR, physical and clone restores. Every member lifecycle change advances a durable index.
// Authentication loss is not evidence of an absent native restore. In particular,
// oplog replay can replace the account PBM preserved while loading the snapshot.
// PITR uses a target-only authenticating identity that source managed-user
// oplog updates cannot replace. Unobservable native outcomes fail closed.
// After success with PITR desired, the provider must enqueue a fresh base snapshot before applying enabled PITR configuration.
func RunRestore(ctx context.Context, env Env, op *domain.Operation) (engine.OpResult, error) {
	cp, err := loadCheckpoint(op)
	if err != nil {
		return engine.OpResult{}, err
	}
	if op.Step == "failure-cleanup" && cp.Step != "done" {
		prepared, prepareErr := PrepareRestoreFailureCleanup(ctx, env.RC, op)
		if prepared.Done || prepareErr != nil {
			return prepared, prepareErr
		}
		cp, err = loadCheckpoint(&domain.Operation{Checkpoint: prepared.Checkpoint})
		if err != nil {
			return prepared, err
		}
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, cp.RestoreCleanupDeadline.Sub(now(env)))
		defer cancel()
	}
	copyOp := *op
	progress := result(cp, false)
	copyOp.Checkpoint = progress.Checkpoint
	progress, err = runRestore(ctx, env, &copyOp)
	if err == nil || progress.Done {
		return progress, err
	}
	updated, decodeErr := loadCheckpoint(&domain.Operation{Checkpoint: progress.Checkpoint})
	if decodeErr != nil {
		return progress, errors.Join(err, decodeErr)
	}
	if restoreAuthenticationFailure(err) && (updated.PBMName != "" || op.Step == "failure-cleanup") {
		return failRestoreObservation(ctx, env, op, updated, err.Error())
	}
	return progress, err
}

type pitrIdentityHooks interface {
	PreparePITRIdentity(context.Context, string) (bool, error)
	FinishPITRIdentity(context.Context, string) (bool, error)
}

// RestoreWorkerObserver requires positive evidence of an abandoned execution
// owner, not just old metadata or a slow collection. Observation failures never
// authorize re-initiation or turn an uncertain native result into an abort.
type RestoreWorkerObserver interface {
	ObserveRestoreWorkerFailure(context.Context, string, Env, map[string]RestoreWorkerIdentity) (string, bool, error)
}

// RestoreWorkerIdentity is a supervisor-scoped launch receipt, not wall time.
// Keys bind this receipt to the native command ID and lock-owner member.
type RestoreWorkerIdentity struct {
	Session string `json:"session"`
	Launch  uint64 `json:"launch"`
}

// PrepareRestoreFailureCleanup runs before resolving storage or runtime state:
// those dependencies must not prevent an expired restore from failing closed.
func PrepareRestoreFailureCleanup(ctx context.Context, rc *engine.Context, op *domain.Operation) (engine.OpResult, error) {
	cp, err := loadCheckpoint(op)
	if err != nil {
		return engine.OpResult{}, err
	}
	env := Env{RC: rc}
	if op.Step != "failure-cleanup" || cp.Step == "done" {
		return result(cp, false), nil
	}
	if cp.RestoreCleanupDeadline == nil {
		started := now(env)
		if op.FailureCleanupStartedAt != nil {
			started = *op.FailureCleanupStartedAt
		}
		deadline := started.Add(time.Minute)
		cp.RestoreCleanupDeadline = &deadline
	}
	if !now(env).Before(*cp.RestoreCleanupDeadline) {
		reason := op.LastError
		if reason == "" {
			reason = cp.Failure
		}
		return failRestoreObservation(ctx, env, op, cp, reason+"; restore failure cleanup exceeded its one-minute deadline")
	}
	return result(cp, false), nil
}

func restoreAuthenticationFailure(err error) bool {
	var failure *commandFailure
	if !errors.As(err, &failure) {
		return false
	}
	// PBM --out=json reports connection errors on stdout, not just stderr.
	// The driver's SASL wrapper also covers EOF, resets and deadlines; only
	// positive server rejection evidence permits permanent auth-failure handling.
	return strings.Contains(failure.message, "(AuthenticationFailed)")
}

func failRestoreObservation(ctx context.Context, env Env, op *domain.Operation, cp checkpoint, reason string) (engine.OpResult, error) {
	if op.LastError != "" && !strings.Contains(reason, op.LastError) {
		reason = op.LastError + "; recovery observation failed: " + reason
	}
	if cp.Failure != "" && !strings.Contains(reason, cp.Failure) {
		reason = cp.Failure + "; recovery observation failed: " + reason
	}
	reason = redact.NativeError(reason)
	cp.Failure = fmt.Sprintf("PBM restore %s: %s; native outcome and partial changes cannot be excluded; no automatic re-initiation. Operator action required: quiesce writers, verify native workers have stopped, and explicitly restore with a fresh name into a fresh cluster or delete this target; the target remains Degraded pending operator recovery.", cp.PBMName, reason)
	cp.RestoreConsistencyUncertain = true
	name, err := payloadName(op, "restore")
	if err != nil {
		return result(cp, false), err
	}
	r, err := env.RC.Store.GetRestore(ctx, env.RC.Cluster.Ref(), name)
	if err != nil {
		return result(cp, false), err
	}
	stamp := now(env)
	r.Phase, r.Error, r.CompletedAt = domain.PhaseFailed, cp.Failure, &stamp
	if err = env.RC.Store.UpdateRestore(ctx, r); err != nil {
		return result(cp, false), err
	}
	cp.Step, cp.FreshBase = "done", false
	return result(cp, true), errors.New(cp.Failure)
}

func runRestore(ctx context.Context, env Env, op *domain.Operation) (engine.OpResult, error) {
	cp, err := loadCheckpoint(op)
	if err != nil {
		return engine.OpResult{}, err
	}
	// A complete fresh observation must renew any previous absence proof.
	// Even failure to reload its recorded source/configuration breaks the window.
	var absentSince *time.Time
	if cp.Step == "poll" {
		absentSince = cp.RestoreAbsentSince
		cp.RestoreAbsentSince = nil
	}
	if cp.Step == "verify-fence" {
		// No receipt validation may survive an interrupted observation,
		// including a legacy persisted per-member verification cursor.
		cp.Index = 0
	}
	name, err := payloadName(op, "restore")
	if err != nil {
		return result(cp, false), err
	}
	r, err := env.RC.Store.GetRestore(ctx, env.RC.Cluster.Ref(), name)
	if err != nil {
		return result(cp, false), err
	}
	if migrated, migrateErr := MigrateRestoreCheckpoint(op, r); migrateErr != nil {
		return result(cp, false), migrateErr
	} else if migrated != nil {
		if err := json.Unmarshal(migrated, &cp); err != nil {
			return result(cp, false), err
		}
	}
	sourceRef := domain.ClusterRef{Namespace: r.SourceNamespace, Name: r.Source.SourceCluster}
	if sourceRef.Namespace == "" {
		sourceRef.Namespace = env.RC.Cluster.Ref().Namespace
	}
	if sourceRef.Name == "" {
		sourceRef.Name = env.RC.Cluster.Name
	}
	if cp.SourceNamespace == "" {
		cp.SourceNamespace = sourceRef.Namespace
	}
	if cp.Step == "done" {
		if cp.Failure != "" {
			return result(cp, true), errors.New(cp.Failure)
		}
		return result(cp, true), nil
	}
	active := env
	active.PITR.Enabled = false
	if cp.Prefix != "" {
		active, err = recordedEnv(ctx, env, cp.SourceNamespace, cp.Storage, cp.Prefix)
	} else if cp.Source != "" {
		active, err = sourceEnv(ctx, env, domain.ClusterRef{Namespace: cp.SourceNamespace, Name: cp.Source}, cp.Storage)
	}
	if err != nil {
		return result(cp, false), err
	}
	if !cp.Physical && (cp.Step == "start" || cp.Step == "poll") && !cp.PBMGoMemoryBudgetObserved {
		cp.PBMGoMemoryBudgetUnknown = !observeGoMemoryBudget(ctx, env)
		cp.PBMGoMemoryBudgetObserved = true
	}
	switch cp.Step {
	case "":
		if env.Hooks == nil {
			return result(cp, false), errors.New("restore requires system-user reset hooks")
		}
		if sourceRef.Key() != env.RC.Cluster.Ref().Key() {
			cp.Source = sourceRef.Name
		}
		if (r.Source.Backup == "") == (r.Source.PITRTime == nil) {
			return result(cp, false), errors.New("restore requires exactly one backup or PITR time")
		}
		cp.TargetTime = r.Source.PITRTime
		if cp.TargetTime != nil && cp.TargetTime.After(now(env)) {
			return rejectRestore(ctx, env, r, cp, ValidatePITRTarget(nil, *cp.TargetTime, now(env)))
		}
		source := sourceRef
		var b *domain.Backup
		if r.Source.Backup != "" {
			b, err = env.RC.Store.GetBackup(ctx, source, r.Source.Backup)
		} else {
			var backups []domain.Backup
			primaryStorage := env.Storage.Name
			if cp.Source != "" {
				sourceCluster, e := env.RC.Store.GetCluster(ctx, source)
				if e != nil {
					return result(cp, false), e
				}
				primaryStorage = sourceCluster.Spec.Backup.Storage
			}
			backups, err = env.RC.Store.ListBackups(ctx, source)
			for i := range backups {
				candidate := &backups[i]
				if candidate.Storage != primaryStorage {
					continue
				}
				if candidate.Phase != domain.PhaseSucceeded || candidate.LastWriteTime == nil || candidate.LastWriteTime.After(*cp.TargetTime) {
					continue
				}
				if b == nil || candidate.LastWriteTime.After(*b.LastWriteTime) {
					copied := *candidate
					b = &copied
				}
			}
			if err == nil && b == nil {
				return rejectRestore(ctx, env, r, cp, errors.New("source.pitrTime has no successful base snapshot at or before the target"))
			}
		}
		if err != nil {
			return result(cp, false), err
		}
		if b.Phase != domain.PhaseSucceeded || b.PBMName == "" {
			return result(cp, false), errors.New("restore source is not a successful PBM snapshot")
		}
		if b.Type != "physical" {
			if err := logicalRestoreMemoryError(env); err != nil {
				return rejectRestore(ctx, env, r, cp, err)
			}
		}
		if err = ensureRecoveryIdentity(ctx, env, b); err != nil {
			return result(cp, false), err
		}
		cp.Snapshot, cp.Physical = b.PBMName, b.Type == "physical"
		cp.Storage = b.Storage
		cp.RecoveryIdentity = b.RecoveryIdentity
		cp.Prefix = b.PBMPrefix
		if cp.Prefix == "" {
			recorded := env
			var e error
			if source.Key() != env.RC.Cluster.Ref().Key() {
				recorded, e = sourceEnv(ctx, env, source, cp.Storage)
			}
			if e != nil {
				return result(cp, false), e
			}
			cp.Prefix = recorded.Prefix
		}
		if cp.Physical && env.RC.Cluster.Spec.Provider != "psmdb" {
			return result(cp, false), errors.New("physical restore requires PSMDB")
		}
		cp.Member, err = healthyMember(ctx, env)
		if err != nil {
			return result(cp, false), err
		}
		stamp := now(env)
		r.Phase, r.StartedAt, r.Error = domain.PhaseRunning, &stamp, ""
		if err = env.RC.Store.UpdateRestore(ctx, r); err != nil {
			return result(cp, false), err
		}
		cp.Step = "configure-source"
	case "configure-source":
		cfg, e := RenderConfig(active)
		if e != nil {
			return result(cp, false), e
		}
		_, err = command(ctx, active, cp.Member, "", "config", cfg, "--file")
		if err != nil {
			return result(cp, false), err
		}
		cp.Step = "source-resync"
	case "source-resync":
		_, err = command(ctx, active, cp.Member, "", "config", nil, "--force-resync")
		if err != nil {
			return result(cp, false), err
		}
		cp.Step = "source-inventory"
	case "source-inventory":
		done, e := resyncDone(ctx, active, cp.Member)
		if e != nil {
			return result(cp, false), e
		}
		if !done {
			return result(cp, false), nil
		}
		inv, e := inventory(ctx, active)
		if e != nil {
			return result(cp, false), e
		}
		found := false
		for _, b := range inv.Snapshots {
			if b.Name == cp.Snapshot {
				found = true
				if b.Status != "done" {
					return result(cp, false), fmt.Errorf("source snapshot %s is present but not successful after resync", cp.Snapshot)
				}
			}
		}
		if !found {
			return result(cp, false), engine.InvalidField("source.backup", domain.ReasonNotFound, "Source snapshot %s is absent from storage after PBM resync; select another successful backup and retry the restore", cp.Snapshot)
		}
		cp.Step = "disable-pitr"
	case "disable-pitr":
		_, err = command(ctx, active, cp.Member, "", "config", nil, "--set", "pitr.enabled=false")
		if err != nil {
			return result(cp, false), err
		}
		if cp.TargetTime != nil {
			ranges, e := PITRWindows(ctx, active)
			if e != nil {
				return result(cp, false), e
			}
			if err := ValidatePITRTarget(ranges, *cp.TargetTime, now(env)); err != nil {
				// PBM source configuration already changed, but no database or
				// router was stopped. Restore target configuration, then fail.
				cp.Failure, cp.Step = err.Error(), "configure-target"
				return result(cp, false), nil
			}
		}
		cp.Step = "balancer-off"
	case "balancer-off":
		if env.Sharded {
			if cp.BalancerWasEnabled == nil {
				enabled, e := env.Hooks.BalancerEnabled(ctx)
				if e != nil {
					return result(cp, false), e
				}
				cp.BalancerWasEnabled = &enabled
				// Commit the original state before changing it. Otherwise a
				// lost response could mistake our disable for operator intent.
				return result(cp, false), nil
			}
			if err = env.Hooks.SetBalancer(ctx, false); err != nil {
				return result(cp, false), err
			}
		}
		cp.Step, cp.Index = "routers-off", 0
	case "routers-off":
		if cp.Cancelling {
			if cp.RouterOp != nil {
				return routerService(ctx, env, &cp, "stop")
			}
			cp.Step, cp.Index, cp.RouterRetry = "mongod-start", 0, nil
			return result(cp, false), nil
		}
		if cp.Index < len(env.Routers) {
			changed, e := desiredStopped(env, env.Routers[cp.Index], "mongos", true)
			if e != nil {
				return result(cp, false), e
			}
			if changed {
				return result(cp, false), nil
			}
			return routerService(ctx, env, &cp, "stop")
		}
		cp.Step, cp.Index = "maintenance-on", 0
	case "maintenance-on":
		if cp.Physical && cp.Index < len(env.Nodes) {
			changed, e := desiredMaintenance(env, env.Nodes[cp.Index].Member, true)
			if e != nil {
				return result(cp, false), e
			}
			if changed {
				return result(cp, false), nil
			}
			if err = maintenance(ctx, env, env.Nodes[cp.Index].Member, true); err != nil {
				return result(cp, false), err
			}
			cp.Index++
			return result(cp, false), nil
		}
		cp.Step, cp.Index = "start", 0
		if cp.TargetTime != nil && !cp.Physical {
			cp.Step = "prepare-pitr-identity"
		}
	case "prepare-pitr-identity":
		identity, ok := env.Hooks.(pitrIdentityHooks)
		if !ok {
			return result(cp, false), errors.New("PITR requires restore-scoped PBM identity hooks")
		}
		done, e := identity.PreparePITRIdentity(ctx, op.ID)
		if e != nil || !done {
			return result(cp, false), e
		}
		cp.PITRIdentityPrepared, cp.Step = true, "start"
	case "start":
		if cp.TargetTime != nil && !cp.Physical && !cp.PITRIdentityPrepared && cp.PBMName == "" {
			// Legacy start checkpoints may already have a native receipt.
			// Do not change their authenticating account or replay initiation.
			return failRestoreObservation(ctx, env, op, cp, "legacy PITR initiation outcome is unknown and has no isolated authentication identity; inspect retained native receipts before explicit fresh-name recovery")
		}
		if op.ID == "" {
			return result(cp, false), errors.New("durable PBM start requires an operation ID")
		}
		if cp.PBMName == "" {
			args := []string{cp.Snapshot, "--yes", "--out=json"}
			if cp.TargetTime != nil {
				args = []string{"--time=" + cp.TargetTime.UTC().Format("2006-01-02T15:04:05"), "--base-snapshot=" + cp.Snapshot, "--yes", "--out=json"}
			}
			data, e := command(ctx, active, cp.Member, op.ID, "restore", nil, args...)
			if e != nil {
				return result(cp, false), e
			}
			var started struct {
				Name string `json:"name"`
			}
			if e = decodeLast(data, &started); e != nil {
				return result(cp, false), e
			}
			if started.Name == "" {
				return result(cp, false), errors.New("PBM restore start returned no name")
			}
			cp.PBMName = started.Name
		}
		if cp.RestoreMetadataDeadline == nil {
			deadline := now(env).Add(restoreMetadataGrace)
			cp.RestoreMetadataDeadline = &deadline
		}
		if err = event(ctx, env, op, "RestoreStarted", "PBM restore "+cp.PBMName+" started"); err != nil {
			return result(cp, false), err
		}
		cp.Step = "poll"
	case "poll":
		args := []string{cp.PBMName, "--out=json"}
		var cfg []byte
		if cp.Physical {
			cfg, err = RenderConfig(active)
			if err != nil {
				return result(cp, false), err
			}
			args = append(args, "--config")
		}
		data, e := command(ctx, active, cp.Member, "", "describe-restore", cfg, args...)
		if e != nil && !cp.Physical && restoreMetadataPending(e) {
			// Logical restores also publish .pbm.restore metadata. A storage
			// lookup uses the immutable source destination, not live PBM config.
			cfg, err = RenderConfig(active)
			if err != nil {
				return result(cp, false), err
			}
			data, e = command(ctx, active, cp.Member, "", "describe-restore", cfg, append(args, "--config")...)
			if e == nil {
				var stored struct {
					Status string `json:"status"`
				}
				if err := decodeLast(data, &stored); err != nil {
					return result(cp, false), err
				}
				if stored.Status != "" {
					return restoreNativeOutcome(ctx, env, r, cp, data)
				}
				// PBM's --config physical-status parser returns a successful
				// name/type-only shell even when storage contains no objects.
				// That shell is not a positive native observation. Require the
				// same complete member/command/lock/storage absence proof as
				// an explicit not-found; never infer an outcome from the shell.
			}
			if cp.RestoreMetadataDeadline != nil {
				commandTS, absent, observed, observationErr := restoreAbsent(ctx, active, cp.PBMName)
				if observed != nil {
					return restoreNativeOutcome(ctx, env, r, cp, observed)
				}
				if absent && observationErr == nil {
					observationErr = restoreFenceSupported(ctx, active)
				}
				if observationErr != nil || !absent {
					cp.RestoreAbsentSince = nil
				} else if absentSince == nil {
					stamp := now(env)
					cp.RestoreAbsentSince = &stamp
				} else {
					cp.RestoreAbsentSince = absentSince
					if !now(env).Before(absentSince.Add(restoreMetadataGrace)) && !now(env).Before(*cp.RestoreMetadataDeadline) {
						cp.RestoreCommandTS = commandTS
						cp.Step, cp.Index = "fence-pbm", 0
						return result(cp, false), nil
					}
				}
				if observationErr != nil && !now(env).Before(*cp.RestoreMetadataDeadline) {
					return result(cp, false), observationErr
				}
				if absent && observationErr == nil {
					progress := result(cp, false)
					progress.Message = "confirming sustained PBM absence before supervised restart fencing"
					return progress, nil
				}
			}
		}
		if e != nil {
			// PBM can accept a restore before publishing its metadata. Only
			// this specific startup absence gets a bounded progress window.
			if cp.RestoreMetadataDeadline != nil && now(env).Before(*cp.RestoreMetadataDeadline) && restoreMetadataPending(e) {
				progress := result(cp, false)
				progress.Message = "restore starting: waiting for PBM restore metadata"
				return progress, nil
			}
			return result(cp, false), e
		}
		return restoreNativeOutcome(ctx, env, r, cp, data)
	case "fence-pbm":
		if cp.RestoreFenceUnknown {
			return result(cp, false), errors.New("PBM restart fence outcome unknown; preserve restore holds until bounded failure cleanup or manual recovery")
		}
		if cp.Index < len(env.Nodes) {
			commandTS, absent, observed, observationErr := restoreAbsent(ctx, active, cp.PBMName)
			if observed != nil {
				return restoreNativeOutcome(ctx, env, r, cp, observed)
			}
			if observationErr != nil {
				return result(cp, false), observationErr
			}
			if !absent || commandTS != cp.RestoreCommandTS {
				return result(cp, false), errors.New("PBM restore restart admission blocked by conflicting work or unknown metadata/storage; preserve restore holds")
			}
			if err := fenceRestoreMember(ctx, active, op.ID, cp.RestoreCommandTS, env.Nodes[cp.Index].Member); err != nil {
				cp.RestoreFenceUnknown = true
				return result(cp, false), err
			}
			cp.Index++
			return result(cp, false), nil
		}
		cp.Step, cp.Index = "verify-fence", 0
	case "verify-fence":
		if cp.RestoreFenceUnknown {
			return result(cp, false), errors.New("PBM restart fence verification unknown; preserve restore holds until bounded failure cleanup or manual recovery")
		}
		// A complete final observation always revalidates every receipt in
		// this attempt. Never persist a cursor that can skip changed sessions,
		// process generations, capabilities or member clocks on a later retry.
		for _, node := range env.Nodes {
			if err := fenceRestoreMember(ctx, active, op.ID, cp.RestoreCommandTS, node.Member); err != nil {
				cp.RestoreFenceUnknown = true
				return result(cp, false), err
			}
		}
		commandTS, absent, observed, observationErr := restoreAbsent(ctx, active, cp.PBMName)
		if observed != nil {
			return restoreNativeOutcome(ctx, env, r, cp, observed)
		}
		if observationErr != nil {
			return result(cp, false), observationErr
		}
		if !absent || commandTS != cp.RestoreCommandTS {
			return result(cp, false), errors.New("PBM restore outcome remains unknown after supervised fencing; preserve restore holds")
		}
		cp.Failure = "RestoreNeverStarted: PBM restore " + cp.PBMName + " never started natively; data unchanged; re-run the restore"
		cp.Step, cp.Index = "mongod-start", 0
	case "mongod-start":
		if cp.Physical && cp.Index < len(env.Nodes) {
			if err = process(ctx, env, env.Nodes[cp.Index].Member, "mongod", "start"); err != nil {
				return result(cp, false), err
			}
			cp.Index++
			return result(cp, false), nil
		}
		cp.Step, cp.Index = "pbm-start", 0
	case "pbm-start":
		if cp.Physical && cp.Index < len(env.Nodes) {
			if err = process(ctx, env, env.Nodes[cp.Index].Member, "pbm-agent", "start"); err != nil {
				return result(cp, false), err
			}
			cp.Index++
			return result(cp, false), nil
		}
		cp.Step, cp.Index = "maintenance-off", 0
	case "maintenance-off":
		if cp.Physical && cp.Index < len(env.Nodes) {
			changed, e := desiredMaintenance(env, env.Nodes[cp.Index].Member, false)
			if e != nil {
				return result(cp, false), e
			}
			if changed {
				return result(cp, false), nil
			}
			if err = maintenance(ctx, env, env.Nodes[cp.Index].Member, false); err != nil {
				return result(cp, false), err
			}
			cp.Index++
			return result(cp, false), nil
		}
		cp.Step, cp.Index = "reset-users", 0
	case "reset-users":
		if cp.Failure == "" || cp.PITRIdentityPrepared {
			identity := cp.RecoveryIdentity
			if identity != "" {
				identity = (domain.ClusterRef{Namespace: cp.SourceNamespace, Name: identity}).Key()
			}
			if err = env.Hooks.ResetSystemUsers(ctx, identity); err != nil {
				return result(cp, false), err
			}
		}
		cp.Step = "configure-target"
		if cp.TargetTime != nil && !cp.Physical {
			cp.Step = "release-pitr-identity"
		}
	case "release-pitr-identity":
		identity, ok := env.Hooks.(pitrIdentityHooks)
		if !ok {
			return result(cp, false), errors.New("PITR requires restore-scoped PBM identity cleanup hooks")
		}
		done, e := identity.FinishPITRIdentity(ctx, op.ID)
		if e != nil || !done {
			return result(cp, false), e
		}
		cp.Step = "configure-target"
	case "configure-target":
		own, e := primaryEnv(ctx, env)
		if e != nil {
			return result(cp, false), e
		}
		own.PITR.Enabled = false
		cfg, e := RenderConfig(own)
		if e != nil {
			return result(cp, false), e
		}
		_, err = command(ctx, env, cp.Member, "", "config", cfg, "--file")
		if err != nil {
			return result(cp, false), err
		}
		cp.Step = "target-resync"
	case "target-resync":
		_, err = command(ctx, env, cp.Member, "", "config", nil, "--force-resync")
		if err != nil {
			return result(cp, false), err
		}
		cp.Step = "target-inventory"
	case "target-inventory":
		done, e := resyncDone(ctx, env, cp.Member)
		if e != nil {
			return result(cp, false), e
		}
		if !done {
			return result(cp, false), nil
		}
		cp.Step, cp.Index = "routers-on", 0
	case "routers-on":
		if cp.Index < len(env.Routers) {
			changed, e := desiredStopped(env, env.Routers[cp.Index], "mongos", false)
			if e != nil {
				return result(cp, false), e
			}
			if changed {
				return result(cp, false), nil
			}
			return routerService(ctx, env, &cp, "start")
		}
		cp.Step = "balancer-on"
	case "balancer-on":
		if env.Sharded && cp.BalancerWasEnabled != nil && *cp.BalancerWasEnabled {
			if err = env.Hooks.SetBalancer(ctx, true); err != nil {
				return result(cp, false), err
			}
		}
		cp.Step = "complete"
	case "complete":
		stamp := now(env)
		if r.CompletedAt == nil {
			r.CompletedAt = &stamp
		}
		r.Phase, r.Error = domain.PhaseSucceeded, ""
		if cp.Failure != "" {
			r.Phase, r.Error = domain.PhaseFailed, cp.Failure
		}
		if err = env.RC.Store.UpdateRestore(ctx, r); err != nil {
			return result(cp, false), err
		}
		if err = event(ctx, env, op, "RestoreCompleted", "PBM restore "+cp.PBMName+": "+string(r.Phase)); err != nil {
			return result(cp, false), err
		}
		cp.FreshBase = env.RC.Cluster.Spec.Backup.PITR.Enabled && cp.Failure == ""
		cp.Step = "done"
		if cp.Failure != "" {
			return result(cp, true), errors.New(cp.Failure)
		}
		return result(cp, true), nil
	default:
		return result(cp, false), fmt.Errorf("unknown restore checkpoint step %q", cp.Step)
	}
	return result(cp, false), nil
}

// A positive native observation supersedes absence fencing, but only a native
// terminal status permits compensation to release the restore holds.
func restoreNativeOutcome(ctx context.Context, env Env, r *domain.Restore, cp checkpoint, data []byte) (engine.OpResult, error) {
	if cp.Step == "fence-pbm" || cp.Step == "verify-fence" {
		cp.Step, cp.Index = "poll", 0
		// An unclassified response during fencing must not permit further
		// restarts or a renewed absence window. Keep the restore holds.
		cp.RestoreMetadataDeadline = nil
		cp.RestoreAbsentSince = nil
	}
	var desc struct {
		Status string     `json:"status"`
		Error  string     `json:"error"`
		Finish *time.Time `json:"finish"`
	}
	if err := decodeLast(data, &desc); err != nil {
		return result(cp, false), err
	}
	if desc.Status != "" {
		// Once observed, later missing metadata cannot prove startup absence.
		cp.RestoreMetadataDeadline = nil
		cp.RestoreAbsentSince = nil
	}
	switch desc.Status {
	case "done":
		if err := restoreWorkerWarning(ctx, env, r, &cp, false); err != nil {
			return result(cp, false), err
		}
		cp.RestoreNativeSucceeded = true
		cp.Failure = ""
	case "error", "partlyDone", "partly-done", "cancelled", "canceled":
		if err := restoreWorkerWarning(ctx, env, r, &cp, false); err != nil {
			return result(cp, false), err
		}
		cp.Failure = redact.NativeError(desc.Error)
		if cp.Failure == "" {
			cp.Failure = "PBM restore ended with status " + desc.Status
		}
		cp.Failure = fmt.Sprintf("PBM restore %s failed: %s; partial changes cannot be excluded; no automatic re-initiation. Ready indicates database health, not restore success; consistency remains uncertain and the cluster stays Degraded pending recovery. Operator action required: quiesce writers, check that native restore workers have stopped, and assess data and authentication; then explicitly submit a new-named restore or recover a verified backup into a fresh cluster.", cp.PBMName, cp.Failure)
		cp.RestoreConsistencyUncertain = true
	default:
		if desc.Status != "" && !cp.Physical {
			if observer, ok := env.Hooks.(RestoreWorkerObserver); ok {
				if cp.RestoreWorkers == nil {
					cp.RestoreWorkers = make(map[string]RestoreWorkerIdentity)
				}
				reason, unconfirmed, err := observer.ObserveRestoreWorkerFailure(ctx, cp.PBMName, env, cp.RestoreWorkers)
				if err == nil || unconfirmed {
					if warningErr := restoreWorkerWarning(ctx, env, r, &cp, unconfirmed); warningErr != nil {
						return result(cp, false), warningErr
					}
				}
				if err != nil {
					// This extra diagnostic read must not exhaust a healthy
					// restore's retry budget. Native polling remains authoritative.
					progress := result(cp, false)
					progress.Message = "native restore running; worker-incarnation observation unavailable"
					if cp.RestoreWorkerUnconfirmed {
						progress.Message = RestoreWorkerUnconfirmedMessage
					}
					return progress, nil
				}
				if reason != "" {
					// Do not run native compensation: another shard may still
					// own work. Retain holds and fail this genuine lost-worker
					// restore closed, without ever replaying its initiation.
					payload, _ := json.Marshal(map[string]string{"restore": r.Name})
					return failRestoreObservation(ctx, env, &domain.Operation{Payload: payload}, cp, reason)
				}
			}
		}
		progress := result(cp, false)
		if cp.RestoreWorkerUnconfirmed {
			progress.Message = RestoreWorkerUnconfirmedMessage
		}
		return progress, nil
	}
	r.CompletedAt = desc.Finish
	if err := env.RC.Store.UpdateRestore(ctx, r); err != nil {
		return result(cp, false), err
	}
	cp.Step, cp.Index = "mongod-start", 0
	return result(cp, false), nil
}

// RestoreWorkerUnconfirmedMessage is a warning, never native failure evidence.
const RestoreWorkerUnconfirmedMessage = "RestoreWorkerUnconfirmed: native restore shows no progress and its worker cannot be confirmed alive; inspect native logs and memory limits; the operator may delete the target; the restore is not re-initiated"

func restoreWorkerWarning(ctx context.Context, env Env, r *domain.Restore, cp *checkpoint, unconfirmed bool) error {
	if cp.RestoreWorkerUnconfirmed == unconfirmed {
		return nil
	}
	cp.RestoreWorkerUnconfirmed = unconfirmed
	reason, message := "RestoreWorkerUnconfirmed", RestoreWorkerUnconfirmedMessage
	if !unconfirmed {
		reason, message = "RestoreWorkerProgressObserved", "Native restore progress or terminal outcome observed; the unconfirmed-worker warning is cleared"
	}
	op := &domain.Operation{ID: r.OperationID}
	if err := event(ctx, env, op, reason, message); err != nil {
		return err
	}
	inventory, hasInventory := env.RC.Store.(interface {
		ListOperations(context.Context, domain.ClusterRef, ...domain.Phase) ([]domain.Operation, error)
	})
	if env.RC.Cluster.Spec.DataSource != nil && env.RC.Events != nil && hasInventory {
		operations, err := inventory.ListOperations(ctx, env.RC.Cluster.Ref(), domain.PhasePending, domain.PhaseRunning)
		if err != nil {
			return err
		}
		for _, owner := range operations {
			if owner.Kind == domain.OperationCreate {
				if err := event(ctx, env, &owner, reason, message); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func rejectRestore(ctx context.Context, env Env, r *domain.Restore, cp checkpoint, reason error) (engine.OpResult, error) {
	stamp := now(env)
	r.Phase, r.Error, r.CompletedAt = domain.PhaseFailed, reason.Error(), &stamp
	if err := env.RC.Store.UpdateRestore(ctx, r); err != nil {
		return result(cp, false), err
	}
	cp.Step, cp.Failure = "done", reason.Error()
	return result(cp, true), reason
}

// Stop/Start acts on the entire Zerops router service, not one DNS-selected
// container. Observe its durable platform process before starting PBM or traffic.
func routerService(ctx context.Context, env Env, cp *checkpoint, action string) (engine.OpResult, error) {
	if env.RC.Backend == nil {
		return result(*cp, false), errors.New("router restore requires infrastructure backend")
	}
	if cp.RouterOp == nil {
		if retry := cp.RouterRetry; retry != nil {
			if retry.Waiting(now(env), retry.RetryAfter) {
				return result(*cp, false), routerRetryError(env, cp, action)
			}
			// A failed process (or a lost reply to its retry) can still have
			// applied the action. Observe the service before safely reissuing it.
			service, err := env.RC.Backend.GetService(ctx, env.Routers[cp.Index])
			if err != nil {
				return result(*cp, false), err
			}
			if service == nil {
				return result(*cp, false), engine.ErrNotFound
			}
			if (action == "stop" && service.Status == infra.StatusStopped) ||
				(action == "start" && (service.Status == infra.StatusActive || service.Status == infra.StatusRunning)) {
				cp.RouterRetry = nil
				cp.Index++
				return result(*cp, false), nil
			}
		}
		var op infra.Op
		var err error
		if action == "stop" {
			op, err = env.RC.Backend.Stop(ctx, env.Routers[cp.Index])
		} else {
			op, err = env.RC.Backend.Start(ctx, env.Routers[cp.Index])
		}
		if err != nil {
			return result(*cp, false), err
		}
		cp.RouterOp = &op
		return result(*cp, false), nil
	}
	if cp.RouterOp.ID != "" {
		status, err := env.RC.Backend.OpStatus(ctx, *cp.RouterOp)
		if err != nil {
			return result(*cp, false), err
		}
		if status.Failed {
			if cp.RouterRetry == nil {
				cp.RouterRetry = &routerRetry{}
			}
			cp.RouterRetry.Failures++
			cp.RouterRetry.Message = status.Message
			retry := cp.RouterRetry
			recorded := now(env)
			retry.RetryAfter = retry.Schedule(recorded, recorded.Add(engine.PlatformRetryDelay(retry.Failures)))
			cp.RouterOp = nil
			return result(*cp, false), routerRetryError(env, cp, action)
		}
		if !status.Done {
			return result(*cp, false), nil
		}
	}
	cp.RouterOp = nil
	cp.RouterRetry = nil
	cp.Index++
	return result(*cp, false), nil
}

func routerRetryError(env Env, cp *checkpoint, action string) error {
	retry := cp.RouterRetry
	return fmt.Errorf("router %s %s failed (attempt %d): %s; retry after %s",
		action, env.Routers[cp.Index], retry.Failures, retry.Message, retry.RetryAfter.Format(time.RFC3339))
}
