// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package pbm

import (
	"context"
	"errors"
	"fmt"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

// RunBackup checkpoints configuration, initiation and terminal metadata. Starts use durable agent receipts keyed by operation ID.
func RunBackup(ctx context.Context, env Env, op *domain.Operation) (engine.OpResult, error) {
	if op.Step == "failure-cleanup" {
		return runBackupFailureCleanup(ctx, env, op)
	}
	cp, err := loadCheckpoint(op)
	if err != nil {
		return engine.OpResult{}, err
	}
	name, err := payloadName(op, "backup")
	if err != nil {
		return result(cp, false), err
	}
	b, err := env.RC.Store.GetBackup(ctx, env.RC.Cluster.Ref(), name)
	if err != nil {
		return result(cp, false), err
	}
	if cp.Step == "done" {
		if cp.Failure != "" {
			return result(cp, true), errors.New(cp.Failure)
		}
		return result(cp, true), nil
	}
	if b.Type != "logical" && b.Type != "physical" {
		return result(cp, false), fmt.Errorf("unsupported backup type %q", b.Type)
	}
	if b.Type == "physical" && env.RC.Cluster.Spec.Provider != "psmdb" {
		return result(cp, false), errors.New("physical backups require PSMDB")
	}
	if cp.Cancelling && (cp.Step == "configure" || cp.Step == "ready") {
		cp.Failure, cp.Step = "Backup cancelled before start", "target-configure"
	}
	switch cp.Step {
	case "":
		if b.PBMPrefix == "" {
			b.PBMPrefix, b.SourceTopology, b.SourceVersion = env.Prefix, env.RC.Cluster.Spec.Topology, env.RC.Cluster.Spec.Version
			b.RecoveryIdentity = "backup-" + secrets.Hash(env.RC.Cluster.Name + "/" + env.Prefix + "/" + b.Name)[:32]
			pw, e := env.RC.Secrets.GetClusterSecret(ctx, env.RC.Cluster.Ref(), "users/userAdmin")
			if e != nil {
				return result(cp, false), e
			}
			if e := env.RC.Secrets.SetClusterSecret(ctx, domain.ClusterRef{Namespace: b.Namespace, Name: b.RecoveryIdentity}, "users/userAdmin", pw); e != nil {
				return result(cp, false), e
			}
			if e := env.RC.Store.UpdateBackup(ctx, b); e != nil {
				return result(cp, false), e
			}
		}
		cp.Member, err = healthyMember(ctx, env)
		if err != nil {
			return result(cp, false), err
		}
		cp.Step = "configure"
	case "configure":
		_, err = EnsureConfig(ctx, env, "")
		if err != nil {
			return result(cp, false), err
		}
		cp.Step = "ready"
	case "ready":
		return waitBackupAgents(ctx, env, op, cp)
	case "start":
		if op.ID == "" {
			return result(cp, false), errors.New("durable PBM start requires an operation ID")
		}
		if cp.PBMName == "" && b.PBMName != "" {
			cp.PBMName = b.PBMName
		}
		if cp.PBMName == "" {
			out, e := command(ctx, env, cp.Member, op.ID, "backup", nil, "--type="+b.Type, "--out=json")
			if e != nil {
				return failBackupStart(ctx, env, op, cp, e)
			}
			var started struct {
				Name string `json:"name"`
			}
			if e = decodeLast(out, &started); e != nil {
				return failBackupStart(ctx, env, op, cp, e)
			}
			if started.Name == "" {
				return failBackupStart(ctx, env, op, cp, errors.New("PBM backup start returned no name"))
			}
			cp.PBMName = started.Name
		}
		b.PBMName, b.Phase = cp.PBMName, domain.PhaseRunning
		setSnapshotLocation(b)
		stamp := now(env)
		if b.StartedAt == nil {
			b.StartedAt = &stamp
		}
		if err = env.RC.Store.UpdateBackup(ctx, b); err != nil {
			return result(cp, false), err
		}
		if err = event(ctx, env, op, "BackupStarted", "PBM backup "+cp.PBMName+" started"); err != nil {
			return result(cp, false), err
		}
		cp.Step = "poll"
	case "poll":
		out, e := command(ctx, env, cp.Member, "", "describe-backup", nil, cp.PBMName, "--out=json")
		if e != nil {
			return result(cp, false), e
		}
		var s snapshot
		if e = decodeLast(out, &s); e != nil {
			return result(cp, false), e
		}
		if s.Name == "" {
			s.Name = cp.PBMName
		}
		// describe-backup exposes persisted metadata, which can remain running
		// after its owner dies. PBM status additionally classifies expired native
		// heartbeats; use only the exact snapshot's failure, never another
		// operation or an absent inventory entry.
		if s.Status != "done" && s.Status != "error" && s.Status != "cancelled" && s.Status != "canceled" {
			out, e = command(ctx, env, "", "", "status", nil, "--out=json")
			var status statusData
			if e == nil {
				e = decodeLast(out, &status)
			}
			if e != nil && (!cp.Cancelling || cp.CancelIssued) {
				return result(cp, false), e
			}
			if e != nil {
				status = statusData{}
			}
			// A secondary status outage must not prevent a requested native
			// cancellation after describe confirmed nonterminal metadata. This
			// never asserts termination: the following polls still require a
			// terminal native outcome.
			for _, observed := range status.Backups.Snapshot {
				if observed.Name == cp.PBMName && observed.Status == "error" {
					s.Status, s.Error = observed.Status, observed.Error
					break
				}
			}
		}
		applySnapshot(b, s, now(env))
		if err = env.RC.Store.UpdateBackup(ctx, b); err != nil {
			return result(cp, false), err
		}
		if b.Phase == domain.PhaseSucceeded || b.Phase == domain.PhaseFailed {
			cp.Step = "target-configure"
			reason := "BackupCompleted"
			if b.Phase == domain.PhaseFailed {
				reason = "BackupFailed"
			}
			if err = event(ctx, env, op, reason, "PBM backup "+cp.PBMName+": "+string(b.Phase)); err != nil {
				return result(cp, false), err
			}
			if b.Phase == domain.PhaseFailed {
				cp.Failure = b.Error
			}
			return result(cp, false), nil
		}
		if cp.Cancelling && !cp.CancelIssued {
			if _, err = command(ctx, env, cp.Member, op.ID+"/cancel", "cancel-backup", nil); err != nil {
				return result(cp, false), err
			}
			cp.CancelIssued = true
			return result(cp, false), nil
		}
	case "target-configure", "target-resync", "target-inventory":
		done, e := reconcilePrimary(ctx, env, &cp)
		if e != nil || !done {
			return result(cp, false), e
		}
		cp.Step = "done"
		if cp.Failure != "" {
			return result(cp, true), errors.New(cp.Failure)
		}
		return result(cp, true), nil
	default:
		return result(cp, false), fmt.Errorf("unknown backup checkpoint step %q", cp.Step)
	}
	return result(cp, false), nil
}

// RunDeleteBackup tolerates an already absent PBM snapshot and waits for inventory disappearance before deleting its record.
func RunDeleteBackup(ctx context.Context, env Env, op *domain.Operation) (engine.OpResult, error) {
	cp, err := loadCheckpoint(op)
	if err != nil {
		return engine.OpResult{}, err
	}
	name, err := payloadName(op, "backup")
	if err != nil {
		return result(cp, false), err
	}
	if cp.Step == "done" {
		return result(cp, true), nil
	}
	b, err := env.RC.Store.GetBackup(ctx, env.RC.Cluster.Ref(), name)
	if errors.Is(err, engine.ErrNotFound) || errors.Is(err, store.ErrNotFound) {
		cp.Step = "done"
		return result(cp, true), nil
	}
	if err != nil {
		return result(cp, false), err
	}
	if b.PBMPrefix != "" {
		env, err = recordedEnv(ctx, env, b.Namespace, b.Storage, b.PBMPrefix)
		if err != nil {
			return result(cp, false), err
		}
	}
	switch cp.Step {
	case "":
		cp.PBMName = b.PBMName
		b.Phase = domain.PhaseDeleting
		if err = env.RC.Store.UpdateBackup(ctx, b); err != nil {
			return result(cp, false), err
		}
		cp.Step = "configure"
	case "configure":
		disabled := env
		disabled.PITR.Enabled = false
		cfg, e := RenderConfig(disabled)
		if e != nil {
			return result(cp, false), e
		}
		if _, e = command(ctx, env, "", "", "config", cfg, "--file"); e != nil {
			return result(cp, false), e
		}
		cp.Step = "resync"
	case "resync":
		if _, e := command(ctx, env, "", "", "config", nil, "--force-resync"); e != nil {
			return result(cp, false), e
		}
		cp.Step = "inventory"
	case "inventory":
		done, e := resyncDone(ctx, env, "")
		if e != nil || !done {
			return result(cp, false), e
		}
		cp.Step = "delete"
	case "delete":
		if cp.PBMName != "" {
			_, err = command(ctx, env, "", "", "delete-backup", nil, cp.PBMName, "--yes")
			if err != nil && !alreadyGone(err) {
				return result(cp, false), err
			}
		}
		cp.Step = "observe"
	case "observe":
		inv, e := inventory(ctx, env)
		if e != nil {
			return result(cp, false), e
		}
		for _, s := range inv.Snapshots {
			if s.Name == cp.PBMName {
				return result(cp, false), nil
			}
		}
		cp.Step = "target-configure"
	case "target-configure", "target-resync", "target-inventory":
		done, e := reconcilePrimary(ctx, env, &cp)
		if e != nil || !done {
			return result(cp, false), e
		}
		if b.RecoveryIdentity != "" {
			if e := env.RC.Secrets.DeleteClusterSecret(ctx, domain.ClusterRef{Namespace: b.Namespace, Name: b.RecoveryIdentity}, "users/userAdmin"); e != nil && !errors.Is(e, engine.ErrNotFound) && !errors.Is(e, store.ErrNotFound) {
				return result(cp, false), e
			}
		}
		if err = env.RC.Store.DeleteBackup(ctx, env.RC.Cluster.Ref(), name); err != nil && !errors.Is(err, engine.ErrNotFound) && !errors.Is(err, store.ErrNotFound) {
			return result(cp, false), err
		}
		if err = event(ctx, env, op, "BackupDeleted", "PBM backup "+cp.PBMName+" deleted"); err != nil {
			return result(cp, false), err
		}
		cp.Step = "done"
		return result(cp, true), nil
	default:
		return result(cp, false), fmt.Errorf("unknown delete-backup checkpoint step %q", cp.Step)
	}
	return result(cp, false), nil
}
