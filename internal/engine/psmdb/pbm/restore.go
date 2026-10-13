// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package pbm

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

// RunRestore resumes logical, PITR, physical and clone restores. Every member lifecycle change advances a durable index.
// After success with PITR desired, the provider must enqueue a fresh base snapshot before applying enabled PITR configuration.
func RunRestore(ctx context.Context, env Env, op *domain.Operation) (engine.OpResult, error) {
	cp, err := loadCheckpoint(op)
	if err != nil {
		return engine.OpResult{}, err
	}
	name, err := payloadName(op, "restore")
	if err != nil {
		return result(cp, false), err
	}
	r, err := env.RC.Store.GetRestore(ctx, env.RC.Cluster.Ref(), name)
	if err != nil {
		return result(cp, false), err
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
				err = errors.New("no successful base snapshot at or before PITR target")
			}
		}
		if err != nil {
			return result(cp, false), err
		}
		if b.Phase != domain.PhaseSucceeded || b.PBMName == "" {
			return result(cp, false), errors.New("restore source is not a successful PBM snapshot")
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
			if b.Name == cp.Snapshot && b.Status == "done" {
				found = true
			}
		}
		if !found {
			return result(cp, false), fmt.Errorf("source snapshot %s absent after resync", cp.Snapshot)
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
			valid := false
			for _, w := range ranges {
				if !cp.TargetTime.Before(w.Start) && !cp.TargetTime.After(w.End) {
					valid = true
				}
			}
			if !valid {
				return result(cp, false), errors.New("PITR target is outside the continuous restorable ranges")
			}
		}
		cp.Step = "balancer-off"
	case "balancer-off":
		if env.Sharded {
			if err = env.Hooks.SetBalancer(ctx, false); err != nil {
				return result(cp, false), err
			}
		}
		cp.Step, cp.Index = "routers-off", 0
	case "routers-off":
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
	case "start":
		if op.ID == "" {
			return result(cp, false), errors.New("durable PBM start requires an operation ID")
		}
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
		if e != nil {
			return result(cp, false), e
		}
		var desc struct {
			Status string     `json:"status"`
			Error  string     `json:"error"`
			Finish *time.Time `json:"finish"`
		}
		if e = decodeLast(data, &desc); e != nil {
			return result(cp, false), e
		}
		switch desc.Status {
		case "done":
		case "error", "partlyDone", "partly-done", "cancelled", "canceled":
			cp.Failure = desc.Error
			if cp.Failure == "" {
				cp.Failure = "PBM restore ended with status " + desc.Status
			}
		default:
			return result(cp, false), nil
		}
		r.CompletedAt = desc.Finish
		if err = env.RC.Store.UpdateRestore(ctx, r); err != nil {
			return result(cp, false), err
		}
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
		if cp.Failure == "" {
			identity := cp.RecoveryIdentity
			if identity != "" {
				identity = (domain.ClusterRef{Namespace: cp.SourceNamespace, Name: identity}).Key()
			}
			if err = env.Hooks.ResetSystemUsers(ctx, identity); err != nil {
				return result(cp, false), err
			}
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
		if env.Sharded {
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

// Stop/Start acts on the entire Zerops router service, not one DNS-selected
// container. Observe its durable platform process before starting PBM or traffic.
func routerService(ctx context.Context, env Env, cp *checkpoint, action string) (engine.OpResult, error) {
	if env.RC.Backend == nil {
		return result(*cp, false), errors.New("router restore requires infrastructure backend")
	}
	if cp.RouterOp == nil {
		if retry := cp.RouterRetry; retry != nil {
			if now(env).Before(retry.RetryAfter) {
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
			if (action == "stop" && service.Status == "STOPPED") ||
				(action == "start" && (service.Status == "ACTIVE" || service.Status == "RUNNING")) {
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
			cp.RouterRetry.RetryAfter = now(env).Add(engine.PlatformRetryDelay(cp.RouterRetry.Failures))
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
