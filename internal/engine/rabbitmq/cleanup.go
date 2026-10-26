// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package rabbitmq

import (
	"context"
	"fmt"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

// Cleanup preserves ownership evidence until all platform processes are terminal.
func (p *Provider) Cleanup(ctx context.Context, rc *engine.Context, opts domain.DeleteOptions) (done bool, err error) {
	s, e := loadState(rc.Cluster)
	if e != nil {
		return false, e
	}
	// Backend-owned deletion fences survive this whole-cluster takeover.
	for _, m := range s.Members {
		m.UnplacedRetirement = nil
	}
	defer func() {
		if e := s.save(rc.Cluster); err == nil {
			err = e
		}
	}()
	if busy, e := drainRuntimeRecovery(ctx, rc, s); busy || e != nil {
		return false, e
	}
	s.Phase = "cleanup"
	rc.Cluster.Status.Message = "Deleting: removing RabbitMQ runtime services and owned resources"
	if s.Pending != nil && engine.CleanupSupersedesDeployment(rc.Backend, s.Pending.Op) {
		s.resetPlatformRetry(s.Pending)
		s.Pending = nil
	}
	for _, m := range s.Members {
		if m.ResumeDeploy != nil && engine.CleanupSupersedesDeployment(rc.Backend, m.ResumeDeploy.Op) {
			s.resetPlatformRetry(m.ResumeDeploy)
			m.ResumeDeploy = nil
		}
	}
	for _, v := range []*pending{s.Pending, s.Retry} {
		if v == nil {
			continue
		}
		if v.Action == "delete-volume" || v.Action == "delete-runtime" {
			old := platformRetryKey(v)
			if v.Action == "delete-volume" {
				v.Action = "cleanup-volume"
			} else {
				v.Action = "cleanup-runtime"
			}
			if r := s.PlatformRetries[old]; r != nil {
				s.PlatformRetries[platformRetryKey(v)] = r
				delete(s.PlatformRetries, old)
			}
		}
	}
	if s.Pending != nil {
		busy, e := p.observePending(ctx, rc, s)
		if busy || e != nil {
			return false, e
		}
	}
	if s.Retry != nil && (s.Retry.Action != "cleanup-runtime" && s.Retry.Action != "cleanup-volume" || opts.KeepVolumes && volumeDelete(s.Retry.Action)) {
		s.resetPlatformRetry(s.Retry)
	}
	ownershipChanged := false
	for _, m := range ordered(s) {
		serviceID, volumeID := m.ServiceID, m.VolumeID
		if e = verifyMemberServices(ctx, rc, m); e != nil {
			return false, e
		}
		if opts.KeepVolumes && m.RuntimeRecovery.VolumeLost {
			return false, fmt.Errorf("%w: cannot suspend while preserving lost durable data on %s (%s); recover the original volume or complete safe replica replacement before suspension", engine.ErrConflict, m.Name, m.Volume)
		}
		ownershipChanged = ownershipChanged || m.ServiceID != serviceID || m.VolumeID != volumeID
	}
	if ownershipChanged {
		// The deferred save returns the adopted IDs to the reconciler for
		// persistence. Delete the runtime only in a subsequent invocation,
		// after its mount evidence is no longer needed to prove ownership.
		return false, nil
	}
	if s.Retry != nil {
		_, e := p.retryPending(ctx, rc, s)
		return false, e
	}
	if busy, e := p.observeResumeDeployments(ctx, rc, s); busy || e != nil {
		return false, e
	}
	if opts.KeepVolumes {
		// Whole-cluster shutdown is not a rolling upgrade. Draining successive
		// nodes transfers leaders to peers that are themselves going away and
		// eventually loses the quorum needed by maintenance operations. Stop
		// locally without changing queue or metadata membership, and finish
		// this pass before removing any peer's runtime/discovery identity.
		if busy, err := p.stopForSuspension(ctx, rc, s); busy || err != nil {
			return false, err
		}
	}
	retained, e := rc.Store.ListRetainedVolumes(ctx, rc.Cluster.Ref())
	if e != nil {
		return false, e
	}
	for _, m := range ordered(s) {
		runtime, e := engine.ObserveRetiringService(ctx, rc.Backend, m.Name, m.ServiceID, m.Volume, m.VolumeID, m.RuntimeRecovery.VolumeLost && !opts.KeepVolumes)
		if e != nil {
			return false, e
		}
		if runtime != nil {
			if e = verifyServiceOwnership(ctx, rc, m, runtime, false); e != nil {
				return false, e
			}
			if e = rc.Backend.DeletePublicPorts(ctx, m.Name); e != nil {
				return false, e
			}
			if sub, err := engine.PublicHTTPS(rc.Backend); err == nil && m.ManagementPublic {
				op, e := sub.SetSubdomainAccess(ctx, m.Name, false)
				if e != nil {
					return false, e
				}
				m.ManagementPublic = false
				s.Pending = &pending{Op: op, Action: "cleanup-management", Member: m.Name}
				return false, nil
			}
			return false, p.submit(ctx, rc, s, &pending{Action: "cleanup-runtime", Member: m.Name})
		}
		if m.VolumeOrphaned {
			continue
		}
		volume, e := engine.ObserveRetiringService(ctx, rc.Backend, m.Volume, m.VolumeID, m.Volume, m.VolumeID, m.RuntimeRecovery.VolumeLost && !opts.KeepVolumes)
		if e != nil {
			return false, e
		}
		if volume == nil {
			continue
		}
		if opts.KeepVolumes {
			exists := false
			for _, v := range retained {
				if v.Member == m.Name {
					exists = true
				}
			}
			if !exists {
				return false, rc.Store.CreateRetainedVolume(ctx, &domain.RetainedVolume{Namespace: rc.Cluster.Ref().Namespace, Cluster: rc.Cluster.Name, Member: m.Name, Hostname: m.Volume, ServiceID: volume.ID, Set: "rabbitmq", Role: "rabbitmq", Index: m.Index, RetainedAt: now(rc)})
			}
		} else {
			return false, p.submit(ctx, rc, s, &pending{Action: "cleanup-volume", Member: m.Name})
		}
	}
	if !opts.KeepBackups && !s.CleanupBackups {
		done, e := p.cleanupBackupArtifacts(ctx, rc, s)
		if e != nil || !done {
			return false, e
		}
	}
	if opts.KeepVolumes {
		s.Suspended = true
		rc.Cluster.Status.Phase = domain.PhaseSuspended
		return true, nil
	}
	if !s.CleanupSecrets {
		for _, m := range s.Members {
			if e = deleteMemberSecrets(ctx, rc, m.Name); e != nil {
				return false, e
			}
		}
		for _, key := range []string{"erlang-cookie", "users/karakoram", "users/admin", "users/monitor", "tls/ca.crt", "tls/ca.key", "tls/user.crt", "tls/user.key"} {
			if e = rc.Secrets.DeleteClusterSecret(ctx, rc.Cluster.Ref(), key); e != nil && !missing(e) {
				return false, e
			}
		}
		for _, key := range s.RotationSecrets {
			if e = rc.Secrets.DeleteClusterSecret(ctx, rc.Cluster.Ref(), key); e != nil && !missing(e) {
				return false, e
			}
		}
		for _, v := range retained {
			if e = rc.Store.DeleteRetainedVolume(ctx, rc.Cluster.Ref(), v.Member); e != nil && !missing(e) {
				return false, e
			}
		}
		s.CleanupSecrets = true
	}
	return true, nil
}

func (p *Provider) stopForSuspension(ctx context.Context, rc *engine.Context, s *internalState) (bool, error) {
	for _, m := range ordered(s) {
		if m.RemoveStep == "cleanup-stopped" || m.RemoveStep == "cleanup-platform-stop" {
			continue
		}
		runtime, err := rc.Backend.GetService(ctx, m.Name)
		if err != nil {
			return false, err
		}
		if runtime == nil {
			m.RemoveStep = "cleanup-stopped"
			continue
		}
		switch runtime.Status {
		case infra.StatusStopped, infra.StatusFailed, infra.StatusReadyToDeploy:
			err = fmt.Errorf("platform runtime is %s", runtime.Status)
		default:
			if m.Stage != "deployed" && m.Stage != "ready" && m.Stage != "joined" {
				m.RemoveStep = "cleanup-platform-stop"
				continue
			}
			var a *agentapi.Client
			a, err = p.agent(ctx, rc, m.Name)
			if err == nil {
				err = a.Maintenance(ctx, agentapi.Maintenance{Enabled: true})
				a.Close()
			}
			if err == nil {
				err = p.action(ctx, rc, m.Name, "stop", nil)
			}
		}
		if err != nil {
			if ctx.Err() != nil {
				return false, ctx.Err()
			}
			// The backend must finish runtime deletion before its volume can
			// be retained. Agent unavailability is not evidence that it is
			// safe to release a mount, and must not block that platform fence.
			m.CleanupStopReason = fmt.Sprintf("Skipped broker shutdown on %s: %v. No drain or membership change attempted; platform runtime deletion must complete before retaining its volume.", m.Name, err)
			if err = warningEvent(ctx, rc, "SuspendBrokerShutdownSkipped", m.CleanupStopReason); err != nil {
				return false, err
			}
			m.RemoveStep = "cleanup-platform-stop"
		} else {
			m.RemoveStep = "cleanup-stopped"
		}
		return true, nil
	}
	return false, nil
}
func deleteMemberSecrets(ctx context.Context, rc *engine.Context, name string) error {
	if err := rc.Secrets.DeleteAgentToken(ctx, rc.Cluster.Ref(), name); err != nil && !missing(err) {
		return err
	}
	for _, kind := range []string{"members", "agents"} {
		for _, suffix := range []string{".pem", ".pending.pem"} {
			if err := rc.Secrets.DeleteClusterSecret(ctx, rc.Cluster.Ref(), "tls/"+kind+"/"+name+suffix); err != nil && !missing(err) {
				return err
			}
		}
	}
	return nil
}
