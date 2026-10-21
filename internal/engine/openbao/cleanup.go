// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package openbao

import (
	"context"
	"fmt"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/pgbinding"
)

func (p *Provider) Cleanup(ctx context.Context, rc *engine.Context, opts domain.DeleteOptions) (done bool, err error) {
	s, err := loadState(rc.Cluster)
	if err != nil {
		return false, err
	}
	defer func() {
		if e := s.save(rc.Cluster); err == nil {
			err = e
		}
	}()
	for _, m := range s.Members {
		m.UnplacedRetirement = nil
	}
	if busy, e := drainRuntimeRecovery(ctx, rc, s); busy || e != nil {
		return false, e
	}
	s.Phase = "cleanup"
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
	if busy, e := p.observePending(ctx, rc, s); busy || e != nil {
		return false, e
	}
	if _, e := p.observeDeployments(ctx, rc, s); e != nil {
		return false, e
	}
	for _, m := range s.Members {
		if m.ResumeDeploy != nil {
			return false, nil
		}
	}
	if s.Retry != nil && (s.Retry.Action != "cleanup-runtime" && s.Retry.Action != "cleanup-volume" || opts.KeepVolumes && volumeDelete(s.Retry.Action)) {
		s.resetPlatformRetry(s.Retry)
	}
	ownershipChanged := false
	for _, m := range ordered(s) {
		serviceID, volumeID := m.ServiceID, m.VolumeID
		if e := verifyMemberServices(ctx, rc, m); e != nil {
			return false, e
		}
		if opts.KeepVolumes && m.RuntimeRecovery.VolumeLost {
			return false, fmt.Errorf("%w: cannot preserve lost durable volume for %s", engine.ErrConflict, m.Name)
		}
		ownershipChanged = ownershipChanged || serviceID != m.ServiceID || volumeID != m.VolumeID
	}
	if ownershipChanged {
		return false, nil
	}
	if busy, e := p.retryPending(ctx, rc, s); busy || e != nil {
		return false, e
	}
	p.cleanupConnections(ctx, rc)
	// Stop every reachable local process before deleting any runtime. There is no
	// peer removal or leadership transfer in a whole-cluster shutdown.
	if busy, e := p.stopForSuspension(ctx, rc, s); busy || e != nil {
		return false, e
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
				exists = exists || v.Member == m.Name
			}
			if !exists {
				return false, rc.Store.CreateRetainedVolume(ctx, &domain.RetainedVolume{Namespace: rc.Cluster.Ref().Namespace, Cluster: rc.Cluster.Name, Member: m.Name, Hostname: m.Volume, ServiceID: volume.ID, Set: "openbao", Role: "openbao", Index: m.Index, RetainedAt: now(rc)})
			}
		} else {
			return false, p.submit(ctx, rc, s, &pending{Action: "cleanup-volume", Member: m.Name})
		}
	}
	if p.postgresStorage(rc.Cluster) && !s.BindingReleased {
		if p.options.Bindings == nil {
			return false, fmt.Errorf("PostgreSQL binding service is unavailable during cleanup")
		}
		request, requestErr := postgresBindingRequest(rc.Cluster)
		if requestErr != nil {
			return false, requestErr
		}
		releaseCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		defer cancel()
		if e = p.options.Bindings.Release(releaseCtx, pgbinding.BindingID(request), opts.KeepVolumes); e != nil {
			return false, fmt.Errorf("OpenBao PostgreSQL binding release failed")
		}
		s.BindingID = ""
		s.BindingReleased = true
		return false, nil
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
		for _, key := range []string{SecretUnseal, SecretManagementToken, SecretInitKey, SecretTransitToken, SecretAWSAccessKeyID, SecretAWSSecretAccessKey, SecretConnections, secretRecoveryCA, secretRecoveryClient, "tls/ca.crt", "tls/ca.key", "tls/user.crt", "tls/user.key"} {
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
			err = fmt.Errorf("runtime is not running")
		default:
			a, e := p.agent(ctx, rc, m.Name)
			err = e
			if e == nil {
				err = a.Maintenance(ctx, agentapi.Maintenance{Enabled: true})
				if err == nil {
					err = a.Process(ctx, agentapi.ProcessAction{Process: "openbao", Action: "stop"})
				}
				a.Close()
			}
		}
		if err != nil {
			if ctx.Err() != nil {
				return false, ctx.Err()
			}
			m.RemoveStep = "cleanup-platform-stop"
			m.CleanupStopReason = "Local process unavailable; awaiting terminal platform runtime deletion"
		} else {
			m.RemoveStep = "cleanup-stopped"
		}
		return true, nil
	}
	return false, nil
}
