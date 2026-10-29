// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package weaviate

import (
	"context"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

// A failed artifact cleanup retains inventory and must not strand teardown.
func (p *Provider) cleanupOwnedBackups(ctx context.Context, rc *engine.Context, keep bool) (bool, error) {
	if keep || rc.Store == nil {
		return true, nil
	}
	backups, err := rc.Store.ListBackups(ctx, rc.Cluster.Ref())
	if err != nil {
		return false, err
	}
	s, err := load(rc.Cluster)
	if err != nil {
		return false, err
	}
	for _, b := range backups {
		if b.Weaviate == nil || b.Weaviate.ClusterUID != s.UID {
			continue
		}
		storage, e := rc.Store.GetBackupStorage(ctx, rc.Cluster.Ref().Namespace, b.Storage)
		if e == nil {
			var stopped bool
			stopped, e = p.cancelBackupWriter(ctx, rc, s, &b, storage)
			if e == nil && !stopped {
				return false, nil
			}
		}
		if e == nil {
			b.Phase = domain.PhaseDeleting
			e = p.DeleteRetainedBackup(ctx, *storage, b)
		}
		if e != nil {
			p.options.Logger.WarnContext(ctx, "Weaviate backup cleanup unavailable; retaining backup inventory", "backup", b.Name)
			continue
		}
		if e = rc.Store.DeleteBackup(ctx, rc.Cluster.Ref(), b.Name); e != nil && !missing(e) {
			return false, e
		}
	}
	return true, nil
}
func (p *Provider) Cleanup(ctx context.Context, rc *engine.Context, opts domain.DeleteOptions) (done bool, err error) {
	s, e := load(rc.Cluster)
	if e != nil {
		return false, e
	}
	defer func() {
		if e := s.save(rc.Cluster); err == nil {
			err = e
		}
	}()
	if e := s.seedExposure(ctx, rc); e != nil {
		return false, e
	}
	if e := s.save(rc.Cluster); e != nil {
		return false, e
	}
	// This phase is committed before removing any ownership credential.
	// Replays must not revisit service verification with partially deleted tokens.
	if s.Phase == "cleanup-secrets" {
		// The checkpoint proves runtime/public-route teardown already completed,
		// including when resuming an older checkpoint that retained assignments.
		p.releaseClusterPublicReservations(rc, s)
		return cleanupSecrets(ctx, rc, s)
	}
	s.Phase = "cleanup"
	if s.Pending != nil && s.Pending.Action == "deploy" && s.Pending.Op == nil {
		s.Pending = nil
	}
	if busy, e := drainRuntimeRecovery(ctx, rc, s); busy || e != nil {
		return false, e
	}
	if s.Pending != nil && s.Pending.Op != nil && engine.CleanupSupersedesDeployment(rc.Backend, *s.Pending.Op) {
		s.Pending = nil
		return false, nil
	}
	if busy, e := p.advancePending(ctx, rc, s); busy || e != nil {
		return false, e
	}
	// Resume durable imports before deleting a runtime that might still be created.
	for _, m := range ordered(s) {
		if m.ImportPlan != nil {
			s.Pending = &pending{Action: "import", Member: m.Name}
			return false, nil
		}
	}
	if done, e := p.cleanupOwnedBackups(ctx, rc, opts.KeepBackups); e != nil || !done {
		return false, e
	}
	// Stop and remove every runtime before touching any volume.
	for _, m := range ordered(s) {
		if e = verifyMemberServices(ctx, rc, m); e != nil {
			return false, e
		}
		runtime, e := rc.Backend.GetService(ctx, m.Name)
		if e != nil {
			return false, e
		}
		if runtime != nil && m.CleanupStep != "stopped" {
			s.Pending = &pending{Action: "stop", Member: m.Name}
			return false, nil
		}
		routes, e := rc.Backend.ListPublicPorts(ctx)
		if e != nil {
			return false, e
		}
		for _, route := range routes {
			if route.Hostname == m.Name {
				if e = verifyMemberServices(ctx, rc, m); e != nil {
					return false, e
				}
				return false, rc.Backend.DeletePublicPorts(ctx, m.Name)
			}
		}
		if runtime == nil {
			continue
		}
		s.Pending = &pending{Action: "delete-runtime", Member: m.Name}
		return false, nil
	}
	// Volumes and credentials do not hold listeners. Suspension must also clear
	// assignments so a later resume allocates against the current namespace pool.
	p.releaseClusterPublicReservations(rc, s)
	retained, e := rc.Store.ListRetainedVolumes(ctx, rc.Cluster.Ref())
	if e != nil {
		return false, e
	}
	for _, m := range ordered(s) {
		volume, e := rc.Backend.GetService(ctx, m.Volume)
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
				return false, rc.Store.CreateRetainedVolume(ctx, &domain.RetainedVolume{Namespace: rc.Cluster.Ref().Namespace, Cluster: rc.Cluster.Name, Member: m.Name, Hostname: m.Volume, ServiceID: m.VolumeID, Set: "weaviate", Role: "weaviate", Index: m.Index, RetainedAt: now(rc)})
			}
			continue
		}
		s.Pending = &pending{Action: "delete-volume", Member: m.Name}
		return false, nil
	}
	if opts.KeepVolumes {
		s.Suspended = true
		rc.Cluster.Status.Phase = domain.PhaseSuspended
		return true, nil
	}
	s.Phase = "cleanup-secrets"
	return false, nil
}

func cleanupSecrets(ctx context.Context, rc *engine.Context, s *state) (bool, error) {
	if !s.CleanupSecrets {
		retained, e := rc.Store.ListRetainedVolumes(ctx, rc.Cluster.Ref())
		if e != nil {
			return false, e
		}
		for _, m := range s.Members {
			if e = deleteMemberSecrets(ctx, rc, m.Name); e != nil && !missing(e) {
				return false, e
			}
		}
		for _, key := range []string{secretAdmin, secretClusterPassword, secretCA, secretCAKey, rotationOld, rotationNew} {
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
