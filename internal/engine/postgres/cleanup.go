// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"context"
	"errors"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"time"
)

// Cleanup removes owned infrastructure, retaining data and credentials on suspension.
func (p *Core) Cleanup(ctx context.Context, rc *engine.Context, opts domain.DeleteOptions) (done bool, err error) {
	s, err := load(rc.Cluster)
	if err != nil {
		return false, err
	}
	if err := seedExposureAdmission(ctx, rc, s); err != nil {
		return false, err
	}
	// Legacy acknowledged private policy is recoverable before cleanup. If
	// neither routes nor an acknowledgement survive, Resolve stays fail-closed.
	if rc.Cluster.Status.ObservedGeneration == rc.Cluster.Generation {
		s.ExposureAdmission.Seed(rc.Cluster.Spec.Exposure)
	}
	if err := save(rc.Cluster, s); err != nil {
		return false, err
	}
	identities := make(map[string]string, len(s.Members))
	for _, m := range s.Members {
		identities[m.Name] = m.ServiceID
	}
	ctx = infra.WithCleanupIntent(ctx, infra.CleanupIntent{
		FullDelete: rc.Cluster.Deleting && !opts.KeepVolumes,
		Services:   identities,
	})
	// Backend-owned deletion fences survive this whole-cluster takeover.
	for _, m := range s.Members {
		m.UnplacedRetirement = nil
	}
	rc.Cluster.Status.Message = "Removing PostgreSQL runtime services and owned backup data"
	defer func() {
		if e := save(rc.Cluster, s); err == nil {
			err = e
		}
	}()
	if s.Pending != nil && engine.CleanupSupersedesDeployment(rc.Backend, s.Pending.Op) {
		s.Pending = nil
	}
	for _, m := range s.Members {
		if m.RuntimeRecovery.Op == nil && m.RuntimeRecovery.ImportPlan == nil {
			continue
		}
		token, e := rc.Secrets.GetAgentToken(ctx, rc.Cluster.Ref(), m.Name)
		if e != nil {
			return false, e
		}
		if token == nil {
			return false, engine.ErrNotFound
		}
		out, e := engine.ObserveRuntimeRecovery(ctx, rc, engine.RuntimeMember{MemberService: engine.MemberService{Cluster: rc.Cluster.Name, Member: m.Name, Token: token.Token, Volume: m.Volume, Resources: m.Resources, Containers: max(1, m.Replicas)}, ServiceID: &m.ServiceID, VolumeID: m.VolumeID, Recovery: &m.RuntimeRecovery})
		if e != nil || out.Busy {
			return false, e
		}
	}
	if s.Pending == nil {
		for _, m := range s.Members {
			if m.ImportBatch == nil {
				continue
			}
			op, e := resumePlannedImport(ctx, rc, m)
			if e != nil {
				return false, e
			}
			if op != nil {
				p.track(s, m, "import", *op)
				return false, nil
			}
			// No incomplete reservation remains. Cleanup may remove completed
			// services, or discard an intent which never submitted any setup.
			m.ImportBatch = nil
		}
	}
	if busy, e := p.observePending(ctx, rc, s); busy || e != nil {
		return false, e
	}
	for _, m := range s.Members {
		if err = verifyMember(ctx, rc, m); err != nil {
			return false, err
		}
		m.DeployOp = nil
		runtime, e := rc.Backend.GetService(ctx, m.Name)
		if e != nil {
			return false, e
		}
		if runtime != nil {
			if e = rc.Backend.DeletePublicPorts(ctx, m.Name); e != nil {
				return false, e
			}
			op, e := rc.Backend.DeleteServiceByID(ctx, m.Name, m.ServiceID)
			if e != nil {
				return false, e
			}
			p.track(s, m, "delete-runtime", op)
			return false, nil
		}
		if m.Volume == "" {
			continue
		}
		volume, e := rc.Backend.GetService(ctx, m.Volume)
		if e != nil {
			return false, e
		}
		if volume == nil {
			continue
		}
		if opts.KeepVolumes {
			if rc.Store != nil {
				retained, e := rc.Store.ListRetainedVolumes(ctx, rc.Cluster.Ref())
				if e != nil {
					return false, e
				}
				found := false
				for _, v := range retained {
					found = found || v.Member == m.Name && v.ServiceID == volume.ID
				}
				if !found {
					if e = rc.Store.CreateRetainedVolume(ctx, &domain.RetainedVolume{Namespace: rc.Cluster.Ref().Namespace, Cluster: rc.Cluster.Name, Member: m.Name, Hostname: m.Volume, ServiceID: volume.ID, Role: m.Role, Index: m.Index, RetainedAt: now(rc)}); e != nil {
						return false, e
					}
				}
			}
			continue
		}
		op, e := rc.Backend.DeleteServiceByID(ctx, m.Volume, m.VolumeID)
		if e != nil {
			if !errors.Is(e, infra.ErrRejected) || rc.Store == nil {
				return false, e
			}
			if e = rc.Store.PutOrphanedVolume(ctx, &domain.OrphanedVolume{Namespace: rc.Cluster.Ref().Namespace, Hostname: m.Volume, ServiceID: m.VolumeID, Cluster: rc.Cluster.Name, Member: m.Name, FirstError: infra.SafeDetail(e), LastError: infra.SafeDetail(e), FirstFailedAt: now(rc), OrphanedAt: now(rc), NextAttemptAt: now(rc).Add(time.Minute)}); e != nil {
				return false, e
			}
			m.Volume = ""
			m.VolumeID = ""
			return false, nil
		}
		p.track(s, m, "delete-volume", op)
		return false, nil
	}
	if complete, e := p.cleanupBackups(ctx, rc, s, opts); e != nil || !complete {
		return false, e
	}
	if opts.KeepVolumes {
		s.Suspended = true
		rc.Cluster.Status.Phase = domain.PhaseSuspended
		return true, nil
	}
	for _, m := range s.Members {
		if err = rc.Secrets.DeleteAgentToken(ctx, rc.Cluster.Ref(), m.Name); err != nil && !missing(err) {
			return false, err
		}
		for _, kind := range []string{"agents", "servers"} {
			for _, suffix := range []string{".crt", ".key", ".pending.crt", ".pending.key"} {
				if err = rc.Secrets.DeleteClusterSecret(ctx, rc.Cluster.Ref(), "tls/"+kind+"/"+m.Name+suffix); err != nil && !missing(err) {
					return false, err
				}
			}
		}
		if rc.Store != nil {
			if err = rc.Store.DeleteRetainedVolume(ctx, rc.Cluster.Ref(), m.Name); err != nil && !missing(err) {
				return false, err
			}
		}
	}
	keys := []string{"tls/ca.crt", "tls/ca.key", "tls/user.crt", "tls/user.key"}
	for _, name := range systemNames {
		keys = append(keys, "users/"+name)
	}
	for _, key := range keys {
		if err = rc.Secrets.DeleteClusterSecret(ctx, rc.Cluster.Ref(), key); err != nil && !missing(err) {
			return false, err
		}
	}
	return true, nil
}

func resumePlannedImport(ctx context.Context, rc *engine.Context, m *member) (*infra.Op, error) {
	if m.ImportBatch == nil {
		for _, name := range []string{m.Volume, m.Name} {
			if name == "" {
				continue
			}
			service, err := rc.Backend.GetService(ctx, name)
			if err != nil {
				return nil, err
			}
			if service != nil && service.Importing {
				return nil, engine.Invalid("incomplete import for %s has no frozen batch; recover the original import intent before continuing", m.Name)
			}
		}
		return nil, nil
	}
	token, err := rc.Secrets.GetAgentToken(ctx, rc.Cluster.Ref(), m.Name)
	if err != nil {
		return nil, err
	}
	if token == nil {
		return nil, engine.ErrNotFound
	}
	return engine.ResumeImportPlan(ctx, rc, m.ImportBatch, m.Name, token.Token)
}
