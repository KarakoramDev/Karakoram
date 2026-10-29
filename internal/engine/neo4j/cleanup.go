// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package neo4j

import (
	"context"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

func (p *Provider) Cleanup(ctx context.Context, rc *engine.Context, opts domain.DeleteOptions) (done bool, err error) {
	s, err := load(rc.Cluster)
	if err != nil {
		return false, err
	}
	defer func() {
		if e := save(rc.Cluster, s); err == nil {
			err = e
		}
	}()
	if err = p.cleanupBackups(ctx, rc, opts); err != nil {
		return false, err
	}
	m := s.Member
	if m == nil {
		return true, nil
	}
	if m.Public || m.Exposure.Public {
		s.ExposureAdmission.Seed(m.Exposure)
	}
	if err = save(rc.Cluster, s); err != nil {
		return false, err
	}
	if m.Pending != nil && engine.CleanupSupersedesDeployment(rc.Backend, *m.Pending) {
		m.Pending = nil
	}
	if m.RuntimeRecovery.Op != nil || m.RuntimeRecovery.ImportPlan != nil {
		tok, e := rc.Secrets.GetAgentToken(ctx, rc.Cluster.Ref(), m.Name)
		if e != nil {
			return false, e
		}
		if tok == nil {
			return false, engine.ErrNotFound
		}
		out, e := engine.ObserveRuntimeRecovery(ctx, rc, engine.RuntimeMember{
			MemberService: engine.MemberService{Cluster: rc.Cluster.Name, Member: m.Name, Token: tok.Token, Volume: m.Volume, Resources: m.Resources, Containers: 1},
			ServiceID:     &m.ServiceID, VolumeID: m.VolumeID, Recovery: &m.RuntimeRecovery,
		})
		if e != nil || out.Busy {
			return false, e
		}
	}
	if busy, e := observePending(ctx, rc, s); busy || e != nil {
		return false, e
	}
	if m.ImportBatch != nil {
		tok, e := rc.Secrets.GetAgentToken(ctx, rc.Cluster.Ref(), m.Name)
		if e != nil {
			return false, e
		}
		if tok == nil {
			return false, engine.ErrNotFound
		}
		op, e := engine.ResumeImportPlan(ctx, rc, m.ImportBatch, m.Name, tok.Token)
		if e != nil {
			return false, e
		}
		if op != nil {
			track(m, "import", *op)
			return false, nil
		}
		m.ImportBatch = nil
	}
	if err = verify(ctx, rc, m); err != nil {
		return false, err
	}
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
		track(m, "delete-runtime", op)
		m.Public = false
		return false, nil
	}
	volume, e := rc.Backend.GetService(ctx, m.Volume)
	if e != nil {
		return false, e
	}
	if volume != nil {
		if opts.KeepVolumes {
			if rc.Store != nil {
				existing, e := rc.Store.ListRetainedVolumes(ctx, rc.Cluster.Ref())
				if e != nil {
					return false, e
				}
				found := false
				for _, v := range existing {
					found = found || v.Member == m.Name && v.ServiceID == volume.ID
				}
				if !found {
					if e = rc.Store.CreateRetainedVolume(ctx, &domain.RetainedVolume{Namespace: rc.Cluster.Ref().Namespace, Cluster: rc.Cluster.Name, Member: m.Name, Hostname: m.Volume, ServiceID: volume.ID, Role: "single", RetainedAt: now(rc)}); e != nil {
						return false, e
					}
				}
			}
			s.Suspended = true
			return true, nil
		}
		op, e := rc.Backend.DeleteServiceByID(ctx, m.Volume, m.VolumeID)
		if e != nil {
			return false, e
		}
		track(m, "delete-volume", op)
		return false, nil
	}
	if rc.Store != nil {
		if err = rc.Store.DeleteRetainedVolume(ctx, rc.Cluster.Ref(), m.Name); err != nil && !missing(err) {
			return false, err
		}
	}
	if err = rc.Secrets.DeleteAgentToken(ctx, rc.Cluster.Ref(), m.Name); err != nil && !missing(err) {
		return false, err
	}
	for _, key := range []string{"tls/ca.crt", "tls/ca.key", "users/neo4j", "tls/agents/" + m.Name + ".crt", "tls/agents/" + m.Name + ".key", "tls/servers/" + m.Name + ".crt", "tls/servers/" + m.Name + ".key"} {
		if err = rc.Secrets.DeleteClusterSecret(ctx, rc.Cluster.Ref(), key); err != nil && !missing(err) {
			return false, err
		}
	}
	s.ExposureAdmission = engine.ExposureAdmission{}
	return true, nil
}
