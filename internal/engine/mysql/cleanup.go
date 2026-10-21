// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

func (p *Core) Cleanup(ctx context.Context, rc *engine.Context, opts domain.DeleteOptions) (done bool, err error) {
	p.accessMu.Lock()
	defer p.accessMu.Unlock()
	if e := p.finishCleanupAccess(ctx, rc); e != nil {
		return false, e
	}
	s, err := load(rc.Cluster)
	if err != nil {
		return false, err
	}
	defer func() {
		if e := save(rc.Cluster, s); err == nil {
			err = e
		}
	}()
	if s.Pending != nil && engine.CleanupSupersedesDeployment(rc.Backend, s.Pending.Op) {
		s.Pending = nil
	}
	if s.Pending == nil {
		for _, m := range s.Members {
			if m.ImportBatch != nil {
				op, e := resumePlannedImport(ctx, rc, m)
				if e != nil {
					return false, e
				}
				if op != nil {
					p.track(s, m, "import", *op)
					return false, nil
				}
				m.ImportBatch = nil
			}
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
			return false, e
		}
		p.track(s, m, "delete-volume", op)
		return false, nil
	}
	if e := finishGaleraTLSCleanup(ctx, rc, s, opts.KeepVolumes); e != nil {
		return false, e
	}
	if done, e := p.cleanupBackups(ctx, rc, s, opts); !done || e != nil {
		return false, e
	}
	if opts.KeepVolumes {
		s.Suspended = true
		rc.Cluster.Status.Phase = domain.PhaseSuspended
		return true, nil
	}
	for _, m := range s.Members {
		if e := rc.Secrets.DeleteAgentToken(ctx, rc.Cluster.Ref(), m.Name); e != nil && !missing(e) {
			return false, e
		}
		for _, kind := range []string{"agents", "servers"} {
			for _, suffix := range []string{".crt", ".key"} {
				if e := rc.Secrets.DeleteClusterSecret(ctx, rc.Cluster.Ref(), "tls/"+kind+"/"+m.Name+suffix); e != nil && !missing(e) {
					return false, e
				}
			}
		}
		if rc.Store != nil {
			if e := rc.Store.DeleteRetainedVolume(ctx, rc.Cluster.Ref(), m.Name); e != nil && !missing(e) {
				return false, e
			}
		}
	}
	keys := []string{"tls/ca.crt", "tls/ca.key", "mysql/access/proxy-users", "mysql/access/credentials", "mysql/access/revision", "mysql/access/pending"}
	for _, name := range systemNames {
		keys = append(keys, "users/"+name)
	}
	for _, key := range keys {
		if e := rc.Secrets.DeleteClusterSecret(ctx, rc.Cluster.Ref(), key); e != nil && !missing(e) {
			return false, e
		}
	}
	return true, nil
}
