// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

// orphanVolume transfers a rejected, ID-fenced deletion to the shared janitor.
// The member is not forgotten until this durable inventory write succeeds.
func orphanVolume(ctx context.Context, rc *engine.Context, m *member, cause error) error {
	if rc.Store == nil {
		return cause
	}
	detail := infra.SafeDetail(cause)
	return rc.Store.PutOrphanedVolume(ctx, &domain.OrphanedVolume{
		Namespace: rc.Cluster.Ref().Namespace, Cluster: rc.Cluster.Name,
		Member: m.Name, Hostname: m.Volume, ServiceID: m.VolumeID,
		FirstError: detail, LastError: detail,
		FirstFailedAt: now(rc), OrphanedAt: now(rc),
		NextAttemptAt: now(rc).Add(time.Minute),
	})
}

func (p *Core) retireMember(ctx context.Context, rc *engine.Context, s *state, m *member) (bool, error) {
	if err := verifyMember(ctx, rc, m); err != nil {
		return false, err
	}
	if m.Role == "data" && m.ServiceID != "" {
		drained, err := drainProxyBackend(ctx, rc, s, m)
		if err != nil || !drained {
			return false, err
		}
	}
	if m.ServiceID != "" {
		runtime, err := rc.Backend.GetService(ctx, m.Name)
		if err != nil {
			return false, err
		}
		if runtime == nil {
			m.ServiceID = ""
			return false, nil
		}
		if err := rc.Backend.DeletePublicPorts(ctx, m.Name); err != nil {
			return false, err
		}
		op, err := rc.Backend.DeleteServiceByID(ctx, m.Name, m.ServiceID)
		if err != nil {
			return false, err
		}
		p.track(s, m, "delete-runtime", op)
		return false, nil
	}
	if m.VolumeID != "" {
		volume, err := rc.Backend.GetService(ctx, m.Volume)
		if err != nil {
			return false, err
		}
		if volume == nil {
			m.VolumeID = ""
			return false, nil
		}
		op, err := rc.Backend.DeleteServiceByID(ctx, m.Volume, m.VolumeID)
		if err != nil {
			if !errors.Is(err, infra.ErrRejected) {
				return false, err
			}
			if err := orphanVolume(ctx, rc, m, err); err != nil {
				return false, err
			}
			m.VolumeID = ""
			m.Volume = ""
			return false, nil
		}
		p.track(s, m, "delete-volume", op)
		return false, nil
	}
	if err := rc.Secrets.DeleteAgentToken(ctx, rc.Cluster.Ref(), m.Name); err != nil && !missing(err) {
		return false, err
	}
	for _, kind := range []string{"agents", "servers"} {
		for _, suffix := range []string{".crt", ".key", ".pending.crt", ".pending.key"} {
			if err := rc.Secrets.DeleteClusterSecret(ctx, rc.Cluster.Ref(), "tls/"+kind+"/"+m.Name+suffix); err != nil && !missing(err) {
				return false, err
			}
		}
	}
	for i, peer := range s.Members {
		if peer == m {
			s.Members = append(s.Members[:i], s.Members[i+1:]...)
			break
		}
	}
	s.ConfigVersion++
	return true, nil
}

// drainProxyBackend removes a retiring hostname while it is still resolvable.
// Otherwise a stale ProxySQL monitor task can block on DNS after infrastructure
// deletion and falsely demote an unrelated healthy writer.
func drainProxyBackend(ctx context.Context, rc *engine.Context, s *state, m *member) (bool, error) {
	if m.ProxyDrainVersion == 0 {
		m.Removing = true
		s.ConfigVersion++
		m.ProxyDrainVersion = s.ConfigVersion
		for _, proxy := range s.Members {
			if proxy.Role == "proxy" {
				proxy.ConfigVersion = s.ConfigVersion
			}
		}
		// Publish before issuing reloads: agents pull this durable snapshot.
		if err := save(rc.Cluster, s); err != nil {
			return false, err
		}
		return false, nil
	}
	for _, proxy := range s.Members {
		if proxy.Role != "proxy" || proxy.ServiceID == "" || proxy.Removing {
			continue
		}
		applied, err := proxyApplied(ctx, rc, proxy, m.ProxyDrainVersion, false)
		if err != nil || !applied {
			return false, err
		}
	}
	return true, nil
}
