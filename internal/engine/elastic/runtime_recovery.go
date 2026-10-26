// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package elastic

import (
	"context"
	"fmt"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/elastic/esapi"
)

func (p *Provider) recoverRuntimes(ctx context.Context, rc *engine.Context, s *state) (bool, error) {
	if s.Suspended || s.RestoreActive || rc.Cluster.Deleting {
		return false, nil
	}
	for _, m := range s.Members {
		if m.Removing || (m.Stage == stagePlanned || m.Stage == stageImported) && !m.RuntimeRecovery.Initialized {
			continue
		}
		if component(rc.Cluster.Spec, m).Replicas == 0 {
			continue
		}
		if m.Stage == stageJoined && m.Volume != "" {
			m.RuntimeRecovery.Initialized = true
		}
		tok, err := rc.Secrets.GetAgentToken(ctx, rc.Cluster.Ref(), m.Name)
		if err != nil {
			return true, err
		}
		if tok == nil {
			return true, engine.ErrNotFound
		}
		out, err := engine.ReconcileRuntime(ctx, rc, engine.RuntimeMember{MemberService: engine.MemberService{Cluster: rc.Cluster.Name, Member: m.Name, Token: tok.Token, Volume: m.Volume, Resources: m.Resources, Containers: max(1, m.Containers)}, ServiceID: &m.ServiceID, VolumeID: m.VolumeID, Recovery: &m.RuntimeRecovery})
		if err != nil {
			return true, err
		}
		if out.VolumeLost {
			replacing := false
			for _, next := range s.Members {
				if next.Replaces == m.Name {
					replacing = true
					break
				}
			}
			if replacing {
				continue
			}
			if len(s.esNodes()) > 1 {
				if err := p.volumeRecoverySafe(ctx, rc, s, m); err != nil {
					return true, err
				}
			}
			return true, engine.RecoverLostVolume(ctx, rc, m.Name, m.Volume, m.VolumeID, len(s.esNodes()) > 1)
		}
		if out.Imported {
			m.Stage = stageImported
			m.ExposureHash = ""
			m.Subdomain = false
		}
		if out.Busy {
			s.Phase = "runtime-recovery"
			return true, nil
		}
	}
	return false, nil
}

func (p *Provider) volumeRecoverySafe(ctx context.Context, rc *engine.Context, s *state, lost *member) error {
	client, err := p.es(ctx, rc, s)
	if err != nil {
		return fmt.Errorf("%w: volume %s is lost and surviving Elasticsearch data cannot be verified; restore quorum or recover retained data before replacement", engine.ErrConflict, lost.Volume)
	}
	defer client.Close()
	health, err := client.Health(ctx, esapi.HealthOptions{})
	if err != nil || health.Status != "green" && health.Status != "yellow" {
		return fmt.Errorf("%w: volume %s is lost and not all primary shards have healthy copies; recover retained data or restore a snapshot before replacement", engine.ErrConflict, lost.Volume)
	}
	nodes, err := client.Nodes(ctx)
	if err != nil {
		return err
	}
	for _, node := range nodes {
		m := s.find(node.Name)
		if m != nil && m.Name != lost.Name && !m.Removing && !m.RuntimeRecovery.VolumeLost {
			return nil
		}
	}
	return fmt.Errorf("%w: volume %s is lost without a verified surviving Elasticsearch member; recover retained data or restore a snapshot", engine.ErrConflict, lost.Volume)
}
