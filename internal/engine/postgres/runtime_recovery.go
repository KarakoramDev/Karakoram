// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"context"
	"fmt"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

func (p *Core) recoverRuntimes(ctx context.Context, rc *engine.Context, s *state) (bool, error) {
	if s.Suspended || rc.Cluster.Deleting {
		return false, nil
	}
	for _, m := range s.Members {
		if m.Removing || m.Offline || (m.Stage == "planned" || m.Stage == "imported") && !m.RuntimeRecovery.Initialized || desiredComponent(rc, m).Replicas == 0 {
			continue
		}
		if m.Stage == "joined" && m.Volume != "" {
			m.RuntimeRecovery.Initialized = true
		}
		tok, err := rc.Secrets.GetAgentToken(ctx, rc.Cluster.Ref(), m.Name)
		if err != nil {
			return true, err
		}
		if tok == nil {
			return true, engine.ErrNotFound
		}
		out, err := engine.ReconcileRuntime(ctx, rc, engine.RuntimeMember{MemberService: engine.MemberService{Cluster: rc.Cluster.Name, Member: m.Name, Token: tok.Token, Volume: m.Volume, Resources: m.Resources, Containers: max(1, m.Replicas)}, ServiceID: &m.ServiceID, VolumeID: m.VolumeID, Recovery: &m.RuntimeRecovery})
		if err != nil {
			return true, err
		}
		if out.VolumeLost {
			if s.Replacement != nil && s.Replacement.Old == m.Name {
				continue
			}
			if patroniTopology(s.Topology) && m.Role == "data" {
				if err := p.volumeRecoverySafe(ctx, rc, s, m); err != nil {
					return true, err
				}
			}
			return true, engine.RecoverLostVolume(ctx, rc, m.Name, m.Volume, m.VolumeID, patroniTopology(s.Topology) && m.Role == "data")
		}
		if out.Imported {
			m.Stage = "imported"
			m.DeployOp = nil
			m.Public = false
			m.ProxyReady = false
		}
		if out.Busy {
			return true, nil
		}
	}
	return false, nil
}
func (p *Core) volumeRecoverySafe(ctx context.Context, rc *engine.Context, s *state, lost *member) error {
	for _, m := range s.group(lost.Group).data() {
		if m.Name == lost.Name || m.Removing || m.Offline || m.RuntimeRecovery.VolumeLost {
			continue
		}
		status, err := live(ctx, rc, m.Name)
		if err == nil && status.Running && status.Role == "primary" && status.EtcdHealthy {
			return nil
		}
	}
	return fmt.Errorf("%w: volume %s is lost and no surviving writable primary with healthy etcd quorum is verified; recover retained data/quorum or restore a backup before replacing %s", engine.ErrConflict, lost.Volume, lost.Name)
}
