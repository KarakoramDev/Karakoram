// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package psmdb

import (
	"context"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

func (p *Provider) recoverRuntimes(ctx context.Context, rc *engine.Context, s *internalState) (bool, error) {
	if s.Suspended || rc.Cluster.Deleting {
		return false, nil
	}
	for _, m := range ordered(s) {
		if m.Removing || (m.Stage == "planned" || m.Stage == "imported") && !m.RuntimeRecovery.Initialized {
			continue
		}
		if m.Stage == "active" {
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
			if s.ReplaceActive {
				continue
			}
			if len(s.setMembers(m.Set)) > 1 {
				if err := p.volumeRecoverySafe(ctx, rc, s, m); err != nil {
					return true, err
				}
			}
			return true, engine.RecoverLostVolume(ctx, rc, m.Name, m.Volume, m.VolumeID, len(s.setMembers(m.Set)) > 1)
		}
		if out.Imported {
			m.Stage = "imported"
			m.PublicHash = ""
		}
		if out.Busy {
			s.Phase = "runtime-recovery"
			return true, nil
		}
	}
	return false, nil
}
