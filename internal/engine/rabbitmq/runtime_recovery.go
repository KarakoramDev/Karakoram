// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package rabbitmq

import (
	"context"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

func (p *Provider) recoverRuntimes(ctx context.Context, rc *engine.Context, s *internalState) (bool, error) {
	if s.Suspended || rc.Cluster.Deleting {
		return false, nil
	}
	for _, m := range ordered(s) {
		if m.Removing || m.ResumeDeploy != nil || (m.Stage == "planned" || m.Stage == "imported") && (!m.RuntimeRecovery.Initialized || m.Resume) {
			continue
		}
		if m.Stage == "ready" {
			m.RuntimeRecovery.Initialized = true
			// The durable ready stage is already a native formation checkpoint,
			// including for state written before FormationVerified existed.
			m.FormationVerified = true
		}
		tok, err := rc.Secrets.GetAgentToken(ctx, rc.Cluster.Ref(), m.Name)
		if err != nil {
			return true, err
		}
		if tok == nil {
			return true, engine.ErrNotFound
		}
		out, err := engine.ReconcileRuntime(ctx, rc, engine.RuntimeMember{MemberService: engine.MemberService{Cluster: rc.Cluster.Name, Member: m.Name, Token: tok.Token, Volume: m.Volume, Resources: m.Resources, Containers: 1}, ServiceID: &m.ServiceID, VolumeID: m.VolumeID, Recovery: &m.RuntimeRecovery})
		if err != nil {
			return true, err
		}
		if out.VolumeLost {
			if s.ReplaceActive {
				continue
			}
			if rc.Cluster.Spec.Components.Engine.Replicas > 1 {
				survivors := []*member{}
				for _, survivor := range p.availableMembers(ctx, rc, s) {
					if survivor.Name != m.Name && !survivor.RuntimeRecovery.VolumeLost {
						survivors = append(survivors, survivor)
					}
				}
				if err := p.guardLostMajority(ctx, rc, s, m, survivors); err != nil {
					return true, err
				}
			}
			return true, engine.RecoverLostVolume(ctx, rc, m.Name, m.Volume, m.VolumeID, rc.Cluster.Spec.Components.Engine.Replicas > 1)
		}
		if out.Imported {
			m.Stage = "imported"
			m.PublicHash = ""
			m.ManagementPublic = false
		}
		if out.Busy {
			s.Phase = "runtime-recovery"
			return true, nil
		}
	}
	return false, nil
}
