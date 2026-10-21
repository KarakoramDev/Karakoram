// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package openbao

import (
	"context"
	"encoding/json"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

func drainRuntimeRecovery(ctx context.Context, rc *engine.Context, s *internalState) (bool, error) {
	for _, m := range ordered(s) {
		if m.RuntimeRecovery.Op == nil && m.RuntimeRecovery.ImportPlan == nil && m.ImportPlan == nil {
			continue
		}
		tok, err := rc.Secrets.GetAgentToken(ctx, rc.Cluster.Ref(), m.Name)
		if err != nil {
			return true, err
		}
		if tok == nil {
			return true, engine.ErrNotFound
		}
		if m.ImportPlan != nil {
			if s.Retry != nil && s.Retry.Member == m.Name && s.Retry.Action == "import" {
				if r := s.PlatformRetries[platformRetryKey(s.Retry)]; r != nil && r.RetryAfter.After(m.ImportPlan.RetryAt) {
					m.ImportPlan.RetryAt = r.RetryAfter
					m.ImportPlan.RetrySchedule = r.RetrySchedule
				}
			}
			op, err := engine.ResumeImportPlan(ctx, rc, m.ImportPlan, m.Name, tok.Token)
			if err != nil {
				return true, err
			}
			if op != nil {
				s.Pending = &pending{Op: *op, Action: "import", Member: m.Name}
				return true, nil
			}
			if m.RuntimeRecovery.Op == nil && m.RuntimeRecovery.ImportPlan == nil {
				continue
			}
		}
		out, err := engine.ObserveRuntimeRecovery(ctx, rc, engine.RuntimeMember{MemberService: engine.MemberService{Cluster: rc.Cluster.Name, Member: m.Name, Token: tok.Token}, ServiceID: &m.ServiceID, Recovery: &m.RuntimeRecovery})
		if out.Imported {
			m.Stage = "imported"
			m.PublicHash = ""
		}
		return true, err
	}
	return false, nil
}
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
			if s.Replacing {
				owned, err := replacementOwnsRecovery(ctx, rc, s, m.Name)
				if err != nil {
					return true, err
				}
				if owned {
					continue
				}
			}
			return true, engine.RecoverLostVolume(ctx, rc, m.Name, m.Volume, m.VolumeID, rc.Cluster.Spec.Components.Engine.Replicas > 1)
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

// Only the replacement of this exact lost member may bypass its recovery gate.
// The old peer remains present until its replacement has joined as a healthy voter.
func replacementOwnsRecovery(ctx context.Context, rc *engine.Context, s *internalState, name string) (bool, error) {
	operations, ok := rc.Store.(interface {
		ListOperations(context.Context, domain.ClusterRef, ...domain.Phase) ([]domain.Operation, error)
	})
	if !ok {
		return false, nil
	}
	ops, err := operations.ListOperations(ctx, rc.Cluster.Ref(), domain.PhaseRunning)
	if err != nil {
		return false, err
	}
	for _, op := range ops {
		if op.Kind != domain.OperationReplaceMember {
			continue
		}
		var payload struct {
			Member string `json:"member"`
		}
		var cp operationState
		if json.Unmarshal(op.Payload, &payload) == nil && json.Unmarshal(op.Checkpoint, &cp) == nil &&
			payload.Member == name && cp.Stage == "join" && s.find(cp.NewMember) != nil {
			return true, nil
		}
	}
	return false, nil
}
