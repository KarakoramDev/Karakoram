// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package psmdb

import (
	"context"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

func (*Provider) CanClaimOperation(c *domain.Cluster) (bool, error) {
	s, err := load(c)
	if err != nil {
		return false, err
	}
	for _, m := range s.Members {
		if m.UnplacedRetirement != nil || m.RuntimeRecovery.Op != nil || m.RuntimeRecovery.ImportPlan != nil || m.ImportPlan != nil {
			return false, nil
		}
	}
	return s.Pending == nil && s.Retry == nil, nil
}

func drainRuntimeRecovery(ctx context.Context, rc *engine.Context, s *internalState) (bool, error) {
	for _, m := range s.Members {
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
