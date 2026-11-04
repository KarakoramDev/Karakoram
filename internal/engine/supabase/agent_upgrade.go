// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package supabase

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/supabase/sbspec"
)

type agentUpgradeCheckpoint struct {
	Target   engine.AgentUpgradePayload `json:"target"`
	Core     json.RawMessage            `json:"core,omitempty"`
	CoreDone bool                       `json:"coreDone"`
	Order    []string                   `json:"order"`
	Index    int                        `json:"index"`
	Waiting  bool                       `json:"waiting"`
}

func (p *Provider) upgradeAgents(ctx context.Context, rc *engine.Context, op *domain.Operation, cancel bool) (result engine.OpResult, err error) {
	previousOperation := rc.Operation
	rc.Operation = op
	defer func() { rc.Operation = previousOperation }()
	var cp agentUpgradeCheckpoint
	if len(op.Checkpoint) > 0 {
		if err := json.Unmarshal(op.Checkpoint, &cp); err != nil {
			return result, err
		}
	} else {
		cp.Target, err = engine.AgentUpgradeTarget(op)
		if err != nil {
			return result, err
		}
		s, e := loadState(rc.Cluster)
		if e != nil {
			return result, e
		}
		cp.Order = []string{}
		for _, def := range stackComponents {
			if m := s.Members[def.Name]; m != nil {
				cp.Order = append(cp.Order, def.Name)
			}
		}
	}
	checkpoint := func() (engine.OpResult, error) {
		raw, e := json.Marshal(cp)
		return engine.OpResult{Checkpoint: raw, Step: "upgrade-agents", RequeueAfter: 2 * time.Second}, e
	}
	if !cp.CoreDone {
		coreOp := *op
		coreOp.Checkpoint = cp.Core
		var next engine.OpResult
		if cancel {
			next, err = p.Core.CancelOperation(ctx, rc, &coreOp)
		} else {
			next, err = p.Core.RunOperation(ctx, rc, &coreOp)
		}
		if err != nil {
			return result, err
		}
		cp.Core = next.Checkpoint
		cp.CoreDone = next.Done
		if cancel && next.Done {
			return engine.OpResult{Done: true}, nil
		}
		return checkpoint()
	}
	if cancel && !cp.Waiting {
		return engine.OpResult{Done: true}, nil
	}
	if cp.Index >= len(cp.Order) {
		return engine.OpResult{Done: true}, nil
	}
	s, err := loadState(rc.Cluster)
	if err != nil {
		return result, err
	}
	defer func() {
		if e := saveState(rc.Cluster, s); err == nil {
			err = e
		}
	}()
	m := s.Members[cp.Order[cp.Index]]
	if m == nil {
		return result, engine.ErrNotFound
	}
	if cp.Waiting {
		if busy, e := p.observeComponent(ctx, rc, s); busy || e != nil {
			returned, ce := checkpoint()
			return returned, errors.Join(e, ce)
		}
		status, identity, e := componentStatusObserved(ctx, rc, m)
		if e != nil {
			return result, e
		}
		if status.Ready && identity.Build == cp.Target.TargetBuild {
			m.Stage = engine.ComponentJoined
			m.NativeReadiness = nil
			cp.Index++
			cp.Waiting = false
		}
		return checkpoint()
	}
	if cp.Target.TargetBuild != engine.CurrentAgentBuild().Build {
		return result, engine.Invalid("agent upgrade target build %q differs from running control-plane build %q", cp.Target.TargetBuild, engine.CurrentAgentBuild().Build)
	}
	status, identity, e := componentStatusObserved(ctx, rc, m)
	if e != nil {
		return result, e
	}
	if !status.Ready {
		return result, engine.ErrUnavailable
	}
	if !engine.MemberNeedsAgentUpgrade(domain.MemberStatus{Agent: domain.AgentStatus{Healthy: true, Build: identity.Build}}, cp.Target) {
		cp.Index++
		return checkpoint()
	}
	raw, e := rc.Secrets.GetClusterSecret(ctx, rc.Cluster.Ref(), "supabase/config/"+m.Name)
	if e != nil {
		return result, e
	}
	var config agentapi.MemberConfig
	if e = json.Unmarshal(raw, &config); e != nil {
		return result, e
	}
	var sp sbspec.Spec
	if e = json.Unmarshal(config.EngineSpec, &sp); e != nil {
		return result, e
	}
	if sp.Version != m.Version {
		return result, fmt.Errorf("component %s applied version differs from published configuration", m.Name)
	}
	if wait := rc.WaitForWindow(); wait.RequeueAfter > 0 {
		result, err := checkpoint()
		result.RequeueAfter = wait.RequeueAfter
		result.Message = "Waiting for maintenance window"
		return result, err
	}
	if e = p.deployComponent(ctx, rc, s, m, sp); e != nil {
		return result, e
	}
	cp.Waiting = true
	return checkpoint()
}

func (p *Provider) CancelOperation(ctx context.Context, rc *engine.Context, op *domain.Operation) (engine.OpResult, error) {
	if op.Kind == domain.OperationUpgradeAgents {
		return p.upgradeAgents(ctx, rc, op, true)
	}
	return p.Core.CancelOperation(ctx, rc, op)
}

func (p *Provider) CanClaimOperation(c *domain.Cluster) (bool, error) {
	allowed, err := p.Core.CanClaimOperation(c)
	if err != nil || !allowed {
		return allowed, err
	}
	s, err := loadState(c)
	if err != nil {
		return false, err
	}
	if s.Pending != nil {
		return false, nil
	}
	for _, m := range s.Members {
		if m.ImportBatch != nil || m.RuntimeRecovery.Op != nil || m.RuntimeRecovery.ImportPlan != nil {
			return false, nil
		}
	}
	return true, nil
}
