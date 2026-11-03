// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package keycloak

import (
	"context"
	"encoding/json"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type agentUpgradeCheckpoint struct {
	Target    engine.AgentUpgradePayload `json:"target"`
	Order     []string                   `json:"order"`
	Done      []string                   `json:"done,omitempty"`
	Waiting   bool                       `json:"waiting,omitempty"`
	Submitted bool                       `json:"submitted,omitempty"`
	Deploy    *infra.Op                  `json:"deploy,omitempty"`
}

// SyncDuringOperation prevents normal convergence from changing the frozen runtime.
func (*Provider) SyncDuringOperation(*domain.Operation) bool { return false }

func (p *Provider) RunOperation(ctx context.Context, rc *engine.Context, op *domain.Operation) (engine.OpResult, error) {
	if op.Kind != domain.OperationUpgradeAgents {
		return engine.OpResult{}, engine.ErrNotSupported
	}
	previous := rc.Operation
	rc.Operation = op
	defer func() { rc.Operation = previous }()
	var cp agentUpgradeCheckpoint
	result := func(done bool) engine.OpResult {
		raw, _ := json.Marshal(cp)
		return engine.OpResult{Done: done, Step: "upgrade-agents", Checkpoint: raw, RequeueAfter: 2 * time.Second}
	}
	if len(op.Checkpoint) > 0 {
		if err := json.Unmarshal(op.Checkpoint, &cp); err != nil {
			return result(false), err
		}
	} else {
		target, err := engine.AgentUpgradeTarget(op)
		if err != nil {
			return result(false), err
		}
		cp.Target = target
	}
	if len(cp.Done) > 0 {
		return result(true), nil
	}
	s, err := load(rc.Cluster)
	if err != nil {
		return result(false), err
	}
	if !cp.Waiting && cp.Target.TargetBuild != engine.CurrentAgentBuild().Build {
		return result(false), engine.Invalid("agent upgrade target build differs from running control-plane build; re-run upgrade-agents")
	}
	if cp.Order == nil {
		if s.Pending != nil || s.Import != nil || !s.Deployed || s.ConfigVersion == 0 {
			return result(false), engine.Invalid("Keycloak agent upgrade requires a formed runtime with no pending structural work")
		}
		cp.Order = []string{s.Member}
		return result(false), nil
	}
	if len(cp.Order) != 1 || cp.Order[0] != s.Member {
		return result(false), engine.Invalid("Keycloak agent upgrade member changed")
	}
	if cp.Waiting && !cp.Submitted {
		if cp.Target.TargetBuild != engine.CurrentAgentBuild().Build {
			return result(false), engine.Invalid("agent upgrade target build differs from running control-plane build; re-run upgrade-agents")
		}
		if rc.WaitForWindow().RequeueAfter > 0 {
			return result(false), nil
		}
		runtime, e := p.runtime(rc, s)
		if e != nil {
			return result(false), e
		}
		deployment, e := engine.Deploy(ctx, rc, s.Member, runtime)
		if e != nil {
			return result(false), e
		}
		cp.Deploy = &deployment
		cp.Submitted = true
		return result(false), nil
	}
	if cp.Deploy != nil {
		st, e := rc.Backend.OpStatus(ctx, *cp.Deploy)
		if e != nil {
			return result(false), e
		}
		if st.Failed {
			return result(false), engine.Invalid("Keycloak agent deployment failed")
		}
		if !st.Done {
			return result(false), nil
		}
		cp.Deploy = nil
	}
	status, err := p.Status(ctx, rc)
	if err != nil {
		return result(false), err
	}
	if len(status.Members) != 1 {
		return result(false), engine.ErrUnavailable
	}
	m := status.Members[0]
	if cp.Waiting {
		if m.Health && m.Agent.Build == cp.Target.TargetBuild {
			cp.Done = append(cp.Done, s.Member)
			return result(true), nil
		}
		return result(false), nil
	}
	// The supported topology is a singleton. Never redeploy an unexpectedly
	// multi-instance service under a false promise of per-instance maintenance.
	inventory := engine.ObserveInstanceInventory(ctx, rc, s.Member)
	if inventory.Err != nil {
		return result(false), inventory.Err
	}
	count := 0
	for _, c := range inventory.Containers {
		if c.Status == infra.ContainerRunning || c.Status == infra.ContainerActive {
			count++
		}
	}
	if count != 1 {
		return result(false), engine.Invalid("Keycloak agent upgrade requires exactly one live runtime instance")
	}
	if !engine.MemberNeedsAgentUpgrade(m, cp.Target) {
		cp.Done = append(cp.Done, s.Member)
		return result(true), nil
	}
	if rc.WaitForWindow().RequeueAfter > 0 {
		return result(false), nil
	}
	// Persist the in-flight member before calling the platform on the next tick.
	cp.Waiting = true
	// A nil receipt denotes a durable deploy intent, not an acknowledged deploy.
	return result(false), nil
}
