// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package temporal

import (
	"context"
	"encoding/json"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/temporal/tspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type agentUpgradeCheckpoint struct {
	operationCheckpoint
	Target    engine.AgentUpgradePayload `json:"target"`
	Done      []string                   `json:"done,omitempty"`
	Submitted bool                       `json:"submitted,omitempty"`
}

func (p *Provider) upgradeAgents(ctx context.Context, rc *engine.Context, op *domain.Operation, cancel bool) (result engine.OpResult, err error) {
	var cp agentUpgradeCheckpoint
	out := func(done bool) engine.OpResult {
		raw, _ := json.Marshal(cp)
		return engine.OpResult{Done: done, Step: "upgrade-agents", Checkpoint: raw, RequeueAfter: 2 * time.Second}
	}
	if len(op.Checkpoint) > 0 {
		if err = json.Unmarshal(op.Checkpoint, &cp); err != nil {
			return out(false), err
		}
	} else {
		cp.Target, err = engine.AgentUpgradeTarget(op)
		if err != nil {
			return out(false), err
		}
	}
	if cancel && !cp.Waiting {
		return out(true), nil
	}
	s, err := loadState(rc.Cluster)
	if err != nil {
		return out(false), err
	}
	defer func() {
		if e := saveState(rc.Cluster, s); err == nil {
			err = e
		}
	}()
	if cp.Stage == "done" {
		return out(true), nil
	}
	if !cp.Waiting && cp.Target.TargetBuild != engine.CurrentAgentBuild().Build {
		return out(false), engine.Invalid("agent upgrade target build differs from running control-plane build; re-run upgrade-agents")
	}
	if cp.Stage == "" {
		if s.Pending != nil || s.Upgrade != nil || s.RestoreRecovery != nil || s.BindingID == "" {
			return out(false), engine.Invalid("Temporal agent upgrade requires a formed cluster with no pending structural work")
		}
		for _, name := range tspec.RollOrder {
			m := s.Members[name]
			if m == nil {
				continue
			}
			if m.Recovering || m.ImportBatch != nil || m.Stage != engine.ComponentJoined {
				return out(false), engine.Invalid("Temporal agent upgrade requires joined components with no pending structural work")
			}
			cp.Order = append(cp.Order, name)
		}
		if len(cp.Order) == 0 {
			return out(false), engine.Invalid("Temporal agent upgrade has no formed components")
		}
		cp.Stage = "roll"
		return out(false), nil
	}
	if !cp.SettleUntil.IsZero() {
		if clockNow(rc).Before(cp.SettleUntil) {
			return out(false), nil
		}
		cp.SettleUntil = time.Time{}
	}
	if cp.Index >= len(cp.Order) {
		cp.Stage = "done"
		return out(true), nil
	}
	m := s.Members[cp.Order[cp.Index]]
	if m == nil {
		return out(false), engine.ErrNotFound
	}
	if cp.Waiting && !cp.Submitted {
		// Intent without a receipt is ambiguous: the backend may have accepted
		// the deployment. Wait for the service and native component to settle.
		svc, e := rc.Backend.GetService(ctx, m.Name)
		if e != nil {
			return out(false), e
		}
		if svc == nil || (svc.Status != infra.StatusRunning && svc.Status != infra.StatusActive) {
			return out(false), nil
		}
		var agent domain.AgentStatus
		status, e := componentStatus(ctx, rc, m, &agent)
		if e != nil {
			return out(false), e
		}
		if !status.Ready {
			return out(false), nil
		}
		if cancel || (!cp.Target.IncludeCurrent && agent.Build == cp.Target.TargetBuild) {
			m.NativeReadiness = nil
			cp.Done = append(cp.Done, m.Component)
			cp.Index++
			cp.Waiting = false
			if m.Component == tspec.History || m.Component == tspec.Server {
				cp.SettleUntil = clockNow(rc).Add(time.Minute)
			}
			return out(cancel), nil
		}
		if cp.Target.TargetBuild != engine.CurrentAgentBuild().Build {
			return out(false), engine.Invalid("agent upgrade target build differs from running control-plane build; re-run upgrade-agents")
		}
		if rc.WaitForWindow().RequeueAfter > 0 {
			return out(false), nil
		}
		accepted := *rc.Cluster
		accepted.Spec.Version = m.Version
		if m.Component == tspec.UI {
			accepted.Spec.Version = s.Version
		}
		view := *rc
		view.Cluster = &accepted
		runtime, e := p.runtimeSpec(&view, m)
		if e != nil {
			return out(false), e
		}
		deployment, e := engine.Deploy(ctx, rc, m.Name, runtime)
		if e != nil {
			return out(false), e
		}
		cp.Pending = &deployment
		cp.Submitted = true
		return out(false), nil
	}
	if cp.Pending != nil {
		st, e := rc.Backend.OpStatus(ctx, *cp.Pending)
		if e != nil {
			return out(false), e
		}
		if st.Failed {
			return out(false), engine.Invalid("Temporal agent deployment failed")
		}
		if !st.Done {
			return out(false), nil
		}
		cp.Pending = nil
	}
	var agent domain.AgentStatus
	st, e := componentStatus(ctx, rc, m, &agent)
	if e != nil {
		return out(false), e
	}
	if cp.Waiting {
		if !st.Ready {
			return out(false), nil
		}
		exact, count, e := engine.VisitLiveInstances(ctx, rc, m.Name, func(client *agentapi.Client) (bool, error) {
			status, e := client.Status(ctx)
			return e == nil && status.Agent.Build == cp.Target.TargetBuild, e
		})
		if e != nil {
			return out(false), e
		}
		if !exact || count != max(1, m.Replicas) {
			return out(false), nil
		}
		m.NativeReadiness = nil
		cp.Done = append(cp.Done, m.Component)
		cp.Index++
		cp.Waiting = false
		cp.Submitted = false
		if m.Component == tspec.History || m.Component == tspec.Server {
			cp.SettleUntil = clockNow(rc).Add(time.Minute)
		}
		return out(cancel), nil
	}
	agent.Healthy = !agent.LastSeen.IsZero()
	if !engine.MemberNeedsAgentUpgrade(domain.MemberStatus{Agent: agent}, cp.Target) {
		cp.Done = append(cp.Done, m.Component)
		cp.Index++
		return out(false), nil
	}
	if !st.Ready {
		return out(false), engine.ErrUnavailable
	}
	if rc.WaitForWindow().RequeueAfter > 0 {
		return out(false), nil
	}
	// The backend deploys a logical component service, not individual instances.
	// Its receipt and all-instance readiness fence advancement to the next role.
	cp.Waiting = true
	return out(false), nil
}

func (*Provider) CanCancelOperation(_ context.Context, op *domain.Operation) error {
	if op.Kind != domain.OperationUpgradeAgents {
		return engine.Invalid("operation does not support cancellation")
	}
	return nil
}

func (p *Provider) CancelOperation(ctx context.Context, rc *engine.Context, op *domain.Operation) (engine.OpResult, error) {
	if err := p.CanCancelOperation(ctx, op); err != nil {
		return engine.OpResult{}, err
	}
	previous := rc.Operation
	rc.Operation = op
	defer func() { rc.Operation = previous }()
	return p.upgradeAgents(ctx, rc, op, true)
}
