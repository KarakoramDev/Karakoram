// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package neo4j

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type agentUpgradeCheckpoint struct {
	Target        engine.AgentUpgradePayload `json:"target"`
	Order         []string                   `json:"order"`
	Done          []string                   `json:"done,omitempty"`
	Step          string                     `json:"step"`
	Deploy        *infra.Op                  `json:"deploy,omitempty"`
	PreviousBuild string                     `json:"previousBuild,omitempty"`
}

func agentUpgradeResult(cp agentUpgradeCheckpoint, done bool) (engine.OpResult, error) {
	raw, err := json.Marshal(cp)
	return engine.OpResult{Done: done, Step: "upgrade-agents", Checkpoint: raw, RequeueAfter: time.Second}, err
}
func (p *Provider) upgradeAgents(ctx context.Context, rc *engine.Context, op *domain.Operation, cancel bool) (engine.OpResult, error) {
	var cp agentUpgradeCheckpoint
	if len(op.Checkpoint) > 0 {
		if err := json.Unmarshal(op.Checkpoint, &cp); err != nil {
			return engine.OpResult{}, err
		}
	}
	s, err := load(rc.Cluster)
	if err != nil {
		return engine.OpResult{}, err
	}
	if cp.Step == "complete" || cancel && (cp.Step == "" || cp.Step == "gate") {
		return agentUpgradeResult(cp, true)
	}
	if cp.Step == "" {
		cp.Target, err = engine.AgentUpgradeTarget(op)
		if err != nil {
			return engine.OpResult{}, err
		}
		if s.Member == nil || s.Member.Stage != "ready" || s.Member.Pending != nil || s.Member.ImportBatch != nil || s.Member.RuntimeRecovery.Op != nil || s.Member.RuntimeRecovery.ImportPlan != nil || s.Member.RuntimeRecovery.Action != "" || s.Member.RuntimeRecovery.VolumeLost || s.Member.Offline || s.Suspended || s.RestoreRecovery != nil || s.Maintenance != "" {
			return engine.OpResult{}, fmt.Errorf("%w: agent upgrade requires no pending structural or maintenance work", engine.ErrConflict)
		}
		cp.Order = []string{s.Member.Name}
		cp.Step = "gate"
	}
	m := s.Member
	if m == nil || len(cp.Order) != 1 || m.Name != cp.Order[0] {
		return engine.OpResult{}, engine.ErrConflict
	}
	if rc.Agents == nil {
		return engine.OpResult{}, engine.ErrUnavailable
	}
	status := func() (domain.MemberStatus, error) {
		a, e := rc.Agents.Dial(ctx, rc.Cluster.Ref(), m.Name)
		if e != nil {
			return domain.MemberStatus{}, e
		}
		defer a.Close()
		st, e := a.Status(ctx)
		return domain.MemberStatus{Agent: domain.AgentStatus{Build: st.Agent.Build, Healthy: e == nil && st.Error == "" && st.ConfigVersion >= s.ConfigVersion && st.Processes["neo4j"].State == "running"}}, e
	}
	settledBuild := ""
	settled := func() bool {
		native, e := readiness(ctx, rc)
		if e != nil || !native.Ready || native.CredentialMismatch || native.ConfigVersion < s.ConfigVersion || native.Version != m.Version {
			return false
		}
		st, e := status()
		settledBuild = st.Agent.Build
		return e == nil && st.Agent.Healthy && (st.Agent.Build == cp.PreviousBuild || st.Agent.Build == cp.Target.TargetBuild)
	}
	intentSettled := false
	if cp.Step == "deploy" {
		intentSettled = settled()
		if intentSettled && cp.PreviousBuild != cp.Target.TargetBuild && settledBuild == cp.Target.TargetBuild {
			containers, err := rc.Backend.Containers(ctx, m.Name)
			if err == nil && len(containers) == 1 && (containers[0].Status == infra.ContainerRunning || containers[0].Status == infra.ContainerActive) {
				cp.Done = append(cp.Done, m.Name)
				cp.Step = "complete"
				return agentUpgradeResult(cp, true)
			}
		}
	}
	if cancel && cp.Step == "deploy" {
		if !intentSettled {
			return agentUpgradeResult(cp, false)
		}
		cp.Step = "complete"
		return agentUpgradeResult(cp, true)
	}
	if !cancel && cp.Step != "ack" && cp.Target.TargetBuild != engine.CurrentAgentBuild().Build {
		return engine.OpResult{}, engine.Invalid("agent upgrade target build differs from running control-plane build; re-run upgrade-agents")
	}
	switch cp.Step {
	case "gate":
		st, e := status()
		if e != nil {
			return agentUpgradeResult(cp, false)
		}
		if e == nil && !engine.MemberNeedsAgentUpgrade(st, cp.Target) {
			cp.Step = "complete"
			return agentUpgradeResult(cp, true)
		}
		if wait := rc.WaitForWindow(); wait.RequeueAfter > 0 {
			r, e := agentUpgradeResult(cp, false)
			r.RequeueAfter = wait.RequeueAfter
			return r, e
		}
		cp.PreviousBuild = st.Agent.Build
		cp.Step = "deploy" // Persist the in-flight member before the platform mutation.
		return agentUpgradeResult(cp, false)
	case "deploy":
		// A closed window before submission is still a safe boundary.
		if wait := rc.WaitForWindow(); wait.RequeueAfter > 0 {
			r, e := agentUpgradeResult(cp, false)
			r.RequeueAfter = wait.RequeueAfter
			return r, e
		}
		// Missing receipt is ambiguous: let any accepted restart settle before
		// another submission, and recheck the native gate on every replay.
		if !intentSettled {
			return agentUpgradeResult(cp, false)
		}
		// Backend exposes receipt polling, not deployment history by member.
		// IncludeCurrent with an unchanged pre-build cannot prove a missing-
		// receipt deployment happened, so conservatively redeploy once more.
		if err := verify(ctx, rc, m); err != nil {
			return engine.OpResult{}, err
		}
		accepted := *rc.Cluster
		accepted.Spec.Version = m.Version
		view := *rc
		view.Cluster = &accepted
		spec, e := p.runtimeSpec(&view, s)
		if e != nil {
			return engine.OpResult{}, e
		}
		receipt, e := engine.Deploy(ctx, rc, m.Name, spec)
		if e != nil {
			return engine.OpResult{}, e
		}
		cp.Deploy = &receipt
		cp.Step = "ack"
	case "ack":
		if cp.Deploy != nil {
			st, e := rc.Backend.OpStatus(ctx, *cp.Deploy)
			if e != nil {
				return engine.OpResult{}, e
			}
			if st.Failed {
				return engine.OpResult{}, fmt.Errorf("agent deployment failed: %s", st.Message)
			}
			if !st.Done {
				return agentUpgradeResult(cp, false)
			}
			cp.Deploy = nil
		}
		r, e := readiness(ctx, rc)
		if e != nil {
			return agentUpgradeResult(cp, false)
		}
		st, e := status()
		if e != nil || !st.Agent.Healthy || !r.Ready || r.CredentialMismatch || r.ConfigVersion < s.ConfigVersion || r.Version != m.Version || st.Agent.Build != cp.Target.TargetBuild {
			return agentUpgradeResult(cp, false)
		}
		cp.Done = append(cp.Done, m.Name)
		cp.Step = "complete"
		return agentUpgradeResult(cp, true)
	default:
		return engine.OpResult{}, engine.Invalid("unknown agent upgrade checkpoint")
	}
	return agentUpgradeResult(cp, false)
}
func (*Provider) CanCancelOperation(_ context.Context, op *domain.Operation) error {
	if op.Kind != domain.OperationUpgradeAgents {
		return engine.ErrNotSupported
	}
	return nil
}
func (p *Provider) CancelOperation(ctx context.Context, rc *engine.Context, op *domain.Operation) (engine.OpResult, error) {
	return p.upgradeAgents(ctx, rc, op, true)
}
