// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package temporal

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/nativeartifacts"
	"github.com/KarakoramDev/Karakoram/internal/engine/temporal/tspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type agentUpgradeCheckpoint struct {
	operationCheckpoint
	Target           engine.AgentUpgradePayload `json:"target"`
	Done             []string                   `json:"done,omitempty"`
	Submitted        bool                       `json:"submitted,omitempty"`
	RetryPreparation bool                       `json:"retryPreparation,omitempty"`
	RetryIntent      string                     `json:"retryIntent,omitempty"`
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
	if err := p.recoverUIVersion(ctx, rc, m); err != nil {
		return out(false), err
	}
	if cp.Waiting && !cp.Submitted {
		if cancel {
			// Intent alone is not a reason to disrupt an untouched incumbent.
			// A healthy applied configuration can settle cancellation before
			// either the target version or the target agent build is installed.
			svc, e := rc.Backend.GetService(ctx, m.Name)
			if e != nil {
				return out(false), e
			}
			if svc != nil && (svc.Status == infra.StatusRunning || svc.Status == infra.StatusActive) {
				status, e := componentStatus(ctx, rc, m)
				if e != nil {
					return out(false), e
				}
				if status.Ready {
					m.NativeReadiness = nil
					cp.Waiting = false
					cp.RetryPreparation = false
					return out(true), nil
				}
			}
		}
		// An admitted intent without a receipt may already have replaced the
		// incumbent. Readiness can adopt a completed deployment, but must not
		// prevent replay of preparation for an unready replacement.
		preparingUI := cp.RetryPreparation
		if m.Component == tspec.UI {
			a, e := nativeartifacts.Get("temporal-ui-server")
			if e != nil {
				return out(false), e
			}
			preparingUI = preparingUI || m.Version != a.Version
		}
		if !preparingUI {
			svc, e := rc.Backend.GetService(ctx, m.Name)
			if e != nil {
				return out(false), e
			}
			if svc == nil || (svc.Status != infra.StatusRunning && svc.Status != infra.StatusActive) {
				return out(false), nil
			}
			status, e := componentStatus(ctx, rc, m)
			if e != nil {
				return out(false), e
			}
			if !status.Ready {
				return out(false), nil
			}
			exact := false
			if !cancel && !cp.Target.IncludeCurrent {
				exact, e = componentAgentBuildMatches(ctx, rc, m, cp.Target.TargetBuild)
				if e != nil {
					return out(false), e
				}
			}
			if cancel || exact {
				m.NativeReadiness = nil
				cp.Done = append(cp.Done, m.Component)
				cp.Index++
				cp.Waiting = false
				cp.RetryPreparation = false
				if m.Component == tspec.History || m.Component == tspec.Server {
					cp.SettleUntil = clockNow(rc).Add(time.Minute)
				}
				return out(cancel), nil
			}
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
		// Deploy has no request-identity receipt lookup in infra.Backend.
		// IncludeCurrent cannot distinguish a completed same-build deployment
		// from untouched instances after a lost receipt, so replay may redeploy.
		deployment, intent, e := p.deployPreparedRuntime(ctx, rc, m, runtime, accepted.Spec.Version)
		cp.RetryIntent = intent
		if e != nil {
			if m.Component == tspec.UI && m.DeploymentFailure != nil {
				cp.RetryPreparation = true
			}
			return out(false), e
		}
		cp.Pending = &deployment
		cp.Submitted = true
		cp.RetryPreparation = false
		if e := p.publishDeploymentUI(ctx, rc, s, m); e != nil {
			return out(false), e
		}
		return out(false), nil
	}
	if cp.Submitted {
		if e := p.publishDeploymentUI(ctx, rc, s, m); e != nil {
			return out(false), e
		}
	}
	if cp.Pending != nil {
		st, e := rc.Backend.OpStatus(ctx, *cp.Pending)
		if e != nil && !errors.Is(e, infra.ErrDeploymentRetriesExhausted) {
			return out(false), e
		}
		if st.Failed || st.Status == "DeploymentRetriesExhausted" || errors.Is(e, infra.ErrDeploymentRetriesExhausted) {
			failed := *cp.Pending
			cp.Pending = nil
			cp.Submitted = false
			cp.RetryPreparation = true
			detail := st.Message
			if e != nil {
				detail = infra.SafeDetail(e)
			}
			return out(false), p.recordComponentDeploymentFailure(ctx, rc, m, failed, st.Status, detail, cp.RetryIntent, e)
		}
		if !st.Done {
			return out(false), nil
		}
		cp.Pending = nil
		cp.RetryPreparation = false
		m.DeploymentFailure = nil
	}
	if !cp.Waiting && m.DeploymentFailure != nil {
		// A previous admitted rollout may have exhausted and ended. Its
		// persisted failure authorizes repair, not a requirement that the
		// still-unprepared UI become healthy before an explicit re-arm works.
		cp.Waiting = true
		cp.RetryPreparation = true
		return out(false), nil
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
		exact, e := componentAgentBuildMatches(ctx, rc, m, cp.Target.TargetBuild)
		if e != nil {
			return out(false), e
		}
		if !exact {
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

// publishDeploymentUI runs only after a deployment submission is acknowledged.
// A failed secret write is retried from the retained submission checkpoint.
func (p *Provider) publishDeploymentUI(ctx context.Context, rc *engine.Context, s *componentState, m *componentMember) error {
	if m.Component != tspec.UI {
		return nil
	}
	config, err := p.memberConfig(ctx, rc, m.Name)
	if err != nil {
		return err
	}
	var sp tspec.Spec
	if err := json.Unmarshal(config.EngineSpec, &sp); err != nil {
		return err
	}
	sp, err = deploymentUISpec(sp, s.CredentialGeneration, s.Version, m.RestartNonce)
	if err != nil || sp.Version == m.Version {
		return err
	}
	ca, err := ensureCA(ctx, rc)
	if err != nil {
		return err
	}
	return p.publishConfig(ctx, rc, m, sp, ca)
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

func componentAgentBuildMatches(ctx context.Context, rc *engine.Context, m *componentMember, target string) (bool, error) {
	exact, count, err := engine.VisitLiveInstances(ctx, rc, m.Name, func(client *agentapi.Client) (bool, error) {
		status, err := client.Status(ctx)
		return err == nil && status.Agent.Build == target, err
	})
	return exact && count == max(1, m.Replicas), err
}
