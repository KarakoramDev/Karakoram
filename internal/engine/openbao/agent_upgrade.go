// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package openbao

import (
	"context"
	"encoding/json"
	"slices"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

type agentUpgradeCheckpoint struct {
	Target  engine.AgentUpgradePayload `json:"target"`
	Order   []string                   `json:"order"`
	Done    []string                   `json:"done,omitempty"`
	Waiting string                     `json:"waiting,omitempty"`
}

func agentUpgradeResult(cp agentUpgradeCheckpoint, done bool) (engine.OpResult, error) {
	b, err := json.Marshal(cp)
	return engine.OpResult{Done: done, Step: "upgrade-agents", Checkpoint: b, RequeueAfter: time.Second}, err
}

// Observe native membership without admitting desired-state rolling changes.
func (p *Provider) observeAgentUpgrade(ctx context.Context, rc *engine.Context, s *internalState) (bool, error) {
	p.observeMembers(ctx, rc, s)
	if p.postgresStorage(rc.Cluster) {
		healthy := true
		for _, m := range s.Members {
			m.Healthy = m.Observed && m.Initialized && !m.Sealed
			healthy = healthy && m.Healthy
		}
		return healthy, nil
	}
	token, err := p.ensureManagementToken(ctx, rc, s)
	if err != nil {
		return false, err
	}
	admin, _, err := p.activeClient(ctx, rc, s, token)
	if err != nil {
		return false, nil
	}
	peers, err := admin.RaftConfiguration(ctx)
	if err != nil {
		return false, err
	}
	ap, err := admin.AutopilotState(ctx)
	if err != nil {
		return false, err
	}
	for _, m := range s.Members {
		m.Voter = false
		for _, peer := range peers {
			if peer.NodeID == m.Name {
				m.Voter = peer.Voter
			}
		}
		for id, server := range ap.Servers {
			if id == m.Name || server.Name == m.Name {
				m.Healthy = m.Observed && m.Initialized && !m.Sealed && m.Voter && server.Healthy && (server.Status == "voter" || server.Status == "leader")
			}
		}
	}
	return ap.Healthy, nil
}

func (p *Provider) upgradeAgents(ctx context.Context, rc *engine.Context, op *domain.Operation, cancel bool) (result engine.OpResult, err error) {
	previous := rc.Operation
	rc.Operation = op
	defer func() { rc.Operation = previous }()
	var cp agentUpgradeCheckpoint
	if len(op.Checkpoint) != 0 {
		if err = json.Unmarshal(op.Checkpoint, &cp); err != nil {
			return result, err
		}
	} else {
		cp.Target, err = engine.AgentUpgradeTarget(op)
		if err != nil {
			return result, err
		}
	}
	s, err := loadState(rc.Cluster)
	if err != nil {
		return result, err
	}
	defer func() {
		if e := s.save(rc.Cluster); err == nil {
			err = e
		}
	}()
	if cp.Waiting == "" && cancel {
		return agentUpgradeResult(cp, true)
	}
	if cp.Waiting == "" && cp.Order != nil && len(cp.Done) == len(cp.Order) {
		return agentUpgradeResult(cp, true)
	}
	mismatch := cp.Target.TargetBuild != engine.CurrentAgentBuild().Build
	if mismatch && cp.Waiting == "" {
		return result, engine.Invalid("agent upgrade target build differs from running control-plane build; re-run upgrade-agents")
	}
	if cp.Order == nil {
		allowed, e := p.CanClaimOperation(rc.Cluster)
		if e != nil {
			return result, e
		}
		for _, m := range s.Members {
			if m.RuntimeRecovery.Action != "" || m.RuntimeRecovery.VolumeLost {
				allowed = false
			}
		}
		if !allowed || s.RollingKind != "" || len(s.Members) != rc.Cluster.Spec.Components.Engine.Replicas {
			return result, engine.Invalid("cannot upgrade agents while rolling, replacement, or pending structural work is in progress")
		}
		healthy, e := p.observeAgentUpgrade(ctx, rc, s)
		if e != nil {
			return result, e
		}
		for _, m := range s.Members {
			healthy = healthy && m.Healthy && !m.Sealed
		}
		if !healthy {
			return result, engine.Invalid("agent upgrade requires healthy unsealed members and voter quorum")
		}
		cp.Order = []string{}
		for _, active := range []bool{false, true} {
			for _, m := range ordered(s) {
				if m.Active == active {
					cp.Order = append(cp.Order, m.Name)
				}
			}
		}
		return agentUpgradeResult(cp, false)
	}
	// Platform receipts are recovered before native readiness and exact-build acknowledgement.
	if cp.Waiting != "" {
		m := s.find(cp.Waiting)
		if m == nil {
			return result, engine.ErrNotFound
		}
		if busy, e := p.observePending(ctx, rc, s); busy || e != nil {
			return agentUpgradeResultError(cp, e)
		}
		if mismatch && s.Retry != nil && s.Retry.Action == "deploy" {
			return result, engine.Invalid("agent upgrade retry requires the frozen target control-plane build")
		}
		if busy, e := p.retryPending(ctx, rc, s); busy || e != nil {
			return agentUpgradeResultError(cp, e)
		}
		if busy, e := p.observeDeployments(ctx, rc, s); busy || e != nil {
			return agentUpgradeResultError(cp, e)
		}
		if m.Stage == "deployed" {
			st, e := p.agentStatus(ctx, rc, m.Name)
			if e != nil {
				return result, e
			}
			if st.ConfigVersion < m.ConfigVersion || st.Processes["openbao"].State != "running" {
				return agentUpgradeResult(cp, false)
			}
			m.AppliedConfigVersion = st.ConfigVersion
			m.Stage = "configured"
		}
		// A sealed single member has no active API yet: unseal before querying Raft.
		p.observeMembers(ctx, rc, s)
		if m.Change != nil && m.Change.Step == "wait" && m.Observed && m.Sealed {
			if !p.autoUnseal(rc.Cluster) {
				if s.Unsealing != m.Name {
					s.Unsealing = m.Name
					return agentUpgradeResult(cp, false)
				}
				if err = p.nativeUnsealStep(ctx, rc, s); err != nil {
					return result, err
				}
			}
			return agentUpgradeResult(cp, false)
		}
		healthy, e := p.observeAgentUpgrade(ctx, rc, s)
		if e != nil {
			return result, e
		}
		if m.Change != nil {
			c := m.Change
			if c.Step != "wait" && mismatch {
				return result, engine.Invalid("agent upgrade target build differs from running control-plane build; restore the target control plane to finish this member")
			}
			if c.Step == "" || c.Step == "apply" || c.Step == "deploy" {
				for _, peer := range s.Members {
					if !peer.Healthy || peer.Sealed || peer.Removing || peer.Name != m.Name && peer.Change != nil {
						return agentUpgradeResult(cp, false)
					}
				}
			}
			if c.Step == "" {
				allowed, e := p.allowDisruption(ctx, rc, s, m)
				if e != nil {
					return result, e
				}
				if !allowed {
					return agentUpgradeResult(cp, false)
				}
			}
			if c.Step == "apply" {
				c.Step = "deploy"
				return agentUpgradeResult(cp, false)
			}
			if err = p.changeStep(ctx, rc, s, m, healthy); err != nil {
				return result, err
			}
			return agentUpgradeResult(cp, false)
		}
		st, e := p.agentStatus(ctx, rc, m.Name)
		if e != nil {
			return result, e
		}
		if !m.Healthy || m.Sealed || !healthy || st.Processes["openbao"].State != "running" || st.Agent.Build != cp.Target.TargetBuild {
			return agentUpgradeResult(cp, false)
		}
		m.Stage = "ready"
		cp.Done = append(cp.Done, m.Name)
		cp.Waiting = ""
		return agentUpgradeResult(cp, false)
	}
	for _, name := range cp.Order {
		if slices.Contains(cp.Done, name) {
			continue
		}
		m := s.find(name)
		if m == nil {
			return result, engine.ErrNotFound
		}
		st, e := p.agentStatus(ctx, rc, name)
		if e == nil && !engine.MemberNeedsAgentUpgrade(domain.MemberStatus{Agent: domain.AgentStatus{Healthy: true, Build: st.Agent.Build}}, cp.Target) {
			cp.Done = append(cp.Done, name)
			return agentUpgradeResult(cp, false)
		}
		if rc.WaitForWindow().RequeueAfter > 0 {
			return agentUpgradeResult(cp, false)
		}
		healthy, e := p.observeAgentUpgrade(ctx, rc, s)
		if e != nil {
			return result, e
		}
		for _, peer := range s.Members {
			if peer.Change != nil || peer.Removing || !peer.Healthy || peer.Sealed {
				return result, engine.Invalid("agent upgrade requires all members healthy, unsealed and free of structural changes")
			}
		}
		if !healthy {
			return agentUpgradeResult(cp, false)
		}
		// Journal before deployment. Reuse rolling step-down, receipt and unseal gates,
		// but never activate desired config or change the engine version.
		m.Change = &memberChange{Kind: "agent", Version: m.DesiredVersion, Fingerprint: m.RestartFingerprint, Started: true}
		cp.Waiting = name
		return agentUpgradeResult(cp, false)
	}
	return agentUpgradeResult(cp, true)
}

func agentUpgradeResultError(cp agentUpgradeCheckpoint, err error) (engine.OpResult, error) {
	if err != nil {
		return engine.OpResult{}, err
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
	if op.Kind != domain.OperationUpgradeAgents {
		return engine.OpResult{}, engine.ErrNotSupported
	}
	return p.upgradeAgents(ctx, rc, op, true)
}
