// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type agentUpgradeCheckpoint struct {
	Target  engine.AgentUpgradePayload `json:"target"`
	Order   []string                   `json:"order"`
	Index   int                        `json:"index"`
	Waiting bool                       `json:"waiting"`
	Deploy  *infra.Op                  `json:"deploy,omitempty"`
	// Non-nil records the proxy rollout intent before submission. Waiting with
	// no Deploy then means submission may have reached the backend.
	ProxyServing *bool  `json:"proxyServing,omitempty"`
	Barrier      string `json:"barrier,omitempty"`
	Timeline     int    `json:"timeline,omitempty"`
	SystemID     string `json:"systemId,omitempty"`
	Primary      string `json:"barrierPrimary,omitempty"`
}

func agentUpgradeResult(cp *agentUpgradeCheckpoint) (engine.OpResult, error) {
	raw, err := json.Marshal(cp)
	return engine.OpResult{Checkpoint: raw, Step: "upgrade-agents", RequeueAfter: 2 * time.Second}, err
}

func (p *Core) upgradeAgents(ctx context.Context, rc *engine.Context, op *domain.Operation, cancel bool) (result engine.OpResult, err error) {
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
	}
	s, err := load(rc.Cluster)
	if err != nil {
		return result, err
	}
	defer func() {
		if e := save(rc.Cluster, s); err == nil {
			err = e
		}
	}()
	if cancel && !cp.Waiting {
		return engine.OpResult{Done: true}, nil
	}
	// Preparation can update extension packages without changing the server
	// release. Wait for every member's binary rollout before updating SQL.
	// Retain the pending flag on failure so normal reconciliation can retry.
	if cp.Order != nil && cp.Index >= len(cp.Order) {
		s.ExtensionsPending = true
		if err := p.UpdateProfileExtensions(ctx, rc); err != nil {
			return result, err
		}
		s.ExtensionsPending = false
		return engine.OpResult{Done: true}, nil
	}
	if !cp.Waiting && cp.Target.TargetBuild != engine.CurrentAgentBuild().Build {
		return result, engine.Invalid("agent upgrade target build %q differs from running control-plane build %q; re-run upgrade-agents", cp.Target.TargetBuild, engine.CurrentAgentBuild().Build)
	}
	if cp.Order == nil {
		cp.Order = []string{}
		if s.Topology == "distributed" {
			for _, m := range s.Members {
				if m.Role == "proxy" {
					cp.Order = append(cp.Order, m.Name)
				}
			}
			for _, group := range s.maintenanceGroups() {
				primary, _, e := group.primary(ctx, rc)
				if e != nil {
					return result, e
				}
				for _, m := range group.data() {
					if m != primary {
						cp.Order = append(cp.Order, m.Name)
					}
				}
				cp.Order = append(cp.Order, primary.Name)
			}
		} else {
			primary := ""
			for _, m := range s.Members {
				st, e := live(ctx, rc, m.Name)
				if e != nil || !st.Running {
					return result, engine.ErrUnavailable
				}
				if st.Role == "primary" {
					primary = m.Name
				} else {
					cp.Order = append(cp.Order, m.Name)
				}
			}
			if primary != "" {
				cp.Order = append(cp.Order, primary)
			}
		}
		return agentUpgradeResult(&cp) // Persist the frozen target and order before disruption.
	}
	m := s.find(cp.Order[cp.Index])
	if m == nil {
		return result, engine.ErrNotFound
	}
	if cp.Waiting {
		if cp.Deploy == nil && cp.ProxyServing != nil {
			if cancel && *cp.ProxyServing {
				lookup, ok := rc.Backend.(infra.ProxyDeploymentLookup)
				if !ok {
					return result, engine.Invalid("cannot recover proxy deployment for %s; manual recovery required", m.Name)
				}
				deployment, e := lookup.LookupProxyDeployment(ctx, m.Name, op.ID)
				if e != nil {
					return result, e
				}
				cp.Deploy = deployment
			}
			if cp.Deploy == nil {
				if cancel {
					// Only an intent without a receipt can stop here.
					// Cancellation never renders or submits another build.
					return engine.OpResult{Done: true}, nil
				}
				// Ordinary execution uses Deploy's idempotent submission/adoption
				// path, which can also re-arm a failed receipt from an earlier
				// operation. Cancellation lookup intentionally cannot do that.
				if cp.Target.TargetBuild != engine.CurrentAgentBuild().Build {
					return result, engine.Invalid("agent upgrade target build %q differs from running control-plane build %q; re-run upgrade-agents", cp.Target.TargetBuild, engine.CurrentAgentBuild().Build)
				}
				spec, e := p.agentUpgradeRuntimeSpec(rc, m)
				if e != nil {
					return result, e
				}
				deployment, e := engine.DeployProxy(ctx, rc, m.Name, spec, *cp.ProxyServing, op.ID)
				if e != nil {
					return result, e
				}
				cp.Deploy = &deployment
				return agentUpgradeResult(&cp)
			}
		}
		if cp.Deploy != nil {
			revision := m.ConfigVersion
			if m.TLSReloadPending {
				revision = m.TLSReloadVersion
			}
			status, e := engine.ObserveProxyRollout(ctx, rc, *cp.Deploy, m.Name, revision, m.Role == "proxy")
			if status.Status == "DeploymentRetriesExhausted" && e == nil {
				e = &infra.DeploymentExhaustedError{Service: m.Name, LastError: status.Message}
			}
			if errors.Is(e, infra.ErrDeploymentRetriesExhausted) {
				spec, specErr := p.agentUpgradeRuntimeSpec(rc, m)
				if specErr != nil {
					return result, specErr
				}
				return result, engine.RecordDeploymentExhausted(ctx, rc, m.Name, spec, e)
			}
			if e != nil {
				return result, e
			}
			if status.Failed {
				if m.Role == "proxy" && !status.Expired && status.Status != "stalled" {
					if cancel {
						result, err = agentUpgradeResult(&cp)
						result.Done = true
						return result, errors.Join(err, engine.Invalid("agent deployment failed for %s: %s; manual recovery required; cancellation cannot settle a failed serving rollout", m.Name, status.Message))
					}
					return result, engine.Invalid("agent deployment failed for %s: %s; manual recovery required; inspect the proxy rollout before re-running upgrade-agents --include-current", m.Name, status.Message)
				}
				return result, fmt.Errorf("agent deployment failed for %s: %s", m.Name, status.Message)
			}
			if !status.Done {
				return agentUpgradeResult(&cp)
			}
			if cp.ProxyServing == nil {
				cp.Deploy = nil
			}
		}
		ok, e := ready(ctx, rc, m)
		if e != nil {
			return result, e
		}
		if !ok {
			return agentUpgradeResult(&cp)
		}
		if m.Role == "data" && patroniTopology(m.Topology) {
			st, e := live(ctx, rc, m.Name)
			if e != nil {
				return result, e
			}
			if cp.Barrier != "" {
				if st.SystemID != "" && st.SystemID != cp.SystemID {
					return result, engine.Invalid("agent rollout member %s has a different PostgreSQL system identity; inspect cluster identity and re-run upgrade-agents after recovery", m.Name)
				}
				if st.Role == "replica" {
					leader, writer, e := s.group(m.Group).primary(ctx, rc)
					if e != nil {
						return result, fmt.Errorf("cannot establish current primary for agent rollout member %s; restore primary observations and re-run upgrade-agents if the operation fails: %w", m.Name, e)
					}
					if writer.Timeline != cp.Timeline || cp.Primary != "" {
						leader, writer, e = agentUpgradePrimary(ctx, rc, s.group(m.Group))
						if e != nil {
							return result, fmt.Errorf("cannot establish authoritative primary for agent rollout member %s; restore all group observations and re-run upgrade-agents if the operation fails: %w", m.Name, e)
						}
					}
					if writer.SystemID != "" && writer.SystemID != cp.SystemID {
						return result, engine.Invalid("agent rollout primary %s has a different PostgreSQL system identity; inspect cluster identity and re-run upgrade-agents after recovery", leader.Name)
					}
					if writer.SystemID == "" || writer.Timeline < 1 || !walAtLeast(writer.WALPosition, "0/0") {
						return result, fmt.Errorf("cannot establish current primary WAL identity for agent rollout member %s; restore observations and re-run upgrade-agents if the operation fails: %w", m.Name, engine.ErrUnavailable)
					}
					if writer.Timeline != cp.Timeline || (cp.Primary != "" && cp.Primary != leader.Name) {
						// A failover makes the old timeline barrier impossible to
						// satisfy. Persist a fresh barrier on the current writer;
						// never compare WAL positions across timelines.
						cp.Primary, cp.Timeline, cp.Barrier = leader.Name, writer.Timeline, writer.WALPosition
						return agentUpgradeResult(&cp)
					}
					if st.SystemID != cp.SystemID || !st.Streaming || st.LagBytes > 0 ||
						st.Timeline != cp.Timeline || !walAtLeast(st.WALPosition, cp.Barrier) ||
						(cp.Primary != "" && st.Leader != leader.Name) {
						if cp.Primary != "" {
							return result, fmt.Errorf("agent rollout member %s has not caught up to current primary %s on timeline %d; restore streaming replication and re-run upgrade-agents if the operation fails: %w", m.Name, leader.Name, cp.Timeline, engine.ErrUnavailable)
						}
						return agentUpgradeResult(&cp)
					}
					if cp.Primary != "" {
						proved, e := agentUpgradeReplayProof(ctx, rc, leader.Name, m.Name, cp.Timeline, cp.Barrier, cp.SystemID)
						if e != nil || !proved {
							return result, fmt.Errorf("primary has not proved agent rollout member %s streaming replay past its barrier; restore replication and re-run upgrade-agents if the operation fails: %w", m.Name, engine.ErrUnavailable)
						}
					}
				}
			}
			if st.Role == "replica" && (!st.Streaming || st.LagBytes > 0) {
				return agentUpgradeResult(&cp)
			}
		}
		a, e := rc.Agents.Dial(ctx, rc.Cluster.Ref(), m.Name)
		if e != nil {
			return result, e
		}
		status, e := a.Status(ctx)
		if e != nil {
			return result, e
		}
		if status.Agent.Build != cp.Target.TargetBuild {
			return agentUpgradeResult(&cp)
		}
		cp.Index++
		m.NativeReadiness = nil
		cp.Waiting = false
		cp.ProxyServing = nil
		cp.Deploy = nil
		return agentUpgradeResult(&cp)
	}
	a, e := rc.Agents.Dial(ctx, rc.Cluster.Ref(), m.Name)
	if e != nil {
		return result, e
	}
	status, e := a.Status(ctx)
	if e != nil {
		return result, e
	}
	if m.Role == "data" {
		if err := guardServerDowngrade(m.Name, status.EngineVersion, m.Version); err != nil {
			return result, err
		}
	}
	if !engine.MemberNeedsAgentUpgrade(domain.MemberStatus{Agent: domain.AgentStatus{Healthy: true, Build: status.Agent.Build}}, cp.Target) {
		cp.Index++
		return agentUpgradeResult(&cp)
	}
	st, e := live(ctx, rc, m.Name)
	if e != nil {
		return result, e
	}
	if !st.Running {
		return result, engine.ErrUnavailable
	}
	if wait := rc.WaitForWindow(); wait.RequeueAfter > 0 {
		result, err := agentUpgradeResult(&cp)
		result.RequeueAfter = wait.RequeueAfter
		result.Message = "Waiting for maintenance window"
		return result, err
	}
	if st.Role == "primary" && patroniTopology(s.Topology) && len(s.group(m.Group).data()) > 1 {
		if e := switchAway(ctx, rc, s, m.Name); e != nil {
			return result, e
		}
		return agentUpgradeResult(&cp)
	}
	if e := verifyMember(ctx, rc, m); e != nil {
		return result, e
	}
	if m.Role == "data" && patroniTopology(m.Topology) {
		cp.Barrier, cp.Timeline, cp.SystemID, cp.Primary = "", 0, "", ""
		for _, peer := range s.group(m.Group).data() {
			observed, e := live(ctx, rc, peer.Name)
			if e != nil {
				return result, e
			}
			if !observed.Running || observed.Paused || (observed.Role == "replica" && !observed.Streaming) ||
				(observed.HealthCapabilities != nil && observed.HealthCapabilities.LocalEtcd && !observed.EtcdHealthy) {
				return agentUpgradeResult(&cp)
			}
			if observed.Role == "primary" && observed.WALPosition != "" {
				cp.Barrier, cp.Timeline, cp.SystemID = observed.WALPosition, observed.Timeline, observed.SystemID
			}
		}
	}
	if m.Role == "proxy" && rc.Backend.Capabilities().ProxyDeploymentPreservesServing {
		if approval, ok := rc.Backend.(infra.ServingProxyUpdates); ok {
			serving, e := approval.ProxyUpdatesPreserveServing(ctx, m.Name)
			if e != nil {
				return result, e
			}
			// Freeze the policy and operation request identity before upload, so
			// cancellation/restart can adopt even a lost submission reply.
			cp.ProxyServing = &serving
			cp.Waiting = true
			return agentUpgradeResult(&cp)
		}
	}
	spec, e := p.agentUpgradeRuntimeSpec(rc, m)
	if e != nil {
		return result, e
	}
	deployment, e := engine.Deploy(ctx, rc, m.Name, spec)
	if e != nil {
		return result, e
	}
	cp.Deploy = &deployment
	cp.Waiting = true
	return agentUpgradeResult(&cp)
}

func (p *Core) agentUpgradeRuntimeSpec(rc *engine.Context, m *member) (infra.RuntimeSpec, error) {
	// Render from applied inputs, not a newer desired engine release/configuration.
	frozen := *rc.Cluster
	frozen.Spec.Version = m.Version
	frozen.Spec.EngineOptions = m.Options
	frozen.Spec.Topology.Type = m.Topology
	applied := *rc
	applied.Cluster = &frozen
	return p.runtimeSpec(&applied, m)
}
