// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type agentUpgradeCheckpoint struct {
	Target        engine.AgentUpgradePayload `json:"target"`
	Order         []string                   `json:"order"`
	Done          []string                   `json:"done,omitempty"`
	Index         int                        `json:"index"`
	Member        string                     `json:"member,omitempty"`
	Stage         string                     `json:"stage,omitempty"`
	Started       bool                       `json:"started,omitempty"`
	HandoffTarget string                     `json:"handoffTarget,omitempty"`
	Deploy        *infra.Op                  `json:"deploy,omitempty"`
}

func agentUpgradeResult(cp *agentUpgradeCheckpoint, message string) (engine.OpResult, error) {
	raw, err := json.Marshal(cp)
	return engine.OpResult{Checkpoint: raw, Step: "upgrade-agents/" + cp.Stage + "/" + cp.Member, Message: message, RequeueAfter: 2 * time.Second}, err
}

func (p *Core) upgradeAgents(ctx context.Context, rc *engine.Context, op *domain.Operation, cancel bool) (result engine.OpResult, err error) {
	ctx = freshMySQLReads(ctx)
	invalidateMySQLReadObservation(ctx, rc)
	defer invalidateMySQLReadObservation(ctx, rc)
	previous := rc.Operation
	rc.Operation = op
	defer func() { rc.Operation = previous }()
	var cp agentUpgradeCheckpoint
	if len(op.Checkpoint) > 0 {
		if err = json.Unmarshal(op.Checkpoint, &cp); err != nil {
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
	if cp.Member == "" && (cancel || cp.Order != nil && cp.Index >= len(cp.Order)) {
		return engine.OpResult{Done: true}, nil
	}
	if cancel && cp.Member != "" && cp.Stage != "waiting" {
		// A pre-deploy intent may have lost an accepted backend reply. Do not
		// submit anything on cancellation: settle native health (old or target
		// build) instead. A persisted receipt still requires exact target build.
		cp.Stage = "settling"
	}
	// Never deploy different bytes under a frozen identity. Recovery of a member
	// already submitted to the backend remains allowed after a CP build change.
	if cp.Stage != "waiting" && cp.Stage != "settling" && cp.Target.TargetBuild != engine.CurrentAgentBuild().Build {
		// A build change before submission must not leave a temporary routing
		// withdrawal behind. No database restart has occurred at this point.
		if m := s.find(cp.Member); m != nil && m.ProxyDraining {
			m.ProxyDraining = false
			publishRevision(s)
		}
		return result, engine.Invalid("agent upgrade target build %q differs from running control-plane build %q; re-run upgrade-agents", cp.Target.TargetBuild, engine.CurrentAgentBuild().Build)
	}
	if cp.Order == nil {
		if err = maintenanceAllowed(ctx, rc, s); err != nil {
			return result, fmt.Errorf("agent rollout requires completed formation, backup, restore and recovery: %w", err)
		}
		if s.Rolling != nil || s.Replacement != nil || s.Pending != nil || s.TLSRotation != nil || s.GaleraTLS != nil {
			return result, engine.Invalid("agent rollout cannot start during rolling maintenance, replacement, scale or TLS rotation")
		}
		for _, m := range s.Members {
			if m.Stage != "joined" || m.ImportBatch != nil || m.Removing || m.Retirement != "" || m.Offline {
				return result, engine.Invalid("agent rollout requires joined members with no pending structural work: %s", m.Name)
			}
			if m.Role == "proxy" && m.Replicas != rc.Cluster.Spec.Components.Extra["proxy"].Replicas {
				return result, engine.Invalid("agent rollout cannot start during ProxySQL scale convergence")
			}
		}
		if len(s.data()) != rc.Cluster.Spec.Components.Engine.Replicas || s.Topology != rc.Cluster.Spec.Topology.Type {
			return result, engine.Invalid("agent rollout cannot start during topology or scale convergence")
		}
		cp.Order, err = rollingOrder(ctx, rc, s)
		if err != nil {
			return result, err
		}
		cp.Stage = "select"
		return agentUpgradeResult(&cp, "Frozen proxy-first, writer-last agent rollout")
	}
	if cp.Index >= len(cp.Order) {
		return result, engine.Invalid("invalid agent rollout index")
	}
	m := s.find(cp.Order[cp.Index])
	if m == nil {
		return result, engine.ErrNotFound
	}
	if cp.Member == "" {
		for _, completed := range cp.Done {
			if completed == m.Name {
				cp.Index++
				return agentUpgradeResult(&cp, "Skipping completed agent "+m.Name)
			}
		}
		// Unknown agent metadata is selected for repair, not treated as current.
		current, _ := agentUpgradeBuildCurrent(ctx, rc, m, cp.Target)
		if current && !cp.Target.IncludeCurrent {
			cp.Done = append(cp.Done, m.Name)
			cp.Index++
			return agentUpgradeResult(&cp, "Skipping current agent "+m.Name)
		}
		if wait := rc.WaitForWindow(); wait.RequeueAfter > 0 {
			result, err = agentUpgradeResult(&cp, "Waiting for maintenance window before "+m.Name)
			result.RequeueAfter = wait.RequeueAfter
			return result, err
		}
		if err = agentUpgradeGate(ctx, rc, s, m); err != nil {
			return result, err
		}
		cp.Member = m.Name
		cp.Stage = "handoff"
		return agentUpgradeResult(&cp, "Preparing agent rollout for "+m.Name)
	}
	if cp.Member != m.Name {
		return result, engine.Invalid("agent rollout in-flight member does not match immutable order")
	}
	switch cp.Stage {
	case "handoff":
		if !cp.Started {
			if wait := rc.WaitForWindow(); wait.RequeueAfter > 0 {
				result, err = agentUpgradeResult(&cp, "Waiting for maintenance window before "+m.Name)
				result.RequeueAfter = wait.RequeueAfter
				return result, err
			}
		}
		if err = agentUpgradeGate(ctx, rc, s, m); err != nil {
			return result, err
		}
		if m.Role == "data" && clustered(s.Topology) {
			st, e := live(ctx, rc, m.Name)
			if e != nil {
				return result, e
			}
			if s.Topology == "group" && onlinePrimary(st) {
				selected := cp.HandoffTarget != "" || !hasServingProxy(s)
				if err = switchAway(ctx, rc, s, m.Name, &cp.HandoffTarget); err != nil {
					return result, err
				}
				// switchAway first checkpoints selection without issuing an
				// election. Only its subsequent successful native call starts
				// disruption and permits completion after the window closes.
				cp.Started = cp.Started || selected
				return agentUpgradeResult(&cp, fmt.Sprintf("handoff writer %s→%s", m.Name, cp.HandoffTarget))
			}
			if s.Topology == "cluster" && onlinePrimary(st) && !m.ProxyDraining {
				// Use the retirement routing exclusion, without retiring the data identity.
				// Select a caught-up, admitted peer before withdrawing the old writer.
				for _, peer := range s.data() {
					if peer.Name == m.Name {
						continue
					}
					candidate, e := live(ctx, rc, peer.Name)
					if e != nil || candidate.MemberState != "ONLINE" {
						continue
					}
					contains, e := myspec.GTIDContains(candidate.GTIDExecuted, st.GTIDExecuted)
					if e != nil || !contains {
						continue
					}
					if e = proxyAdmission(ctx, rc, s, peer.Name, myspec.ProxyReaderGroup, myspec.ProxyBackupWriterGroup); e != nil {
						continue
					}
					cp.HandoffTarget = peer.Name
					break
				}
				if cp.HandoffTarget == "" {
					return result, fmt.Errorf("no caught-up proxy-eligible PXC writer successor for %s: %w", m.Name, engine.ErrUnavailable)
				}
				m.ProxyDraining = true
				cp.Started = true
				publishRevision(s)
				return agentUpgradeResult(&cp, fmt.Sprintf("handoff writer %s→%s", m.Name, cp.HandoffTarget))
			}
			if err = proxyHandoffReady(ctx, rc, s, m.Name, &cp.HandoffTarget); err != nil {
				return agentUpgradeWait(&cp, s.ProxyAdmissionWait, err)
			}
			if m.ProxyDraining {
				for _, proxy := range s.Members {
					if proxy.Role == "proxy" {
						ok, e := ready(ctx, rc, proxy)
						if e != nil || !ok {
							return agentUpgradeWait(&cp, "Waiting for proxy writer handoff", e)
						}
					}
				}
				if err = proxyAdmission(ctx, rc, s, cp.HandoffTarget, myspec.ProxyWriterGroup); err != nil {
					return result, err
				}
			}
		}
		// Persist the in-flight identity before the first runtime side effect.
		cp.Stage = "deploy"
		return agentUpgradeResult(&cp, "Deploying unchanged runtime for "+m.Name)
	case "deploy":
		if err = verifyMember(ctx, rc, m); err != nil {
			return result, err
		}
		spec, e := p.runtimeSpec(rc, m)
		if e != nil {
			return result, e
		}
		if engine.DeploymentBlocked(rc, m.Name, spec) {
			return result, engine.ErrUnavailable
		}
		// A crash can lose the deployment response. A native-ready exact-build
		// acknowledgement proves that submission completed without redeploying it.
		current, e := agentUpgradeBuildCurrent(ctx, rc, m, cp.Target)
		if e == nil && current && !cp.Target.IncludeCurrent {
			cp.Stage = "waiting"
			return agentUpgradeResult(&cp, "Observing recovered deployment for "+m.Name)
		}
		if !cancel && !cp.Started {
			if wait := rc.WaitForWindow(); wait.RequeueAfter > 0 {
				result, err = agentUpgradeResult(&cp, "Waiting for maintenance window before "+m.Name)
				result.RequeueAfter = wait.RequeueAfter
				return result, err
			}
		}
		if err = agentUpgradeGate(ctx, rc, s, m); err != nil {
			return result, err
		}
		if m.Role == "data" && s.Topology == "group" {
			native, observeErr := live(ctx, rc, m.Name)
			if observeErr != nil {
				return result, observeErr
			}
			if onlinePrimary(native) {
				cp.Stage = "handoff"
				return agentUpgradeResult(&cp, "Writer changed; handing off "+m.Name+" before deployment")
			}
			if err = proxyHandoffReady(ctx, rc, s, m.Name, &cp.HandoffTarget); err != nil {
				return agentUpgradeWait(&cp, s.ProxyAdmissionWait, err)
			}
		}
		deployment, e := engine.Deploy(ctx, rc, m.Name, spec)
		if e != nil {
			return result, e
		}
		cp.Deploy = &deployment
		cp.Started = true
		cp.Stage = "waiting"
		return agentUpgradeResult(&cp, "Waiting for "+m.Name+" ONLINE with build "+cp.Target.TargetBuild)
	case "waiting", "settling":
		if cp.Deploy != nil {
			status, e := rc.Backend.OpStatus(ctx, *cp.Deploy)
			if e != nil {
				return result, e
			}
			if status.Failed {
				return result, fmt.Errorf("agent deployment failed for %s: %s", m.Name, status.Message)
			}
			if !status.Done {
				return agentUpgradeResult(&cp, "Waiting for deployment of "+m.Name)
			}
		}
		if m.Role == "data" && clustered(s.Topology) {
			// Ordinary Sync is disabled during imperative maintenance. Reuse its
			// retained-identity rejoin path for peers as well as this member,
			// otherwise a stopped peer can indefinitely block proxy readmission.
			if _, e := p.reconcileFormation(ctx, rc, s); e != nil {
				return agentUpgradeWait(&cp, "Waiting for native member rejoin", e)
			}
			st, e := live(ctx, rc, m.Name)
			if e != nil {
				return agentUpgradeResult(&cp, "Waiting for "+m.Name+" ONLINE with build "+cp.Target.TargetBuild)
			}
			if st.MemberState != "ONLINE" {
				return agentUpgradeResult(&cp, "Waiting for "+m.Name+" ONLINE (recovery finished)")
			}
			if s.Topology == "cluster" {
				native, e := galeraStatus(ctx, rc, m.Name)
				if e != nil || !native.Eligible() || native.ClusterSize != len(s.data()) {
					return agentUpgradeWait(&cp, "Waiting for "+m.Name+" Synced in full Primary component", e)
				}
			}
		}
		ok, e := ready(ctx, rc, m)
		if e != nil || !ok {
			return agentUpgradeWait(&cp, "Waiting for native readiness of "+m.Name, e)
		}
		if cp.Stage != "settling" {
			current, observeErr := agentUpgradeBuildCurrent(ctx, rc, m, cp.Target)
			if observeErr != nil || !current {
				return agentUpgradeWait(&cp, "Waiting for "+m.Name+" ONLINE with build "+cp.Target.TargetBuild, observeErr)
			}
		}
		if m.ProxyDraining {
			m.ProxyDraining = false
			publishRevision(s)
			return agentUpgradeResult(&cp, "Readmitting "+m.Name+" to proxies")
		}
		if m.Role == "data" && clustered(s.Topology) {
			if e = proxyRejoinReady(ctx, rc, s, m.Name); e != nil {
				return agentUpgradeWait(&cp, "Waiting for proxy admission of "+m.Name, e)
			}
		}
		cp.Done = append(cp.Done, m.Name)
		cp.Index++
		cp.Member = ""
		cp.HandoffTarget = ""
		cp.Deploy = nil
		cp.Started = false
		cp.Stage = "select"
		return agentUpgradeResult(&cp, "Completed agent rollout for "+m.Name)
	default:
		return result, engine.Invalid("unknown MySQL agent rollout checkpoint %s", cp.Stage)
	}
}

func agentUpgradeWait(cp *agentUpgradeCheckpoint, message string, err error) (engine.OpResult, error) {
	result, marshalErr := agentUpgradeResult(cp, message)
	if marshalErr != nil {
		return result, marshalErr
	}
	return result, err
}

func agentUpgradeBuildCurrent(ctx context.Context, rc *engine.Context, m *member, target engine.AgentUpgradePayload) (bool, error) {
	if m.Role == "proxy" {
		all, count, err := engine.VisitLiveInstancesConcurrent(ctx, rc, m.Name, func(a *agentapi.Client) (bool, error) {
			status, err := a.Status(ctx)
			return err == nil && !engine.MemberNeedsAgentUpgrade(domain.MemberStatus{Agent: domain.AgentStatus{Healthy: true, Build: status.Agent.Build}}, engine.AgentUpgradePayload{TargetBuild: target.TargetBuild}), err
		})
		return all && count >= proxyExpectedReplicas(m), err
	}
	status, _, err := mysqlAgentStatus(ctx, rc, m.Name)
	return err == nil && !engine.MemberNeedsAgentUpgrade(domain.MemberStatus{Agent: domain.AgentStatus{Healthy: true, Build: status.Agent.Build}}, engine.AgentUpgradePayload{TargetBuild: target.TargetBuild}), err
}

func agentUpgradeGate(ctx context.Context, rc *engine.Context, s *state, m *member) error {
	if m.Role == "proxy" {
		ok, err := ready(ctx, rc, m)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("proxy %s is not healthy with current configuration: %w", m.Name, engine.ErrUnavailable)
		}
		return nil
	}
	if !clustered(s.Topology) {
		return nil
	}
	n := len(s.data())
	if n-1 < n/2+1 {
		return engine.Invalid("cannot restart %s while retaining a configured majority", m.Name)
	}
	if s.Topology == "cluster" {
		if err := checkPXCQuorumGTIDs(ctx, rc, s); err != nil {
			return fmt.Errorf("PXC agent rollout requires safe GTID history: %w", err)
		}
		if incident := activeGTIDIncident(s); incident != nil {
			return engine.Invalid("PXC agent rollout blocked by latched GTID history divergence: %s", incident.message())
		}
		for _, peer := range s.data() {
			native, err := galeraStatus(ctx, rc, peer.Name)
			if err != nil || !native.Eligible() || native.ClusterSize != n {
				return fmt.Errorf("PXC agent rollout requires %s Synced, ready and connected in full Primary component: %w", peer.Name, engine.ErrUnavailable)
			}
		}
		return nil
	}
	primary := false
	for _, peer := range s.data() {
		st, err := live(ctx, rc, peer.Name)
		if err != nil || st.MemberState != "ONLINE" || st.GroupMembers != n {
			return fmt.Errorf("GR agent rollout requires all configured members ONLINE (%s): %w", peer.Name, engine.ErrUnavailable)
		}
		primary = primary || onlinePrimary(st)
	}
	if !primary {
		return fmt.Errorf("GR agent rollout requires an ONLINE primary: %w", engine.ErrUnavailable)
	}
	return nil
}
