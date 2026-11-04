// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

type agentUpgradeCheckpoint struct {
	Target           engine.AgentUpgradePayload `json:"target"`
	Order            []string                   `json:"order"`
	Done             []string                   `json:"done,omitempty"`
	Index            int                        `json:"index"`
	Member           string                     `json:"member,omitempty"`
	Stage            string                     `json:"stage,omitempty"`
	Started          bool                       `json:"started,omitempty"`
	BeforeBuilds     map[string]string          `json:"beforeBuilds,omitempty"`
	HandoffTarget    string                     `json:"handoffTarget,omitempty"`
	HandoffStartedAt time.Time                  `json:"handoffStartedAt,omitempty"`
	Deploy           *infra.Op                  `json:"deploy,omitempty"`
	StopReason       string                     `json:"stopReason,omitempty"`
	ProxyServing     *bool                      `json:"proxyServing,omitempty"`
}

func agentUpgradeResult(cp *agentUpgradeCheckpoint, message string) (engine.OpResult, error) {
	raw, err := json.Marshal(cp)
	return engine.OpResult{Checkpoint: raw, Step: "upgrade-agents/" + cp.Stage + "/" + cp.Member, Message: message, RequeueAfter: 2 * time.Second}, err
}

func agentUpgradeRetired(cp *agentUpgradeCheckpoint, cancel bool) (engine.OpResult, error) {
	raw, err := json.Marshal(cp)
	result := engine.OpResult{Done: true, Checkpoint: raw, Message: cp.StopReason}
	if err != nil || cancel {
		// Cancellation/failure cleanup must report successful settlement; the
		// reconciler retains Cancelled or the original failure in that path.
		return result, err
	}
	result.Message += "; re-run upgrade-agents after recovery"
	return result, engine.Invalid("%s", result.Message)
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
	if cp.Member == "" && cp.StopReason != "" {
		return agentUpgradeRetired(&cp, cancel)
	}
	if cp.Member == "" && (cancel || cp.Order != nil && cp.Index >= len(cp.Order)) {
		return engine.OpResult{Done: true}, nil
	}
	if cancel && cp.Member != "" && cp.Deploy == nil && cp.ProxyServing != nil && *cp.ProxyServing {
		if lookup, ok := rc.Backend.(infra.ProxyDeploymentLookup); ok {
			deployment, e := lookup.LookupProxyDeployment(ctx, cp.Member, op.ID)
			if e != nil {
				return result, e
			}
			if deployment != nil {
				cp.Deploy = deployment
				cp.Started = true
				cp.Stage = "waiting"
			}
		}
	}
	if cancel && cp.Member != "" && cp.Stage != "waiting" {
		// Cancellation never submits a deployment. A recovered serving receipt
		// above must finish backend admission and exact-build acknowledgement;
		// only an intent without a receipt may settle on incumbent native health.
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
	if recoveryErr := agentUpgradeRecoveryGate(ctx, rc, s); recoveryErr != nil {
		if cp.Order == nil {
			return result, recoveryErr
		}
		cp.StopReason = "Agent rollout stopped for recovery: " + recoveryErr.Error()
		if cp.Member == "" {
			return agentUpgradeRetired(&cp, cancel)
		}
		// Settle the already selected identity without another deployment.
		// Keep a known receipt in waiting so its exact build is acknowledged.
		if cp.Stage != "waiting" {
			cp.Stage = "settling"
		}
	}
	if cp.Order == nil {
		if err = maintenanceAllowed(ctx, rc, s); err != nil {
			return result, fmt.Errorf("agent rollout requires completed formation, backup, restore and recovery: %w", err)
		}
		if s.Rolling != nil || s.Replacement != nil || s.Pending != nil || s.TLSRotation != nil || s.GaleraTLS != nil {
			return result, engine.Invalid("agent rollout cannot start during rolling maintenance, replacement, scale or TLS rotation")
		}
		for _, m := range s.Members {
			if m.Stage != "joined" || m.ImportBatch != nil || m.Removing || m.Retirement != "" || m.Offline || m.ProxyDraining || m.ProxyDrainOperation != "" {
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
		before, current, _ := agentUpgradeInitialBuilds(ctx, rc, m, cp.Target)
		if current && !cp.Target.IncludeCurrent {
			if m.Role == "proxy" {
				ok, observeErr := ready(ctx, rc, m)
				if observeErr != nil || !ok {
					return agentUpgradeWait(&cp, "Waiting for current proxy "+m.Name+" to acknowledge configuration and healthy writer", observeErr)
				}
			}
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
		cp.BeforeBuilds = before
		cp.Stage = "handoff"
		return agentUpgradeResult(&cp, "Preparing agent rollout for "+m.Name)
	}
	if cp.Member != m.Name {
		return result, engine.Invalid("agent rollout in-flight member does not match immutable order")
	}
	if (cp.Stage == "handoff" || cp.Stage == "deploy") && s.Topology == "cluster" && m.ProxyDraining {
		if cp.HandoffStartedAt.IsZero() {
			// Older in-flight checkpoints also receive a durable deadline.
			cp.HandoffStartedAt = time.Now()
			return agentUpgradeResult(&cp, "Observing PXC proxy writer handoff")
		}
		if time.Since(cp.HandoffStartedAt) >= 3*time.Minute {
			return result, engine.Invalid("PXC writer handoff for %s timed out after 3 minutes: no qualified common writer acknowledged by every proxy; inspect ProxySQL hostgroup 10, applied configuration and member GTIDs, then re-run upgrade-agents after routing recovery", m.Name)
		}
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
			routedWriter := false
			if s.Topology == "cluster" {
				routedWriter, err = agentUpgradePXCWriter(ctx, rc, s, m.Name)
				if err != nil {
					return result, err
				}
			}
			if s.Topology == "cluster" && routedWriter && !m.ProxyDraining {
				// Use the retirement routing exclusion, without retiring the data identity.
				// Select a caught-up, admitted peer before withdrawing the old writer.
				cp.HandoffTarget, err = agentUpgradePXCSuccessor(ctx, rc, s, m.Name, st.GTIDExecuted, myspec.ProxyReaderGroup, myspec.ProxyBackupWriterGroup)
				if cp.HandoffTarget == "" {
					return result, fmt.Errorf("no caught-up proxy-eligible PXC writer successor for %s: %w", m.Name, engine.ErrUnavailable)
				}
				m.ProxyDraining = true
				m.ProxyDrainOperation = op.ID
				cp.Started = true
				cp.HandoffStartedAt = time.Now()
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
				if s.Topology == "cluster" {
					// Galera monitors choose the writer, not backend order. Adopt
					// only a qualified peer covering the incumbent's GTIDs and
					// ONLINE in HG10 on every pinned proxy instance.
					target, e := agentUpgradePXCSuccessor(ctx, rc, s, m.Name, st.GTIDExecuted, myspec.ProxyWriterGroup)
					if e != nil {
						return agentUpgradeWait(&cp, "Waiting for a GTID-containing common PXC writer on every proxy", e)
					}
					if cp.HandoffTarget != target {
						cp.HandoffTarget = target
						return agentUpgradeResult(&cp, "Checkpointing proxy-elected PXC writer "+target)
					}
				}
				if err = proxyAdmission(ctx, rc, s, cp.HandoffTarget, myspec.ProxyWriterGroup); err != nil {
					return result, err
				}
			}
		}
		if m.Role == "proxy" && cp.ProxyServing == nil {
			serving, e := proxyAgentDeploymentPreservesServing(ctx, rc, m)
			if e != nil {
				return result, e
			}
			cp.ProxyServing = &serving
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
		// A changed build on every expected instance can prove a lost-receipt
		// deployment completed, even with IncludeCurrent. An initially current
		// refresh has no distinguishable submission evidence: the backend has
		// no deployment lookup, so retain the fully gated redeploy in that case.
		changed := false
		for _, build := range cp.BeforeBuilds {
			changed = changed || build != cp.Target.TargetBuild
		}
		current, e := agentUpgradeBuildCurrent(ctx, rc, m, cp.Target)
		// Serving rollouts must recover the backend receipt even if every build
		// changed: native observations cannot disprove paused/rollback state.
		servingProxy := m.Role == "proxy" && cp.ProxyServing != nil && *cp.ProxyServing
		if e == nil && current && changed && !servingProxy {
			if m.Role == "data" && clustered(s.Topology) {
				if _, e = p.reconcileFormation(ctx, rc, s); e != nil {
					return agentUpgradeWait(&cp, "Waiting for recovered deployment of "+m.Name, e)
				}
			}
			ok, observeErr := ready(ctx, rc, m)
			if observeErr != nil || !ok {
				return agentUpgradeWait(&cp, "Waiting for native readiness of recovered deployment "+m.Name, observeErr)
			}
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
		if m.Role == "data" && clustered(s.Topology) {
			native, observeErr := live(ctx, rc, m.Name)
			if observeErr != nil {
				return result, observeErr
			}
			writer := onlinePrimary(native)
			if s.Topology == "cluster" {
				writer, err = agentUpgradePXCWriter(ctx, rc, s, m.Name)
				if err != nil {
					return result, err
				}
			}
			if writer {
				cp.Stage = "handoff"
				return agentUpgradeResult(&cp, "Writer changed; handing off "+m.Name+" before deployment")
			}
			if err = proxyHandoffReady(ctx, rc, s, m.Name, &cp.HandoffTarget); err != nil {
				return agentUpgradeWait(&cp, s.ProxyAdmissionWait, err)
			}
			if m.ProxyDraining {
				for _, proxy := range s.Members {
					if proxy.Role == "proxy" {
						ok, observeErr := ready(ctx, rc, proxy)
						if observeErr != nil || !ok {
							return agentUpgradeWait(&cp, "Waiting for current proxy configuration before writer deployment", observeErr)
						}
					}
				}
				target, observeErr := agentUpgradePXCSuccessor(ctx, rc, s, m.Name, native.GTIDExecuted, myspec.ProxyWriterGroup)
				if observeErr != nil || target != cp.HandoffTarget {
					cp.Stage = "handoff"
					return agentUpgradeResult(&cp, "Proxy writer changed; rechecking qualified PXC handoff before deployment")
				}
			}
		}
		var deployment infra.Op
		if m.Role == "proxy" {
			// Old journals without a rollout decision keep their original redeploy
			// policy. Never reselect a policy after an ambiguous submission.
			serving := cp.ProxyServing != nil && *cp.ProxyServing
			deployment, e = engine.DeployProxy(ctx, rc, m.Name, spec, serving, op.ID)
		} else {
			deployment, e = engine.Deploy(ctx, rc, m.Name, spec)
		}
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
			servingProxy := m.Role == "proxy" && cp.ProxyServing != nil && *cp.ProxyServing
			if status.Failed && (cp.StopReason == "" || servingProxy) {
				if cancel && servingProxy {
					result, err = agentUpgradeResult(&cp, "Proxy rollout failed; manual recovery required before cancelling")
					result.Done = true
					return result, errors.Join(err, fmt.Errorf("agent deployment failed for %s: %s; manual recovery required; cancellation cannot settle a failed serving rollout", m.Name, status.Message))
				}
				return result, fmt.Errorf("agent deployment failed for %s: %s", m.Name, status.Message)
			}
			if !status.Done && !status.Failed {
				return agentUpgradeResult(&cp, "Waiting for deployment of "+m.Name)
			}
		}
		if m.ConfirmedVolumeLost {
			// Storage loss supersedes native rejoin and routing readmission.
			// The independent loss fence keeps the released identity unrouted.
			m.ProxyDraining = false
			m.ProxyDrainOperation = ""
			publishRevision(s)
			cp.Member = ""
			cp.Index = len(cp.Order)
			return agentUpgradeRetired(&cp, cancel)
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
				expected := len(s.data())
				if cp.StopReason != "" {
					for _, peer := range s.data() {
						if peer.ConfirmedVolumeLost {
							expected--
						}
					}
				}
				if e != nil || !native.Eligible() || native.ClusterSize != expected {
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
		m.ProxyDrainOperation = ""
		cp.Done = append(cp.Done, m.Name)
		cp.Index++
		cp.Member = ""
		cp.HandoffTarget = ""
		cp.HandoffStartedAt = time.Time{}
		cp.Deploy = nil
		cp.Started = false
		cp.ProxyServing = nil
		cp.Stage = "select"
		return agentUpgradeResult(&cp, "Completed agent rollout for "+m.Name)
	default:
		return result, engine.Invalid("unknown MySQL agent rollout checkpoint %s", cp.Stage)
	}
}

// A proxy-elected successor must meet the same native and GTID gates as the
// pre-withdrawal candidate. A healthy writer row alone cannot authorize restart.
func agentUpgradePXCSuccessor(ctx context.Context, rc *engine.Context, s *state, old, requiredGTIDs string, groups ...int) (string, error) {
	for _, peer := range s.data() {
		if peer.Name == old || peer.Stage != "joined" || peer.Removing || peer.Offline || peer.Retirement != "" || peer.ConfirmedVolumeLost || peer.ProxyDraining || pxcJoinRoutingFenced(s, peer) {
			continue
		}
		candidate, err := live(ctx, rc, peer.Name)
		if err != nil || candidate.MemberState != "ONLINE" {
			continue
		}
		contains, err := myspec.GTIDContains(candidate.GTIDExecuted, requiredGTIDs)
		if err != nil || !contains {
			continue
		}
		native, err := galeraStatus(ctx, rc, peer.Name)
		if err != nil || !native.Eligible() || native.ClusterSize != len(s.data()) {
			continue
		}
		if err := proxyAdmission(ctx, rc, s, peer.Name, groups...); err == nil {
			return peer.Name, nil
		}
	}
	return "", engine.ErrUnavailable
}

func proxyAgentDeploymentPreservesServing(ctx context.Context, rc *engine.Context, m *member) (bool, error) {
	if !rc.Backend.Capabilities().ProxyDeploymentPreservesServing {
		return false, nil
	}
	// Unlike count-only incumbent retention, new-build replacement always needs
	// affirmative deployed-runtime admission support.
	if _, ok := rc.Backend.(infra.ServingProxyUpdates); !ok {
		return false, nil
	}
	return proxyUpdatesPreserveServing(ctx, rc, m)
}

func agentUpgradeWait(cp *agentUpgradeCheckpoint, message string, err error) (engine.OpResult, error) {
	result, marshalErr := agentUpgradeResult(cp, message)
	if marshalErr != nil {
		return result, marshalErr
	}
	return result, err
}

func agentUpgradeInitialBuilds(ctx context.Context, rc *engine.Context, m *member, target engine.AgentUpgradePayload) (map[string]string, bool, error) {
	builds := map[string]string{}
	if m.Role == "proxy" {
		all, count, err := engine.VisitLiveInstancesWithIdentity(ctx, rc, m.Name, func(a *agentapi.Client, id string) (bool, error) {
			status, err := a.Status(ctx)
			builds[id] = status.Agent.Build
			return !engine.MemberNeedsAgentUpgrade(domain.MemberStatus{Agent: domain.AgentStatus{Healthy: err == nil, Build: status.Agent.Build}}, engine.AgentUpgradePayload{TargetBuild: target.TargetBuild}), err
		})
		return builds, all && count >= proxyExpectedReplicas(m), err
	}
	status, _, err := mysqlAgentStatus(ctx, rc, m.Name)
	builds[m.Name] = status.Agent.Build
	return builds, !engine.MemberNeedsAgentUpgrade(domain.MemberStatus{Agent: domain.AgentStatus{Healthy: err == nil, Build: status.Agent.Build}}, engine.AgentUpgradePayload{TargetBuild: target.TargetBuild}), err
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
	if err := agentUpgradeRecoveryGate(ctx, rc, s); err != nil {
		return err
	}
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
			// A nested active-SID prefix is compatible with restart catch-up
			// or torn reads. It withholds continuity claims, not maintenance.
			if comparison, ok := err.(*myspec.GTIDQuorumError); !ok || comparison.Diverged || !comparison.PrefixInFlight {
				return fmt.Errorf("PXC agent rollout requires safe GTID history: %w", err)
			}
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

// Infrastructure repair and replica replacement own the data identity before
// executable maintenance may start, even if a stale agent still reports ONLINE.
func agentUpgradeRecoveryGate(ctx context.Context, rc *engine.Context, s *state) error {
	if s.Replacement != nil {
		return fmt.Errorf("agent rollout waits for member replacement: %w", engine.ErrUnavailable)
	}
	for _, m := range s.Members {
		q := m.RuntimeRecovery
		if m.ConfirmedVolumeLost || q.VolumeLost || q.Op != nil || q.ImportPlan != nil || q.Action != "" || !q.RetryAt.IsZero() {
			return fmt.Errorf("agent rollout waits for runtime recovery of %s: %w", m.Name, engine.ErrUnavailable)
		}
	}
	if rc.Store != nil {
		inventory, ok := rc.Store.(interface {
			ListOperations(context.Context, domain.ClusterRef, ...domain.Phase) ([]domain.Operation, error)
		})
		if !ok {
			return fmt.Errorf("agent rollout requires replacement queue inventory: %w", engine.ErrUnavailable)
		}
		ops, err := inventory.ListOperations(ctx, rc.Cluster.Ref(), domain.PhasePending, domain.PhaseRunning)
		if err != nil {
			return err
		}
		for _, op := range ops {
			if op.Kind == domain.OperationReplaceMember {
				return fmt.Errorf("agent rollout waits for replacement operation %s: %w", op.ID, engine.ErrUnavailable)
			}
		}
	}
	return nil
}

// Only the temporary PXC routing withdrawal survives outside the operation
// checkpoint. A terminal/missing owner must not strand that control forever.
// Recovery here never submits an executable, including after a CP build change.
func (p *Core) reconcileOrphanedAgentDrain(ctx context.Context, rc *engine.Context, s *state) (bool, error) {
	for _, m := range s.Members {
		if !m.ProxyDraining && m.ProxyDrainOperation == "" {
			continue
		}
		ctx = freshMySQLReads(ctx)
		invalidateMySQLReadObservation(ctx, rc)
		defer invalidateMySQLReadObservation(ctx, rc)
		if m.ProxyDrainOperation != "" {
			if rc.Store == nil {
				return true, engine.ErrUnavailable
			}
			owner, err := rc.Store.GetOperation(ctx, m.ProxyDrainOperation)
			if err != nil && !errors.Is(err, store.ErrNotFound) {
				return true, err
			}
			if owner != nil && owner.Kind == domain.OperationUpgradeAgents &&
				owner.Cluster == rc.Cluster.Name && domain.NormalizedNamespace(owner.Namespace) == domain.NormalizedNamespace(rc.Cluster.Namespace) &&
				(owner.Phase == domain.PhasePending || owner.Phase == domain.PhaseRunning) {
				return true, nil
			}
		}
		if m.ConfirmedVolumeLost {
			// No native health or readmission is possible for this identity.
			// Confirmed loss remains a routing fence after releasing the owner.
			m.ProxyDraining = false
			m.ProxyDrainOperation = ""
			publishRevision(s)
			return true, nil
		}
		if _, err := p.reconcileFormation(ctx, rc, s); err != nil {
			return true, err
		}
		ok, err := ready(ctx, rc, m)
		if err != nil || !ok {
			return true, err
		}
		for _, proxy := range s.Members {
			if proxy.Role != "proxy" {
				continue
			}
			ok, err := ready(ctx, rc, proxy)
			if err != nil || !ok {
				return true, err
			}
		}
		if m.ProxyDraining {
			writer := false
			for _, peer := range s.data() {
				if peer.Name == m.Name {
					continue
				}
				native, err := live(ctx, rc, peer.Name)
				if err == nil && onlinePrimary(native) {
					if err := proxyAdmission(ctx, rc, s, peer.Name, myspec.ProxyWriterGroup); err == nil {
						writer = true
						break
					}
				}
			}
			if !writer {
				return true, fmt.Errorf("waiting for an admitted writer while settling orphaned agent rollout of %s: %w", m.Name, engine.ErrUnavailable)
			}
			m.ProxyDraining = false
			publishRevision(s)
			return true, nil
		}
		if err := proxyRejoinReady(ctx, rc, s, m.Name); err != nil {
			return true, err
		}
		m.ProxyDrainOperation = ""
		return true, nil
	}
	return false, nil
}

// A logical service probe can land on a proxy whose writer already moved.
// A member is still a routed writer while any expected instance serves it.
func agentUpgradePXCWriter(ctx context.Context, rc *engine.Context, s *state, name string) (bool, error) {
	found := false
	writer := false
	for _, proxy := range s.Members {
		if proxy.Role != "proxy" || proxy.Removing || proxy.Offline {
			continue
		}
		found = true
		notWriter, count, err := engine.VisitLiveInstancesConcurrent(ctx, rc, proxy.Name, func(a *agentapi.Client) (bool, error) {
			var status myspec.ProxyStatusResponse
			err := mysqlReadClientAction(ctx, rc, proxy.Name, a, myspec.ActionProxyStatus, struct{}{}, &status)
			return !proxyTargetOnline(status, name, myspec.ProxyWriterGroup), err
		})
		if err != nil {
			return false, err
		}
		if count < proxyExpectedReplicas(proxy) {
			return false, fmt.Errorf("waiting for all proxy instances before PXC writer disruption: %w", engine.ErrUnavailable)
		}
		writer = writer || !notWriter
	}
	if !found {
		return false, fmt.Errorf("PXC agent rollout requires a serving proxy: %w", engine.ErrUnavailable)
	}
	return writer, nil
}
