// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package weaviate

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sort"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/weaviate/wvspec"
)

func (p *Provider) upgradeAgents(ctx context.Context, rc *engine.Context, s *state, op *domain.Operation, cancel bool) (engine.OpResult, error) {
	cp := operationState{}
	if len(op.Checkpoint) > 0 {
		if err := json.Unmarshal(op.Checkpoint, &cp); err != nil {
			return engine.OpResult{}, err
		}
	}
	r := s.Rolling
	if r != nil && (r.Reason != "upgrade-agents" || r.OperationID != op.ID) {
		return engine.OpResult{}, fmt.Errorf("%w: another rolling operation is active", engine.ErrConflict)
	}
	if r == nil {
		if cp.Stage != "" || cancel {
			return operationResult(operationState{Stage: "complete"}, true)
		}
		target, err := engine.AgentUpgradeTarget(op)
		if err != nil {
			return engine.OpResult{}, err
		}
		if target.TargetBuild != engine.CurrentAgentBuild().Build {
			return engine.OpResult{}, invalid("agent upgrade target build differs from running control-plane build; re-run upgrade-agents")
		}
		if s.Pending != nil || s.ScaleDown != nil || s.Rotation != nil || s.InitialRestore != "" || s.Suspended {
			return engine.OpResult{}, fmt.Errorf("%w: agent upgrade requires no pending structural work", engine.ErrConflict)
		}
		for _, m := range s.Members {
			if m.Stage != "ready" || m.Version != m.DesiredVersion || m.ImportPlan != nil || m.RuntimeRecovery.Op != nil || m.RuntimeRecovery.ImportPlan != nil || m.RuntimeRecovery.Action != "" || m.RuntimeRecovery.VolumeLost || m.UnplacedRetirement != nil || m.Removing {
				return engine.OpResult{}, fmt.Errorf("%w: member %s has pending structural work", engine.ErrConflict, m.Name)
			}
		}
		ready, err := p.clusterReady(ctx, rc, s)
		if err != nil {
			return engine.OpResult{}, err
		}
		if !ready {
			return engine.OpResult{}, fmt.Errorf("%w: agent upgrade requires healthy synchronized peers", engine.ErrConflict)
		}
		c, err := p.native(ctx, rc, s)
		if err != nil {
			return engine.OpResult{}, err
		}
		stats, err := c.ClusterStatistics(ctx)
		c.Close()
		if err != nil {
			return engine.OpResult{}, err
		}
		leader := ""
		for _, st := range stats.Statistics {
			var name string
			if json.Unmarshal(st.LeaderID, &name) == nil && slices.Contains(s.Voters, name) {
				leader = name
				break
			}
		}
		members := ordered(s)
		sort.SliceStable(members, func(i, j int) bool {
			rank := func(m *member) int {
				if m.Name == leader {
					return 2
				}
				if slices.Contains(s.Voters, m.Name) {
					return 1
				}
				return 0
			}
			return rank(members[i]) < rank(members[j])
		})
		r = &rolling{Reason: "upgrade-agents", OperationID: op.ID, Step: "gate", AgentTarget: &target, Order: []string{}}
		for _, m := range members {
			r.Order = append(r.Order, m.Name)
		}
		s.Rolling = r
		return operationResult(operationState{Stage: "rolling"}, false)
	}
	if r.AgentTarget == nil {
		return engine.OpResult{}, invalid("agent rollout is missing its frozen target")
	}
	if r.Position == len(r.Order) || cancel && r.Step == "gate" {
		s.Rolling = nil
		return operationResult(operationState{Stage: "complete"}, true)
	}
	if s.Pending != nil {
		if cancel && r.Step == "deploy" {
			ready, err := p.agentUpgradeIntentSettled(ctx, rc, s)
			if err != nil || !ready {
				res, _ := operationResult(operationState{Stage: "rolling"}, false)
				return res, err
			}
			s.Pending = nil
			s.Rolling = nil
			return operationResult(operationState{Stage: "complete"}, true)
		}
		if r.Step != "ack" {
			if err := agentUpgradeBuildFence(s); err != nil {
				return engine.OpResult{}, err
			}
		}
		_, err := p.advancePending(ctx, rc, s)
		res, _ := operationResult(operationState{Stage: "rolling"}, false)
		return res, err
	}
	err := p.rollingStep(ctx, rc, s)
	res, _ := operationResult(operationState{Stage: "rolling"}, false)
	return res, err
}

func (p *Provider) agentUpgradeStep(ctx context.Context, rc *engine.Context, s *state) error {
	r := s.Rolling
	m := s.find(r.Order[r.Position])
	target := r.AgentTarget
	if target == nil {
		return invalid("agent rollout is missing its frozen target")
	}
	if r.Step != "ack" {
		if err := agentUpgradeBuildFence(s); err != nil {
			return err
		}
	}
	switch r.Step {
	case "gate":
		st, _, err := p.memberReady(ctx, rc, m)
		if err == nil && !engine.MemberNeedsAgentUpgrade(domain.MemberStatus{Agent: domain.AgentStatus{Healthy: true, Build: st.Agent.Build}}, *target) {
			r.Done = append(r.Done, m.Name)
			r.Position++
			return nil
		}
		if rc.WaitForWindow().RequeueAfter > 0 {
			return nil
		}
		if err = p.agentUpgradeSafe(ctx, rc, s, m); err != nil {
			return err
		}
		r.AgentPreviousBuild = st.Agent.Build
		// The pending owner persists the in-flight member before deployment.
		s.Pending = &pending{Action: "deploy", Member: m.Name}
		r.Step = "deploy"
	case "ack":
		st, _, err := p.memberReady(ctx, rc, m)
		if err != nil || st.Agent.Build != target.TargetBuild {
			return nil
		}
		ready, err := p.clusterReady(ctx, rc, s)
		if err != nil {
			return err
		}
		if !ready {
			return nil
		}
		m.Stage = "ready"
		r.Done = append(r.Done, m.Name)
		r.Position++
		r.Step = "gate"
	default:
		return invalid("unknown agent rollout step %q", r.Step)
	}
	return nil
}
func (p *Provider) agentUpgradeSafe(ctx context.Context, rc *engine.Context, s *state, m *member) error {
	ready, err := p.clusterReady(ctx, rc, s)
	if err != nil {
		return err
	}
	if !ready {
		return fmt.Errorf("%w: all peers must be ready and synchronized before agent redeployment", engine.ErrConflict)
	}
	if len(s.Members) == 1 {
		return nil
	}
	remaining := len(s.Voters)
	if slices.Contains(s.Voters, m.Name) {
		remaining--
	}
	if remaining < len(s.Voters)/2+1 {
		return fmt.Errorf("%w: agent redeployment would lose voting majority", engine.ErrConflict)
	}
	cfg, err := p.MemberConfig(ctx, rc, m.Name)
	if err != nil {
		return err
	}
	var spec wvspec.MemberSpec
	if err = json.Unmarshal(cfg.EngineSpec, &spec); err != nil {
		return err
	}
	if spec.ReplicationMinimumFactor < 2 {
		return fmt.Errorf("%w: replication factor must allow one member down", engine.ErrConflict)
	}
	c, err := p.native(ctx, rc, s)
	if err != nil {
		return err
	}
	defer c.Close()
	collections, err := c.Collections(ctx)
	if err != nil {
		return err
	}
	for _, col := range collections {
		var rf struct {
			Factor int `json:"factor"`
		}
		if err = json.Unmarshal(col.ReplicationConfig, &rf); err != nil {
			return err
		}
		if rf.Factor < 2 {
			return fmt.Errorf("%w: collection %s replication factor does not allow one member down", engine.ErrConflict, col.Class)
		}
		sharding, err := c.ShardingState(ctx, col.Class)
		if err != nil {
			return err
		}
		if sharding.ShardingState.Collection != col.Class || sharding.ShardingState.Shards == nil {
			return fmt.Errorf("%w: missing native shard replica evidence for collection %s", engine.ErrConflict, col.Class)
		}
		for _, shard := range sharding.ShardingState.Shards {
			if shard.Shard == "" || len(shard.Replicas) == 0 {
				return fmt.Errorf("%w: missing native shard replica evidence", engine.ErrConflict)
			}
			if !slices.Contains(shard.Replicas, m.Name) {
				continue
			}
			surviving := false
			for _, replica := range shard.Replicas {
				surviving = surviving || replica != m.Name && s.find(replica) != nil
			}
			if !surviving {
				return fmt.Errorf("%w: member %s holds the only replica of collection %s shard %s", engine.ErrConflict, m.Name, col.Class, shard.Shard)
			}
		}
	}
	return nil
}
func (*Provider) CanCancelOperation(_ context.Context, op *domain.Operation) error {
	if op.Kind != domain.OperationUpgradeAgents {
		return engine.ErrNotSupported
	}
	return nil
}
func (p *Provider) CancelOperation(ctx context.Context, rc *engine.Context, op *domain.Operation) (result engine.OpResult, err error) {
	previous := rc.Operation
	rc.Operation = op
	defer func() { rc.Operation = previous }()
	s, err := load(rc.Cluster)
	if err != nil {
		return result, err
	}
	defer func() {
		if e := s.save(rc.Cluster); err == nil {
			err = e
		}
	}()
	return p.upgradeAgents(ctx, rc, s, op, true)
}

// A terminal operation releases undisrupted work, never authorization for Sync
// to start the next deployment. Accepted work remains owned until it settles.
func (p *Provider) recoverAgentUpgrade(ctx context.Context, rc *engine.Context, s *state) error {
	raw, _ := json.Marshal(operationState{Stage: "rolling"})
	op := &domain.Operation{ID: s.Rolling.OperationID, Kind: domain.OperationUpgradeAgents, Checkpoint: raw}
	_, err := p.upgradeAgents(ctx, rc, s, op, true)
	return err
}

func agentUpgradeBuildFence(s *state) error {
	r := s.Rolling
	if r.AgentTarget == nil {
		return invalid("agent rollout is missing its frozen target")
	}
	if r.AgentTarget.TargetBuild == engine.CurrentAgentBuild().Build {
		return nil
	}
	if r.Step == "gate" {
		s.Rolling = nil
	}
	return invalid("agent upgrade target build differs from running control-plane build; re-run upgrade-agents")
}

func (p *Provider) agentUpgradeIntentSettled(ctx context.Context, rc *engine.Context, s *state) (bool, error) {
	r := s.Rolling
	m := s.find(r.Order[r.Position])
	if m == nil {
		return false, engine.ErrNotFound
	}
	st, _, err := p.memberReady(ctx, rc, m)
	if err != nil {
		return false, nil
	}
	if st.Agent.Build != r.AgentPreviousBuild && st.Agent.Build != r.AgentTarget.TargetBuild {
		return false, nil
	}
	return p.clusterReady(ctx, rc, s)
}
