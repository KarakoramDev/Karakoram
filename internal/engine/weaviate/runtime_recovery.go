// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package weaviate

import (
	"context"
	"fmt"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"slices"
)

func runtimeMember(ctx context.Context, rc *engine.Context, m *member) (engine.RuntimeMember, error) {
	tok, e := rc.Secrets.GetAgentToken(ctx, rc.Cluster.Ref(), m.Name)
	if e != nil {
		return engine.RuntimeMember{}, e
	}
	if tok == nil {
		return engine.RuntimeMember{}, engine.ErrUnavailable
	}
	return engine.RuntimeMember{MemberService: engine.MemberService{Cluster: rc.Cluster.Name, Member: m.Name, Volume: m.Volume, Token: tok.Token, Resources: runtimeResources(m.Resources), Containers: 1}, ServiceID: &m.ServiceID, VolumeID: m.VolumeID, Recovery: &m.RuntimeRecovery}, nil
}
func drainRuntimeRecovery(ctx context.Context, rc *engine.Context, s *state) (bool, error) {
	for _, m := range ordered(s) {
		if m.RuntimeRecovery.Op == nil && m.RuntimeRecovery.ImportPlan == nil {
			continue
		}
		// Import recovery may resubmit its persisted batch, not merely observe.
		if m.RuntimeRecovery.ImportPlan != nil {
			if e := verifyMemberServices(ctx, rc, m); e != nil {
				return true, e
			}
		}
		input, e := runtimeMember(ctx, rc, m)
		if e != nil {
			return true, e
		}
		out, e := engine.ObserveRuntimeRecovery(ctx, rc, input)
		if out.Imported {
			m.Stage = "imported"
		}
		return true, e
	}
	return false, nil
}
func (p *Provider) reconcileRuntime(ctx context.Context, rc *engine.Context, s *state) (bool, error) {
	for _, m := range ordered(s) {
		if m.Stage != "ready" && m.Stage != "deployed" {
			continue
		}
		input, e := runtimeMember(ctx, rc, m)
		if e != nil {
			return true, e
		}
		out, e := engine.ReconcileRuntime(ctx, rc, input)
		if e != nil {
			return true, e
		}
		if out.Imported {
			m.Stage = "imported"
		}
		if out.VolumeLost {
			safe, e := p.volumeRecoverySafe(ctx, rc, s, m)
			if e != nil {
				return true, e
			}
			return true, engine.RecoverLostVolume(ctx, rc, m.Name, m.Volume, m.VolumeID, safe)
		}
		if out.Busy {
			return true, nil
		}
	}
	return false, nil
}

// volumeRecoverySafe requires native evidence for every collection/shard. A
// configured RF alone does not prove that existing data has surviving replicas.
func (p *Provider) volumeRecoverySafe(ctx context.Context, rc *engine.Context, s *state, m *member) (bool, error) {
	o, e := parseOptionsWithDefaults(rc.Cluster.Spec.EngineOptions, rc.Cluster.Spec.Topology.Type)
	if e != nil {
		return false, e
	}
	if rc.Cluster.Spec.Topology.Type != "cluster" || o.ReplicationMinimumFactor < 2 {
		return false, nil
	}
	names := []string{}
	for _, peer := range s.Members {
		if peer.Name != m.Name {
			if _, _, e := p.memberReady(ctx, rc, peer); e != nil {
				return false, e
			}
			names = append(names, peer.Name)
		}
	}
	c, e := p.native(ctx, rc, s, names...)
	if e != nil {
		return false, e
	}
	defer c.Close()
	statistics, e := c.ClusterStatistics(ctx)
	if e != nil {
		return false, e
	}
	if !statistics.Synchronized {
		return false, fmt.Errorf("%w: surviving metadata replicas are not synchronized", engine.ErrConflict)
	}
	liveVoters := 0
	for _, stat := range statistics.Statistics {
		if slices.Contains(names, stat.Name) && slices.Contains(s.Voters, stat.Name) && stat.IsVoter && stat.Ready && stat.DBLoaded && stat.Open && stat.Status == "HEALTHY" {
			liveVoters++
		}
	}
	if liveVoters < len(s.Voters)/2+1 {
		return false, fmt.Errorf("%w: lost-volume replacement requires a surviving Raft voting quorum", engine.ErrConflict)
	}
	collections, e := c.Collections(ctx)
	if e != nil {
		return false, e
	}
	for _, collection := range collections {
		sharding, e := c.ShardingState(ctx, collection.Class)
		if e != nil {
			return false, e
		}
		if sharding.ShardingState.Collection != collection.Class || sharding.ShardingState.Shards == nil {
			return false, fmt.Errorf("missing native sharding evidence; restore original volume or a backup")
		}
		for _, shard := range sharding.ShardingState.Shards {
			if shard.Shard == "" || len(shard.Replicas) == 0 {
				return false, fmt.Errorf("missing native shard replica evidence; restore original volume or a backup")
			}
			if slices.Contains(shard.Replicas, m.Name) {
				surviving := false
				for _, replica := range shard.Replicas {
					surviving = surviving || slices.Contains(names, replica)
				}
				if !surviving {
					return false, fmt.Errorf("member %s holds the only replica of collection %s shard %s; recover its original volume or restore a backup", m.Name, collection.Class, shard.Shard)
				}
			}
		}
	}
	return true, nil
}
