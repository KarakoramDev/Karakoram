// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package weaviate

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/weaviate/wvapi"
	"slices"
	"sort"
	"time"
)

type scaleDown struct {
	engine.RetrySchedule
	Member     string    `json:"member"`
	Stage      string    `json:"stage"`
	MoveID     string    `json:"moveId,omitempty"`
	EmptyScans int       `json:"emptyScans,omitempty"`
	Attempts   int       `json:"attempts,omitempty"`
	RetryAt    time.Time `json:"retryAt,omitempty"`
	Failure    string    `json:"failure,omitempty"`
	Target     int       `json:"target"`
}

func (q *scaleDown) fail(rc *engine.Context, err error) {
	q.Attempts++
	q.Failure = err.Error()
	q.RetryAt = q.Schedule(now(rc), now(rc).Add(30*time.Second))
	rc.Cluster.Status.Message = "Scale-down blocked: " + q.Failure
}

func (p *Provider) advanceScaleDown(ctx context.Context, rc *engine.Context, s *state) (bool, error) {
	target := rc.Cluster.Spec.Components.Engine.Replicas
	q := s.ScaleDown
	if q == nil {
		if len(s.Members) <= target {
			return false, nil
		}
		if target < 3 || target < len(s.Voters) {
			return true, invalid("scale-down cannot remove fixed Raft voters")
		}
		members := ordered(s)
		var m *member
		for i := len(members) - 1; i >= 0; i-- {
			if !slices.Contains(s.Voters, members[i].Name) {
				m = members[i]
				break
			}
		}
		if m == nil {
			return true, invalid("scale-down would remove a fixed Raft voter")
		}
		if !m.RuntimeRecovery.Initialized {
			unplaced, e := engine.RuntimeUnplaced(ctx, rc, m.Name)
			if e != nil {
				return true, e
			}
			if unplaced {
				m.Removing = true
				m.UnplacedRetirement = &engine.UnplacedRetirement{}
				s.Phase = "scaling"
				rc.Cluster.Status.Phase = domain.PhaseScaling
				return true, nil
			}
		}
		s.ScaleDown = &scaleDown{Member: m.Name, Stage: "feasibility", Target: target}
		s.Phase = "scaling"
		rc.Cluster.Status.Phase = domain.PhaseScaling
		return true, nil
	}
	m := s.find(q.Member)
	if m == nil {
		s.ScaleDown = nil
		return true, nil
	}
	rc.Cluster.Status.Message = "Scaling down: " + q.Stage + " " + m.Name
	if q.Stage == "feasibility" || q.Stage == "drain" || q.Stage == "verify" {
		if target >= len(s.Members) {
			if q.MoveID != "" {
				c, e := p.native(ctx, rc, s)
				if e != nil {
					return true, e
				}
				defer c.Close()
				if e = c.CancelReplication(ctx, q.MoveID); e != nil && !wvapi.IsNotFound(e) {
					if !wvapi.IsConflict(e) {
						return true, e
					}
					// Native late MOVE states cannot be cancelled. Keep the
					// checkpoint until the same operation finishes.
					op, err := c.Replication(ctx, q.MoveID)
					if err != nil && !wvapi.IsNotFound(err) {
						return true, err
					}
					if err == nil && op.Status.State != wvapi.ReplicaReady && op.Status.State != wvapi.ReplicaCancelled {
						return true, nil
					}
				}
			}
			m.Removing = false
			s.ScaleDown = nil
			return true, nil
		}
	}
	if q.Target != target && q.Stage == "feasibility" {
		q.Target = target
		q.Attempts = 0
		q.Failure = ""
	}
	if q.Failure != "" {
		rc.Cluster.Status.Message = "Scale-down blocked: " + q.Failure
		if q.Attempts >= 3 || q.Waiting(now(rc), q.RetryAt) {
			return true, nil
		}
		q.Failure = ""
	}
	switch q.Stage {
	case "feasibility", "drain":
		c, e := p.native(ctx, rc, s)
		if e != nil {
			q.fail(rc, e)
			return true, nil
		}
		defer c.Close()
		if q.MoveID != "" {
			op, e := c.Replication(ctx, q.MoveID)
			if e != nil {
				if !wvapi.IsNotFound(e) {
					return true, e
				}
				q.MoveID = ""
				q.fail(rc, e)
				return true, nil
			}
			if op.Status.State == wvapi.ReplicaCancelled {
				q.MoveID = ""
				q.fail(rc, fmt.Errorf("replica MOVE failed for %s", m.Name))
				return true, nil
			}
			if op.Status.State != wvapi.ReplicaReady {
				return true, nil
			}
			q.MoveID = ""
			return true, nil
		}
		collections, e := c.Collections(ctx)
		if e != nil {
			q.fail(rc, e)
			return true, nil
		}
		type shard struct {
			collection string
			state      wvapi.ShardReplicas
		}
		shards := []shard{}
		loads := map[string]int{}
		for _, col := range collections {
			var replication struct {
				Factor int `json:"factor"`
			}
			if e = json.Unmarshal(col.ReplicationConfig, &replication); e != nil && len(col.ReplicationConfig) > 0 {
				q.fail(rc, e)
				return true, nil
			}
			if replication.Factor > target {
				q.fail(rc, fmt.Errorf("collection %s replication factor %d exceeds remaining members %d", col.Class, replication.Factor, target))
				return true, nil
			}
			sh, e := c.ShardingState(ctx, col.Class)
			if e != nil {
				q.fail(rc, e)
				return true, nil
			}
			for _, v := range sh.ShardingState.Shards {
				if len(v.Replicas) > target {
					q.fail(rc, fmt.Errorf("collection %s shard %s has %d replicas, exceeds remaining members %d", col.Class, v.Shard, len(v.Replicas), target))
					return true, nil
				}
				shards = append(shards, shard{col.Class, v})
				for _, name := range v.Replicas {
					loads[name]++
				}
			}
		}
		if q.Stage == "feasibility" {
			m.Removing = true
			q.Stage = "drain"
			return true, nil
		}
		nodes, e := c.Nodes(ctx)
		if e != nil {
			q.fail(rc, e)
			return true, nil
		}
		healthy := map[string]bool{}
		for _, n := range nodes {
			healthy[n.Name] = n.Status == "HEALTHY"
		}
		for _, sh := range shards {
			if !slices.Contains(sh.state.Replicas, m.Name) {
				continue
			}
			q.EmptyScans = 0
			retained := ordered(s)[:target]
			candidates := []*member{}
			for _, peer := range retained {
				if !peer.Removing && healthy[peer.Name] && !slices.Contains(sh.state.Replicas, peer.Name) {
					candidates = append(candidates, peer)
				}
			}
			sort.Slice(candidates, func(i, j int) bool {
				a, b := candidates[i], candidates[j]
				if loads[a.Name] == loads[b.Name] {
					return a.Index < b.Index
				}
				return loads[a.Name] < loads[b.Name]
			})
			if len(candidates) == 0 {
				q.fail(rc, fmt.Errorf("no healthy MOVE target for collection %s shard %s", sh.collection, sh.state.Shard))
				return true, nil
			}
			// Recover a submission whose response was lost before its ID was checkpointed.
			ops, e := c.Replications(ctx)
			if e != nil {
				q.fail(rc, e)
				return true, nil
			}
			for _, op := range ops {
				if op.SourceNode == m.Name && op.Collection == sh.collection && op.Shard == sh.state.Shard && op.Status.State != wvapi.ReplicaCancelled && op.Status.State != wvapi.ReplicaReady {
					q.MoveID = op.ID
					return true, nil
				}
			}
			op, e := c.Replicate(ctx, wvapi.ReplicateRequest{SourceNode: m.Name, TargetNode: candidates[0].Name, Collection: sh.collection, Shard: sh.state.Shard, Type: wvapi.ReplicaMove})
			if e != nil {
				q.fail(rc, e)
				return true, nil
			}
			q.MoveID = op.ID
			return true, nil
		}
		q.EmptyScans++
		if q.EmptyScans >= 2 {
			q.Stage = "verify"
		}
		return true, nil
	case "verify":
		c, e := p.native(ctx, rc, s)
		if e != nil {
			return true, e
		}
		defer c.Close()
		nodes, e := c.Nodes(ctx)
		if e != nil {
			return true, e
		}
		for _, n := range nodes {
			if n.Name == m.Name {
				if len(n.Shards) > 0 || n.Stats.ShardCount > 0 {
					q.Stage = "drain"
					q.EmptyScans = 0
					return true, nil
				}
				q.Stage = "stop-config"
				return true, nil
			}
		}
		return true, nil
	case "stop-config":
		cfg, e := p.MemberConfig(ctx, rc, m.Name)
		if e != nil {
			return true, e
		}
		cfg.DesiredProcesses = []string{}
		if e = activateConfig(ctx, rc, s, m, cfg); e != nil {
			return true, e
		}
		q.Stage = "stop"
		return true, nil
	case "stop":
		if e := verifyMemberServices(ctx, rc, m); e != nil {
			return true, e
		}
		s.Pending = &pending{Action: "stop", Member: m.Name}
		q.Stage = "fence"
		return true, nil
	case "fence":
		// A stopped process is not yet fenced: memberlist failure detection
		// must exclude it from placement at every surviving member first.
		for _, peer := range ordered(s) {
			if peer.Removing {
				continue
			}
			c, e := p.native(ctx, rc, s, peer.Name)
			if e != nil {
				return true, e
			}
			nodes, e := c.Nodes(ctx)
			c.Close()
			if e != nil {
				return true, e
			}
			for _, n := range nodes {
				// UNAVAILABLE/TIMEOUT only prove a failed RPC, not removal
				// from memberlist's storage candidates. Require absence.
				if n.Name == m.Name {
					return true, nil
				}
			}
		}
		q.Stage = "fenced-scan"
		return true, nil
	case "fenced-scan":
		// Never reuse pre-stop evidence. Sharding state includes physical
		// tenant shards, including tenants not currently loaded on the node.
		c, e := p.native(ctx, rc, s)
		if e != nil {
			return true, e
		}
		defer c.Close()
		collections, e := c.Collections(ctx)
		if e != nil {
			return true, e
		}
		for _, col := range collections {
			sh, e := c.ShardingState(ctx, col.Class)
			if e != nil {
				return true, e
			}
			for _, shard := range sh.ShardingState.Shards {
				if slices.Contains(shard.Replicas, m.Name) {
					q.Stage = "restart-config"
					q.EmptyScans = 0
					return true, nil
				}
			}
		}
		q.Stage = "raft"
		return true, nil
	case "restart-config":
		cfg, e := p.MemberConfig(ctx, rc, m.Name)
		if e != nil {
			return true, e
		}
		cfg.DesiredProcesses = []string{"weaviate"}
		if e = activateConfig(ctx, rc, s, m, cfg); e != nil {
			return true, e
		}
		q.Stage = "restart"
		return true, nil
	case "restart":
		s.Pending = &pending{Action: "deploy", Member: m.Name}
		q.Stage = "restart-ack"
		return true, nil
	case "restart-ack":
		if _, _, e := p.memberReady(ctx, rc, m); e != nil {
			return true, nil
		}
		m.CleanupStep = ""
		q.Stage = "drain"
		return true, nil
	case "raft":
		pw, e := secret(ctx, rc, secretClusterPassword)
		if e != nil {
			return true, e
		}
		cfg := wvapi.Config{Scheme: "http", Timeout: engine.ObservationTimeout}
		for _, peer := range ordered(s) {
			if !peer.Removing {
				cfg.Endpoints = append(cfg.Endpoints, peer.Name+":7947")
			}
		}
		if rc.Dialer != nil {
			cfg.DialContext = rc.Dialer.DialContext
		}
		c, e := wvapi.NewInternal(cfg, ClusterUser, string(pw))
		if e != nil {
			return true, e
		}
		defer c.Close()
		if e = c.RemoveNode(ctx, m.Name); e != nil && !wvapi.IsNotFound(e) {
			return true, e
		}
		q.Stage = "runtime"
		return true, nil
	case "runtime":
		if e := verifyMemberServices(ctx, rc, m); e != nil {
			return true, e
		}
		s.Pending = &pending{Action: "delete-runtime", Member: m.Name}
		q.Stage = "volume"
		return true, nil
	case "volume":
		if e := verifyMemberServices(ctx, rc, m); e != nil {
			return true, e
		}
		s.Pending = &pending{Action: "delete-volume", Member: m.Name}
		q.Stage = "forget"
		return true, nil
	case "forget":
		if e := deleteMemberSecrets(ctx, rc, m.Name); e != nil {
			return true, e
		}
		s.Members = slices.DeleteFunc(s.Members, func(v *member) bool { return v.Name == m.Name })
		s.ConfigVersion++
		s.ScaleDown = nil
		return true, nil
	}
	return true, fmt.Errorf("unknown scale-down stage %q", q.Stage)
}
