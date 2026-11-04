// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"context"

	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
)

// groupView narrows replication decisions without copying formation state.
// Credentials, configuration versions, PKI and etcd remain formation-wide.
type groupView struct {
	formation *state
	id        int
}

func (s *state) group(id int) groupView { return groupView{formation: s, id: id} }

func (s *state) memberGroup(name string) groupView {
	if m := s.find(name); m != nil {
		return s.group(m.Group)
	}
	return s.group(0)
}

func (g groupView) formed() bool {
	if g.id == 0 {
		return g.formation.Formed
	}
	return g.formation.GroupFormed[g.id]
}

func (g groupView) data() []*member {
	members := []*member{}
	for _, m := range g.formation.Members {
		if m.Role == "data" && m.Group == g.id {
			members = append(members, m)
		}
	}
	return members
}

// primary requires exactly one observed leader in this replication group.
func (g groupView) primary(ctx context.Context, rc *engine.Context) (*member, pgspec.StatusResponse, error) {
	var leader *member
	var status pgspec.StatusResponse
	for _, m := range g.data() {
		if m.Removing {
			continue
		}
		st, err := live(ctx, rc, m.Name)
		if err != nil {
			if ctx.Err() != nil {
				return nil, status, ctx.Err()
			}
			continue
		}
		if g.formation.Topology == "distributed" && st.Leader != m.Name {
			continue
		}
		if st.Running && (st.Role == "primary" || st.Role == "standalone") {
			if leader != nil {
				return nil, status, engine.ErrUnavailable
			}
			leader, status = m, st
		}
	}
	if leader == nil {
		return nil, status, engine.ErrUnavailable
	}
	return leader, status, nil
}

func (s *state) groups() []groupView {
	groups := []groupView{}
	seen := map[int]bool{}
	for _, m := range s.data() {
		if !seen[m.Group] {
			groups = append(groups, s.group(m.Group))
			seen[m.Group] = true
		}
	}
	return groups
}

// maintenanceGroups keeps the client-facing coordinator available until all
// worker groups have converged.
func (s *state) maintenanceGroups() []groupView {
	groups := s.groups()
	for i, group := range groups {
		if group.id == 0 {
			copy(groups[i:], groups[i+1:])
			groups[len(groups)-1] = group
			break
		}
	}
	return groups
}

func (s *state) desiredDataCount(rc *engine.Context) int {
	count := rc.Cluster.Spec.Components.Engine.Replicas
	if s.Topology == "distributed" {
		o, err := ParseOptions(rc.Cluster.Spec.EngineOptions)
		if err == nil {
			count *= 1 + o.citusOptions().WorkerGroups
		}
	}
	return count
}

func patroniTopology(topology string) bool { return topology == "ha" || topology == "distributed" }
