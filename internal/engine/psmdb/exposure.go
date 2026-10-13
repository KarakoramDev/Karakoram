// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package psmdb

import (
	"context"
	"strings"

	"github.com/KarakoramDev/Karakoram/internal/engine"
)

// allocatePublicPort reserves the first free project-wide TCP port. The mutex
// covers concurrently reconciling clusters until their Internal reservation lands
// in SQLite; persisted reservations reconstruct the map after a process restart.
func (p *Provider) allocatePublicPort(ctx context.Context, rc *engine.Context, s *internalState, m *member) (int, error) {
	p.portMu.Lock()
	defer p.portMu.Unlock()
	routes, e := rc.Backend.ListPublicPorts(ctx)
	if e != nil {
		return 0, e
	}
	external := map[int]string{}
	for _, route := range routes {
		external[route.Port] = route.Hostname
	}
	clusters, e := rc.Store.ListClusters(ctx)
	if e != nil {
		return 0, e
	}
	live := map[string]bool{rc.Cluster.Name: true}
	for _, cluster := range clusters {
		live[cluster.Name] = true
		state, e := load(&cluster)
		if e != nil {
			return 0, e
		}
		for _, member := range state.Members {
			if member.PublicPort > 0 {
				p.portReservations[member.PublicPort] = cluster.Name + "/" + member.Name
			}
		}
	}
	for port, owner := range p.portReservations {
		cluster, _, _ := strings.Cut(owner, "/")
		if !live[cluster] {
			delete(p.portReservations, port)
		}
	}
	for _, member := range s.Members {
		if member.PublicPort > 0 {
			p.portReservations[member.PublicPort] = rc.Cluster.Name + "/" + member.Name
		}
	}
	owner := rc.Cluster.Name + "/" + m.Name
	for port, existing := range p.portReservations {
		if existing == owner && (external[port] == "" || external[port] == m.Name) {
			return port, nil
		}
	}
	for port := 27017; port <= 65535; port++ {
		if _, used := p.portReservations[port]; !used && external[port] == "" {
			p.portReservations[port] = owner
			return port, nil
		}
	}
	return 0, invalid("no project TCP ports remain")
}
