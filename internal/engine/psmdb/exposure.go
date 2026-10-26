// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package psmdb

import (
	"context"
	"strings"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

// restrictExposure removes owned routes before native health gates. Public
// endpoint creation remains in the health-gated portion of Sync.
func restrictExposure(ctx context.Context, rc *engine.Context, s *internalState) error {
	for _, m := range ordered(s) {
		ex := rc.Cluster.Spec.Exposure
		if rc.Cluster.Spec.Topology.Type == "sharded" && m.Role != agentapi.RoleRouter {
			ex.Public = false
		}
		h := hash(ex)
		if ex.Public || m.PublicHash == h && m.PublicPort == 0 {
			continue
		}
		service, err := rc.Backend.GetService(ctx, m.Name)
		if err != nil {
			return err
		}
		// Planned members do not have a service yet; positive absence also
		// proves there is no route to revoke.
		if service != nil {
			if err := verifyServiceOwnership(ctx, rc, m, service, false); err != nil {
				return err
			}
			if err := rc.Backend.DeletePublicPorts(ctx, m.Name); err != nil {
				return err
			}
		}
		m.PublicPort = 0
		m.PublicHash = h
	}
	return nil
}

// allocatePublicPort reserves the first free project-wide TCP port. The mutex
// covers concurrently reconciling clusters until their Internal reservation lands
// in SQLite; persisted reservations reconstruct the map after a process restart.
func (p *Provider) allocatePublicPort(ctx context.Context, rc *engine.Context, s *internalState, m *member) (int, error) {
	p.portMu.Lock()
	defer p.portMu.Unlock()
	namespace := rc.Cluster.Ref().Namespace
	reservations := p.portReservations[namespace]
	if reservations == nil {
		reservations = make(map[int]string)
		p.portReservations[namespace] = reservations
	}
	routes, e := rc.Backend.ListPublicPorts(ctx)
	if e != nil {
		return 0, e
	}
	external := map[int]string{}
	for _, route := range routes {
		external[route.Port] = route.Hostname
	}
	clusters, e := rc.Store.ListClusters(ctx, rc.Cluster.Ref().Namespace)
	if e != nil {
		return 0, e
	}
	live := map[string]bool{rc.Cluster.Name: true}
	for _, cluster := range clusters {
		live[cluster.Name] = true
		if cluster.Spec.Provider != p.Name() {
			// Other engines' state has a different shape; their routes are
			// reserved through ListPublicPorts above.
			continue
		}
		state, e := load(&cluster)
		if e != nil {
			return 0, e
		}
		for _, member := range state.Members {
			if member.PublicPort > 0 {
				reservations[member.PublicPort] = cluster.Name + "/" + member.Name
			}
		}
	}
	for port, owner := range reservations {
		cluster, _, _ := strings.Cut(owner, "/")
		if !live[cluster] {
			delete(reservations, port)
		}
	}
	for _, member := range s.Members {
		if member.PublicPort > 0 {
			reservations[member.PublicPort] = rc.Cluster.Name + "/" + member.Name
		}
	}
	owner := rc.Cluster.Name + "/" + m.Name
	for port, existing := range reservations {
		if existing == owner && (external[port] == "" || external[port] == m.Name) {
			return port, nil
		}
	}
	for port := 27017; port <= 65535; port++ {
		if _, used := reservations[port]; !used && external[port] == "" {
			reservations[port] = owner
			return port, nil
		}
	}
	return 0, invalid("no project TCP ports remain")
}
