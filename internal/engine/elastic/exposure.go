// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"context"
	"encoding/json"

	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func (p *Provider) allocatePublicPort(ctx context.Context, rc *engine.Context, s *state, m *member) (int, error) {
	p.portMu.Lock()
	defer p.portMu.Unlock()
	ns := rc.Cluster.Ref().Namespace
	reserved := p.portReservations[ns]
	if reserved == nil {
		reserved = map[int]string{}
		p.portReservations[ns] = reserved
	}
	routes, err := rc.Backend.ListPublicPorts(ctx)
	if err != nil {
		return 0, err
	}
	external := map[int]string{}
	for _, route := range routes {
		external[route.Port] = route.Hostname
	}
	clusters, err := rc.Store.ListClusters(ctx, ns)
	if err != nil {
		return 0, err
	}
	for _, cluster := range clusters {
		if cluster.Spec.Provider != p.Name() {
			continue
		}
		state, err := load(&cluster)
		if err != nil {
			return 0, err
		}
		for _, member := range state.Members {
			if member.PublicPort > 0 {
				reserved[member.PublicPort] = cluster.Name + "/" + member.Name
			}
		}
	}
	for _, member := range s.Members {
		if member.PublicPort > 0 {
			reserved[member.PublicPort] = rc.Cluster.Name + "/" + member.Name
		}
	}
	owner := rc.Cluster.Name + "/" + m.Name
	for port, existing := range reserved {
		if existing == owner && (external[port] == "" || external[port] == m.Name) {
			return port, nil
		}
	}
	for port := 9200; port <= 65535; port++ {
		if reserved[port] == "" && external[port] == "" {
			reserved[port] = owner
			return port, nil
		}
	}
	return 0, invalid("no project TCP ports remain")
}
func (p *Provider) expose(ctx context.Context, rc *engine.Context, s *state) (bool, error) {
	options, err := parseStoredOptions(rc.Cluster.Spec.EngineOptions)
	if err != nil {
		return false, err
	}
	for _, m := range s.members(kindNode) {
		if m.Removing {
			continue
		}
		if !rc.Cluster.Spec.Exposure.Public {
			if m.PublicPort > 0 {
				if err = rc.Backend.DeletePublicPorts(ctx, m.Name); err != nil {
					return true, err
				}
				m.PublicPort = 0
				m.ExposureHash = ""
				return true, nil
			}
			continue
		}
		if err := engine.RequirePublicTCP(rc.Backend, rc.Cluster.Spec.Exposure.Allowlist); err != nil {
			return true, err
		}
		if m.PublicPort == 0 {
			port, err := p.allocatePublicPort(ctx, rc, s, m)
			if err != nil {
				return true, err
			}
			m.PublicPort = port
			if err = save(rc.Cluster, s); err != nil {
				return true, err
			}
		}
		route := infra.PublicPort{Port: m.PublicPort, TargetPort: 9200, Protocol: "tcp", IPType: rc.Cluster.Spec.Exposure.IPType, Allowlist: rc.Cluster.Spec.Exposure.Allowlist}
		b, _ := json.Marshal(route)
		hash := digest(b)
		if m.ExposureHash != hash {
			if err = verifyMemberServices(ctx, rc, m); err != nil {
				return true, err
			}
			if err = rc.Backend.EnsurePublicPort(ctx, m.Name, route); err != nil {
				return true, err
			}
			m.ExposureHash = hash
			return true, nil
		}
	}
	for _, m := range s.componentMembers() {
		desired := m.Kind == kindKibana && options.Kibana.Public || m.Kind == kindAPM && options.APM.Public
		if m.Subdomain != desired {
			action := "subdomain-off"
			if desired {
				action = "subdomain-on"
			}
			if _, err := engine.PublicHTTPS(rc.Backend); err != nil {
				return true, err
			}
			return true, p.issueAction(ctx, rc, s, &pending{Action: action, Member: m.Name})
		}
	}
	return false, nil
}
