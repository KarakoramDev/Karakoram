// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"context"
	"encoding/json"

	"github.com/KarakoramDev/Karakoram/internal/domain"
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

// seedExposure preserves only recoverable legacy policy, not an unadmitted spec.
// A TCP receipt contains a digest, so changed allowlists cannot be reconstructed.
func seedExposure(rc *engine.Context, s *state) {
	if rc.Cluster.Status.ObservedGeneration == rc.Cluster.Generation {
		s.NodeExposure.Seed(rc.Cluster.Spec.Exposure)
	}
	for _, m := range s.members(kindNode) {
		route := infra.PublicPort{Port: m.PublicPort, TargetPort: 9200, Protocol: "tcp", IPType: rc.Cluster.Spec.Exposure.IPType, Allowlist: rc.Cluster.Spec.Exposure.Allowlist}
		b, _ := json.Marshal(route)
		if m.PublicPort > 0 && m.ExposureHash == digest(b) {
			applied := rc.Cluster.Spec.Exposure
			applied.Public = true
			s.NodeExposure.Seed(applied)
		}
	}
	for _, m := range s.componentMembers() {
		if m.Subdomain {
			if m.Kind == kindKibana {
				s.KibanaExposure.Seed(domain.Exposure{Public: true})
			} else if m.Kind == kindAPM {
				s.APMExposure.Seed(domain.Exposure{Public: true})
			}
		}
	}
}

type effectiveExposure struct {
	node                  domain.Exposure
	kibana, apm, deferred bool
}

func resolveExposure(rc *engine.Context, s *state) (effectiveExposure, error) {
	options, err := parseStoredOptions(rc.Cluster.Spec.EngineOptions)
	if err != nil {
		return effectiveExposure{}, err
	}
	seedExposure(rc, s)
	node, nodeDeferred := s.NodeExposure.Resolve(rc, rc.Cluster.Spec.Exposure)
	kibana, kibanaDeferred := s.KibanaExposure.Resolve(rc, domain.Exposure{Public: options.Kibana.Public})
	apm, apmDeferred := s.APMExposure.Resolve(rc, domain.Exposure{Public: options.APM.Public})
	effective := effectiveExposure{node: node, kibana: kibana.Public, apm: apm.Public, deferred: nodeDeferred || kibanaDeferred || apmDeferred}
	return effective, save(rc.Cluster, s)
}

// revokeComponentExposure only removes explicitly disabled HTTPS routes. It is
// safe before readiness/configuration reconciliation; enabling stays in expose.
func (p *Provider) revokeComponentExposure(ctx context.Context, rc *engine.Context, s *state) (bool, error) {
	effective, err := resolveExposure(rc, s)
	if err != nil {
		return false, err
	}
	for _, m := range s.componentMembers() {
		desired := m.Kind == kindKibana && effective.kibana || m.Kind == kindAPM && effective.apm
		if m.Subdomain && !desired {
			return true, p.issueAction(ctx, rc, s, &pending{Action: "subdomain-off", Member: m.Name})
		}
	}
	return false, nil
}

func (p *Provider) expose(ctx context.Context, rc *engine.Context, s *state) (bool, error) {
	effective, err := resolveExposure(rc, s)
	if err != nil {
		return false, err
	}
	for _, m := range s.members(kindNode) {
		if m.Removing {
			continue
		}
		if !effective.node.Public {
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
		if err := engine.RequirePublicTCP(rc.Backend, effective.node); err != nil {
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
		route := infra.PublicPort{Port: m.PublicPort, TargetPort: 9200, Protocol: "tcp", IPType: effective.node.IPType, Allowlist: effective.node.Allowlist}
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
		desired := m.Kind == kindKibana && effective.kibana || m.Kind == kindAPM && effective.apm
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
	return effective.deferred, nil
}
