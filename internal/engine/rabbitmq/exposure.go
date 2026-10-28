// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package rabbitmq

import (
	"context"
	"encoding/json"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"strings"
)

func (p *Provider) allocatePublicPort(ctx context.Context, rc *engine.Context, s *internalState, m *member) (int, error) {
	p.portMu.Lock()
	defer p.portMu.Unlock()
	ns := rc.Cluster.Ref().Namespace
	reservations := p.portReservations[ns]
	if reservations == nil {
		reservations = map[int]string{}
		p.portReservations[ns] = reservations
	}
	routes, err := rc.Backend.ListPublicPorts(ctx)
	if err != nil {
		return 0, err
	}
	external := map[int]string{}
	for _, r := range routes {
		external[r.Port] = r.Hostname
	}
	clusters, err := rc.Store.ListClusters(ctx, ns)
	if err != nil {
		return 0, err
	}
	live := map[string]bool{rc.Cluster.Name: true}
	for _, c := range clusters {
		live[c.Name] = true
		var state struct {
			Members []struct {
				Name       string `json:"name"`
				PublicPort int    `json:"publicPort"`
			} `json:"members"`
		}
		if err = json.Unmarshal(c.Internal, &state); err != nil && len(c.Internal) > 0 {
			return 0, err
		}
		for _, m := range state.Members {
			if m.PublicPort > 0 {
				reservations[m.PublicPort] = c.Name + "/" + m.Name
			}
		}
	}
	for port, owner := range reservations {
		cluster, _, _ := strings.Cut(owner, "/")
		if !live[cluster] {
			delete(reservations, port)
		}
	}
	for _, m := range s.Members {
		if m.PublicPort > 0 {
			reservations[m.PublicPort] = rc.Cluster.Name + "/" + m.Name
		}
	}
	owner := rc.Cluster.Name + "/" + m.Name
	for port, existing := range reservations {
		if existing == owner && (external[port] == "" || external[port] == m.Name) {
			return port, nil
		}
	}
	for port := 5671; port <= 65535; port++ {
		if _, used := reservations[port]; !used && external[port] == "" {
			reservations[port] = owner
			return port, nil
		}
	}
	return 0, invalid("no project TCP ports remain")
}
func (p *Provider) syncExposure(ctx context.Context, rc *engine.Context, s *internalState) (bool, error) {
	for _, m := range s.runningMembers() {
		if err := verifyMemberServices(ctx, rc, m); err != nil {
			return false, err
		}
		target := 5671
		if m.TLSMode == "disabled" {
			target = 5672
		}
		desired := hash(struct {
			Exposure any
			Target   int
		}{rc.Cluster.Spec.Exposure, target})
		if m.PublicHash != desired {
			if rc.Cluster.Spec.Exposure.Public {
				if err := engine.RequirePublicTCP(rc.Backend, rc.Cluster.Spec.Exposure); err != nil {
					return false, err
				}
				if m.PublicPort == 0 {
					port, err := p.allocatePublicPort(ctx, rc, s, m)
					if err != nil {
						return false, err
					}
					m.PublicPort = port
					if err = s.save(rc.Cluster); err != nil {
						return false, err
					}
				}
				routes, err := rc.Backend.ListPublicPorts(ctx)
				if err != nil {
					return false, err
				}
				for _, route := range routes {
					if route.Hostname == m.Name && route.Port == m.PublicPort && route.TargetPort != target {
						if err := rc.Backend.DeletePublicPorts(ctx, m.Name); err != nil {
							return false, err
						}
						break
					}
				}
				if err := rc.Backend.EnsurePublicPort(ctx, m.Name, infra.PublicPort{Port: m.PublicPort, TargetPort: target, Protocol: "tcp", IPType: rc.Cluster.Spec.Exposure.IPType, Allowlist: rc.Cluster.Spec.Exposure.Allowlist}); err != nil {
					return false, err
				}
			} else {
				if err := rc.Backend.DeletePublicPorts(ctx, m.Name); err != nil {
					return false, err
				}
				p.portMu.Lock()
				delete(p.portReservations[rc.Cluster.Ref().Namespace], m.PublicPort)
				p.portMu.Unlock()
				m.PublicPort = 0
			}
			m.PublicHash = desired
			return true, nil
		}
		o, err := parseOptions(m.EngineOptions)
		if err != nil {
			return false, err
		}
		if m.ManagementPublic != o.ManagementUI.Public {
			if rc.Cluster.Status.ObservedGeneration != 0 && rc.WaitForWindow().RequeueAfter > 0 {
				return true, nil
			}
			return true, p.submit(ctx, rc, s, &pending{Action: "management", Member: m.Name})
		}
	}
	return false, nil
}
