// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package weaviate

import (
	"context"
	"encoding/json"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"slices"
	"strings"
)

// releasePublicReservations only releases this owner's namespace entries.
// A stale checkpoint must never release a port since reserved by another worker.
// An empty member name releases the cluster after all runtime routes are gone.
func (p *Provider) releasePublicReservations(rc *engine.Context, memberName string) {
	p.portMu.Lock()
	defer p.portMu.Unlock()
	for port, owner := range p.portReservations[rc.Cluster.Ref().Namespace] {
		cluster, name, _ := strings.Cut(owner, "/")
		if cluster == rc.Cluster.Name && (memberName == "" || name == memberName) {
			delete(p.portReservations[rc.Cluster.Ref().Namespace], port)
		}
	}
}

func (p *Provider) releaseClusterPublicReservations(rc *engine.Context, s *state) {
	p.releasePublicReservations(rc, "")
	for _, m := range s.Members {
		m.PublicHTTPPort = 0
		m.PublicGRPCPort = 0
	}
}

func (p *Provider) allocatePublicPort(ctx context.Context, rc *engine.Context, s *state, m *member, target int) (int, error) {
	p.portMu.Lock()
	defer p.portMu.Unlock()
	ns := rc.Cluster.Ref().Namespace
	reserved := p.portReservations[ns]
	if reserved == nil {
		reserved = map[int]string{}
		p.portReservations[ns] = reserved
	}
	// Only this cluster's current checkpoint can release its retired members.
	// Other workers may hold reservations not yet represented in the store or
	// backend routes; rebuilding the namespace map would lose that exclusion.
	for port, owner := range reserved {
		cluster, name, _ := strings.Cut(owner, "/")
		if cluster == rc.Cluster.Name && s.find(name) == nil {
			delete(reserved, port)
		}
	}
	routes, err := rc.Backend.ListPublicPorts(ctx)
	if err != nil {
		return 0, err
	}
	external := map[int]bool{}
	for _, route := range routes {
		external[route.Port] = true
	}
	if rc.Store == nil {
		return 0, invalid("public port allocation requires store")
	}
	clusters, err := rc.Store.ListClusters(ctx, ns)
	if err != nil {
		return 0, err
	}
	// Stored assignments are only occupancy for this allocation. Importing them
	// into live reservations would resurrect a release whose cleared checkpoint
	// has not reached the store yet, and retain it after that checkpoint commits.
	for _, c := range clusters {
		if c.Name == rc.Cluster.Name {
			continue // The caller's checkpoint is newer than the stored cluster.
		}
		var stored struct {
			Members []struct {
				PublicPort int `json:"publicPort"`
				HTTP       int `json:"publicHttpPort"`
				GRPC       int `json:"publicGrpcPort"`
			} `json:"members"`
		}
		if len(c.Internal) > 0 {
			if err := json.Unmarshal(c.Internal, &stored); err != nil {
				return 0, err
			}
		}
		for _, member := range stored.Members {
			for _, port := range []int{member.PublicPort, member.HTTP, member.GRPC} {
				if port > 0 {
					external[port] = true
				}
			}
		}
	}
	for _, member := range s.Members {
		for _, port := range []int{member.PublicHTTPPort, member.PublicGRPCPort} {
			if port > 0 {
				external[port] = true
			}
		}
	}
	// Each listener is independently allocated; never return a member's other port.
	for port := target; port <= 65535; port++ {
		if !external[port] && reserved[port] == "" {
			reserved[port] = rc.Cluster.Name + "/" + m.Name
			return port, nil
		}
	}
	return 0, invalid("no project TCP ports remain")
}

// reconcileExposure performs at most one backend mutation per reconciliation.
// Port assignments are saved before publication, making uncertain replies safe
// to resolve from observed routes rather than allocating replacement ports.
func (p *Provider) reconcileExposure(ctx context.Context, rc *engine.Context, s *state) (bool, error) {
	if !rc.Cluster.Spec.Exposure.Public {
		assigned := false
		for _, m := range s.Members {
			assigned = assigned || m.PublicHTTPPort > 0 || m.PublicGRPCPort > 0
		}
		if !assigned {
			return false, nil
		}
	}
	routes, err := rc.Backend.ListPublicPorts(ctx)
	if err != nil {
		return false, err
	}
	for _, m := range ordered(s) {
		if m.Removing {
			continue
		}
		if !rc.Cluster.Spec.Exposure.Public {
			found := false
			for _, route := range routes {
				found = found || route.Hostname == m.Name
			}
			if found {
				if rc.Cluster.Status.ObservedGeneration != 0 && rc.WaitForWindow().RequeueAfter > 0 {
					return true, nil
				}
				if err := verifyMemberServices(ctx, rc, m); err != nil {
					return false, err
				}
				if err := rc.Backend.DeletePublicPorts(ctx, m.Name); err != nil {
					return true, err
				}
			}
			if found || m.PublicHTTPPort > 0 || m.PublicGRPCPort > 0 {
				p.releasePublicReservations(rc, m.Name)
				m.PublicHTTPPort = 0
				m.PublicGRPCPort = 0
				return true, s.save(rc.Cluster)
			}
			continue
		}
		if rc.Cluster.Spec.TLS.Mode != "requireTLS" {
			return false, invalid("public exposure requires requireTLS")
		}
		if err := engine.RequirePublicTCP(rc.Backend, rc.Cluster.Spec.Exposure); err != nil {
			return false, err
		}
		for _, listener := range []struct {
			port   *int
			target int
		}{{&m.PublicHTTPPort, 8080}, {&m.PublicGRPCPort, 50051}} {
			if *listener.port == 0 {
				port, err := p.allocatePublicPort(ctx, rc, s, m, listener.target)
				if err != nil {
					return false, err
				}
				*listener.port = port
				if err := s.save(rc.Cluster); err != nil {
					return false, err
				}
			}
			desired := infra.PublicPort{Port: *listener.port, TargetPort: listener.target, Protocol: "tcp", IPType: rc.Cluster.Spec.Exposure.IPType, Allowlist: rc.Cluster.Spec.Exposure.Allowlist}
			matched := false
			for _, route := range routes {
				if route.Port == desired.Port {
					if route.Hostname != m.Name {
						return false, invalid("public port %d is owned by another service", desired.Port)
					}
					matched = route.TargetPort == desired.TargetPort && strings.EqualFold(route.Protocol, desired.Protocol) && strings.EqualFold(route.IPType, desired.IPType) && slices.Equal(route.Allowlist, desired.Allowlist)
				}
			}
			if matched {
				continue
			}
			if rc.Cluster.Status.ObservedGeneration != 0 && rc.WaitForWindow().RequeueAfter > 0 {
				return true, nil
			}
			if err := verifyMemberServices(ctx, rc, m); err != nil {
				return false, err
			}
			if err := rc.Backend.EnsurePublicPort(ctx, m.Name, desired); err != nil {
				return true, err
			}
			return true, nil
		}
	}
	return false, nil
}
