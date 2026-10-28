// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package neo4j

import (
	"context"
	"encoding/json"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/neo4j/neo4jspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"net"
	"strconv"
)

func discovery(ctx context.Context, rc *engine.Context, s *state) (domain.Connection, error) {
	c := domain.Connection{Hosts: []string{}, PublicHosts: []string{}}
	if s.Member == nil {
		return c, nil
	}
	host := net.JoinHostPort(s.Member.Name, "7687")
	c.Hosts = []string{host}
	c.URI = "neo4j+s://" + host
	c.Endpoints = []domain.Endpoint{{Name: "bolt", URL: c.URI}, {Name: "https", URL: "https://" + net.JoinHostPort(s.Member.Name, "7473")}}
	if s.Member.Public && rc.Cluster.Spec.Exposure.Public {
		project, err := rc.Backend.ProjectInfo(ctx)
		if err != nil {
			return c, err
		}
		for _, ip := range []struct{ kind, host string }{{"IPV4", project.PublicIPv4}, {"IPV6", project.PublicIPv6}} {
			if ip.host != "" && (rc.Cluster.Spec.Exposure.IPType == ip.kind || rc.Cluster.Spec.Exposure.IPType == "BOTH") {
				h := net.JoinHostPort(ip.host, "7687")
				c.PublicHosts = append(c.PublicHosts, h)
				c.Endpoints = append(c.Endpoints, domain.Endpoint{Name: "bolt", URL: "neo4j+s://" + h, Public: true})
			}
		}
	}
	return c, nil
}
func (p *Provider) Status(ctx context.Context, rc *engine.Context) (domain.ClusterStatus, error) {
	s, err := load(rc.Cluster)
	if err != nil {
		return domain.ClusterStatus{}, err
	}
	out := domain.ClusterStatus{Phase: domain.PhasePending, ObservedGeneration: rc.Cluster.Status.ObservedGeneration, ObservedAt: now(rc), Conditions: []domain.Condition{}, Members: []domain.MemberStatus{}, DeploymentFailures: rc.Cluster.Status.DeploymentFailures, Placement: domain.Placement{Groups: map[string][]string{}}}
	out.Connection, err = discovery(ctx, rc, s)
	if err != nil {
		return out, err
	}
	if s.Suspended {
		out.Phase = domain.PhaseSuspended
		return out, nil
	}
	if s.Member == nil {
		return out, nil
	}
	m := s.Member
	member := domain.MemberStatus{Name: m.Name, Set: "engine", Role: "single", State: m.Stage, Health: false}
	service, serviceErr := rc.Backend.GetService(ctx, m.Name)
	if serviceErr != nil {
		return out, serviceErr
	}
	if service == nil || service.Importing || (service.Status != infra.StatusActive && service.Status != infra.StatusRunning) {
		out.Phase = domain.PhaseUpdating
		out.Members = append(out.Members, member)
		engine.ApplyDeploymentStatus(rc, &out)
		return out, nil
	}
	r, e := readiness(ctx, rc)
	if e == nil {
		member.Version = r.Version
		out.Version = r.Version
		member.Agent = domain.AgentStatus{Healthy: true, ConfigVersion: r.ConfigVersion, LastSeen: now(rc)}
		cfg, e := p.MemberConfig(ctx, rc, m.Name)
		if e != nil {
			return out, e
		}
		var sp neo4jspec.Spec
		if e = json.Unmarshal(cfg.EngineSpec, &sp); e != nil {
			return out, e
		}
		member.Health = r.Ready && !r.CredentialMismatch && r.ConfigurationError == "" && r.ConfigVersion >= s.ConfigVersion && r.Version == rc.Cluster.Spec.Version && r.AppliedRestartToken == sp.RestartToken && m.Stage == "ready" && m.Pending == nil
		if s.Maintenance != "" || (rc.Cluster.Spec.DataSource != nil && !s.CloneComplete) {
			member.Health = false
		}
		if r.CredentialMismatch {
			out.Conditions = append(out.Conditions, domain.Condition{Type: "CredentialMismatch", Status: "True", Reason: "AdminAuthenticationRejected", Message: "Managed administrator credentials do not authenticate; rotate the neo4j system user to repair them (offline recovery causes a database outage)", LastTransitionTime: now(rc)})
		}
		if r.ConfigurationError != "" {
			out.Conditions = append(out.Conditions, domain.Condition{Type: "Ready", Status: "False", Reason: "ConfigurationApplyFailed", Message: "Neo4j managed configuration could not be applied; inspect agent diagnostics", LastTransitionTime: now(rc)})
		}
	}
	if rc.Agents != nil {
		probeCtx, cancel := context.WithTimeout(ctx, engine.ObservationTimeout)
		a, dialErr := rc.Agents.Dial(probeCtx, rc.Cluster.Ref(), m.Name)
		if dialErr == nil {
			generic, statusErr := a.Status(probeCtx)
			a.Close()
			if statusErr != nil {
				member.Health = false
				member.Agent.Healthy = false
			} else {
				member.Agent.Version, member.Agent.Commit, member.Agent.Build = generic.Agent.Version, generic.Agent.Commit, generic.Agent.Build
				if generic.Error != "" {
					engine.ApplyAgentDiagnostic(&member, "Neo4j agent could not apply its managed configuration")
				}
			}
		} else {
			member.Health = false
			member.Agent.Healthy = false
		}
		cancel()
	}
	if member.Health {
		out.Phase = domain.PhaseReady
	} else {
		out.Phase = domain.PhaseUpdating
	}
	if s.Maintenance != "" || m.Offline {
		out.Phase = domain.PhaseUpdating
		member.Health = false
	}
	if m.ServiceID == "" {
		out.Members = append(out.Members, member)
		engine.ApplyDeploymentStatus(rc, &out)
		return out, nil
	}
	containers, e := rc.Backend.Containers(ctx, m.Name)
	if e != nil {
		return out, e
	}
	for _, c := range containers {
		if c.PlacementGroup != "" {
			member.PlacementGroup = c.PlacementGroup
			out.Placement.Groups[c.PlacementGroup] = append(out.Placement.Groups[c.PlacementGroup], m.Name)
		}
	}
	out.Members = append(out.Members, member)
	engine.ApplyDeploymentStatus(rc, &out)
	return out, nil
}
func (*Provider) RelayTargets(c *domain.Cluster) []string {
	s, err := load(c)
	if err != nil {
		return nil
	}
	name := c.Name + "n0"
	if s.Member != nil {
		name = s.Member.Name
	}
	var out []string
	for _, port := range []int{7687, 7473, 9443} {
		out = append(out, net.JoinHostPort(name, strconv.Itoa(port)))
	}
	return out
}
func (*Provider) Metrics() []domain.MetricDescriptor {
	var out []domain.MetricDescriptor
	for _, name := range []string{"neo4j_up", "neo4j_heap_used_bytes", "neo4j_heap_committed_bytes", "neo4j_heap_max_bytes", "neo4j_heap_configured_bytes", "neo4j_pagecache_configured_bytes", "neo4j_nodes", "neo4j_relationships", "neo4j_transactions_active", "neo4j_store_size_bytes"} {
		unit := "count"
		if len(name) >= 6 && name[len(name)-6:] == "_bytes" {
			unit = "bytes"
		}
		out = append(out, domain.MetricDescriptor{Exposition: name, Name: name, Unit: unit, Scale: 1})
	}
	return out
}
