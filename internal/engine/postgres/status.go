// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"context"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"net"
	"net/url"
	"strings"
	"time"
)

func connectionMember(rc *engine.Context, s *state) string {
	if rc.Cluster.Spec.Components.Extra["proxy"].Replicas > 0 {
		return rc.Cluster.Name + "px"
	}
	data := s.data()
	if len(data) > 0 {
		return data[0].Name
	}
	return rc.Cluster.Name + "p0"
}
func discovery(rc *engine.Context, s *state) domain.Connection {
	host := net.JoinHostPort(connectionMember(rc, s), "5432")
	u := url.URL{Scheme: "postgresql", Host: host, Path: "/postgres"}
	q := u.Query()
	mode := "verify-full"
	if rc.Cluster.Spec.TLS.Mode == "disabled" {
		mode = "disable"
	}
	q.Set("sslmode", mode)
	u.RawQuery = q.Encode()
	return domain.Connection{Hosts: []string{host}, URI: u.String(), PublicHosts: []string{}, Endpoints: []domain.Endpoint{{Name: "postgres", URL: u.String()}}}
}

func publicConnection(ctx context.Context, rc *engine.Context, conn domain.Connection) (domain.Connection, error) {
	if !rc.Cluster.Spec.Exposure.Public {
		return conn, nil
	}
	project, err := rc.Backend.ProjectInfo(ctx)
	if err != nil {
		return conn, err
	}
	host := project.PublicIPv6
	if rc.Cluster.Spec.Exposure.IPType == "IPV4" {
		host = project.PublicIPv4
	}
	if host == "" {
		return conn, nil
	}
	address := net.JoinHostPort(host, "5432")
	conn.PublicHosts = []string{address}
	u, err := url.Parse(conn.URI)
	if err != nil {
		return conn, err
	}
	u.Host = address
	conn.Endpoints = append(conn.Endpoints, domain.Endpoint{Name: "postgres-public", URL: u.String(), Public: true})
	return conn, nil
}
func (p *Core) Status(ctx context.Context, rc *engine.Context) (domain.ClusterStatus, error) {
	s, err := load(rc.Cluster)
	if err != nil {
		return domain.ClusterStatus{}, err
	}
	out := domain.ClusterStatus{Phase: domain.PhaseCreating, Version: rc.Cluster.Spec.Version, Members: []domain.MemberStatus{}, Connection: discovery(rc, s), Placement: domain.Placement{Groups: map[string][]string{}}}
	if s.Suspended {
		out.Phase = domain.PhaseSuspended
		return out, nil
	}
	healthy := len(s.data()) == rc.Cluster.Spec.Components.Engine.Replicas && len(s.Members) > 0
	writer := false
	quorumHealthy := false
	var replicationIssues []string
	for _, m := range s.Members {
		member := domain.MemberStatus{Name: m.Name, Set: "postgres", Role: "unknown", State: "starting", Version: m.Version}
		if m.Role == "proxy" {
			member.Set = "proxy"
			member.Role = "proxy"
		}
		if m.Stage == "planned" || m.Stage == "imported" {
			healthy = false
			out.Members = append(out.Members, member)
			continue
		}
		memberContext, cancel := context.WithTimeout(ctx, 5*time.Second)
		if rc.Agents != nil && m.Role != "proxy" {
			a, e := rc.Agents.Dial(ctx, rc.Cluster.Ref(), m.Name)
			if e == nil {
				st, e := a.Status(memberContext)
				if e == nil {
					member.Agent = domain.AgentStatus{Healthy: true, ConfigVersion: st.ConfigVersion, LastSeen: now(rc)}
					member.Uptime = st.UptimeSeconds
					member.Resources = engine.MemberResources(st.Resources, &m.Resources)
				}
			}
		}
		if member.Agent.Healthy {
			st, e := live(memberContext, rc, m.Name)
			if e == nil {
				member.Role = st.Role
				member.Health = st.Running
				member.State = st.Role
				if m.Role == "data" && s.Topology == "ha" {
					quorumHealthy = quorumHealthy || st.EtcdHealthy
					if st.Role == "replica" && (!st.Streaming || st.LagBytes > 16<<20) {
						member.State = "lagging"
						member.Health = false
						replicationIssues = append(replicationIssues, m.Name)
					} else if !st.Running {
						member.State = "stopped"
						replicationIssues = append(replicationIssues, m.Name)
					}
				}
				if st.Version != "" && m.Role == "data" {
					member.Version = st.Version
				}
				writer = writer || st.Running && (st.Role == "primary" || st.Role == "standalone")
			}
		}
		if m.Role == "proxy" {
			first := true
			all, count, e := engine.VisitLiveInstances(memberContext, rc, m.Name, func(a *agentapi.Client) (bool, error) {
				status, err := a.Status(memberContext)
				if err != nil {
					return false, err
				}
				if first {
					member.Agent.LastSeen = now(rc)
					member.Uptime = status.UptimeSeconds
					member.Resources = engine.MemberResources(status.Resources, &m.Resources)
				}
				if first || status.ConfigVersion < member.Agent.ConfigVersion {
					member.Agent.ConfigVersion = status.ConfigVersion
					first = false
				}
				var statusPG pgspec.StatusResponse
				err = a.Engine(memberContext, "postgres", "status", struct{}{}, &statusPG)
				return statusPG.Running, err
			})
			member.Agent.Healthy = e == nil && count > 0 && !first
			member.Health = member.Agent.Healthy && all
			if member.Health {
				member.State = "proxy"
			}
		}
		cancel()
		if m.Role == "data" && s.Topology == "ha" && !member.Agent.Healthy {
			member.State = "unreachable"
			replicationIssues = append(replicationIssues, m.Name)
		}
		healthy = healthy && member.Health && m.Stage == "joined" && member.Agent.ConfigVersion >= m.ConfigVersion && m.ConfigVersion == s.ConfigVersion
		containers, e := rc.Backend.Containers(ctx, m.Name)
		if e != nil {
			return out, e
		}
		for _, container := range containers {
			if container.PlacementGroup != "" {
				member.PlacementGroup = container.PlacementGroup
				out.Placement.Groups[container.PlacementGroup] = append(out.Placement.Groups[container.PlacementGroup], m.Name)
			}
		}
		out.Members = append(out.Members, member)
	}
	if healthy && writer && s.Pending == nil && s.Formed && s.Upgrade == nil && s.Replacement == nil && s.Rolling == nil && (s.Topology != "ha" || quorumHealthy) {
		out.Phase = domain.PhaseReady
		out.ObservedGeneration = rc.Cluster.Generation
	} else if rc.Cluster.Status.ObservedGeneration > 0 {
		out.Phase = domain.PhaseUpdating
	}
	condition := "False"
	reason := "MembersStarting"
	if out.Phase == domain.PhaseReady {
		condition = "True"
		reason = "MembersReady"
	}
	out.Conditions = []domain.Condition{{Type: "Ready", Status: condition, Reason: reason, LastTransitionTime: now(rc)}}
	if s.Topology == "ha" {
		quorum := domain.Condition{
			Type: "EtcdQuorum", Status: "True", Reason: "QuorumAvailable",
			LastTransitionTime: now(rc),
		}
		if !quorumHealthy {
			quorum.Status, quorum.Reason = "False", "QuorumUnavailable"
			quorum.Message = "No data member can complete a linearizable etcd request."
		}
		replication := domain.Condition{
			Type: "Replication", Status: "True", Reason: "ReplicasStreaming",
			LastTransitionTime: now(rc),
		}
		if len(replicationIssues) > 0 {
			replication.Status, replication.Reason = "False", "ReplicasUnavailable"
			replication.Message = "Stopped, non-streaming or more than 16 MiB behind: " + strings.Join(replicationIssues, ", ")
		}
		out.Conditions = append(out.Conditions, quorum, replication)
	}
	tlsCondition, err := providedCertificateCondition(ctx, rc)
	if err != nil {
		return out, err
	}
	if tlsCondition != nil {
		out.Conditions = append(out.Conditions, *tlsCondition)
	}
	out.Connection, err = publicConnection(ctx, rc, out.Connection)
	if err != nil {
		return out, err
	}
	if err := p.backupStatus(ctx, rc, &out); err != nil {
		return out, err
	}
	return out, nil
}
