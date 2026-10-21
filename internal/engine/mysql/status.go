// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"net"
	"net/url"
)

func connectionMember(rc *engine.Context, s *state) string {
	if rc.Cluster.Spec.Components.Extra["proxy"].Replicas > 0 {
		return rc.Cluster.Name + "mx"
	}
	if data := s.data(); len(data) > 0 {
		return data[0].Name
	}
	return rc.Cluster.Name + "m0"
}
func discovery(rc *engine.Context, s *state) domain.Connection {
	host := net.JoinHostPort(connectionMember(rc, s), "3306")
	u := url.URL{Scheme: "mysql", Host: host}
	q := u.Query()
	q.Set("tls", "true")
	u.RawQuery = q.Encode()
	return domain.Connection{Hosts: []string{host}, URI: u.String(), PublicHosts: []string{}, Endpoints: []domain.Endpoint{{Name: "mysql", URL: u.String()}}}
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
	address := net.JoinHostPort(host, "3306")
	conn.PublicHosts = []string{address}
	u, err := url.Parse(conn.URI)
	if err != nil {
		return conn, err
	}
	u.Host = address
	conn.Endpoints = append(conn.Endpoints, domain.Endpoint{Name: "mysql-public", URL: u.String(), Public: true})
	return conn, nil
}
func (p *Core) Status(ctx context.Context, rc *engine.Context) (status domain.ClusterStatus, err error) {
	defer func() { engine.ApplyDeploymentStatus(rc, &status) }()
	s, err := load(rc.Cluster)
	if err != nil {
		return status, err
	}
	status = domain.ClusterStatus{Phase: domain.PhaseCreating, Version: rc.Cluster.Spec.Version, ObservedAt: now(rc), ObservedGeneration: rc.Cluster.Status.ObservedGeneration, Members: []domain.MemberStatus{}, Conditions: []domain.Condition{}, Connection: discovery(rc, s), Placement: domain.Placement{Groups: map[string][]string{}}}
	if s.Suspended {
		status.Phase = domain.PhaseSuspended
		return status, nil
	}
	observed := observeStatusMembers(ctx, rc, s.Members)
	healthy := len(s.data()) == rc.Cluster.Spec.Components.Engine.Replicas && len(s.Members) > 0
	writer := false
	primary := ""
	for i, m := range s.Members {
		o := observed[i]
		member := domain.MemberStatus{Name: m.Name, Set: "mysql", Role: "unknown", State: "UNREACHABLE", Version: m.Version}
		if m.Role == "data" {
			version := m.Version
			if o.err == nil && o.status.Version != "" {
				version = o.status.Version
				member.Version = version
			}
			if version != "" && (status.Version == "" || upgradeAllowed(version, status.Version)) {
				status.Version = version
			}
		}
		if o.err == nil {
			member.Agent = domain.AgentStatus{Healthy: true, ConfigVersion: o.agent.ConfigVersion, LastSeen: now(rc)}
			member.Uptime = o.agent.UptimeSeconds
			member.Resources = engine.MemberResources(o.agent.Resources, &m.Resources)
			member.State = o.status.MemberState
			member.Role = o.status.MemberRole
			if member.State == "" {
				member.State = "OFFLINE"
			}
			if m.Role == "proxy" {
				member.Set = "proxy"
				member.Role = "proxy"
				member.State = "ONLINE"
			}
			member.Health = member.State == "ONLINE" && o.agent.ConfigVersion >= m.ConfigVersion && m.Stage == "joined"
			if m.Role == "data" && (s.Topology == "single" && member.State == "ONLINE" || onlinePrimary(o.status)) {
				writer = true
				primary = m.Name
			}
		}
		service, e := rc.Backend.GetService(ctx, m.Name)
		if e != nil {
			return status, e
		}
		if service == nil || service.Status != infra.StatusRunning && service.Status != infra.StatusActive {
			member.Health = false
		}
		containers, e := rc.Backend.Containers(ctx, m.Name)
		if e != nil {
			return status, e
		}
		for _, container := range containers {
			if container.PlacementGroup != "" {
				member.PlacementGroup = container.PlacementGroup
				status.Placement.Groups[container.PlacementGroup] = append(status.Placement.Groups[container.PlacementGroup], m.Name)
				break
			}
		}
		if !member.Health {
			healthy = false
		}
		status.Members = append(status.Members, member)
	}
	status.Conditions = append(status.Conditions, domain.Condition{Type: "WriterReady", Status: boolCondition(writer), Reason: "PrimaryObservation", Message: primary, LastTransitionTime: now(rc)})
	if rc.Cluster.Spec.Provider == "pxc" {
		condition := domain.Condition{Type: "GTIDHistorySafe", Status: "True", Reason: "QuorumHistoryCompatible", LastTransitionTime: now(rc)}
		if e := checkPXCQuorumGTIDs(ctx, rc, s); e != nil {
			healthy = false
			condition.Status, condition.Reason, condition.Message = "Unknown", "QuorumComparisonUnavailable", e.Error()
			if conflict, ok := e.(*myspec.GTIDQuorumError); ok && conflict.Diverged {
				condition.Status, condition.Reason = "False", "GTIDHistoryDiverged"
				for i := range status.Members {
					for _, name := range conflict.Members {
						if status.Members[i].Name == name {
							status.Members[i].Health = false
						}
					}
				}
			}
		}
		if incident := activeGTIDIncident(s); incident != nil {
			healthy = false
			condition.Status, condition.Reason, condition.Message = "False", "GTIDHistoryDiscontinuityLatched", incident.message()
		}
		status.Conditions = append(status.Conditions, condition)
	}
	if r := s.Formation.Reformation; r != nil && r.Blocked != "" {
		healthy = false
		status.Conditions = append(status.Conditions, domain.Condition{Type: "GroupReformationBlocked", Status: "True", Reason: "UnsafeOfflineGTID", Message: r.Blocked, LastTransitionTime: now(rc)})
	}
	if s.Formation.Blocked != "" {
		healthy = false
		status.Conditions = append(status.Conditions, domain.Condition{Type: "GroupReformationBlocked", Status: "True", Reason: "BootstrapEpochConsumed", Message: s.Formation.Blocked, LastTransitionTime: now(rc)})
	}
	if healthy && writer && (!clustered(s.Topology) || s.Formation.State == "formed") {
		status.Phase = domain.PhaseReady
	} else if s.Formation.State == "formed" {
		status.Phase = domain.PhaseDegraded
	}
	if s.Rolling != nil {
		status.Phase = domain.PhaseUpdating
		if s.Rolling.From != s.Rolling.To {
			status.Phase = domain.PhaseUpgrading
		}
		status.Conditions = append(status.Conditions, domain.Condition{Type: "MySQLMaintenance", Status: "True", Reason: "RollingMaintenance", Message: s.Rolling.Stage, LastTransitionTime: now(rc)})
	}
	if s.Replacement != nil || s.TLSRotation != nil || s.GaleraTLS != nil {
		status.Phase = domain.PhaseUpdating
	}
	for _, m := range s.data() {
		if m.Retirement != "" {
			status.Phase = domain.PhaseScaling
		}
	}
	if err = p.backupStatus(ctx, rc, &status); err != nil {
		return status, err
	}
	status.Connection, err = publicConnection(ctx, rc, status.Connection)
	return status, err
}
func boolCondition(v bool) string {
	if v {
		return "True"
	}
	return "False"
}
