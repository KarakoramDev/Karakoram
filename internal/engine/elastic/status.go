// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"context"
	"net"
	"strconv"
	"strings"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/elastic/esapi"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func phase(s *state) domain.Phase {
	if s.Suspended {
		return domain.PhaseSuspended
	}
	if s.RestoreActive {
		return domain.PhaseRestoring
	}
	switch s.Phase {
	case "ready":
		return domain.PhaseReady
	case "provisioning":
		return domain.PhaseCreating
	case "forming", "components":
		return domain.PhaseInitializing
	case "scaling":
		return domain.PhaseScaling
	case "rolling":
		if s.Rolling != nil && s.Rolling.Reason == "upgrade" {
			return domain.PhaseUpgrading
		}
		return domain.PhaseUpdating
	case "cleanup":
		return domain.PhaseDeleting
	default:
		return domain.PhaseUpdating
	}
}
func connection(c *domain.Cluster, s *state) domain.Connection {
	v := domain.Connection{Hosts: []string{}, PublicHosts: []string{}, Endpoints: []domain.Endpoint{}}
	for _, m := range s.members(kindNode) {
		if !m.Removing {
			host := m.Name + ":9200"
			v.Hosts = append(v.Hosts, host)
			v.Endpoints = append(v.Endpoints, domain.Endpoint{Name: "elasticsearch", URL: "https://" + host})
		}
	}
	if len(v.Hosts) > 0 {
		v.URI = "https://" + strings.Join(v.Hosts, ",")
	}
	for _, m := range s.componentMembers() {
		switch m.Kind {
		case kindKibana:
			v.Endpoints = append(v.Endpoints, domain.Endpoint{Name: "kibana", URL: "http://" + m.Name + ":5601"})
		case kindAPM:
			v.Endpoints = append(v.Endpoints, domain.Endpoint{Name: "apm", URL: "http://" + m.Name + ":8200"})
		case kindLogstash:
			v.Endpoints = append(v.Endpoints, domain.Endpoint{Name: "logstash-beats", URL: "tcp://" + m.Name + ":5044"}, domain.Endpoint{Name: "logstash-http", URL: "http://" + m.Name + ":8080"})
		}
	}
	return v
}
func (p *Provider) observedConnection(ctx context.Context, rc *engine.Context, s *state) (domain.Connection, error) {
	v := connection(rc.Cluster, s)
	project, err := rc.Backend.ProjectInfo(ctx)
	if err != nil {
		return v, err
	}
	for _, m := range s.members(kindNode) {
		if m.PublicPort > 0 && !m.Removing {
			ip := project.PublicIPv6
			if rc.Cluster.Spec.Exposure.IPType == "IPV4" {
				ip = project.PublicIPv4
			}
			if ip != "" {
				host := net.JoinHostPort(ip, strconv.Itoa(m.PublicPort))
				v.PublicHosts = append(v.PublicHosts, host)
				v.Endpoints = append(v.Endpoints, domain.Endpoint{Name: "elasticsearch", URL: "https://" + host, Public: true})
			}
		}
	}
	if sub, ok := rc.Backend.(infra.SubdomainAccess); ok {
		for _, m := range s.componentMembers() {
			if !m.Subdomain {
				continue
			}
			urls, err := sub.SubdomainURLs(ctx, m.Name)
			if err != nil {
				return v, err
			}
			for _, url := range urls {
				v.Endpoints = append(v.Endpoints, domain.Endpoint{Name: m.Kind, URL: url, Public: true})
			}
		}
	}
	return v, nil
}

// Status combines Elasticsearch health, member agents and infrastructure placement.
func (p *Provider) Status(ctx context.Context, rc *engine.Context) (domain.ClusterStatus, error) {
	s, err := load(rc.Cluster)
	if err != nil {
		return domain.ClusterStatus{}, err
	}
	v := domain.ClusterStatus{Phase: phase(s), Version: rc.Cluster.Spec.Version, ObservedGeneration: rc.Cluster.Status.ObservedGeneration, Members: []domain.MemberStatus{}, Conditions: []domain.Condition{}}
	condition := func(kind string, ok bool, reason, message string) {
		value := "False"
		if ok {
			value = "True"
		}
		v.Conditions = append(v.Conditions, domain.Condition{Type: kind, Status: value, Reason: reason, Message: message, LastTransitionTime: now(rc)})
	}
	condition("Bootstrapped", s.ClusterUUID != "", "ClusterFormation", "")
	certificatesValid := true
	checkCertificate := func(key string) {
		cert, e := secret(ctx, rc, key)
		if e != nil {
			certificatesValid = false
			return
		}
		expired, e := NeedsRenewal(cert, 0, now(rc))
		certificatesValid = certificatesValid && e == nil && !expired
	}
	checkCertificate(secretCA)
	for _, m := range s.Members {
		if m.Removing {
			continue
		}
		checkCertificate("tls/agents/" + m.Name + ".crt")
		if isES(m) {
			checkCertificate("tls/nodes/" + m.Name + "/transport.crt")
			checkCertificate("tls/nodes/" + m.Name + "/http.crt")
		}
	}
	condition("Certificates", certificatesValid, "ClusterTLS", "Cluster and member TLS certificate validity")
	if rc.Cluster.Spec.Topology.Type == "cluster" && rc.Cluster.Spec.Components.Engine.Replicas%2 == 0 {
		condition("QuorumWarning", true, "EvenVotingCount", "Even combined-node counts do not improve voting fault tolerance over the preceding odd count")
	}
	if s.DrainBlocked != "" {
		condition("ShardDrainBlocked", true, "InsufficientDataNodes", s.DrainBlocked)
	}
	if s.Retry != nil {
		condition("PlatformRetry", true, "Backoff", "A failed platform action is scheduled for retry")
		v.Message = "Waiting for a scheduled platform retry"
	}
	if v.Connection, err = p.observedConnection(ctx, rc, s); err != nil {
		return v, err
	}
	nodes := map[string]esapi.Node{}
	master := ""
	healthy := false
	if c, e := p.es(ctx, rc, s); e == nil {
		defer c.Close()
		h, e := c.Health(ctx, esapi.HealthOptions{})
		if e == nil {
			healthy, e = healthAcceptable(ctx, c, h, rc.Cluster.Spec.Components.Engine.Replicas)
			condition("ClusterHealth", healthy && e == nil, h.Status, "Elasticsearch shard health: "+h.Status)
		} else {
			condition("ClusterHealth", false, "Unreachable", "Elasticsearch is unreachable")
		}
		observed, e := c.Nodes(ctx)
		if e == nil {
			for _, node := range observed {
				nodes[node.Name] = node
			}
		}
		master, _ = c.MasterNode(ctx)
	} else {
		condition("ClusterHealth", false, "Unreachable", "Elasticsearch is not yet reachable")
	}
	componentsReady := true
	allReady := healthy
	for _, m := range s.Members {
		if m.Removing {
			continue
		}
		ms := domain.MemberStatus{Name: m.Name, Role: m.Kind, Version: m.Version, State: "absent"}
		if isES(m) {
			if m.Kind == kindMaster {
				ms.Role = "master"
			} else if masterEligible(rc.Cluster.Spec, m) {
				ms.Role = "master,data"
			} else {
				ms.Role = "data"
			}
			if node, ok := nodes[m.Name]; ok {
				ms.State = "joined"
				ms.Version = node.Version
				ms.Health = true
				if m.Name == master {
					ms.State = "elected-master"
				}
			}
		}
		a, e := p.agentClient(ctx, rc, m.Name)
		if e == nil {
			status, e := a.Status(ctx)
			a.Close()
			if e == nil {
				ms.Agent = domain.AgentStatus{Healthy: strings.EqualFold(status.Processes[product(m)].State, "running"), ConfigVersion: status.ConfigVersion, LastSeen: now(rc)}
				ms.Uptime = status.UptimeSeconds
				ms.DiskUsage = float64(status.Disk.UsedBytes)
				ms.PlacementGroup = status.HostFingerprint
				if !isES(m) {
					ms.Version = status.EngineVersion
					ms.Health = ms.Agent.Healthy
					if ms.Health {
						ms.State = "joined"
					}
				}
			} else {
				ms.Agent.Message = "Member status unavailable"
			}
		} else {
			ms.Agent.Message = "Member agent unreachable"
		}
		ms.Health = ms.Health && ms.Agent.Healthy
		if !isES(m) {
			componentsReady = componentsReady && ms.Health
		}
		if service, e := rc.Backend.GetService(ctx, m.Name); e == nil && service != nil {
			resources := service.Resources
			ms.Autoscaling = &resources
		}
		containers, e := rc.Backend.Containers(ctx, m.Name)
		if e != nil || len(containers) == 0 {
			ms.Health = false
		} else {
			for _, container := range containers {
				if !strings.EqualFold(container.Status, "ACTIVE") && !strings.EqualFold(container.Status, "RUNNING") {
					ms.Health = false
				}
				if ms.Resources == nil {
					ms.Resources = container.Resources
				}
				if ms.PlacementGroup == "" {
					ms.PlacementGroup = container.PlacementGroup
				}
			}
		}
		if m.PublicPort > 0 {
			for _, host := range v.Connection.PublicHosts {
				_, port, _ := net.SplitHostPort(host)
				if port == strconv.Itoa(m.PublicPort) {
					ms.PublicHost = host
					break
				}
			}
		}
		allReady = allReady && ms.Health
		v.Members = append(v.Members, ms)
	}
	condition("ComponentsReady", componentsReady, "ComponentReadiness", "")
	if v.Phase == domain.PhaseReady && !allReady {
		v.Phase = domain.PhaseDegraded
	}
	v.Placement, err = p.Placement(ctx, rc)
	if err != nil {
		return v, err
	}
	if rc.Store != nil {
		backups, e := rc.Store.ListBackups(ctx, rc.Cluster.Ref())
		if e != nil {
			return v, e
		}
		for _, backup := range backups {
			if backup.Phase == domain.PhaseSucceeded && backup.CompletedAt != nil && (v.Backup.LastBackup == nil || backup.CompletedAt.After(*v.Backup.LastBackup)) {
				at := *backup.CompletedAt
				v.Backup.LastBackup = &at
			}
		}
	}
	return v, nil
}

// Connection returns credentials only through the protected data-plane endpoint.
func (p *Provider) Connection(ctx context.Context, rc *engine.Context) (domain.ConnectionDetails, error) {
	s, err := load(rc.Cluster)
	if err != nil {
		return domain.ConnectionDetails{}, err
	}
	v := domain.ConnectionDetails{}
	if v.Connection, err = p.observedConnection(ctx, rc, s); err != nil {
		return v, err
	}
	ca, err := secret(ctx, rc, secretCA)
	if err != nil {
		return v, err
	}
	v.CACertificate = string(ca)
	for _, entry := range []struct{ name, key string }{{"elastic", secretElastic}, {"apm-secret-token", secretAPMToken}} {
		pw, err := secret(ctx, rc, entry.key)
		if err != nil {
			return v, err
		}
		v.Credentials = append(v.Credentials, domain.ConnectionCredential{Username: entry.name, Password: string(pw), URI: v.Connection.URI})
	}
	return v, nil
}

// SystemUsers lists managed identities without credential material.
func (*Provider) SystemUsers(context.Context, *engine.Context) ([]domain.SystemUser, error) {
	out := []domain.SystemUser{}
	for _, name := range []string{AdminUser, MonitorUser, "elastic", LogstashUser, APMUser, "kibana-token", "apm-secret-token"} {
		out = append(out, domain.SystemUser{Name: name})
	}
	return out, nil
}

// Placement reports best-effort placement without promising anti-affinity.
// Members whose runtime is not imported yet, or whose containers cannot be
// listed, are omitted rather than failing status reconciliation.
func (*Provider) Placement(ctx context.Context, rc *engine.Context) (domain.Placement, error) {
	s, err := load(rc.Cluster)
	v := domain.Placement{Warning: "Infrastructure placement is best effort; independent failure domains are not guaranteed", Groups: map[string][]string{}}
	if err != nil {
		return v, err
	}
	for _, m := range s.Members {
		if m.Stage == stagePlanned {
			continue
		}
		containers, err := rc.Backend.Containers(ctx, m.Name)
		if err != nil {
			continue
		}
		for _, container := range containers {
			if container.PlacementGroup != "" {
				v.Groups[container.PlacementGroup] = append(v.Groups[container.PlacementGroup], m.Name)
			}
		}
	}
	return v, nil
}

// MemberLogs reads a bounded, allowlisted member process log.
func (p *Provider) MemberLogs(ctx context.Context, rc *engine.Context, name, process string, lines int) ([]domain.MemberLogLine, error) {
	s, err := load(rc.Cluster)
	if err != nil {
		return nil, err
	}
	m := s.find(name)
	if m == nil {
		return nil, engine.ErrNotFound
	}
	if process != "agent" && process != product(m) {
		return nil, invalid("invalid member process")
	}
	a, err := p.agentClient(ctx, rc, name)
	if err != nil {
		return nil, err
	}
	defer a.Close()
	logs, err := a.Logs(ctx, process, lines)
	if err != nil {
		return nil, err
	}
	out := []domain.MemberLogLine{}
	for _, line := range logs.Lines {
		out = append(out, domain.MemberLogLine{Timestamp: line.Timestamp, Member: name, Process: process, Stream: line.Stream, Message: line.Message})
	}
	return out, nil
}

// ListDatabaseUsers rejects MongoDB-shaped requests; use native Elasticsearch access.
func (*Provider) ListDatabaseUsers(context.Context, *engine.Context) ([]domain.DatabaseUser, error) {
	return nil, engine.ErrNotSupported
}

// GetDatabaseUser rejects MongoDB-shaped requests; use native Elasticsearch access.
func (*Provider) GetDatabaseUser(context.Context, *engine.Context, string, string) (*domain.DatabaseUser, error) {
	return nil, engine.ErrNotSupported
}

// CreateDatabaseUser rejects MongoDB-shaped requests; use native Elasticsearch access.
func (*Provider) CreateDatabaseUser(context.Context, *engine.Context, domain.DatabaseUser, string) (string, error) {
	return "", engine.ErrNotSupported
}

// UpdateDatabaseUser rejects MongoDB-shaped requests; use native Elasticsearch access.
func (*Provider) UpdateDatabaseUser(context.Context, *engine.Context, domain.DatabaseUser, string) error {
	return engine.ErrNotSupported
}

// DeleteDatabaseUser rejects MongoDB-shaped requests; use native Elasticsearch access.
func (*Provider) DeleteDatabaseUser(context.Context, *engine.Context, string, string) error {
	return engine.ErrNotSupported
}

// ListDatabaseRoles rejects MongoDB-shaped requests; use native Elasticsearch access.
func (*Provider) ListDatabaseRoles(context.Context, *engine.Context) ([]domain.DatabaseRole, error) {
	return nil, engine.ErrNotSupported
}

// GetDatabaseRole rejects MongoDB-shaped requests; use native Elasticsearch access.
func (*Provider) GetDatabaseRole(context.Context, *engine.Context, string, string) (*domain.DatabaseRole, error) {
	return nil, engine.ErrNotSupported
}

// CreateDatabaseRole rejects MongoDB-shaped requests; use native Elasticsearch access.
func (*Provider) CreateDatabaseRole(context.Context, *engine.Context, domain.DatabaseRole) error {
	return engine.ErrNotSupported
}

// UpdateDatabaseRole rejects MongoDB-shaped requests; use native Elasticsearch access.
func (*Provider) UpdateDatabaseRole(context.Context, *engine.Context, domain.DatabaseRole) error {
	return engine.ErrNotSupported
}

// DeleteDatabaseRole rejects MongoDB-shaped requests; use native Elasticsearch access.
func (*Provider) DeleteDatabaseRole(context.Context, *engine.Context, string, string) error {
	return engine.ErrNotSupported
}

// Balancer is unsupported; Elasticsearch manages shard allocation itself.
func (*Provider) Balancer(context.Context, *engine.Context) (domain.BalancerStatus, error) {
	return domain.BalancerStatus{}, engine.ErrNotSupported
}

// SetBalancer is unsupported; Elasticsearch manages shard allocation itself.
func (*Provider) SetBalancer(context.Context, *engine.Context, bool) error {
	return engine.ErrNotSupported
}

// PITRWindow is unsupported; Elasticsearch recovery uses snapshots.
func (*Provider) PITRWindow(context.Context, *engine.Context) ([]domain.TimeWindow, error) {
	return nil, engine.ErrNotSupported
}

var _ engine.DataPlane = (*Provider)(nil)
