// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package weaviate

import (
	"context"
	"fmt"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/weaviate/wvapi"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

func phase(s *state) domain.Phase {
	if s.Suspended {
		return domain.PhaseSuspended
	}
	if s.ScaleDown != nil {
		return domain.PhaseScaling
	}
	switch s.Phase {
	case "ready":
		return domain.PhaseReady
	case "", "creating", "provisioning":
		return domain.PhaseCreating
	case "forming":
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
	case "restoring":
		return domain.PhaseRestoring
	default:
		return domain.PhaseUpdating
	}
}
func connection(c *domain.Cluster, s *state) domain.Connection {
	v := domain.Connection{Hosts: []string{}, PublicHosts: []string{}, Endpoints: []domain.Endpoint{}}
	scheme := "https"
	if c.Spec.TLS.Mode == "disabled" {
		scheme = "http"
	}
	for _, m := range ordered(s) {
		if m.Removing {
			continue
		}
		host := m.Name + ":8080"
		v.Hosts = append(v.Hosts, host)
		v.Endpoints = append(v.Endpoints, domain.Endpoint{Name: "rest", URL: scheme + "://" + host}, domain.Endpoint{Name: "grpc", URL: m.Name + ":50051"})
	}
	if len(v.Hosts) > 0 {
		v.URI = scheme + "://" + v.Hosts[0]
	}
	return v
}
func connectionWithProject(c *domain.Cluster, s *state, project infra.Project) domain.Connection {
	v := connection(c, s)
	if !c.Spec.Exposure.Public {
		return v
	}
	ip := project.PublicIPv6
	if c.Spec.Exposure.IPType == "IPV4" {
		ip = project.PublicIPv4
	}
	if ip == "" {
		return v
	}
	for _, m := range ordered(s) {
		if m.Removing {
			continue
		}
		if m.PublicHTTPPort > 0 {
			host := net.JoinHostPort(ip, strconv.Itoa(m.PublicHTTPPort))
			v.PublicHosts = append(v.PublicHosts, host)
			v.Endpoints = append(v.Endpoints, domain.Endpoint{Name: "rest", URL: "https://" + host, Public: true})
		}
		if m.PublicGRPCPort > 0 {
			v.Endpoints = append(v.Endpoints, domain.Endpoint{Name: "grpc", URL: net.JoinHostPort(ip, strconv.Itoa(m.PublicGRPCPort)), Public: true})
		}
	}
	return v
}
func (p *Provider) Status(ctx context.Context, rc *engine.Context) (status domain.ClusterStatus, statusErr error) {
	defer func() { engine.ApplyDeploymentStatus(rc, &status) }()
	s, err := load(rc.Cluster)
	if err != nil {
		return status, err
	}
	status = domain.ClusterStatus{Phase: phase(s), Version: rc.Cluster.Spec.Version, ObservedGeneration: rc.Cluster.Status.ObservedGeneration, Members: []domain.MemberStatus{}, Conditions: []domain.Condition{}}
	condition := func(kind, value, reason, message string) {
		status.Conditions = append(status.Conditions, domain.Condition{Type: kind, Status: value, Reason: reason, Message: message, LastTransitionTime: now(rc)})
	}
	expired, err := expiredActiveLeaves(ctx, rc, s)
	if err != nil {
		return status, err
	}
	type sample struct {
		status agentapi.Status
		err    error
		at     time.Time
	}
	samples := make([]sample, len(s.Members))
	names := make([]string, len(s.Members))
	for i, m := range s.Members {
		names[i] = m.Name
	}
	var inventory engine.PlatformObservation
	var nodes []wvapi.Node
	var nativeErr error
	var quorumReady bool
	var wg sync.WaitGroup
	wg.Go(func() { inventory = engine.ObservePlatform(ctx, rc.Backend, rc.Cluster.Name, names) })
	if !s.Suspended {
		wg.Go(func() {
			probe, cancel := context.WithTimeout(ctx, engine.ObservationTimeout)
			defer cancel()
			client, e := p.native(probe, rc, s)
			if e != nil {
				nativeErr = e
				return
			}
			defer client.Close()
			nodes, nativeErr = client.Nodes(probe)
			if nativeErr != nil {
				return
			}
			stats, e := client.ClusterStatistics(probe)
			if e != nil {
				nativeErr = e
				return
			}
			quorumReady, nativeErr = validateClusterStatistics(s, stats)
		})
		for i, m := range s.Members {
			wg.Go(func() { samples[i].status, _, samples[i].err = p.memberReady(ctx, rc, m); samples[i].at = now(rc) })
		}
	}
	wg.Wait()
	status.Connection = connectionWithProject(rc.Cluster, s, inventory.Project)
	byName := map[string]wvapi.Node{}
	var objects int64
	var shards int
	for _, node := range nodes {
		byName[node.Name] = node
		objects += node.Stats.ObjectCount
		shards += node.Stats.ShardCount
	}
	allReady := !s.Suspended && nativeErr == nil && quorumReady && len(s.Members) == rc.Cluster.Spec.Components.Engine.Replicas && len(s.Members) > 0
	for i, m := range s.Members {
		ms := domain.MemberStatus{Name: m.Name, Role: "non-voter", Version: m.Version, State: "absent"}
		for _, service := range inventory.Services {
			if service.Hostname == m.Name {
				ms.Autoscaling = service.Autoscaling
				break
			}
		}
		if m.Index < 3 || rc.Cluster.Spec.Topology.Type == "single" {
			ms.Role = "voter"
		}
		sample := samples[i]
		if s.Suspended {
			ms.State = "suspended"
			ms.Agent.Message = "Runtime suspended; member agent is not running"
		} else {
			ms.Agent = domain.AgentStatus{Healthy: sample.err == nil, ConfigVersion: sample.status.ConfigVersion}
			if sample.err == nil {
				ms.Agent.Version, ms.Agent.Commit, ms.Agent.Build = sample.status.Agent.Version, sample.status.Agent.Commit, sample.status.Agent.Build
				ms.Agent.LastSeen = sample.at
				ms.Uptime = sample.status.UptimeSeconds
				ms.DiskUsage = float64(sample.status.Disk.UsedBytes)
				ms.PlacementGroup = sample.status.HostFingerprint
			} else {
				ms.Agent.Message = "Member readiness unavailable"
			}
			if node, ok := byName[m.Name]; ok {
				ms.State = strings.ToLower(node.Status)
				ms.Version = node.Version
				ms.Health = nativeErr == nil && quorumReady && node.Status == "HEALTHY" && node.Version == m.DesiredVersion && ms.Agent.Healthy
			}
			containers := inventory.Members[i]
			if containers.Err != nil || len(containers.Containers) == 0 {
				ms.Health = false
			}
			for _, container := range containers.Containers {
				if !strings.EqualFold(container.Status, infra.ContainerActive) && !strings.EqualFold(container.Status, infra.ContainerRunning) {
					ms.Health = false
				}
				if ms.PlacementGroup == "" && rc.Backend.Capabilities().Placement {
					ms.PlacementGroup = container.PlacementGroup
				}
			}
			ms.Resources = engine.MemberResources(sample.status.Resources, ms.Autoscaling)
			engine.ApplyAgentDiagnostic(&ms, sample.status.Error)
		}
		if expired[m.Name] != nil {
			ms.Health = false
			ms.Agent.Message = "Native certificate expired"
		}
		if m.Removing {
			ms.State = "removing"
			ms.Health = false
		}
		allReady = allReady && ms.Health
		status.Members = append(status.Members, ms)
		engine.ApplyRuntimeRecoveryStatus(&status, m.Name, m.Volume, m.RuntimeRecovery, now(rc))
	}
	if nativeErr != nil {
		condition("ClusterHealth", "Unknown", "Unreachable", "Native node or Raft observation unavailable")
		status.Message = "Native node or Raft observation unavailable"
	} else if s.Suspended {
		condition("ClusterHealth", "False", "Suspended", "Runtime suspended")
	} else {
		value := "False"
		if allReady {
			value = "True"
		}
		condition("ClusterHealth", value, "NativeNodes", fmt.Sprintf("%d objects, %d shards across %d observed nodes", objects, shards, len(nodes)))
		quorum := "False"
		if quorumReady {
			quorum = "True"
		} else {
			status.Message = "Raft members are not synchronized with the expected voter configuration"
		}
		condition("Quorum", quorum, "RaftConfiguration", "Synchronized Raft membership, leader and fixed voter configuration")
	}
	if s.ScaleDown != nil {
		status.Phase = domain.PhaseScaling
		if s.ScaleDown.Failure != "" {
			condition("ScaleDown", "False", "DrainBlocked", s.ScaleDown.Failure)
			status.Message = "Scale-down blocked: " + s.ScaleDown.Failure
		} else {
			condition("ScaleDown", "False", "Draining", "Retiring non-voter "+s.ScaleDown.Member)
		}
	}
	if len(expired) > 0 {
		message := "Native certificate expired; native TLS clients cannot connect"
		if s.Rolling != nil {
			message += "; renewal in progress"
		}
		condition("NativeTLS", "False", "NativeCertificateExpired", message)
		status.Message = message
		allReady = false
	}
	if status.Phase == domain.PhaseReady && !allReady {
		status.Phase = domain.PhaseDegraded
	}
	if inventory.ProjectErr != nil {
		return status, inventory.ProjectErr
	}
	if ctx.Err() != nil {
		return status, ctx.Err()
	}
	return status, nil
}
func (p *Provider) Connection(ctx context.Context, rc *engine.Context) (domain.ConnectionDetails, error) {
	s, err := load(rc.Cluster)
	if err != nil {
		return domain.ConnectionDetails{}, err
	}
	project, err := rc.Backend.ProjectInfo(ctx)
	if err != nil {
		return domain.ConnectionDetails{}, err
	}
	v := domain.ConnectionDetails{Connection: connectionWithProject(rc.Cluster, s, project)}
	ca, err := secret(ctx, rc, secretCA)
	if err != nil {
		return v, err
	}
	v.CACertificate = string(ca)
	key, err := secret(ctx, rc, secretAdmin)
	if err != nil {
		return v, err
	}
	v.Credentials = []domain.ConnectionCredential{{Username: AdminUser, Password: string(key), URI: v.Connection.URI}}
	return v, nil
}
func (*Provider) SystemUsers(_ context.Context, rc *engine.Context) ([]domain.SystemUser, error) {
	users := []domain.SystemUser{{Name: AdminUser}, {Name: ClusterUser}}
	if rc == nil || rc.Cluster == nil {
		return users, nil
	}
	s, err := load(rc.Cluster)
	if err != nil {
		return nil, err
	}
	for i := range users {
		if at, ok := s.LastRotated[users[i].Name]; ok {
			users[i].LastRotatedAt = &at
		}
	}
	return users, nil
}
func (*Provider) Placement(ctx context.Context, rc *engine.Context) (domain.Placement, error) {
	v := domain.Placement{Warning: "Infrastructure placement is best effort; independent failure domains are not guaranteed", Groups: map[string][]string{}}
	s, err := load(rc.Cluster)
	if err != nil {
		return v, err
	}
	for _, m := range s.Members {
		if m.Stage == "planned" {
			continue
		}
		containers, err := rc.Backend.Containers(ctx, m.Name)
		if err != nil {
			continue
		}
		for _, container := range containers {
			if rc.Backend.Capabilities().Placement && container.PlacementGroup != "" {
				v.Groups[container.PlacementGroup] = append(v.Groups[container.PlacementGroup], m.Name)
			}
		}
	}
	return v, nil
}
func (*Provider) MemberLogs(ctx context.Context, rc *engine.Context, name, process string, lines int) ([]domain.MemberLogLine, error) {
	s, err := load(rc.Cluster)
	if err != nil {
		return nil, err
	}
	if s.find(name) == nil {
		return nil, engine.ErrNotFound
	}
	if process != "agent" && process != "weaviate" {
		return nil, invalid("invalid member process")
	}
	if rc.Agents == nil {
		return nil, engine.ErrUnavailable
	}
	a, err := rc.Agents.Dial(ctx, rc.Cluster.Ref(), name)
	if err != nil {
		return nil, err
	}
	defer a.Close()
	logs, err := a.Logs(ctx, process, lines)
	if err != nil {
		return nil, err
	}
	out := make([]domain.MemberLogLine, 0, len(logs.Lines))
	for _, line := range logs.Lines {
		out = append(out, domain.MemberLogLine{Timestamp: line.Timestamp, Member: name, Process: process, Stream: line.Stream, Message: line.Message})
	}
	return out, nil
}
func (*Provider) ListDatabaseUsers(context.Context, *engine.Context) ([]domain.DatabaseUser, error) {
	return nil, engine.ErrNotSupported
}
func (*Provider) GetDatabaseUser(context.Context, *engine.Context, string, string) (*domain.DatabaseUser, error) {
	return nil, engine.ErrNotSupported
}
func (*Provider) CreateDatabaseUser(context.Context, *engine.Context, domain.DatabaseUser, string) (string, error) {
	return "", engine.ErrNotSupported
}
func (*Provider) UpdateDatabaseUser(context.Context, *engine.Context, domain.DatabaseUser, string) error {
	return engine.ErrNotSupported
}
func (*Provider) DeleteDatabaseUser(context.Context, *engine.Context, string, string) error {
	return engine.ErrNotSupported
}
func (*Provider) ListDatabaseRoles(context.Context, *engine.Context) ([]domain.DatabaseRole, error) {
	return nil, engine.ErrNotSupported
}
func (*Provider) GetDatabaseRole(context.Context, *engine.Context, string, string) (*domain.DatabaseRole, error) {
	return nil, engine.ErrNotSupported
}
func (*Provider) CreateDatabaseRole(context.Context, *engine.Context, domain.DatabaseRole) error {
	return engine.ErrNotSupported
}
func (*Provider) UpdateDatabaseRole(context.Context, *engine.Context, domain.DatabaseRole) error {
	return engine.ErrNotSupported
}
func (*Provider) DeleteDatabaseRole(context.Context, *engine.Context, string, string) error {
	return engine.ErrNotSupported
}
func (*Provider) Balancer(context.Context, *engine.Context) (domain.BalancerStatus, error) {
	return domain.BalancerStatus{}, engine.ErrNotSupported
}
func (*Provider) SetBalancer(context.Context, *engine.Context, bool) error {
	return engine.ErrNotSupported
}
func (*Provider) PITRWindow(context.Context, *engine.Context) ([]domain.TimeWindow, error) {
	return nil, engine.ErrNotSupported
}

var _ engine.DataPlane = (*Provider)(nil)
