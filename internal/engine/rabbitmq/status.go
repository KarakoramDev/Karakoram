// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package rabbitmq

import (
	"context"
	"fmt"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"net"
	"slices"
	"strconv"
)

func phase(s *internalState) domain.Phase {
	switch s.Phase {
	case "ready":
		return domain.PhaseReady
	case "import", "deploy", "agents", "secrets":
		return domain.PhaseCreating
	case "bootstrap":
		return domain.PhaseInitializing
	case "join", "remove", "scaling":
		return domain.PhaseScaling
	case "upgrade":
		return domain.PhaseUpgrading
	case "restore":
		return domain.PhaseRestoring
	case "cleanup":
		return domain.PhaseDeleting
	default:
		return domain.PhaseUpdating
	}
}
func (p *Provider) Status(ctx context.Context, rc *engine.Context) (domain.ClusterStatus, error) {
	s, err := loadState(rc.Cluster)
	if err != nil {
		return domain.ClusterStatus{}, err
	}
	view := *rc.Cluster
	view.Status.Connection = domain.Connection{}
	v := domain.ClusterStatus{Phase: phase(s), Version: rc.Cluster.Spec.Version, ObservedGeneration: rc.Cluster.Status.ObservedGeneration, Connection: connection(&view, s), Members: []domain.MemberStatus{}, Conditions: []domain.Condition{}, Placement: domain.Placement{Warning: "Best-effort observations only; host separation is not guaranteed.", Groups: map[string][]string{}}, Message: s.platformFailureMessage()}
	if s.Suspended {
		v.Phase = domain.PhaseSuspended
		return v, nil
	}
	v.Connection.PublicHosts = nil
	project, err := rc.Backend.ProjectInfo(ctx)
	if err != nil {
		return v, err
	}
	services, err := rc.Backend.ListServices(ctx, rc.Cluster.Name)
	if err != nil {
		return v, err
	}
	resources := map[string]domain.Resources{}
	for _, service := range services {
		resources[service.Hostname] = service.Resources
	}
	allHealthy := len(s.runningMembers()) == rc.Cluster.Spec.Components.Engine.Replicas
	alarms, partition := false, false
	names := []string{}
	for _, m := range s.runningMembers() {
		names = append(names, "rabbit@"+m.Name)
	}
	for _, m := range ordered(s) {
		if m.Removing {
			continue
		}
		ms := domain.MemberStatus{Name: m.Name, Set: "rabbitmq", Role: "broker", Version: m.Version, State: "UNKNOWN"}
		if r, ok := resources[m.Name]; ok {
			copy := r
			ms.Autoscaling = &copy
		}
		st, e := p.agentStatus(ctx, rc, m.Name)
		if e == nil {
			ms.Agent = domain.AgentStatus{Healthy: running(st), ConfigVersion: st.ConfigVersion, LastSeen: now(rc)}
			ms.Version = st.EngineVersion
			ms.Uptime = st.UptimeSeconds
			ms.DiskUsage = float64(st.Disk.UsedBytes)
			ms.PlacementGroup = st.HostFingerprint
			if st.HostFingerprint != "" {
				v.Placement.Groups[st.HostFingerprint] = append(v.Placement.Groups[st.HostFingerprint], m.Name)
			}
		} else {
			ms.Agent.Message = e.Error()
		}
		b, e := p.brokerStatus(ctx, rc, m.Name)
		if e == nil {
			ms.Health = ms.Agent.Healthy && slices.Contains(b.RunningNodes, "rabbit@"+m.Name) && !b.Maintenance
			ms.State = "RUNNING"
			if !ms.Health {
				ms.State = "DOWN"
			}
			if !noProblems(b.Alarms) {
				alarms = true
				ms.Health = false
			}
			if !noProblems(b.Partitions) {
				partition = true
				ms.Health = false
			}
			for _, name := range names {
				if !slices.Contains(b.RunningNodes, name) {
					// A stopped peer degrades the cluster, not this broker or
					// the partition condition without actual partition evidence.
					allHealthy = false
				}
			}
		} else {
			ms.Health = false
			ms.Agent.Message = e.Error()
		}
		containers, e := rc.Backend.Containers(ctx, m.Name)
		if e != nil {
			ms.Health = false
			ms.Agent.Message = fmt.Sprintf("container observation: %v", e)
		} else if len(containers) == 0 {
			ms.Health = false
		} else {
			for _, c := range containers {
				if c.Status != "ACTIVE" && c.Status != "RUNNING" {
					ms.Health = false
				}
				if ms.Resources == nil && c.Resources != nil {
					copy := *c.Resources
					ms.Resources = &copy
				}
			}
		}
		if m.PublicPort > 0 {
			ip := project.PublicIPv6
			if rc.Cluster.Spec.Exposure.IPType == "IPV4" {
				ip = project.PublicIPv4
			}
			if ip != "" {
				ms.PublicHost = net.JoinHostPort(ip, strconv.Itoa(m.PublicPort))
				v.Connection.PublicHosts = append(v.Connection.PublicHosts, ms.PublicHost)
			}
		}
		allHealthy = allHealthy && ms.Health
		v.Members = append(v.Members, ms)
		if m.ManagementPublic {
			if sub, ok := rc.Backend.(infra.SubdomainAccess); ok {
				urls, e := sub.SubdomainURLs(ctx, m.Name)
				if e != nil {
					return v, e
				}
				for _, url := range urls {
					v.Connection.Endpoints = append(v.Connection.Endpoints, domain.Endpoint{Name: "management", URL: url, Public: true})
				}
			}
		}
	}
	view.Status.Connection = v.Connection
	v.Connection = connection(&view, s)
	certificateCondition, err := providedCertificateCondition(ctx, rc)
	if err != nil {
		return v, err
	}
	if certificateCondition != nil {
		v.Conditions = append(v.Conditions, *certificateCondition)
	}
	condition := func(kind string, present bool, reason string) {
		status := "False"
		if present {
			status = "True"
		}
		v.Conditions = append(v.Conditions, domain.Condition{Type: kind, Status: status, Reason: reason, LastTransitionTime: now(rc)})
	}
	condition("Healthy", allHealthy, "ObservedMembers")
	condition("Alarms", alarms, "MemoryOrDisk")
	condition("Partition", partition, "ClusterMembership")
	v.Conditions = append(v.Conditions, domain.Condition{Type: "Placement", Status: "Unknown", Reason: "BestEffortOnly", Message: v.Placement.Warning, LastTransitionTime: now(rc)})
	for _, m := range ordered(s) {
		if m.RemovalBlocked != "" {
			v.Conditions = append(v.Conditions, domain.Condition{Type: "MemberRemovalBlocked", Status: "True", Reason: "UnsafeRemoval", Message: m.RemovalBlocked, LastTransitionTime: now(rc)})
			v.Message = m.RemovalBlocked
		}
	}
	if s.Retry != nil {
		if r := s.PlatformRetries[platformRetryKey(s.Retry)]; r != nil && r.CPUModeCooldown {
			condition("PlatformCooldown", true, "CPUModeChangeTooRecent")
		}
	}
	if !allHealthy && v.Phase == domain.PhaseReady {
		v.Phase = domain.PhaseDegraded
	}
	return v, nil
}
