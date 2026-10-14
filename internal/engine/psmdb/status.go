// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package psmdb

import (
	"context"
	"fmt"
	"strings"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/psmdb/mongoadmin"
	"github.com/KarakoramDev/Karakoram/internal/engine/psmdb/pbm"
)

func phase(s *internalState) domain.Phase {
	switch s.Phase {
	case "ready":
		return domain.PhaseReady
	case "secrets", "import", "deploy", "agents", "resume":
		return domain.PhaseCreating
	case "bootstrap", "users", "add-shard":
		return domain.PhaseInitializing
	case "scaling", "join", "remove":
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

// Status combines wire-protocol replication state, agent health and infrastructure observations.
func (p *Provider) Status(ctx context.Context, rc *engine.Context) (domain.ClusterStatus, error) {
	s, e := load(rc.Cluster)
	if e != nil {
		return domain.ClusterStatus{}, e
	}
	v := domain.ClusterStatus{Phase: phase(s), ObservedGeneration: rc.Cluster.Status.ObservedGeneration, Connection: connection(rc.Cluster, s), Members: []domain.MemberStatus{}, Conditions: []domain.Condition{}}
	if rc.Cluster.Generation == 1 && rc.Cluster.Status.ObservedGeneration == 0 && (v.Phase == domain.PhaseScaling || v.Phase == domain.PhaseUpdating) {
		v.Phase = domain.PhaseCreating
	}
	v.Message = s.platformFailureMessage()
	if v.Message == "" && v.Phase != domain.PhaseReady {
		v.Message = fmt.Sprintf("%s: stage %s; waiting for database members", v.Phase, s.Phase)
		if s.Pending != nil {
			v.Message = fmt.Sprintf("%s: platform %s for %s", v.Phase, s.Pending.Action, s.Pending.Member)
		}
	}
	if s.Retry != nil {
		if r := s.PlatformRetries[platformRetryKey(s.Retry)]; r != nil && r.CPUModeCooldown {
			v.Conditions = append(v.Conditions, domain.Condition{Type: "PlatformCooldown", Status: "True", Reason: "CPUModeChangeTooRecent", Message: v.Message, LastTransitionTime: now(rc)})
		}
	}
	certificateCondition, e := providedCertificateCondition(ctx, rc)
	if e != nil {
		return v, e
	}
	if certificateCondition != nil {
		v.Conditions = append(v.Conditions, *certificateCondition)
	}
	if s.Suspended {
		v.Phase = domain.PhaseSuspended
		v.ObservedGeneration = rc.Cluster.Generation
		v.Message = "Runtime suspended; retained volumes are available for resume"
		for _, member := range rc.Cluster.Status.Members {
			member.State, member.Health, member.Agent.Healthy = "SUSPENDED", false, false
			member.Agent.Message, member.PublicHost = "Runtime suspended; observations are historical", ""
			v.Members = append(v.Members, member)
		}
		if len(v.Members) == 0 {
			for _, m := range ordered(s) {
				v.Members = append(v.Members, domain.MemberStatus{Name: m.Name, Set: m.Set, Role: "unknown", State: "SUSPENDED", Agent: domain.AgentStatus{Message: "Runtime suspended"}})
			}
		}
		return v, nil
	}
	v.Connection.PublicHosts = nil
	project, e := rc.Backend.ProjectInfo(ctx)
	if e != nil {
		return v, e
	}
	services, e := rc.Backend.ListServices(ctx, rc.Cluster.Name)
	if e != nil {
		return v, e
	}
	autoscaling := make(map[string]domain.Resources, len(services))
	for _, service := range services {
		if service.Resources.CPUMode != "" {
			autoscaling[service.Hostname] = service.Resources
		}
	}
	observed := map[string]mongoadmin.Status{}
	for set, st := range s.Sets {
		if st.Stage != "ready" {
			continue
		}
		c, e := p.dial(ctx, rc, s, set, "clusterMonitor")
		if e != nil {
			v.Conditions = append(v.Conditions, domain.Condition{Type: "Replication/" + set, Status: "False", Reason: "Unreachable", Message: e.Error(), LastTransitionTime: now(rc)})
			continue
		}
		rs, e := mongoadmin.GetStatus(ctx, c)
		if e == nil {
			observed[set] = rs
		}
		if v.FCV == "" {
			v.FCV, _ = mongoadmin.FCV(ctx, c)
		}
		_ = c.Disconnect(ctx)
	}
	allHealthy := len(s.Members) > 0
	for _, m := range ordered(s) {
		ms := domain.MemberStatus{Name: m.Name, Set: m.Set, Role: "unknown", State: "UNKNOWN"}
		if r, ok := autoscaling[m.Name]; ok {
			ms.Autoscaling = &r
		}
		st, e := agentStatus(ctx, rc, m.Name)
		if e == nil {
			ms.Agent = domain.AgentStatus{Healthy: running(st, m.Role), ConfigVersion: st.ConfigVersion, LastSeen: now(rc)}
			ms.Version = st.EngineVersion
			ms.Uptime = st.UptimeSeconds
			ms.DiskUsage = float64(st.Disk.UsedBytes)
			ms.PlacementGroup = st.HostFingerprint
		} else {
			ms.Agent.Message = e.Error()
			v.Conditions = append(v.Conditions, domain.Condition{Type: "Observation/" + m.Name, Status: "Unknown", Reason: "AgentUnavailable", Message: "Member " + m.Name + " runtime version and process health could not be observed: " + e.Error(), LastTransitionTime: now(rc)})
		}
		if m.Role == agentapi.RoleRouter {
			ms.Role = "router"
			ms.State = "MONGOS"
			ms.Health = ms.Agent.Healthy
		} else {
			rs := observed[m.Set]
			for _, x := range rs.Members {
				if x.Name == m.Name+":27017" {
					ms.Role = memberRole(x.State)
					ms.State = x.State
					ms.Health = x.Health == 1 && ms.Agent.Healthy
					ms.LagSeconds = rs.Lag(x)
					ms.Optime = x.OptimeDate
					ms.Uptime = x.Uptime
				}
			}
		}
		containers, e := rc.Backend.Containers(ctx, m.Name)
		if e != nil {
			ms.Health = false
			ms.Agent.Message = fmt.Sprintf("container observation: %v", e)
		} else if len(containers) == 0 {
			ms.Health = false
		} else {
			for _, container := range containers {
				if container.Status != "ACTIVE" && container.Status != "RUNNING" {
					ms.Health = false
				}
				if ms.Resources == nil && container.Resources != nil {
					r := *container.Resources
					ms.Resources = &r
				}
			}
		}
		if m.PublicPort > 0 {
			ip := project.PublicIPv6
			if rc.Cluster.Spec.Exposure.IPType == "IPV4" {
				ip = project.PublicIPv4
			}
			if ip != "" {
				ms.PublicHost = publicHost(ip, m.PublicPort)
				if rc.Cluster.Spec.Topology.Type == "replicaSet" || m.Role == agentapi.RoleRouter {
					v.Connection.PublicHosts = append(v.Connection.PublicHosts, ms.PublicHost)
				}
			}
		}
		if m.Removing {
			ms.State, ms.Health = "LEAVING", false
			ms.Agent.Message = "Member is leaving; runtime remains visible until removal completes"
		}
		if ms.Version != "" && (v.Version == "" || newer(v.Version, ms.Version)) {
			v.Version = ms.Version
		}
		v.Members = append(v.Members, ms)
		allHealthy = allHealthy && ms.Health
	}
	v.Placement, e = p.Placement(ctx, rc)
	if e != nil {
		return v, e
	}
	v.Conditions = append(v.Conditions, domain.Condition{Type: "Placement", Status: "Unknown", Reason: "BestEffortOnly", Message: v.Placement.Warning, LastTransitionTime: now(rc)})
	health := "False"
	if allHealthy {
		health = "True"
	}
	v.Conditions = append(v.Conditions, domain.Condition{Type: "Healthy", Status: health, Reason: "MemberHealth", Message: fmt.Sprintf("%d members observed; unhealthy or leaving members remain visible", len(v.Members)), LastTransitionTime: now(rc)})
	if !allHealthy && v.Phase == domain.PhaseReady {
		v.Phase = domain.PhaseDegraded
		v.Message = "One or more members are unhealthy or cannot be observed; inspect member and replication conditions"
	}
	if rc.Cluster.Spec.Backup.Storage != "" && s.Phase == "ready" {
		env, e := p.pbmEnv(ctx, rc, s)
		if e != nil {
			return v, e
		}
		backup, _, e := pbm.SyncInventory(ctx, env)
		if e != nil {
			v.Conditions = append(v.Conditions, domain.Condition{Type: "Backup", Status: "False", Reason: "InventoryUnavailable", Message: e.Error(), LastTransitionTime: now(rc)})
		} else {
			v.Backup = backup
		}
	}
	for i := range v.Conditions {
		for _, old := range rc.Cluster.Status.Conditions {
			if old.Type == v.Conditions[i].Type && old.Status == v.Conditions[i].Status && old.Reason == v.Conditions[i].Reason {
				v.Conditions[i].LastTransitionTime = old.LastTransitionTime
				break
			}
		}
	}
	return v, nil
}

func memberRole(state string) string {
	switch strings.ToLower(state) {
	case "primary", "secondary", "arbiter", "recovering":
		return strings.ToLower(state)
	case "startup", "startup2":
		return "startup"
	default:
		return "unknown"
	}
}
