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
	v := domain.ClusterStatus{Phase: phase(s), Version: rc.Cluster.Spec.Version, ObservedGeneration: rc.Cluster.Status.ObservedGeneration, Connection: connection(rc.Cluster, s), Members: []domain.MemberStatus{}, Conditions: []domain.Condition{}}
	v.Message = s.platformFailureMessage()
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
	allHealthy := true
	for _, m := range ordered(s) {
		if m.Removing {
			continue
		}
		ms := domain.MemberStatus{Name: m.Name, Set: m.Set, Role: "unknown", Version: m.Version, State: "UNKNOWN"}
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
	v.Conditions = append(v.Conditions, domain.Condition{Type: "Healthy", Status: health, LastTransitionTime: now(rc)})
	if !allHealthy && v.Phase == domain.PhaseReady {
		v.Phase = domain.PhaseDegraded
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
