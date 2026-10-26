// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package psmdb

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/psmdb/mongoadmin"
	"github.com/KarakoramDev/Karakoram/internal/engine/psmdb/pbm"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func phase(s *internalState) domain.Phase {
	switch s.Phase {
	case "initial-restore-failed":
		return domain.PhaseFailed
	case "ready":
		return domain.PhaseReady
	case "replication":
		return domain.PhaseDegraded
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
func (p *Provider) Status(ctx context.Context, rc *engine.Context) (status domain.ClusterStatus, statusErr error) {
	defer func() { engine.ApplyDeploymentStatus(rc, &status) }()
	defer func() {
		statusErr = errors.Join(statusErr, engine.ApplyInitialDataSourceStatus(p, rc.Cluster, &status))
	}()
	s, e := load(rc.Cluster)
	if e != nil {
		return domain.ClusterStatus{}, e
	}
	v := domain.ClusterStatus{Phase: phase(s), ObservedGeneration: rc.Cluster.Status.ObservedGeneration, Connection: connection(rc.Cluster, s), Members: []domain.MemberStatus{}, Conditions: []domain.Condition{}}
	if s.RestoreConsistency != nil {
		v.Conditions = append(v.Conditions, domain.Condition{Type: "RestoreConsistency", Status: "False", Reason: "OperatorRecoveryRequired", Message: s.RestoreConsistency.Failure, LastTransitionTime: now(rc)})
	}
	if s.RestoreWorkerUnconfirmed {
		v.Conditions = append(v.Conditions, domain.Condition{Type: "RestoreWorker", Status: "Unknown", Reason: "RestoreWorkerUnconfirmed", Message: pbm.RestoreWorkerUnconfirmedMessage, LastTransitionTime: now(rc)})
	}
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
	observed := map[string]mongoadmin.Status{}
	// Probe every member concurrently: a dead early member must not prevent a
	// healthy peer from being observed within this sweep.
	type memberObservation struct {
		status          agentapi.Status
		err             error
		at              time.Time
		routerReady     bool
		routerErr       error
		routerProcesses []routerProcessObservation
		runtimeOK       bool
	}
	members := ordered(s)
	memberNames := make([]string, len(members))
	for i, m := range members {
		memberNames[i] = m.Name
	}
	var inventory engine.PlatformObservation
	inventoryReady := make(chan struct{})
	setNames := []string{}
	for set, state := range s.Sets {
		if state.Stage == "ready" {
			setNames = append(setNames, set)
		}
	}
	slices.Sort(setNames)
	setSamples := make([]replicationObservation, len(setNames))
	samples := make([]memberObservation, len(members))
	var probes sync.WaitGroup
	backup := rc.Cluster.Status.Backup
	var backupErr error
	if !rc.AvailabilityOnly && rc.Cluster.Spec.Backup.Storage != "" && s.Phase == "ready" {
		probes.Go(func() {
			// Backup metadata is independent of member reachability. Give the
			// entire inventory path its own budget alongside the member probes,
			// not the remainder of their shared sweep.
			probe, cancel := context.WithTimeout(ctx, engine.ObservationTimeout)
			defer cancel()
			env, err := p.pbmEnv(probe, rc, s)
			if err != nil {
				backupErr = err
				return
			}
			summary, _, err := pbm.SyncInventory(probe, env)
			backupErr = err
			if err == nil {
				backup = summary
			}
		})
	}
	probes.Go(func() {
		inventory = engine.ObservePlatform(ctx, rc.Backend, rc.Cluster.Name, memberNames)
		close(inventoryReady)
	})
	setSlots := make(chan struct{}, 8)
	for i, set := range setNames {
		probes.Go(func() {
			setSlots <- struct{}{}
			defer func() { <-setSlots }()
			setSamples[i] = p.observeReplication(ctx, rc, s, set)
		})
	}
	for i, m := range members {
		probes.Go(func() {
			samples[i].status, samples[i].err = agentStatus(ctx, rc, m.Name)
			samples[i].at = now(rc)
		})
		if m.Role == agentapi.RoleRouter {
			probes.Go(func() {
				<-inventoryReady
				probe, cancel := context.WithTimeout(ctx, engine.ObservationTimeout)
				defer cancel()
				samples[i].routerReady, samples[i].routerProcesses, samples[i].routerErr = routerInstancesHealthy(probe, rc, m, m.ConfigVersion, inventory.Members[i])
			})
		}
	}
	probes.Wait()
	if inventory.ProjectErr != nil || inventory.ServicesErr != nil {
		return v, errors.Join(inventory.ProjectErr, inventory.ServicesErr)
	}
	project := inventory.Project
	autoscaling := make(map[string]domain.Resources, len(inventory.Services))
	for _, service := range inventory.Services {
		if service.Resources.CPUMode != "" {
			autoscaling[service.Hostname] = service.Resources
		}
	}
	replicationHealthy := true
	if s.ManagedUsersError != "" {
		replicationHealthy = false
		reason, state := "CredentialRepairRequired", "False"
		if s.ManagedUsersRetryable {
			reason, state = "RepairRetrying", "Unknown"
		}
		v.Conditions = append(v.Conditions, domain.Condition{Type: "ManagedSystemUsers", Status: state, Reason: reason, Message: s.ManagedUsersError, LastTransitionTime: now(rc)})
	}
	for i, sample := range setSamples {
		observed[setNames[i]] = sample.status
		v.Conditions = append(v.Conditions, sample.condition)
		replicationHealthy = replicationHealthy && sample.condition.Status == "True"
		if v.FCV == "" {
			v.FCV = sample.fcv
		}
	}
	allHealthy := len(s.Members) > 0 && replicationHealthy
	for i, m := range members {
		ms := domain.MemberStatus{Name: m.Name, Set: m.Set, Role: "unknown", State: "UNKNOWN"}
		if r, ok := autoscaling[m.Name]; ok {
			ms.Autoscaling = &r
		}
		st, e := samples[i].status, samples[i].err
		var allocated *agentapi.ContainerResources
		if e == nil {
			allocated = st.Resources
			ms.Agent = domain.AgentStatus{Healthy: true, ConfigVersion: st.ConfigVersion, LastSeen: samples[i].at}
			ms.Version = st.EngineVersion
			ms.Uptime = st.UptimeSeconds
			ms.DiskUsage = float64(st.Disk.UsedBytes)
			if !rc.Backend.Capabilities().Placement {
				ms.PlacementGroup = st.HostFingerprint
			}
		} else {
			ms.Agent.Message = e.Error()
			v.Conditions = append(v.Conditions, domain.Condition{Type: "Observation/" + m.Name, Status: "Unknown", Reason: "AgentUnavailable", Message: "Member " + m.Name + " runtime version and process health could not be observed: " + e.Error(), LastTransitionTime: now(rc)})
		}
		if m.Role == agentapi.RoleRouter {
			ms.Role = "router"
			ms.State = "MONGOS"
			ms.Health = e == nil && running(st, m.Role)
			ready, err := samples[i].routerReady, samples[i].routerErr
			ms.Health = ms.Health && ready && err == nil
			if err != nil {
				ms.Agent.Message = err.Error()
				v.Conditions = append(v.Conditions, domain.Condition{Type: "Router/" + m.Name, Status: "False", Reason: "ConfigMetadataUnavailable", Message: err.Error(), LastTransitionTime: now(rc)})
			}
		} else {
			rs := observed[m.Set]
			for _, x := range rs.Members {
				if x.Name == m.Name+":27017" {
					ms.Role = memberRole(x.State)
					ms.State = x.State
					ms.Health = x.Health == 1 && e == nil && running(st, m.Role) && (x.State == "PRIMARY" || x.State == "SECONDARY") && rs.Lag(x) <= 10
					ms.LagSeconds = rs.Lag(x)
					ms.Optime = x.OptimeDate
					ms.Uptime = x.Uptime
				}
			}
		}
		containers, e := inventory.Members[i].Containers, inventory.Members[i].Err
		samples[i].runtimeOK = e == nil && len(containers) > 0
		if e != nil {
			ms.Health = false
			ms.Agent.Message = fmt.Sprintf("container observation: %v", e)
		} else if len(containers) == 0 {
			ms.Health = false
		} else {
			for _, container := range containers {
				if container.Status != infra.ContainerActive && container.Status != infra.ContainerRunning {
					ms.Health = false
					samples[i].runtimeOK = false
				}
				if rc.Backend.Capabilities().Placement && container.PlacementGroup != "" {
					ms.PlacementGroup = container.PlacementGroup
				}
			}
			ms.Resources = engine.MemberResources(allocated, ms.Autoscaling)
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
		engine.ApplyAgentDiagnostic(&ms, st.Error)
		if samples[i].err == nil {
			v.Conditions = append(v.Conditions, engine.ApplyProcessDiagnostics(&ms, st.Processes, "", samples[i].at)...)
		}
		for _, observation := range samples[i].routerProcesses {
			v.Conditions = append(v.Conditions, engine.ApplyProcessDiagnostics(&ms, observation.processes, observation.instance, observation.at)...)
		}
		v.Members = append(v.Members, ms)
		allHealthy = allHealthy && ms.Health
	}
	// A peer's replSetGetStatus can retain a dead primary until heartbeat
	// expiry. Do not publish WritablePrimary independently of this sweep's
	// member reachability, even if the earlier hello succeeded.
	for i := range v.Conditions {
		c := &v.Conditions[i]
		if c.Status != "True" || !strings.HasPrefix(c.Type, "Replication/") {
			continue
		}
		set := strings.TrimPrefix(c.Type, "Replication/")
		var details strings.Builder
		unavailable, affected, omitted := false, 0, 0
		for j, ms := range v.Members {
			if ms.Set != set || ms.Health && ms.Agent.Healthy {
				continue
			}
			affected++
			lagging := false
			// A reachable secondary can genuinely lag while changing away
			// from a partitioned sync source. Keep the health threshold, but
			// distinguish observed lag from an unavailable observation.
			if samples[j].runtimeOK && ms.Agent.Healthy && (ms.State == "PRIMARY" || ms.State == "SECONDARY") && ms.LagSeconds > 10 {
				for _, native := range observed[set].Members {
					if native.Name == ms.Name+":27017" && native.Health == 1 && !native.OptimeDate.IsZero() {
						lagging = true
						break
					}
				}
			}
			detail := "observation unavailable"
			if lagging {
				detail = fmt.Sprintf("replication lag %.1f seconds (10-second health threshold)", ms.LagSeconds)
			} else {
				unavailable = true
			}
			// Names and fixed diagnostic text only: never copy native/agent
			// errors into this bounded summary. Keep room for an overflow count.
			if details.Len()+len(ms.Name)+len(detail)+4 > 2048-96 {
				omitted++
				continue
			}
			if details.Len() > 0 {
				details.WriteString("; ")
			}
			details.WriteString(ms.Name)
			details.WriteString(": ")
			details.WriteString(detail)
		}
		if affected > 0 {
			c.Status, c.Reason = "False", "ReplicationLag"
			if unavailable {
				c.Status, c.Reason = "Unknown", "MemberObservationUnavailable"
			}
			if omitted > 0 {
				fmt.Fprintf(&details, "; %d more affected members; inspect member health", omitted)
			}
			c.Message = details.String()
		}
	}
	v.Placement = domain.Placement{Groups: map[string][]string{}}
	for _, ms := range v.Members {
		group := ms.PlacementGroup
		if group == "" {
			group = "unknown"
		}
		v.Placement.Groups[group] = append(v.Placement.Groups[group], ms.Name)
	}
	v.Placement = describePlacement(v.Placement, rc.Backend.Capabilities().Placement)
	placementCondition := domain.Condition{Type: "Placement", Status: "Unknown", Reason: "BestEffortOnly", Message: v.Placement.Warning, LastTransitionTime: now(rc)}
	if rc.Backend.Capabilities().Placement {
		placementCondition.Status, placementCondition.Reason = "True", "BackendReported"
		if len(v.Placement.Groups["unknown"]) > 0 {
			placementCondition.Status, placementCondition.Reason = "Unknown", "BackendPlacementUnavailable"
		}
	}
	v.Conditions = append(v.Conditions, placementCondition)
	health := "False"
	if allHealthy {
		health = "True"
	}
	unhealthy := []string{}
	for _, ms := range v.Members {
		if !ms.Health {
			unhealthy = append(unhealthy, ms.Name)
		}
	}
	healthMessage := fmt.Sprintf("%d members observed", len(v.Members))
	if len(unhealthy) > 0 {
		healthMessage += "; unhealthy or unobserved: " + strings.Join(unhealthy, ", ")
	}
	v.Conditions = append(v.Conditions, domain.Condition{Type: "Healthy", Status: health, Reason: "MemberHealth", Message: healthMessage, LastTransitionTime: now(rc)})
	if !allHealthy && v.Phase == domain.PhaseReady {
		v.Phase = domain.PhaseDegraded
		v.Message = "One or more members are unhealthy or cannot be observed; inspect member and replication conditions"
	}
	if s.RestoreConsistency != nil && (v.Phase == domain.PhaseReady || v.Phase == domain.PhaseDegraded) {
		v.Phase = domain.PhaseDegraded
		v.Message = s.RestoreConsistency.Failure
	}
	if rc.Cluster.Spec.DataSource != nil && (s.InitialRestore == nil || !s.InitialRestore.Completed || s.InitialRestore.NativeName == "" || s.InitialRestore.Failure != "") {
		v.Phase = domain.PhaseRestoring
		v.Message = "Waiting for durable completion of initial restore operation " + initialRestoreID(rc.Cluster, s)
		if s.InitialRestore != nil && s.InitialRestore.Failure != "" {
			v.Phase, v.Message = domain.PhaseFailed, s.InitialRestore.Failure
		}
		v.Conditions = append(v.Conditions, domain.Condition{Type: "InitialRestore", Status: "False", Reason: "RestoreIncomplete", Message: v.Message, LastTransitionTime: now(rc)})
		if engine.InitialDataSourceUnproven(rc.Cluster) {
			v.Phase = domain.PhaseDegraded
			v.Message = "Legacy initial restore completion is unproven; runtime convergence continues without replaying the restore"
			v.Conditions[len(v.Conditions)-1].Reason = "LegacyRestoreUnproven"
			v.Conditions[len(v.Conditions)-1].Message = v.Message
		}
	}
	v.Backup = backup
	if backupErr != nil {
		v.Conditions = append(v.Conditions, domain.Condition{Type: "Backup", Status: "False", Reason: "InventoryUnavailable", Message: backupErr.Error(), LastTransitionTime: now(rc)})
	}
	for _, m := range ordered(s) {
		engine.ApplyRuntimeRecoveryStatus(&v, m.Name, m.Volume, m.RuntimeRecovery, now(rc))
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

type replicationObservation struct {
	status    mongoadmin.Status
	condition domain.Condition
	fcv       string
}

func (p *Provider) observeReplication(ctx context.Context, rc *engine.Context, s *internalState, set string) replicationObservation {
	probe, cancel := context.WithTimeout(ctx, engine.ObservationTimeout)
	defer cancel()
	out := replicationObservation{condition: domain.Condition{Type: "Replication/" + set, Status: "False", Reason: "Unreachable", LastTransitionTime: now(rc)}}
	c, err := p.dial(probe, rc, s, set, "clusterMonitor")
	if err != nil {
		out.condition.Message = err.Error()
		return out
	}
	defer c.Disconnect(ctx)
	out.status, err = mongoadmin.GetPrimaryStatus(probe, c)
	switch {
	case err != nil:
		out.condition.Status, out.condition.Reason, out.condition.Message = "Unknown", "ObservationFailed", err.Error()
	case !out.status.HasWritablePrimary():
		out.condition.Reason = "NoWritablePrimary"
		out.condition.Message = "Replica set " + set + " has no unique healthy primary; restore retained voters and inspect election, disk, DNS and network diagnostics. Automatic reinitialization and forced reconfiguration are refused."
	default:
		if err := mongoadmin.CheckWritablePrimary(probe, c); err != nil {
			out.condition.Reason, out.condition.Message = "NoWritablePrimary", "Replica set "+set+" cannot confirm a writable primary: "+err.Error()
		} else {
			out.condition.Status, out.condition.Reason = "True", "WritablePrimary"
		}
	}
	out.fcv, _ = mongoadmin.FCV(probe, c)
	return out
}
