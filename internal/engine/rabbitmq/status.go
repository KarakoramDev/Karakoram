// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/rabbitmq/rmqspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"net"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
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
func (p *Provider) Status(ctx context.Context, rc *engine.Context) (status domain.ClusterStatus, statusErr error) {
	defer func() {
		if status.Placement.Source == "" {
			engine.ObservePlacement(&status, rc.Backend.Capabilities().Placement)
		}
		engine.ApplyDeploymentStatus(rc, &status)
	}()
	s, err := loadState(rc.Cluster)
	if err != nil {
		return domain.ClusterStatus{}, err
	}
	defer func() {
		statusErr = errors.Join(statusErr, engine.ApplyInitialDataSourceStatus(p, rc.Cluster, &status))
	}()
	view := *rc.Cluster
	view.Status.Connection = domain.Connection{}
	v := domain.ClusterStatus{Phase: phase(s), Version: rc.Cluster.Spec.Version, ObservedGeneration: rc.Cluster.Status.ObservedGeneration, Connection: connection(&view, s), Members: []domain.MemberStatus{}, Conditions: []domain.Condition{}, Placement: domain.Placement{Warning: "Best-effort observations only; host separation is not guaranteed.", Groups: map[string][]string{}}, Message: s.platformFailureMessage()}
	for _, m := range ordered(s) {
		if m.ChangeStartupMessage != "" {
			v.Message = m.ChangeStartupMessage
			break
		}
	}
	if s.Suspended {
		v.Phase = domain.PhaseSuspended
		return v, nil
	}
	v.Connection.PublicHosts = nil
	allHealthy := len(s.runningMembers()) == rc.Cluster.Spec.Components.Engine.Replicas
	alarms, partition := false, false
	alarmMessages, partitionMessages, observationMessages := []string{}, []string{}, []string{}
	membershipUnknown := false
	// Keep a proven rejection through inconclusive probes, but not through a
	// verified acceptance. Legacy agents omit the signal and retain their
	// existing readiness behavior.
	previousCredentialDrift := false
	for _, c := range rc.Cluster.Status.Conditions {
		if c.Type == "ManagedAccessReady" && c.Status == "False" && c.Reason == "CredentialDrift" {
			previousCredentialDrift = true
		}
	}
	credentialDrift := false
	adminRotationStaged := false
	for _, m := range s.Members {
		if m.ChangeAction == "password" && m.TargetConfig == "admin" {
			adminRotationStaged = true
		}
	}
	names := []string{}
	for _, m := range s.runningMembers() {
		names = append(names, "rabbit@"+m.Name)
	}
	type memberObservation struct {
		agent               agentapi.Status
		broker              rmqspec.StatusResponse
		agentErr, brokerErr error
		at                  time.Time
	}
	members := ordered(s)
	memberNames := make([]string, len(members))
	for i, m := range members {
		memberNames[i] = m.Name
	}
	var inventory engine.PlatformObservation
	samples := make([]memberObservation, len(members))
	var probes sync.WaitGroup
	probes.Go(func() {
		inventory = engine.ObservePlatform(ctx, rc.Backend, rc.Cluster.Name, memberNames)
	})
	for i, m := range members {
		if m.Stage == "planned" || m.Stage == "imported" {
			// No applied deployment has been observed yet, so neither agent
			// nor broker can establish readiness. Keep platform observations
			// and unavailable status without paying a nonexistent endpoint's
			// timeout after every durable provisioning checkpoint.
			samples[i].agentErr, samples[i].brokerErr = engine.ErrNotFound, engine.ErrNotFound
			continue
		}
		probes.Go(func() {
			samples[i].agent, samples[i].agentErr = p.agentStatus(ctx, rc, m.Name)
			samples[i].at = now(rc)
		})
		probes.Go(func() {
			// The generic and engine clients use independent TLS transports.
			// Neither cold connection may consume the other's sweep budget.
			samples[i].broker, samples[i].brokerErr = p.brokerStatus(ctx, rc, m.Name)
		})
	}
	probes.Wait()
	// Only fresh native self-membership proves a peer is running. Agent
	// reachability or a supervised wrapper alone cannot prove a partition.
	livePeers := map[string]bool{}
	for i, m := range members {
		b := samples[i].broker
		if samples[i].brokerErr == nil && b.MembershipError == "" && b.Version != "" && slices.Contains(b.RunningNodes, "rabbit@"+m.Name) {
			livePeers["rabbit@"+m.Name] = true
		}
	}
	if inventory.ProjectErr != nil || inventory.ServicesErr != nil {
		return v, errors.Join(inventory.ProjectErr, inventory.ServicesErr)
	}
	project := inventory.Project
	resources := map[string]domain.Resources{}
	for _, service := range inventory.Services {
		resources[service.Hostname] = service.Resources
	}
	for i, m := range members {
		ms := domain.MemberStatus{Name: m.Name, Set: "rabbitmq", Role: "broker", Version: m.Version, State: "UNKNOWN"}
		if r, ok := resources[m.Name]; ok {
			copy := r
			ms.Autoscaling = &copy
		}
		st, e := samples[i].agent, samples[i].agentErr
		var allocated *agentapi.ContainerResources
		if e == nil {
			allocated = st.Resources
			ms.Agent = domain.AgentStatus{Version: st.Agent.Version, Commit: st.Agent.Commit, Build: st.Agent.Build, Healthy: true, ConfigVersion: st.ConfigVersion, LastSeen: samples[i].at}
			ms.Uptime = st.UptimeSeconds
			ms.DiskUsage = float64(st.Disk.UsedBytes)
			if !rc.Backend.Capabilities().Placement {
				ms.PlacementGroup = st.HostFingerprint
			}
		} else {
			ms.Agent.Message = e.Error()
		}
		b, e := samples[i].broker, samples[i].brokerErr
		if e == nil {
			// The agent's live local service/alarm/identity probes establish
			// member health independently of peer-dependent membership.
			ms.Version = b.Version
			ms.Health = ms.Agent.Healthy && running(st) && b.Version != "" && !b.Maintenance
			credentialStatus := b.AdminCredentialStatus
			if credentialStatus != "" && (adminRotationStaged || b.ConfigVersion < s.AdminCredentialVersion) {
				// A staged password may not be native yet, and old peers may
				// still probe the previous password after native commit.
				credentialStatus = "unknown"
			}
			if credentialStatus == "rejected" || (credentialStatus == "unknown" && previousCredentialDrift) {
				credentialDrift = true
				ms.Health = false
				ms.Agent.Message = "Desired reserved admin credential was rejected; automatic managed-access reconciliation will restore it."
			}
			ms.State = "RUNNING"
			if !ms.Health {
				ms.State = "DOWN"
			}
			if !noProblems(b.Alarms) {
				alarms = true
				ms.Health = false
				messages := alarmDetails(m.Name, b.Alarms)
				alarmMessages = append(alarmMessages, messages...)
				ms.State = "ALARM"
				ms.Agent.Message = strings.Join(messages, "; ")
			}
			if !noProblems(b.Partitions) {
				partition = true
				ms.Health = false
				messages := partitionDetails(m.Name, b.Partitions)
				partitionMessages = append(partitionMessages, messages...)
				ms.State = "PARTITIONED"
				ms.Agent.Message = strings.Join(append(alarmDetails(m.Name, b.Alarms), messages...), "; ")
			}
			if b.MembershipError != "" {
				allHealthy = false
				membershipUnknown = true
				message := fmt.Sprintf("%s: RabbitMQ membership observation unavailable: %s; local health is independently sampled; inspect peer connectivity with rabbitmq-diagnostics/cluster_status; a timeout alone does not prove a partition", m.Name, b.MembershipError)
				ms.Agent.Message = joinedDiagnostics([]string{ms.Agent.Message, message})
				observationMessages = append(observationMessages, message)
			}
			for _, name := range names {
				if b.MembershipError != "" {
					break
				}
				if !slices.Contains(b.RunningNodes, name) {
					allHealthy = false
					if name == "rabbit@"+m.Name {
						// An observed negative local running state is not an
						// unavailable membership sample or a failed peer link.
						// Preserve this evidence even if the independent local
						// service probe raced an application shutdown.
						ms.Health = false
						if ms.State == "RUNNING" {
							ms.State = "DOWN"
						}
						message := fmt.Sprintf("%s does not report its own RabbitMQ application running in the fresh membership view; inspect local rabbitmq-diagnostics check_running/status and supervised processes; recover the local application through supported lifecycle controls; do not reset or force-boot brokers", m.Name)
						ms.Agent.Message = joinedDiagnostics([]string{ms.Agent.Message, message})
						observationMessages = append(observationMessages, message)
						continue
					}
					message := fmt.Sprintf("%s cannot see expected peer %s; inspect each member's cluster_status, DNS, distribution ports, cookie and TLS; restore peer connectivity before resuming changes; do not reset or force-boot brokers", m.Name, name)
					ms.Agent.Message = joinedDiagnostics([]string{ms.Agent.Message, message})
					partitionMessages = append(partitionMessages, message)
					if livePeers[name] {
						// Both brokers freshly report themselves running, but
						// this observer cannot see the other: peer-link evidence.
						partition = true
						ms.Health = false
						ms.State = "PARTITIONED"
					}
				}
			}
		} else {
			credentialDrift = credentialDrift || previousCredentialDrift
			ms.Health = false
			membershipUnknown = true
			message := fmt.Sprintf("%s: RabbitMQ management observation unavailable or unresponsive: %v; inspect the local management API and rabbitmq-diagnostics/cluster_status, then check peer DNS, distribution ports, cookie and TLS; restore peer connectivity if broken; a timeout alone does not prove a partition", m.Name, e)
			ms.Agent.Message = joinedDiagnostics([]string{ms.Agent.Message, message})
			observationMessages = append(observationMessages, message)
		}
		containers, e := inventory.Members[i].Containers, inventory.Members[i].Err
		if e != nil {
			ms.Health = false
			ms.Agent.Message = fmt.Sprintf("container observation: %v", e)
		} else if len(containers) == 0 {
			ms.Health = false
		} else {
			for _, c := range containers {
				if c.Status != infra.ContainerActive && c.Status != infra.ContainerRunning {
					ms.Health = false
				}
				if rc.Backend.Capabilities().Placement && c.PlacementGroup != "" {
					ms.PlacementGroup = c.PlacementGroup
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
				ms.PublicHost = net.JoinHostPort(ip, strconv.Itoa(m.PublicPort))
				v.Connection.PublicHosts = append(v.Connection.PublicHosts, ms.PublicHost)
			}
		}
		if m.Removing {
			ms.State = "LEAVING"
			if m.RemovalBlocked != "" {
				ms.State = "LEAVING_BLOCKED"
			}
			ms.Health = false
			ms.Agent.Message = m.RemovalBlocked
		} else if m.Stage != "ready" && m.Stage != "joined" {
			ms.State = memberStage(s, m, ms.Agent.Healthy && running(st))
			ms.Health = false
		}
		engine.ApplyAgentDiagnostic(&ms, st.Error)
		v.Conditions = append(v.Conditions, engine.ApplyProcessDiagnostics(&ms, st.Processes, "", samples[i].at)...)
		allHealthy = allHealthy && ms.Health
		v.Members = append(v.Members, ms)
		if m.ManagementPublic {
			if sub, err := engine.PublicHTTPS(rc.Backend); err == nil {
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
	if s.SystemUsersError != "" && !credentialDrift {
		allHealthy = false
		v.Message = s.SystemUsersError
		v.Conditions = append(v.Conditions, domain.Condition{Type: "ManagedAccessReady", Status: "False", Reason: "ManagedAccessReconciliationFailed", Message: s.SystemUsersError, LastTransitionTime: now(rc)})
	}
	if credentialDrift {
		allHealthy = false
		message := "Desired reserved admin credential was rejected; automatic managed-access reconciliation will restore it. A fresh successful credential probe is required to clear this condition."
		v.Message = message
		v.Conditions = append(v.Conditions, domain.Condition{Type: "ManagedAccessReady", Status: "False", Reason: "CredentialDrift", Message: message, LastTransitionTime: now(rc)})
	}
	condition("Healthy", allHealthy, "ObservedMembers")
	v.Conditions[len(v.Conditions)-1].Message = joinedDiagnostics(append(append([]string{}, observationMessages...), partitionMessages...))
	if v.Message == "" && !allHealthy {
		v.Message = v.Conditions[len(v.Conditions)-1].Message
	}
	condition("Alarms", alarms, "MemoryOrDisk")
	v.Conditions[len(v.Conditions)-1].Message = joinedDiagnostics(alarmMessages)
	if alarms && v.Message == "" {
		v.Message = joinedDiagnostics(alarmMessages)
	}
	condition("Partition", partition, "ClusterMembership")
	if !partition && membershipUnknown {
		v.Conditions[len(v.Conditions)-1].Status = "Unknown"
		v.Conditions[len(v.Conditions)-1].Reason = "MembershipObservationUnavailable"
		partitionMessages = append(partitionMessages, observationMessages...)
	}
	v.Conditions[len(v.Conditions)-1].Message = joinedDiagnostics(partitionMessages)
	if partition && v.Message == "" {
		v.Message = joinedDiagnostics(partitionMessages)
	}
	engine.ObservePlacement(&v, rc.Backend.Capabilities().Placement)
	placementCondition := domain.Condition{Type: "Placement", Status: "Unknown", Reason: "BestEffortOnly", Message: v.Placement.Warning, LastTransitionTime: now(rc)}
	if rc.Backend.Capabilities().Placement {
		placementCondition.Status, placementCondition.Reason, placementCondition.Message = "True", "BackendReported", v.Placement.Warning
		if len(v.Placement.Groups["unknown"]) > 0 {
			placementCondition.Status, placementCondition.Reason = "Unknown", "BackendPlacementUnavailable"
		}
	}
	v.Conditions = append(v.Conditions, placementCondition)
	for _, m := range ordered(s) {
		if m.RemovalBlocked != "" {
			v.Conditions = append(v.Conditions, domain.Condition{Type: "MemberRemovalBlocked", Status: "True", Reason: "UnsafeRemoval", Message: m.RemovalBlocked, LastTransitionTime: now(rc)})
			v.Message = m.RemovalBlocked
		}
	}
	projectQuorumStatus(rc, s, &v)
	if s.Retry != nil {
		if r := s.PlatformRetries[platformRetryKey(s.Retry)]; r != nil && r.CPUModeCooldown {
			condition("PlatformCooldown", true, "CPUModeChangeTooRecent")
		}
	}
	if !allHealthy && v.Phase == domain.PhaseReady {
		v.Phase = domain.PhaseDegraded
	}
	for _, m := range s.Members {
		engine.ApplyRuntimeRecoveryStatus(&v, m.Name, m.Volume, m.RuntimeRecovery, now(rc))
	}
	return v, nil
}
