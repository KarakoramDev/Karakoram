// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package openbao

import (
	"context"
	"errors"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/openbao/baospec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"net"
	"strconv"
	"sync"
	"time"
)

func phase(s *internalState, observedGeneration int64) domain.Phase {
	switch s.Phase {
	case "ready":
		return domain.PhaseReady
	case "import", "deploy", "agents", "secrets":
		return domain.PhaseCreating
	case "init", "initialize", "initializing", "bootstrap", "unseal":
		// Sync also traverses native initialization/unseal checks on formed
		// clusters. That traversal is not a new initialization lifecycle.
		if s.Initialized && observedGeneration > 0 {
			for _, m := range s.Members {
				if m.Removing {
					return domain.PhaseScaling
				}
				if m.Change != nil {
					if m.Change.Kind == "deploy" {
						return domain.PhaseUpgrading
					}
					return domain.PhaseUpdating
				}
				if m.Stage != "ready" {
					return domain.PhaseScaling
				}
			}
			// A member receipt is cleared before the next member is planned.
			// Keep the admitted roll visible across that gap and final checks.
			if s.RollingKind == "deploy" {
				return domain.PhaseUpgrading
			}
			if s.RollingKind != "" {
				return domain.PhaseUpdating
			}
			return domain.PhaseReady
		}
		return domain.PhaseInitializing
	case "initialization-outcome-lost":
		return domain.PhaseDegraded
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
func projectNativeStatus(ms *domain.MemberStatus, b baospec.StatusResponse) {
	ms.Version = b.Version
	ms.Role = "unknown"
	ms.State = "UNINITIALIZED"
	if b.Initialized {
		ms.State = "SEALED"
		if !b.Sealed {
			ms.State = "RUNNING"
			if b.Active {
				ms.Role = "primary"
			} else if b.Standby {
				ms.Role = "secondary"
			}
		}
	}
	ms.Health = ms.Agent.Healthy && b.Initialized && !b.Sealed && (b.Active || b.Standby) && b.Error == ""
}
func (p *Provider) Status(ctx context.Context, rc *engine.Context) (status domain.ClusterStatus, statusErr error) {
	defer func() { engine.ApplyDeploymentStatus(rc, &status) }()
	s, err := loadState(rc.Cluster)
	if err != nil {
		return status, err
	}
	view := *rc.Cluster
	view.Status.Connection = domain.Connection{}
	status = domain.ClusterStatus{Phase: phase(s, rc.Cluster.Status.ObservedGeneration), Version: rc.Cluster.Spec.Version, ObservedGeneration: rc.Cluster.Status.ObservedGeneration, ObservedAt: now(rc), Members: []domain.MemberStatus{}, Conditions: []domain.Condition{}, Connection: connection(&view, s), Placement: domain.Placement{Warning: "Best-effort observations only; host separation is not guaranteed.", Groups: map[string][]string{}}}
	if s.Suspended {
		status.Phase = domain.PhaseSuspended
		return status, nil
	}
	members := ordered(s)
	names := make([]string, len(members))
	for i, m := range members {
		names[i] = m.Name
	}
	type sample struct {
		a      agentapi.Status
		b      baospec.StatusResponse
		ae, be error
	}
	samples := make([]sample, len(members))
	var inventory engine.PlatformObservation
	var wg sync.WaitGroup
	wg.Go(func() { inventory = engine.ObservePlatform(ctx, rc.Backend, rc.Cluster.Name, names) })
	for i, m := range members {
		if m.Stage == "planned" || m.Stage == "imported" {
			samples[i].ae = engine.ErrUnavailable
			samples[i].be = engine.ErrUnavailable
			continue
		}
		wg.Go(func() {
			probe, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			samples[i].a, samples[i].ae = p.agentStatus(probe, rc, m.Name)
		})
		wg.Go(func() {
			probe, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			samples[i].b, samples[i].be = p.nativeStatus(probe, rc, m.Name)
		})
	}
	wg.Wait()
	autoscaling := make(map[string]*domain.Resources, len(inventory.Services))
	for _, service := range inventory.Services {
		autoscaling[service.Hostname] = service.Autoscaling
	}
	allHealthy := len(s.runningMembers()) == rc.Cluster.Spec.Components.Engine.Replicas
	sealed := false
	status.Connection.PublicHosts = nil
	for i, m := range members {
		v := samples[i]
		ms := domain.MemberStatus{Name: m.Name, Set: "openbao", Role: "unknown", Version: m.Version, State: "UNKNOWN"}
		ms.Autoscaling = autoscaling[m.Name]
		if v.ae == nil {
			ms.Agent = domain.AgentStatus{Version: v.a.Agent.Version, Commit: v.a.Agent.Commit, Build: v.a.Agent.Build, Healthy: v.a.Processes[baospec.Process].State == "running", ConfigVersion: v.a.ConfigVersion, LastSeen: now(rc)}
			ms.Uptime = v.a.UptimeSeconds
			ms.DiskUsage = float64(v.a.Disk.UsedBytes)
			ms.Resources = engine.MemberResources(v.a.Resources, ms.Autoscaling)
			if !rc.Backend.Capabilities().Placement {
				ms.PlacementGroup = v.a.HostFingerprint
			}
		}
		if v.be == nil {
			projectNativeStatus(&ms, v.b)
			sealed = sealed || v.b.Initialized && v.b.Sealed
			ms.Health = ms.Health && m.Healthy
			if !p.postgresStorage(rc.Cluster) {
				ms.Health = ms.Health && m.Voter
				voterStatus, voterReason := "False", "NotVoter"
				if m.Voter {
					voterStatus, voterReason = "True", "HealthyVoter"
					if !m.Healthy {
						voterReason = "UnhealthyVoter"
					}
				}
				status.Conditions = append(status.Conditions, domain.Condition{Type: "RaftVoter/" + m.Name, Key: m.Name, Status: voterStatus, Reason: voterReason, LastTransitionTime: now(rc)})
			}
		} else {
			ms.Agent.Message = "OpenBao observation unavailable"
			if !p.postgresStorage(rc.Cluster) {
				status.Conditions = append(status.Conditions, domain.Condition{Type: "RaftVoter/" + m.Name, Key: m.Name, Status: "Unknown", Reason: "ObservationUnavailable", LastTransitionTime: now(rc)})
			}
		}
		if i < len(inventory.Members) {
			obs := inventory.Members[i]
			if obs.Err != nil || len(obs.Containers) == 0 {
				ms.Health = false
			}
			for _, container := range obs.Containers {
				if container.Status != infra.ContainerActive && container.Status != infra.ContainerRunning {
					ms.Health = false
				}
				if rc.Backend.Capabilities().Placement && container.PlacementGroup != "" {
					ms.PlacementGroup = container.PlacementGroup
				}
			}
		}
		if m.Removing {
			ms.Health = false
			ms.State = "LEAVING"
		}
		if m.Stage == "planned" || m.Stage == "imported" {
			ms.Health = false
			ms.State = "PENDING"
		}
		if ms.PlacementGroup != "" {
			status.Placement.Groups[ms.PlacementGroup] = append(status.Placement.Groups[ms.PlacementGroup], m.Name)
		}
		if m.PublicPort > 0 {
			ip := inventory.Project.PublicIPv6
			exposure := rc.Cluster.Spec.Exposure
			if s.ExposureAdmission.Admitted != nil {
				exposure = *s.ExposureAdmission.Admitted
			}
			if exposure.IPType == "IPV4" {
				ip = inventory.Project.PublicIPv4
			}
			if ip != "" {
				ms.PublicHost = net.JoinHostPort(ip, strconv.Itoa(m.PublicPort))
				status.Connection.PublicHosts = append(status.Connection.PublicHosts, ms.PublicHost)
			}
		}
		status.Members = append(status.Members, ms)
		allHealthy = allHealthy && ms.Health
	}
	for _, host := range status.Connection.PublicHosts {
		status.Connection.Endpoints = append(status.Connection.Endpoints, domain.Endpoint{Name: "https", URL: "https://" + host, Public: true})
	}
	sealedStatus := "False"
	if sealed {
		sealedStatus = "True"
	}
	status.Conditions = append(status.Conditions, domain.Condition{Type: "Sealed", Status: sealedStatus, LastTransitionTime: now(rc)})
	if s.InitializationOutcomeLost != "" {
		status.Conditions = append(status.Conditions, domain.Condition{Type: "InitializationOutcomeLost", Status: "True", Reason: "ReceiptMissing", Message: "Initialization material is unavailable; operator recovery is required.", LastTransitionTime: now(rc)})
	}
	if s.QuorumBlocked != "" {
		status.Conditions = append(status.Conditions, domain.Condition{Type: "QuorumBlocked", Status: "True", Reason: "InsufficientQuorum", Message: s.QuorumBlocked, LastTransitionTime: now(rc)})
	}
	if status.Phase == domain.PhaseReady && !allHealthy {
		status.Phase = domain.PhaseDegraded
	}
	return status, errors.Join(inventory.ProjectErr, inventory.ServicesErr)
}
