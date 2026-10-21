// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"net"
	"net/url"
	"strings"
)

func connectionMember(rc *engine.Context, s *state) string {
	if rc.Cluster.Spec.Components.Extra["proxy"].Replicas > 0 {
		return rc.Cluster.Name + "px"
	}
	data := s.group(0).data()
	if len(data) > 0 {
		return data[0].Name
	}
	return rc.Cluster.Name + "p0"
}
func discovery(rc *engine.Context, s *state) domain.Connection {
	host := net.JoinHostPort(connectionMember(rc, s), "5432")
	u := url.URL{Scheme: "postgresql", Host: host, Path: "/postgres"}
	if s.Topology == "distributed" {
		if o, err := ParseOptions(rc.Cluster.Spec.EngineOptions); err == nil {
			u.Path = "/" + o.citusOptions().Database
		}
	}
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

// ProjectCredentialsStatus fences cached publication observations against
// access mutations completed since the last reconciliation.
func (p *Core) ProjectCredentialsStatus(ctx context.Context, rc *engine.Context, status domain.ClusterStatus) domain.ClusterStatus {
	status.Conditions = append([]domain.Condition(nil), status.Conditions...)
	for i := range status.Conditions {
		condition := &status.Conditions[i]
		if condition.Type != "CredentialsPublished" || condition.Status != "True" {
			continue
		}
		revision, err := p.accessRevision(ctx, rc)
		hash, hashErr := desiredConfigHash(rc, revision)
		pending, pendingErr := accessIntentPending(ctx, rc)
		if err != nil || hashErr != nil || pendingErr != nil || pending || condition.Key != hash {
			condition.Status = "False"
			condition.Reason = "ProxyPublicationPending"
			condition.Message = "Waiting for a fresh observation of the current managed access revision on every proxy."
		}
	}
	return status
}

func (p *Core) Status(ctx context.Context, rc *engine.Context) (status domain.ClusterStatus, statusErr error) {
	defer func() { engine.ApplyDeploymentStatus(rc, &status) }()
	s, err := load(rc.Cluster)
	if err != nil {
		return domain.ClusterStatus{}, err
	}
	out := domain.ClusterStatus{Phase: domain.PhaseCreating, Version: rc.Cluster.Spec.Version, Members: []domain.MemberStatus{}, Connection: discovery(rc, s), Placement: domain.Placement{Groups: map[string][]string{}}}
	if s.Suspended {
		out.Phase = domain.PhaseSuspended
		return out, nil
	}
	credentialsPending, err := accessIntentPending(ctx, rc)
	credentialsKnown := err == nil
	healthy := credentialsKnown && len(s.data()) == s.desiredDataCount(rc) && len(s.Members) > 0
	writer := false
	quorumHealthy := false
	quorumRequired := false
	healthProbeFailed := false
	var replicationIssues []string
	var observationIssues, observationUnknown []string
	var paused, etcdIssues, archiveIssues []string
	var dataIssues, capacityIssues, slotIssues, slotUnknown, archiveUnknown []string
	var proxyIssues []string
	var proxyDetails []string
	var proxyPlacementIssues, proxyRolloutIssues, proxyRolloutFailures []string
	var processConditions []domain.Condition
	var slotUnsupported, archiveUnsupported, etcdUnsupported, distanceUnsupported, pauseUnsupported, credentialLegacy []string
	observations := make(map[string]pgspec.StatusResponse, len(s.Members))
	credentialsPublished := rc.Cluster.Spec.Components.Extra["proxy"].Replicas == 0
	proxyPublicationObserved := false
	if !credentialsPublished {
		// A previously Ready cluster can still acknowledge the old revision
		// between a role mutation and the next reconciliation.
		revision, revisionErr := p.accessRevision(ctx, rc)
		hash, hashErr := desiredConfigHash(rc, revision)
		credentialsPublished = credentialsKnown && !credentialsPending && revisionErr == nil && hashErr == nil && s.ConfigHash == hash
	}
	// With no proxy the data runtime owns public-port changes. Observe its
	// infrastructure barrier before collecting fresh native/config status.
	direct := s.find(connectionMember(rc, s))
	directReady := true
	if direct != nil && direct.Role == "data" && direct.Stage != "planned" && direct.Stage != "imported" {
		runtime, err := rc.Backend.GetService(ctx, direct.Name)
		if err != nil {
			return out, err
		}
		directReady = runtime != nil && (runtime.Status == infra.StatusRunning || runtime.Status == infra.StatusActive)
	}
	liveMembers := observeStatusMembers(ctx, rc, s.Members)
	// Bind every proxy's fresh runtime rows to the writer observed in this
	// sweep. Running alone only promises some ONLINE writer (older agents may
	// only promise a listening frontend), not the current primary.
	primaryHost := ""
	primaryCount := 0
	for i, m := range s.Members {
		observation := liveMembers[i]
		st := observation.status
		if m.Role == "data" && observation.agentHealthy && observation.err == nil && st.Running && (st.Role == "primary" || st.Role == "standalone") {
			primaryHost = m.Name
			primaryCount++
		}
	}
	if primaryCount != 1 {
		primaryHost = ""
	}
	for i, m := range s.Members {
		member := domain.MemberStatus{Name: m.Name, Set: "postgres", Role: "unknown", State: "starting", Version: m.Version}
		if s.Topology == "distributed" && m.Role == "data" {
			group := m.Group
			member.Group = &group
		}
		if m.Role == "proxy" {
			member.Set = "proxy"
			member.Role = "proxy"
		}
		if m.Stage == "planned" || m.Stage == "imported" {
			healthy = false
			out.Members = append(out.Members, member)
			continue
		}
		observation := liveMembers[i]
		var proxyProcesses []struct {
			instance  string
			processes map[string]agentapi.ProcessStatus
		}
		if observation.agentHealthy {
			st := observation.agent
			member.Agent = domain.AgentStatus{Healthy: true, ConfigVersion: st.ConfigVersion, LastSeen: now(rc)}
			member.Uptime = st.UptimeSeconds
			member.Resources = engine.MemberResources(st.Resources, &m.Resources)
			if st.Disk.TotalBytes > 0 && st.Disk.FreeBytes < st.Disk.TotalBytes/10 {
				capacityIssues = append(capacityIssues, m.Name)
				healthy = false
			}
		}
		if member.Agent.Healthy {
			st, e := observation.status, observation.err
			if e == nil {
				observations[m.Name] = st
				capabilities := pgspec.HealthCapabilities{}
				if st.HealthCapabilities != nil {
					capabilities = *st.HealthCapabilities
				}
				if m.Role == "data" && !st.CredentialReceipts {
					credentialLegacy = append(credentialLegacy, m.Name)
				}
				member.Role = st.Role
				member.Health = st.Running
				member.State = st.Role
				if m.Role == "data" && patroniTopology(s.Topology) {
					quorumHealthy = quorumHealthy || st.EtcdHealthy
					if capabilities.LocalEtcd {
						quorumRequired = true
						if !st.EtcdHealthy {
							etcdIssues = append(etcdIssues, m.Name)
							member.Health = false
							member.State = "dcs-unavailable"
							healthProbeFailed = true
						}
					} else {
						etcdUnsupported = append(etcdUnsupported, m.Name)
					}
					if st.Paused {
						paused = append(paused, m.Name)
						member.Health = false
						member.State = "paused"
					} else if !capabilities.PatroniPause {
						pauseUnsupported = append(pauseUnsupported, m.Name)
					}
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
				if st.DataRecoveryRequired {
					dataIssues = append(dataIssues, m.Name)
					member.Health = false
					member.State = "restore-required"
				}
				if st.Role == "primary" || st.Role == "standalone" {
					if !capabilities.ReplicationSlots {
						slotUnsupported = append(slotUnsupported, m.Name)
					} else if !st.SlotStatusKnown {
						slotUnknown = append(slotUnknown, m.Name)
						healthy = false
						healthProbeFailed = true
					}
					for _, slot := range st.ReplicationSlots {
						if slot.RetainedBytes > 1<<30 || slot.WALStatus == "lost" || slot.WALStatus == "unreserved" {
							slotIssues = append(slotIssues, fmt.Sprintf("%s/%s (%d bytes, %s)", m.Name, slot.Name, slot.RetainedBytes, slot.WALStatus))
							healthy = false
						}
					}
					if rc.Cluster.Spec.Backup.Storage != "" {
						if !capabilities.WALArchive {
							archiveUnsupported = append(archiveUnsupported, m.Name)
						} else if !st.WALStatusKnown || !st.ArchiveStatusKnown {
							archiveUnknown = append(archiveUnknown, m.Name)
							healthy = false
							healthProbeFailed = true
						}
					}
				}
				if (st.Role == "primary" || st.Role == "standalone") && st.ArchiveFailing && rc.Cluster.Spec.Backup.Storage != "" {
					archiveIssues = append(archiveIssues, m.Name)
					healthy = false
				}
			}
		}
		if m.Role == "data" && (!observation.agentHealthy || observation.err != nil) {
			message := "PostgreSQL native observation unavailable on " + m.Name + "; inspect the member agent, PostgreSQL/Patroni health and connectivity; restore observations before failover or maintenance"
			member.Agent.Message = message
			member.State = "unknown"
			member.Health = false
			healthy = false
			healthProbeFailed = true
			observationUnknown = append(observationUnknown, m.Name)
			observationIssues = append(observationIssues, message)
		}
		if m.Role == "proxy" {
			runtime, err := rc.Backend.GetService(ctx, m.Name)
			if err != nil {
				return out, err
			}
			runtimeReady := runtime != nil && (runtime.Status == infra.StatusRunning || runtime.Status == infra.StatusActive)
			first := true
			responded := 0
			count, e := observation.proxyCount, observation.err
			all := e == nil
			for _, sample := range observation.proxyInstances {
				if !sample.agentHealthy {
					all = false
					continue
				}
				status := sample.agent
				responded++
				proxyProcesses = append(proxyProcesses, struct {
					instance  string
					processes map[string]agentapi.ProcessStatus
				}{sample.instance, status.Processes})
				engine.ApplyAgentDiagnostic(&member, status.Error)
				if first {
					member.Agent.LastSeen = now(rc)
					member.Uptime = status.UptimeSeconds
					member.Resources = engine.MemberResources(status.Resources, &m.Resources)
				}
				if first || status.ConfigVersion < member.Agent.ConfigVersion {
					member.Agent.ConfigVersion = status.ConfigVersion
					first = false
				}
				if status.Error != "" || sample.err != nil {
					all = false
					continue
				}
				statusPG := sample.status
				writerReady := statusPG.Running && proxyHasCurrentWriter(primaryHost, statusPG.ProxyBackends)
				all = all && writerReady
				if !writerReady && len(proxyDetails) < 2 {
					if detail := proxyFailureSummary(m.Name, statusPG.ProxyBackends); detail != "" {
						proxyDetails = append(proxyDetails, detail)
					}
				}
			}
			member.Agent.Healthy = count > 0 && responded == count
			desired := rc.Cluster.Spec.Components.Extra["proxy"].Replicas
			member.Health = runtimeReady && member.Agent.Healthy && all && e == nil && observation.proxyEndpointErr == nil
			proxyPublicationObserved = true
			credentialsPublished = credentialsPublished && count >= desired && member.Health &&
				m.Stage == "joined" && member.Agent.ConfigVersion >= m.ConfigVersion && m.ConfigVersion == s.ConfigVersion
			if e != nil {
				member.Agent.Message = fmt.Sprintf("Proxy observation unavailable: %v; inspect each proxy instance's native writer route and restore agent/backend connectivity before serving traffic", e)
			}
			if observation.proxyEndpointErr != nil {
				message := "Private service endpoint " + m.Name + " is unreachable or not running: " + observationErrorSummary(observation.proxyEndpointErr) + "; check the private service DNS alias/VIP, overlay connectivity and agent/native process health; healthy individual tasks do not prove endpoint reachability"
				if member.Agent.Message != "" {
					member.Agent.Message += "; "
				}
				member.Agent.Message += message
				member.State = "endpoint-unavailable"
				healthProbeFailed = true
				observationIssues = append(observationIssues, message)
			}
			if !member.Agent.Healthy || !all {
				proxyIssues = append(proxyIssues, m.Name)
			} else if runtime != nil && runtime.Status == infra.StatusFailed {
				proxyRolloutFailures = append(proxyRolloutFailures, m.Name)
			} else if count < desired {
				detail := fmt.Sprintf("%s: %d of %d desired proxy instances are serving; desired proxy instance unplaced", m.Name, count, desired)
				blocked := false
				if observer, ok := rc.Backend.(infra.PlacementObserver); ok {
					failures, err := observer.PlacementFailures(ctx, []string{m.Name})
					if err != nil {
						return out, err
					}
					for _, failure := range failures {
						blocked = true
						detail += "; " + failure.Message
					}
				}
				if blocked {
					member.Health = false
					proxyPlacementIssues = append(proxyPlacementIssues, detail)
				} else if !runtimeReady {
					proxyRolloutIssues = append(proxyRolloutIssues, m.Name)
				}
			} else if !runtimeReady {
				proxyRolloutIssues = append(proxyRolloutIssues, m.Name)
			}
			if member.Health {
				member.State = "proxy"
			}
		}
		if m == direct && m.Role == "data" {
			member.Health = member.Health && directReady
		}
		engine.ApplyAgentDiagnostic(&member, observation.agent.Error)
		if m.Role == "data" {
			processConditions = append(processConditions, engine.ApplyProcessDiagnostics(&member, observation.agent.Processes, "", now(rc))...)
		}
		for _, diagnostic := range proxyProcesses {
			processConditions = append(processConditions, engine.ApplyProcessDiagnostics(&member, diagnostic.processes, diagnostic.instance, now(rc))...)
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
	if patroniTopology(s.Topology) {
		primaries := map[int]pgspec.StatusResponse{}
		for name, st := range observations {
			if m := s.find(name); m != nil && m.Role == "data" && st.Running && st.Role == "primary" {
				primaries[m.Group] = st
			}
		}
		for i := range out.Members {
			m := &out.Members[i]
			st, ok := observations[m.Name]
			if !ok || st.Role != "replica" {
				continue
			}
			primary := primaries[s.find(m.Name).Group]
			if primary.HealthCapabilities == nil || !primary.HealthCapabilities.ReplicationDistance || st.HealthCapabilities == nil || !st.HealthCapabilities.ReplicationDistance {
				distanceUnsupported = append(distanceUnsupported, m.Name)
				continue
			}
			distance, known := replicaDistance(primary, st)
			if !known || distance > 16<<20 {
				m.Health = false
				if m.State != "paused" && m.State != "error" {
					m.State = "lag-unknown"
					if known {
						m.State = "lagging"
					}
				}
				healthy = false
				healthProbeFailed = true
				replicationIssues = append(replicationIssues, m.Name)
			}
		}
	}
	var citusCondition domain.Condition
	if s.Topology == "distributed" {
		citusCondition = citusTopology(ctx, rc, s, observations)
		if citusCondition.Status != "True" {
			healthy, healthProbeFailed, writer = false, true, false
		}
	}
	if s.Formed && s.Topology == "ha" && !writer && s.Upgrade == nil && s.RestoreRecovery == nil {
		bs, backupErr := loadBackupState(rc.Cluster)
		allMissing := backupErr == nil && !bs.ManagedRolesHeld && bs.Restore == nil && len(s.data()) > 0
		for _, m := range s.data() {
			observation, known := observations[m.Name]
			allMissing = allMissing && known && observation.DataMissing
		}
		if allMissing {
			healthy = false
			for _, m := range s.data() {
				dataIssues = append(dataIssues, m.Name)
			}
		}
	}
	// Member health alone cannot acknowledge a profile generation: curated SQL
	// is a post-readiness prerequisite and may still be pending or have failed.
	profilesReady, err := p.profileBootstrapApplied(ctx, rc, s)
	if err != nil {
		return out, err
	}
	if healthy && profilesReady && !s.ExtensionsPending && !credentialsPending && writer && s.Pending == nil && s.Formed && s.CitusScale == nil && s.Upgrade == nil && s.Replacement == nil && s.Rolling == nil && s.RestoreRecovery == nil && (!patroniTopology(s.Topology) || !quorumRequired || quorumHealthy) {
		out.Phase = domain.PhaseReady
		out.ObservedGeneration = rc.Cluster.Generation
	} else if rc.Cluster.Status.ObservedGeneration > 0 {
		out.Phase = domain.PhaseUpdating
	}
	if s.Formed && (len(proxyIssues) > 0 || len(proxyPlacementIssues) > 0 || len(proxyRolloutIssues) > 0 || len(proxyRolloutFailures) > 0 || healthProbeFailed) {
		out.Phase = domain.PhaseDegraded
	}
	condition := "False"
	reason := "MembersStarting"
	if len(slotIssues) > 0 {
		reason = "SlotWALRetention"
	}
	if out.Phase == domain.PhaseReady {
		condition = "True"
		reason = "MembersReady"
	}
	if !profilesReady {
		reason = "ProfileBootstrapPending"
	}
	if s.ExtensionsPending {
		reason = "ExtensionUpdatePending"
	}
	if healthProbeFailed {
		reason = "HealthObservationFailed"
	}
	if len(proxyRolloutIssues) > 0 {
		reason = "ProxyRolloutPending"
	}
	if len(proxyPlacementIssues) > 0 {
		reason = "ProxyInstanceUnplaced"
	}
	if len(proxyRolloutFailures) > 0 {
		reason = "ProxyRolloutFailed"
	}
	if len(proxyIssues) > 0 {
		reason = "ProxyUnavailable"
	}
	if s.SwitchoverBlocked != "" {
		condition, reason = "False", "SwitchoverBlocked"
		out.Phase, out.Message = domain.PhaseUpdating, s.SwitchoverBlocked
	}
	out.Conditions = []domain.Condition{{Type: "Ready", Status: condition, Reason: reason, LastTransitionTime: now(rc)}}
	if len(observationIssues) > 0 {
		message := strings.Join(observationIssues, "; ")
		out.Conditions[0].Message = message
		if out.Message != "" {
			out.Message += "; "
		}
		out.Message += message
	}
	out.Conditions = append(out.Conditions, processConditions...)
	if rc.Cluster.Spec.Components.Extra["proxy"].Replicas > 0 {
		publication := domain.Condition{
			Type: "CredentialsPublished", Status: "False", Reason: "ProxyPublicationPending",
			Key:     s.ConfigHash,
			Message: "Waiting for every proxy to apply the current managed access revision and report healthy runtime users.", LastTransitionTime: now(rc),
		}
		if credentialsPublished && proxyPublicationObserved {
			publication.Status = "True"
			publication.Reason = "ProxyPublicationComplete"
			publication.Message = "Every proxy has applied the current managed access revision and reports healthy runtime users."
		}
		out.Conditions = append(out.Conditions, publication)
	}
	if err := engine.ApplyInitialDataSourceStatus(p, rc.Cluster, &out); err != nil {
		return out, err
	}
	if len(proxyPlacementIssues) > 0 {
		out.Conditions[0].Message = strings.Join(proxyPlacementIssues, " | ")
		out.Conditions = append(out.Conditions, domain.Condition{
			Type: "ProxyPlacement", Status: "False", Reason: "ProxyInstanceUnplaced",
			Message: strings.Join(proxyPlacementIssues, " | "), LastTransitionTime: now(rc),
		})
	}
	if len(proxyRolloutIssues) > 0 {
		out.Conditions = append(out.Conditions, domain.Condition{
			Type: "ProxyRollout", Status: "False", Reason: "ProxyRolloutPending",
			Message: "Serving proxy writer routes are healthy; waiting for runtime rollout completion on: " + strings.Join(proxyRolloutIssues, ", "), LastTransitionTime: now(rc),
		})
	}
	if len(proxyRolloutFailures) > 0 {
		message := "Proxy infrastructure rollout failed on: " + strings.Join(proxyRolloutFailures, ", ") + ". Inspect the terminal backend operation and deployment; restore placement capacity or application health and explicitly reapply the desired runtime."
		out.Conditions[0].Message = message
		out.Conditions = append(out.Conditions, domain.Condition{
			Type: "ProxyRollout", Status: "False", Reason: "ProxyRolloutFailed", Message: message, LastTransitionTime: now(rc),
		})
	}
	if s.ExtensionsPending {
		out.Conditions = append(out.Conditions, domain.Condition{Type: "ExtensionsUpdated", Status: "False", Reason: "ExtensionUpdatePending", Message: "Installed profile extension updates have not completed. Restore extension availability and database connectivity so reconciliation can retry.", LastTransitionTime: now(rc)})
	}
	if s.Topology == "distributed" {
		out.Conditions = append(out.Conditions, citusCondition)
	}
	if s.SwitchoverBlocked != "" {
		out.Conditions = append(out.Conditions, domain.Condition{Type: "Switchover", Status: "False", Reason: "SwitchoverBlocked", Message: s.SwitchoverBlocked, LastTransitionTime: now(rc)})
	}
	if len(proxyIssues) > 0 {
		out.Conditions = append(out.Conditions, domain.Condition{
			Type: "ProxyWriter", Status: "False", Reason: "WriterRouteUnavailable",
			Message:            "Proxy writer route is unavailable on: " + strings.Join(proxyIssues, ", ") + ". Inspect runtime hostgroup 10 for an ONLINE writer and restore backend connectivity/capacity; a listening frontend alone is not ready. " + strings.Join(proxyDetails, " | "),
			LastTransitionTime: now(rc),
		})
	}
	if len(credentialLegacy) > 0 {
		out.Conditions = append(out.Conditions, domain.Condition{
			Type: "CredentialRecovery", Status: "False", Reason: "LegacyAgent",
			Message:            "Durable credential receipts are unsupported on: " + strings.Join(credentialLegacy, ", ") + ". Refresh these member binaries to enable crash-safe credential mutation replay; this informational condition does not gate legacy readiness.",
			LastTransitionTime: now(rc),
		})
	}
	if credentialsPending {
		out.Conditions = append(out.Conditions, domain.Condition{Type: "CredentialsConsistent", Status: "False", Reason: "CredentialRecoveryPending", Message: "A durably selected credential change is being replayed. Restore database and secret-store access; do not rotate again or discard its journal.", LastTransitionTime: now(rc)})
	}
	if !credentialsKnown {
		out.Conditions = append(out.Conditions, domain.Condition{Type: "CredentialsConsistent", Status: "Unknown", Reason: "ObservationUnavailable", Message: "The credential journal could not be read. Restore secret-store access; current database and DCS health are reported independently.", LastTransitionTime: now(rc)})
	}
	if len(dataIssues) > 0 {
		out.Conditions = append(out.Conditions, domain.Condition{Type: "DataRecoveryRequired", Status: "True", Reason: "PostgresDataMissing", Message: "PostgreSQL data is missing on " + strings.Join(dataIssues, ", ") + "; restore a validated backup into this cluster; automatic empty initialization is disabled.", LastTransitionTime: now(rc)})
	}
	if s.RestoreRecovery != nil {
		var checkpoint backupCheckpoint
		if err := json.Unmarshal(s.RestoreRecovery.Checkpoint, &checkpoint); err != nil {
			out.Message = "Restore recovery checkpoint is unreadable; preserve the stopped data and repair the operation record."
		} else {
			source := "source selection not yet completed"
			if checkpoint.Source != nil {
				source = fmt.Sprintf("source %s/%s, backup %s", checkpoint.Source.Namespace, checkpoint.Source.Cluster, checkpoint.Source.Backup)
			}
			if checkpoint.Label != "" {
				source += ", set " + checkpoint.Label
			}
			out.Message = fmt.Sprintf("Resuming restore %s at checkpoint %s (generation %d; %s); repair the source repository or member dependency. Client and process holds remain until the same restore completes.", s.RestoreRecovery.ID, checkpoint.Step, checkpoint.Generation, source)
			if s.RestoreRecovery.Kind == domain.OperationBackup {
				out.Message = fmt.Sprintf("Aborting coordinated backup %s; formation membership remains fenced until all original native backup receipts are terminal.", s.RestoreRecovery.ID)
			}
		}
		out.Conditions = append(out.Conditions, domain.Condition{Type: "RestoreRecovery", Status: "False", Reason: "RestoreRecoveryPending", Message: out.Message, LastTransitionTime: now(rc)})
	}
	if s.Replacement != nil && (s.Replacement.Stage == "remove-etcd" || s.Replacement.Stage == "promote") {
		message := fmt.Sprintf("Replacement %s -> %s is at etcd checkpoint %s. Membership changes require existing voting quorum and a caught-up learner. Restore failed voters/connectivity if reconfiguration cannot proceed; do not force membership or reset DCS.", s.Replacement.Old, s.Replacement.New, s.Replacement.Stage)
		out.Conditions = append(out.Conditions, domain.Condition{Type: "EtcdReconfiguration", Status: "False", Reason: "MembershipChangePending", Message: message, LastTransitionTime: now(rc)})
	}
	slots := domain.Condition{Type: "ReplicationSlots", Status: "True", Reason: "RetentionObserved", LastTransitionTime: now(rc)}
	if len(slotIssues) > 0 {
		slots.Status, slots.Reason = "False", "SlotWALRetention"
		slots.Message = "Slots retaining over 1 GiB or missing required WAL: " + strings.Join(slotIssues, ", ") + ". Restore the consumer or explicitly rebuild/remove only a verified obsolete slot; slots and WAL are never dropped automatically."
	} else if !writer || len(slotUnknown) > 0 {
		slots.Status, slots.Reason = "Unknown", "ObservationUnavailable"
		slots.Message = "Slot retention could not be sampled on the writer."
	} else if len(slotUnsupported) > 0 {
		slots.Status, slots.Reason = "Unknown", "Unsupported"
		slots.Message = "Replication-slot health is unsupported on: " + strings.Join(slotUnsupported, ", ") + ". Refresh member binaries to enable these observations; legacy readiness checks remain active."
	}
	out.Conditions = append(out.Conditions, slots)
	capacity := domain.Condition{Type: "WALCapacity", Status: "True", Reason: "CapacityAvailable", LastTransitionTime: now(rc)}
	if len(capacityIssues) > 0 {
		capacity.Status, capacity.Reason = "False", "DataDiskPressure"
		capacity.Message = "Less than 10 percent data filesystem space remains on: " + strings.Join(capacityIssues, ", ") + ". Inspect WAL archive backlog and replication slots; restore dependencies or grow the volume. Do not remove PostgreSQL WAL files."
	}
	out.Conditions = append(out.Conditions, capacity)
	if patroniTopology(s.Topology) {
		quorum := domain.Condition{
			Type: "EtcdQuorum", Status: "True", Reason: "QuorumAvailable",
			LastTransitionTime: now(rc),
		}
		if !quorumHealthy {
			quorum.Status, quorum.Reason = "False", "QuorumUnavailable"
			quorum.Message = "No data member can complete a linearizable etcd request. Restore a majority of the existing voters; if their stores are permanently lost, preserve surviving data and use an explicit etcd snapshot/backup disaster-recovery procedure. Automatic DCS reset and writer selection are refused."
			if !quorumRequired {
				quorum.Status, quorum.Reason = "Unknown", "Unsupported"
				quorum.Message = "Local linearizable DCS health is not advertised by the observed member protocol. Refresh member binaries to enable these observations; legacy readiness checks remain active."
			}
			if len(observationUnknown) > 0 {
				quorum.Status, quorum.Reason = "Unknown", "ObservationUnavailable"
				quorum.Message = strings.Join(observationIssues, "; ")
			}
		}
		replication := domain.Condition{
			Type: "Replication", Status: "True", Reason: "ReplicasStreaming",
			LastTransitionTime: now(rc),
		}
		if len(replicationIssues) > 0 {
			replication.Status, replication.Reason = "False", "ReplicasUnavailable"
			replication.Message = "Stopped, non-streaming, lag unknown or more than 16 MiB behind the primary: " + strings.Join(replicationIssues, ", ")
		} else if len(observationUnknown) > 0 {
			replication.Status, replication.Reason = "Unknown", "ObservationUnavailable"
			replication.Message = strings.Join(observationIssues, "; ")
		} else if len(distanceUnsupported) > 0 {
			replication.Status, replication.Reason = "Unknown", "Unsupported"
			replication.Message = "Cross-member WAL-distance health is unsupported on: " + strings.Join(distanceUnsupported, ", ") + ". Legacy streaming and reported lag checks remain active; refresh member binaries for transport-lag observations."
		}
		out.Conditions = append(out.Conditions, quorum, replication)
		dcs := domain.Condition{Type: "EtcdMembers", Status: "True", Reason: "MembersAvailable", LastTransitionTime: now(rc)}
		if len(etcdIssues) > 0 {
			dcs.Status, dcs.Reason = "False", "LocalEtcdUnavailable"
			dcs.Message = "Local etcd linearizable probe failed on: " + strings.Join(etcdIssues, ", ") + ". Restore connectivity/process health; for persistent store corruption replace the affected member while quorum survives. Do not reset DCS or force a new cluster."
		} else if len(observationUnknown) > 0 {
			dcs.Status, dcs.Reason = "Unknown", "ObservationUnavailable"
			dcs.Message = strings.Join(observationIssues, "; ")
		} else if len(etcdUnsupported) > 0 {
			dcs.Status, dcs.Reason = "Unknown", "Unsupported"
			dcs.Message = "Local DCS health is unsupported on: " + strings.Join(etcdUnsupported, ", ") + ". Refresh member binaries to enable these observations; legacy readiness checks remain active."
		}
		failover := domain.Condition{Type: "Failover", Status: "True", Reason: "PatroniActive", LastTransitionTime: now(rc)}
		if len(paused) > 0 {
			failover.Status, failover.Reason = "False", "UnexpectedPatroniPause"
			failover.Message = "Patroni is paused on: " + strings.Join(paused, ", ") + ". Preserve operator maintenance ownership; resume only through the workflow or operator that established the pause."
			if s.Upgrade != nil {
				failover.Reason = "MajorUpgradePaused"
				failover.Message = "The major-upgrade journal owns the Patroni pause; automatic failover remains fenced until that workflow resumes it."
			}
		} else if len(observationUnknown) > 0 {
			failover.Status, failover.Reason = "Unknown", "ObservationUnavailable"
			failover.Message = strings.Join(observationIssues, "; ")
		} else if len(pauseUnsupported) > 0 {
			failover.Status, failover.Reason = "Unknown", "Unsupported"
			failover.Message = "Patroni pause observations are unsupported on: " + strings.Join(pauseUnsupported, ", ") + ". Refresh member binaries to enable them; no pause ownership or native state is inferred from absent fields."
		}
		out.Conditions = append(out.Conditions, dcs, failover)
	}
	if rc.Cluster.Spec.Backup.Storage != "" {
		archive := domain.Condition{Type: "WALArchive", Status: "True", Reason: "ArchiveAvailable", LastTransitionTime: now(rc)}
		if len(archiveIssues) > 0 {
			archive.Status, archive.Reason = "False", "ArchiveFailing"
			archive.Message = "WAL archive failed on " + strings.Join(archiveIssues, ", ") + " for backup storage " + rc.Cluster.Spec.Backup.Storage + ". Restore repository access and monitor free disk/WAL backlog; grow the data volume if necessary. WAL is preserved, never discarded automatically."
		} else if !writer {
			archive.Status, archive.Reason = "Unknown", "WriterUnavailable"
			archive.Message = "Archive health requires an available writer."
		} else if len(archiveUnknown) > 0 {
			archive.Status, archive.Reason = "Unknown", "ObservationUnavailable"
			archive.Message = "WAL archive/backlog observations are unavailable on: " + strings.Join(archiveUnknown, ", ")
		} else if len(archiveUnsupported) > 0 {
			archive.Status, archive.Reason = "Unknown", "Unsupported"
			archive.Message = "WAL archive/backlog health is unsupported on: " + strings.Join(archiveUnsupported, ", ") + ". Refresh member binaries to enable these observations; legacy readiness checks remain active."
		}
		out.Conditions = append(out.Conditions, archive)
	}
	tlsCondition, err := providedCertificateCondition(ctx, rc)
	if err != nil {
		return out, err
	}
	if tlsCondition != nil {
		out.Conditions = append(out.Conditions, *tlsCondition)
	}
	memberTLS, err := memberCertificateCondition(ctx, rc, s)
	if err != nil {
		return out, err
	}
	if memberTLS != nil {
		out.Conditions = append(out.Conditions, *memberTLS)
	}
	if condition := proxyClockCondition(rc, s); condition != nil {
		out.Conditions = append(out.Conditions, *condition)
	}
	out.Connection, err = publicConnection(ctx, rc, out.Connection)
	if err != nil {
		return out, err
	}
	// Backup observations must not discard a fresh database/quorum snapshot.
	if err := p.backupStatus(ctx, rc, &out); err != nil {
		out.Conditions = append(out.Conditions, domain.Condition{Type: "Backup", Status: "Unknown", Reason: "ObservationUnavailable", Message: "Backup inventory could not be collected; database health is reported independently.", LastTransitionTime: now(rc)})
	}
	for _, m := range s.Members {
		engine.ApplyRuntimeRecoveryStatus(&out, m.Name, m.Volume, m.RuntimeRecovery, now(rc))
	}
	return out, nil
}

func proxyHasCurrentWriter(primary string, backends []pgspec.ProxyBackend) bool {
	if primary == "" {
		return false
	}
	for _, backend := range backends {
		if backend.Host == primary && backend.Hostgroup == 10 && backend.Status == "ONLINE" {
			return true
		}
	}
	return false
}
