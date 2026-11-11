// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"errors"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
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
	ctx = normalMySQLReads(ctx)
	if engine.ObservationPassCache(ctx) == nil {
		ctx = engine.WithObservationPass(ctx)
	}
	defer func() { engine.ApplyDeploymentStatus(rc, &status) }()
	s, err := load(rc.Cluster)
	if err != nil {
		return status, err
	}
	defer func() {
		p.applyMySQLRuntimeRecoveryStatus(&status, s, now(rc))
		if cp := s.Replacement; cp != nil {
			if next := s.find(cp.New); next != nil && next.ImportAmbiguity != "" {
				status.Message = next.ImportAmbiguity
				status.Conditions = append(status.Conditions, domain.Condition{Type: "ReplacementWaiting", Status: "True", Reason: "ImportOutcomeUnknown", Message: next.ImportAmbiguity, LastTransitionTime: now(rc)})
			}
		}
	}()
	status = domain.ClusterStatus{Phase: domain.PhaseCreating, Version: rc.Cluster.Spec.Version, ObservedAt: now(rc), ObservedGeneration: rc.Cluster.Status.ObservedGeneration, Members: []domain.MemberStatus{}, Conditions: []domain.Condition{}, Connection: discovery(rc, s), Placement: domain.Placement{Groups: map[string][]string{}}}
	if s.Suspended {
		status.Phase = domain.PhaseSuspended
		return status, nil
	}
	var observed []memberObservation
	var quorumErr error
	if rc.Cluster.Spec.Provider == "pxc" {
		// Required readiness reads and continuity sampling fit under the same
		// outer status-sweep deadline. Neither depends on the other's RPCs.
		// Readiness gets an immutable cluster snapshot: the quorum check alone
		// owns journal/state writes to the caller's Cluster.Internal.
		readRC, readCluster := *rc, *rc.Cluster
		readRC.Cluster = &readCluster
		required := make(chan []memberObservation, 1)
		go func() {
			required <- observeStatusMembers(ctx, &readRC, s.Members)
		}()
		quorumErr = checkPXCQuorumGTIDs(ctx, rc, s)
		observed = <-required
	} else {
		observed = observeStatusMembers(ctx, rc, s.Members)
	}
	if deferred, deferredErr, ok := deferredMySQLObservation(rc, s, observed); ok {
		return deferred, deferredErr
	}
	healthy := len(s.data()) == rc.Cluster.Spec.Components.Engine.Replicas && len(s.Members) > 0
	writer := false
	primary := ""
	nativeDiagnosticPublished := false
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
		if o.agentObserved {
			member.Agent = domain.AgentStatus{Version: o.agent.Agent.Version, Commit: o.agent.Agent.Commit, Build: o.agent.Agent.Build, Healthy: true, ConfigVersion: o.agent.ConfigVersion, LastSeen: o.agentAt}
			member.Uptime = o.agent.UptimeSeconds
			member.Resources = engine.MemberResources(o.agent.Resources, &m.Resources)
		}
		if o.agentObserved && o.err != nil {
			// Only structured, bounded agent diagnostics cross into public status.
			// A successful transport probe is not native database readiness.
			var remote *agentapi.HTTPError
			if !nativeDiagnosticPublished && errors.As(o.err, &remote) && remote.Message != "" {
				nativeDiagnosticPublished = true
				status.Conditions = append(status.Conditions, domain.Condition{Type: "NativeObservationFailed", Status: "True", Reason: "AgentNativeStatusError", Message: m.Name + ": " + remote.Message, LastTransitionTime: now(rc)})
			}
		}
		if o.err == nil {
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
		service := o.infrastructure.service
		if o.infrastructureErr != nil {
			return status, o.infrastructureErr
		}
		if service == nil || service.Status != infra.StatusRunning && service.Status != infra.StatusActive {
			member.Health = false
		}
		// Planned members, including the proxy waiting for its first writer,
		// have no infrastructure identity yet. Observe them as unavailable
		// without asking the backend for containers of a nonexistent service.
		if service != nil {
			containers := o.infrastructure.containers
			for _, container := range containers {
				if container.PlacementGroup != "" {
					member.PlacementGroup = container.PlacementGroup
					status.Placement.Groups[container.PlacementGroup] = append(status.Placement.Groups[container.PlacementGroup], m.Name)
					break
				}
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
		if e := quorumErr; e != nil {
			healthy = false
			condition.Status, condition.Reason, condition.Message = "Unknown", "QuorumComparisonUnavailable", e.Error()
			if _, comparison := e.(*myspec.GTIDQuorumError); !comparison {
				condition.Reason = "GTIDHistoryPersistenceUnavailable"
				condition.Message = "GTID history journal, state or audit persistence unavailable; continuity cannot be certified until persistence recovers"
				if _, persistence := e.(*gtidHistoryPersistenceError); persistence {
					condition.Message = e.Error()
				}
			}
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
		if s.RestoreGTIDBlocked != "" {
			healthy = false
			status.Conditions = append(status.Conditions, domain.Condition{Type: "RestoreGTIDCertified", Status: "False", Reason: "GTIDMappingCertificationBlocked", Message: s.RestoreGTIDBlocked + "; restore again or delete; restart/backup blocked until certified", LastTransitionTime: now(rc)})
		} else if s.RestoreGTIDUnavailable != "" {
			status.Conditions = append(status.Conditions, domain.Condition{Type: "RestoreGTIDCertified", Status: "Unknown", Reason: "RestoreCertificationUnavailable", Message: s.RestoreGTIDUnavailable, LastTransitionTime: now(rc)})
		} else if s.RestoreGTIDFence != "" && (s.Formation.GTIDCertification == nil || s.Formation.GTIDCertification.Owner != s.RestoreGTIDFence) {
			healthy = false
			status.Conditions = append(status.Conditions, domain.Condition{Type: "RestoreGTIDCertified", Status: "Unknown", Reason: "GTIDMappingCertificationInProgress", Message: "Client SQL is fenced while the restored PXC formation certifies replicated GTID mapping", LastTransitionTime: now(rc)})
		}
		mappingCondition, mappingReady := pxcFormationMappingCondition(s)
		mappingCondition.LastTransitionTime = now(rc)
		status.Conditions = append(status.Conditions, mappingCondition)
		if !mappingReady {
			healthy = false
		}
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
		if s.Rolling.Stage == "scale-failed" {
			status.Phase = domain.PhaseDegraded
			status.Conditions = append(status.Conditions, domain.Condition{Type: "MySQLProxyScale", Status: "False", Reason: "ScaleFailed", Message: "ProxySQL scale operation failed; observing surviving replicas and waiting for backend-proven recovery or a superseding desired count", LastTransitionTime: now(rc)})
		}
	}
	if s.ProxyAdmissionWait != "" {
		status.Conditions = append(status.Conditions, domain.Condition{Type: "MySQLProxyAdmission", Status: "False", Reason: "WaitingForProxyAdmission", Message: s.ProxyAdmissionWait, LastTransitionTime: now(rc)})
	}
	if s.Replacement != nil || s.TLSRotation != nil || s.GaleraTLS != nil {
		status.Phase = domain.PhaseUpdating
	}
	for _, m := range s.data() {
		if m.Retirement != "" {
			status.Phase = domain.PhaseScaling
		}
	}
	if clustered(s.Topology) {
		var voters []string
		for _, m := range s.data() {
			if m.Stage == "joined" && !m.Removing && m.Retirement == "" {
				voters = append(voters, m.Name)
			}
		}
		// Placement is part of the same read-only pass as member inventory.
		// Reuse its platform snapshot and absolute observation deadline.
		probe, cancel := mysqlReadContext(ctx, rc)
		defer cancel()
		if err = engine.ObserveFailureDomains(probe, rc, &status, voters); err != nil {
			return status, err
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

func deferredMySQLObservation(rc *engine.Context, s *state, observed []memberObservation) (domain.ClusterStatus, error, bool) {
	deferred := false
	for _, observation := range observed {
		deferred = deferred || errors.Is(observation.err, engine.ErrObservationBudgetExhausted) || errors.Is(observation.infrastructureErr, engine.ErrObservationBudgetExhausted)
	}
	if !deferred {
		return domain.ClusterStatus{}, nil, false
	}
	status := engine.DeferredObservation(rc.Cluster.Status, now(rc))
	result := &engine.ObservationBudgetError{}
	var causes []error
	for i, observation := range observed {
		metadataCause := observedFailureCause(observation.metadataErr)
		nativeCause := observedFailureCause(observation.nativeErr)
		platformCause := observedFailureCause(observation.infrastructureErr)
		metadataFailure := metadataCause != nil
		nativeFailure := nativeCause != nil
		platformFailure := platformCause != nil
		for _, err := range [3]error{metadataCause, nativeCause, platformCause} {
			if err != nil {
				causes = append(causes, err)
			}
		}
		negative := metadataFailure || nativeFailure || platformFailure
		if observation.nativeObserved && s.Members[i].Role != "proxy" && observation.status.MemberState != "ONLINE" {
			negative = true
		}
		if observation.agentObserved && observation.agent.ConfigVersion < s.Members[i].ConfigVersion {
			negative = true
		}
		if observation.infrastructureObserved {
			service := observation.infrastructure.service
			negative = negative || service == nil || service.Status != infra.StatusRunning && service.Status != infra.StatusActive
		}
		if negative {
			result.Failures = append(result.Failures, engine.ObservationFailure{Member: s.Members[i].Name, Agent: metadataFailure})
		}
	}
	for _, failure := range result.Failures {
		for i := range status.Members {
			if status.Members[i].Name == failure.Member {
				status.Members[i].Health = false
				if failure.Agent {
					status.Members[i].Agent.Healthy = false
				}
			}
		}
	}
	result.Cause = errors.Join(causes...)
	if incident := activeGTIDIncident(s); incident != nil {
		condition := domain.Condition{Type: "GTIDHistorySafe", Status: "False", Reason: "GTIDHistoryDiscontinuityLatched", Message: incident.message(), LastTransitionTime: now(rc)}
		replaced := false
		for i := range status.Conditions {
			if status.Conditions[i].Type == condition.Type {
				status.Conditions[i] = condition
				replaced = true
			}
		}
		if !replaced {
			status.Conditions = append(status.Conditions, condition)
		}
	}
	return status, result, true
}

// Strip only budget deferrals from joined failures; a concurrent HTTP/platform
// failure must not disappear merely because another component ran out of time.
func observedFailureCause(err error) error {
	if err == nil || err == engine.ErrObservationBudgetExhausted {
		return nil
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		var causes []error
		for _, cause := range joined.Unwrap() {
			if actual := observedFailureCause(cause); actual != nil {
				causes = append(causes, actual)
			}
		}
		return errors.Join(causes...)
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok && errors.Is(err, engine.ErrObservationBudgetExhausted) {
		return observedFailureCause(wrapped.Unwrap())
	}
	return err
}
