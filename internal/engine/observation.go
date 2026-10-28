// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package engine

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

// ObservationTimeout includes cold namespace/TLS setup and the native response.
// Keep headroom inside the sweep without extending the persisted readiness lease.
const ObservationTimeout = 4 * time.Second

// ObserveAgentStatus bounds a short configuration/process observation without
// shortening the caller's subsequent administration or engine operations.
func ObserveAgentStatus(ctx context.Context, client *agentapi.Client, engineName, member string) (agentapi.Status, error) {
	readCtx, cancel := context.WithTimeout(ctx, ObservationTimeout)
	defer cancel()
	status, err := client.Status(readCtx)
	if errors.Is(err, context.DeadlineExceeded) {
		return status, fmt.Errorf("%s generic status read exceeded %s budget: inspect member %s responsiveness/connectivity: %w", engineName, ObservationTimeout, member, err)
	}
	return status, err
}

// A sweep gets five seconds and one immediate retry. Never carry a failed
// sample across cycles to suppress flapping: only a successful fresh retry may
// restore readiness. A stalled reconciliation cannot extend the readiness lease.
const ObservationSweepTimeout = 5 * time.Second
const ReadinessInterval = 5 * time.Second

// InitialDataSourceObserver reports durable native restore completion, not
// process health or the desired generation observed by an earlier operation.
// A provider supporting dataSource must implement this fail-closed contract.
type InitialDataSourceObserver interface {
	InitialDataSourceApplied(*domain.Cluster) (bool, error)
}

// InitialDataSourceMigrator upgrades validated pre-marker native receipts before
// historical convergence-tracker retirement is interpreted as an aborted restore.
// It must not enqueue, replay native work, or infer success from process health.
type InitialDataSourceMigrator interface {
	MigrateInitialDataSource(context.Context, *Context) error
}

// InitialDataSourceRecoveryPolicy permits only an already-owned, frozen native
// recovery journal to progress through Sync after its queue operation retired.
// Providers must return before ordinary convergence while this policy is true.
type InitialDataSourceRecoveryPolicy interface {
	InitialDataSourceRecoveryPending(*domain.Cluster) (bool, error)
}

// InitialDataSourceUnproven marks only a legacy superseded create without any
// native restore receipt. Runtime management must continue, but initial data
// recovery must never be replayed against a database that may now be in use.
func InitialDataSourceUnproven(c *domain.Cluster) bool {
	if state := c.Status.InitialDataSource; state != nil && state.AbortMessage != "" {
		return false
	}
	unproven := false
	if c.Spec.DataSource != nil {
		for _, condition := range c.Status.Conditions {
			if condition.Type == "InitialDataSource" && condition.Reason == "RestoreAborted" && condition.Status == "False" {
				return false
			}
			if condition.Type == "InitialDataSource" && condition.Reason == "LegacyRestoreUnproven" && condition.Status == "False" {
				unproven = true
			}
		}
	}
	return unproven
}

// ApplyInitialDataSourceStatus prevents an empty but healthy target from being
// published as Ready after supersession, cancellation or control-plane restart.
func ApplyInitialDataSourceStatus(p Provider, c *domain.Cluster, status *domain.ClusterStatus) error {
	status.InitialDataSource = c.Status.InitialDataSource
	if c.Spec.DataSource == nil || c.Deleting || status.Phase == domain.PhaseSuspended {
		return nil
	}
	applied := false
	var err error
	if observer, ok := p.(InitialDataSourceObserver); ok {
		applied, err = observer.InitialDataSourceApplied(c)
	}
	for _, condition := range c.Status.Conditions {
		if condition.Type == "InitialDataSource" && condition.Reason == "RestoreAborted" && condition.Status == "False" {
			applied, err = false, errors.New(condition.Message)
		}
	}
	if state := c.Status.InitialDataSource; state != nil && state.AbortMessage != "" {
		applied, err = false, errors.New(state.AbortMessage)
		abort := domain.Condition{Type: "InitialDataSource", Status: "False", Reason: "RestoreAborted", Message: state.AbortMessage}
		found := false
		for i := range status.Conditions {
			if status.Conditions[i].Type == abort.Type {
				status.Conditions[i] = abort
				found = true
			}
		}
		if !found {
			status.Conditions = append(status.Conditions, abort)
		}
	}
	if applied && err == nil {
		return nil
	}
	message := "Initial data source has not completed; healthy database processes do not prove that the requested data was restored"
	reason := "RestoreIncomplete"
	if status.Phase != domain.PhaseFailed && status.Phase != domain.PhaseRestoring {
		status.Phase = domain.PhaseCreating
	}
	if err != nil {
		status.Phase, message = domain.PhaseFailed, err.Error()
	}
	for _, condition := range c.Status.Conditions {
		if condition.Type == "InitialDataSource" && condition.Reason == "RestoreAborted" && condition.Status == "False" {
			status.Phase, message = domain.PhaseFailed, condition.Message
			found := false
			for _, next := range status.Conditions {
				found = found || next.Type == "InitialDataSource"
			}
			if !found {
				status.Conditions = append(status.Conditions, condition)
			}
		}
	}
	if InitialDataSourceUnproven(c) {
		status.Phase, reason, err = domain.PhaseDegraded, "LegacyRestoreUnproven", nil
		for _, condition := range c.Status.Conditions {
			if condition.Type == "InitialDataSource" && condition.Reason == reason {
				message = condition.Message
				found := false
				for _, next := range status.Conditions {
					found = found || next.Type == condition.Type
				}
				if !found {
					status.Conditions = append(status.Conditions, condition)
				}
				break
			}
		}
	}
	status.Message = message
	status.ObservedGeneration = c.Status.ObservedGeneration
	found := false
	for i := range status.Conditions {
		condition := &status.Conditions[i]
		if condition.Type == "Ready" {
			condition.Status, condition.Reason, condition.Message = "False", reason, message
			found = true
		}
	}
	if !found {
		status.Conditions = append(status.Conditions, domain.Condition{Type: "Ready", Status: "False", Reason: reason, Message: message})
	}
	return err
}

const ReadinessMaxAge = ReadinessInterval + 2*ObservationSweepTimeout

func readinessCondition(c domain.Condition) bool {
	switch c.Type {
	case "Healthy", "Ready", "ClusterHealth", "ComponentsReady", "Quorum",
		"EtcdQuorum", "EtcdMembers", "Failover", "Replication", "ReplicationSlots",
		"WALCapacity", "WALArchive", "ProxyWriter", "CredentialsPublished", "PITR", "NamespaceReady":
		return true
	}
	return strings.HasPrefix(c.Type, "Replication/") || strings.HasPrefix(c.Type, "Router/") || c.Reason == "WritablePrimary"
}

// ApplyAgentDiagnostic keeps engine configuration failures in control-plane
// health, not platform deployment readiness. A running old process must not hide
// an Apply failure or make a member/cluster Ready.
func ApplyAgentDiagnostic(member *domain.MemberStatus, diagnostic string) {
	if diagnostic == "" {
		return
	}
	member.Health = false
	if strings.Contains(member.Agent.Message, diagnostic) {
		return
	}
	if member.Agent.Message != "" {
		member.Agent.Message += "; "
	}
	member.Agent.Message += "Configuration apply: " + diagnostic
}

// UnknownObservation retains diagnostics, but withdraws positive availability
// claims. Clone slices because API projections must not mutate stored snapshots.
func UnknownObservation(status domain.ClusterStatus, at time.Time, reason string) domain.ClusterStatus {
	status.Conditions = slices.Clone(status.Conditions)
	status.Members = slices.Clone(status.Members)
	if status.Phase == domain.PhaseReady {
		status.Phase = domain.PhaseDegraded
	}
	found := false
	for i := range status.Conditions {
		c := &status.Conditions[i]
		if readinessCondition(*c) {
			found = found || c.Type == "Healthy"
			if c.Status == "True" {
				c.Status, c.Reason, c.Message, c.LastTransitionTime = "Unknown", reason, "Fresh availability observation required", at
			}
		}
	}
	if !found {
		status.Conditions = append(status.Conditions, domain.Condition{Type: "Healthy", Status: "Unknown", Reason: reason, Message: "Fresh availability observation required", LastTransitionTime: at})
	}
	for i := range status.Members {
		status.Members[i].Health = false
		status.Members[i].Agent.Healthy = false
		if status.Members[i].Agent.Message == "" {
			status.Members[i].Agent.Message = "Fresh availability observation required"
		}
	}
	status.Message = "Availability observation unavailable or stale; inspect member reachability"
	ObserveAgentVersions(&status, CurrentAgentBuild(), at)
	return status
}

// FreshStatus applies a lease to a persisted observation, including legacy
// snapshots with no sample timestamp. Transition times are not sample times.
func FreshStatus(status domain.ClusterStatus, at time.Time) domain.ClusterStatus {
	status.Conditions = slices.Clone(status.Conditions)
	ObserveAgentVersions(&status, CurrentAgentBuild(), at)
	active := status.Phase == domain.PhaseReady || status.Phase == domain.PhaseDegraded
	for _, condition := range status.Conditions {
		active = active || readinessCondition(condition) && condition.Status == "True"
	}
	for _, member := range status.Members {
		active = active || member.Health || member.Agent.Healthy
	}
	if !active || status.Phase == domain.PhaseSuspended || status.Phase == domain.PhaseDeleting {
		return status
	}
	if status.ObservedAt.IsZero() || status.ObservedAt.After(at) || at.Sub(status.ObservedAt) > ReadinessMaxAge {
		return UnknownObservation(status, at, "ObservationStale")
	}
	return status
}

// ObserveStatus obtains a bounded fresh snapshot independently of recovery work.
// Providers must honor context cancellation; read-only probes must never inherit
// the much longer engine-mutation timeout.
func ObserveStatus(ctx context.Context, p Provider, rc *Context) (domain.ClusterStatus, error) {
	var status domain.ClusterStatus
	var err error
	clock := rc.Clock
	if clock == nil {
		clock = RealClock{}
	}
	// Status may return only an early partial result, or mutate its input while
	// probing. Capture the inspected lifecycle/inventory before either attempt:
	// an observation error cannot establish new provisioning progress or remove
	// durable member identities.
	previous := rc.Cluster.Status
	previous.Members = slices.Clone(previous.Members)
	previous.Conditions = slices.Clone(previous.Conditions)
	for attempt := range 2 {
		started := clock.Now()
		probe, cancel := context.WithTimeout(ctx, ObservationSweepTimeout)
		probe = infra.ObservationContext(probe)
		status, err = p.Status(probe, rc)
		err = errors.Join(err, ApplyInitialDataSourceStatus(p, rc.Cluster, &status))
		err = errors.Join(err, observePlacementFailures(probe, rc, &status, attempt == 0))
		err = errors.Join(err, ObserveRuntimeStops(probe, rc, &status, attempt == 0))
		err = errors.Join(err, probe.Err())
		cancel()
		status.ObservedAt = started
		// A tagged result belongs to a completed/cached sweep, not a newly
		// observed provider result. Never relabel it as this process's sample.
		failed := err != nil || status.Phase == "" || status.ObservationEpoch != ""
		retry := status.Phase == domain.PhaseDegraded
		for _, condition := range status.Conditions {
			if readinessCondition(condition) && (condition.Status == "Unknown" || condition.Reason == "Unreachable" || condition.Reason == "NoWritablePrimary") {
				retry = true
			}
		}
		for _, member := range status.Members {
			if member.Agent.Healthy {
				// Only positive samples need a current observation timestamp.
				// A fresh negative read is useful even when no agent replied.
				if member.Agent.LastSeen.IsZero() || member.Agent.LastSeen.Before(started) {
					failed = true
				}
			} else {
				retry = true
				// Contradictory positive claims still cannot renew readiness.
				if member.Health || status.Phase == domain.PhaseReady {
					failed = true
				}
			}
		}
		if !failed && (!retry || attempt == 1) {
			status.ObservationEpoch = rc.ObservationEpoch
			ObserveAgentVersions(&status, CurrentAgentBuild(), clock.Now())
			return status, nil
		}
		if attempt == 1 || ctx.Err() != nil {
			if err != nil {
				status.Phase = previous.Phase
				status.Members = previous.Members
				status.ObservedGeneration = previous.ObservedGeneration
				if len(status.Conditions) == 0 {
					status.Conditions = previous.Conditions
				}
			} else if status.Phase == "" {
				status = previous
			}
			status = UnknownObservation(status, clock.Now(), "ObservationFailed")
			// Failed samples never renew a previously successful readiness lease.
			status.ObservedAt = time.Time{}
			status.ObservationEpoch = ""
			return status, err
		}
	}
	panic("unreachable")
}
