// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package reconciler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
	"unicode/utf8"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/maintenance"
	"github.com/KarakoramDev/Karakoram/internal/redact"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

type operationEventKey struct{}

func boundedMessage(message string, limit int) string {
	if len(message) <= limit {
		return message
	}
	message = message[:limit-3]
	for !utf8.ValidString(message) {
		message = message[:len(message)-1]
	}
	return message + "..."
}

// failureCleanupStep keeps cleanup ownership durable without changing the
// provider's checkpoint. LastError carries the original terminal reason.
const failureCleanupStep = "failure-cleanup"

type operationFailureCleanup interface {
	engine.OperationCanceller
	SupportsFailureCleanup(*domain.Operation) bool
}

// Failure preflight can publish a terminal observation failure without resolving
// infrastructure dependencies that may themselves be the reason for cleanup.
type operationFailureCleanupPreparer interface {
	PrepareOperationFailureCleanup(context.Context, *engine.Context, *domain.Operation) (engine.OpResult, error)
}

func (m *Manager) failQueueOperation(p engine.Provider, op *domain.Operation, err error) bool {
	if !m.operationFailure(op, err) {
		return false
	}
	if cleanup, ok := p.(operationFailureCleanup); ok && cleanup.SupportsFailureCleanup(op) {
		op.LastError = op.Message
		now := m.opts.Context.Clock.Now()
		op.FailureCleanupStartedAt = &now
		op.Phase, op.CompletedAt, op.Step = domain.PhaseRunning, nil, failureCleanupStep
		return false
	}
	return true
}

// DefaultOperationTimeout bounds observed control-plane execution time, excluding
// maintenance waits. Wall-clock steps and time spent offline cannot exhaust it.
const DefaultOperationTimeout = 2 * time.Hour
const DefaultRestoreTimeout = 6 * time.Hour
const DefaultOperationRetryLimit = 20
const DefaultFailureCleanupTimeout = 6 * time.Hour

func compensationStep(op *domain.Operation) string {
	var cp struct {
		Step  string `json:"step"`
		Stage string `json:"stage"`
	}
	if json.Unmarshal(op.Checkpoint, &cp) == nil {
		if cp.Step != "" {
			return cp.Step
		}
		if cp.Stage != "" {
			return cp.Stage
		}
	}
	return op.Step
}

func (m *Manager) operationTimeout(op *domain.Operation) time.Duration {
	if d := m.opts.OperationTimeouts[op.Kind]; d > 0 {
		return d
	}
	if op.Kind == domain.OperationRestore {
		return DefaultRestoreTimeout
	}
	return DefaultOperationTimeout
}

type operationBudget struct {
	observed time.Time
	cleanup  bool
	paused   bool
}

// elapsedOperationTime accumulates monotonic intervals only. Persisted elapsed
// time survives restart, but offline time is deliberately excluded: a wall-clock
// step is indistinguishable from downtime and cannot safely retire owned work.
func (m *Manager) elapsedOperationTime(op *domain.Operation, cleanup bool) time.Duration {
	now := time.Now()
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.budgets == nil {
		m.budgets = make(map[string]map[string]operationBudget)
	}
	key := domain.ClusterRef{Namespace: op.Namespace, Name: op.Cluster}.Key()
	budgets := m.budgets[key]
	if budgets == nil {
		budgets = make(map[string]operationBudget)
		m.budgets[key] = budgets
	}
	elapsed := &op.Elapsed
	if cleanup {
		elapsed = &op.FailureCleanupElapsed
	}
	previous, ok := budgets[op.ID]
	if ok && previous.cleanup == cleanup && (!previous.paused || cleanup) {
		*elapsed += now.Sub(previous.observed)
	}
	budgets[op.ID] = operationBudget{observed: now, cleanup: cleanup, paused: previous.paused && !cleanup}
	return *elapsed
}

func (m *Manager) retainOperationBudgets(ref domain.ClusterRef, ops []domain.Operation) {
	m.mu.Lock()
	defer m.mu.Unlock()
	budgets := m.budgets[ref.Key()]
	for id := range budgets {
		active := false
		for i := range ops {
			if ops[i].ID == id {
				active = true
				break
			}
		}
		if !active {
			delete(budgets, id)
		}
	}
	if len(budgets) == 0 {
		delete(m.budgets, ref.Key())
	}
}

func (m *Manager) observeOperationBlocker(op *domain.Operation, message string, observed time.Time) {
	op.BlockingMessage = boundedMessage(redact.Diagnostic(message), 768)
	op.BlockingObservedAt = &observed
}

func (m *Manager) operationFailure(op *domain.Operation, err error) bool {
	// Exhaustion is an operator-action boundary, not an operation retry. Fail
	// tracking work explicitly so it cannot claim success or block a re-arm.
	var endpointExhausted *infra.EndpointRepairExhaustedError
	if errors.Is(err, infra.ErrDeploymentRetriesExhausted) || errors.As(err, &endpointExhausted) {
		m.observeOperationBlocker(op, err.Error(), m.opts.Context.Clock.Now())
		op.Message = boundedMessage(fmt.Sprintf("last observed at %s: %s", op.BlockingObservedAt.UTC().Format(time.RFC3339Nano), op.BlockingMessage), 1024)
		op.LastError = op.BlockingMessage
		op.Phase = domain.PhaseFailed
		now := m.opts.Context.Clock.Now()
		op.CompletedAt = &now
		return true
	}
	if err != nil {
		op.Message = boundedMessage(err.Error(), 1024)
		op.LastError = op.Message
		m.observeOperationBlocker(op, err.Error(), m.opts.Context.Clock.Now())
		if scheduledPlatformWait(err) == nil {
			op.Failures++
		}
	}
	var field *domain.FieldError
	permanent := errors.As(err, &field) || errors.Is(err, engine.ErrInvalid) || errors.Is(err, engine.ErrNotSupported)
	expired := m.elapsedOperationTime(op, false) >= m.operationTimeout(op)
	if !permanent && !expired && op.Failures < m.opts.OperationRetryLimit {
		return false
	}
	last := op.LastError
	if last == "" {
		last = op.Message
	}
	if last == "" {
		last = "No terminal result was observed"
	}
	if op.BlockingMessage != "" {
		last = op.BlockingMessage
		if op.BlockingObservedAt != nil {
			last = fmt.Sprintf("last observed at %s: %s", op.BlockingObservedAt.UTC().Format(time.RFC3339Nano), last)
		}
	} else {
		last = "last observation time unknown: " + last
	}
	if expired {
		op.Message = fmt.Sprintf("%s exceeded its %s deadline: %s", op.Kind, m.operationTimeout(op), last)
	}
	if !expired && !permanent {
		op.Message = fmt.Sprintf("%s exhausted its retry budget (%d failures): %s", op.Kind, op.Failures, last)
	}
	if permanent && !expired {
		op.Message = last
	}
	op.Message = boundedMessage(redact.Diagnostic(op.Message), 1024)
	op.Phase = domain.PhaseFailed
	now := m.opts.Context.Clock.Now()
	op.CompletedAt = &now
	return true
}

// trackingOperations do not own the imperative queue: ordinary Sync converges
// desired state while these records observe the target generation. Failed tracking
// does not discard desired state or stop eventual cleanup.
func (m *Manager) trackingOperations(ctx context.Context, p engine.Provider, c *domain.Cluster, cause error, convergenceAttempted bool) error {
	ops, err := m.opts.Store.ListOperations(ctx, c.Ref(), domain.PhasePending, domain.PhaseRunning)
	if err != nil {
		return err
	}
	now := m.opts.Context.Clock.Now()
	for i := range ops {
		op := &ops[i]
		if !domain.IsConvergenceOperation(op.Kind) {
			// Pending maintenance work has not started an execution budget.
			if op.Phase == domain.PhasePending && maintenance.Gated(op) {
				continue
			}
			// Expire Pending work even if ordinary convergence keeps failing before claim.
			_, deferred := p.(engine.OperationClaimPolicy)
			if op.Phase == domain.PhasePending && !deferred {
				m.operationFailure(op, nil)
				if err = m.opts.Store.UpdateOperation(ctx, op); err != nil {
					return err
				}
			}
			continue
		}
		if op.Phase == domain.PhasePending {
			op.Phase, op.StartedAt = domain.PhaseRunning, &now
		}
		if convergenceAttempted && cause == nil {
			op.Failures = 0
		}
		op.Attempts++
		// Successful reconciliation can still be blocked on backend placement
		// or readiness. Those observations supersede resolved historical errors.
		if convergenceAttempted && cause == nil {
			message := c.Status.Message
			for _, condition := range c.Status.Conditions {
				if condition.Reason == "BackendPlacementFailed" && condition.Status == "False" && condition.Message != "" {
					message = condition.Message
					break
				}
			}
			if message == "" {
				message = fmt.Sprintf("%s: waiting for generation %d", c.Status.Phase, op.TargetGeneration)
			}
			observed := c.Status.ObservedAt
			if observed.IsZero() {
				observed = now
			}
			m.observeOperationBlocker(op, message, observed)
		}
		m.maintenanceBudget(op, c, maintenance.Waiting(c))
		if c.Generation > op.TargetGeneration {
			op.Phase, op.Message, op.CompletedAt = domain.PhaseCancelled, "Superseded by a newer cluster generation", &now
		} else if !m.operationFailure(op, cause) {
			op.Step = string(c.Status.Phase)
			if op.Kind == domain.OperationUpgrade {
				for _, condition := range c.Status.Conditions {
					if condition.Type == "RollingProgress" && condition.Status == "True" {
						op.Step = condition.Reason
						break
					}
				}
			}
			if c.Status.Message != "" {
				op.Message = c.Status.Message
			} else {
				op.Message = fmt.Sprintf("%s: waiting for generation %d", c.Status.Phase, op.TargetGeneration)
			}
			// Creation promises an initially usable cluster, not merely an
			// applied generation with an observation-failure explanation.
			ready := c.Status.Phase == domain.PhaseReady || (op.Kind != domain.OperationCreate && c.Status.Phase == domain.PhaseDegraded && c.Status.Message != "")
			if op.Kind != domain.OperationDelete && c.Status.ObservedGeneration >= op.TargetGeneration && ready {
				op.Phase, op.Message, op.CompletedAt = domain.PhaseSucceeded, fmt.Sprintf("%s applied generation %d (%s)", op.Kind, op.TargetGeneration, c.Status.Phase), &now
			}
		}
		m.enrichInitialCreateFailure(ctx, c, op, cause)
		if err = m.opts.Store.UpdateOperation(ctx, op); err != nil {
			return err
		}
	}
	return nil
}

// enrichInitialCreateFailure is shared by the preflight budget check and the
// post-convergence tracker: either can be the first path to persist failure.
func (m *Manager) enrichInitialCreateFailure(ctx context.Context, c *domain.Cluster, op *domain.Operation, cause error) {
	if op.Kind != domain.OperationCreate || c.Spec.DataSource == nil || op.Phase != domain.PhaseFailed {
		return
	}
	op.Step = m.initialCreateFailureStep(op, cause)
	outcome := "pending"
	restore, readErr := m.opts.Store.GetRestore(ctx, c.Ref(), "initial-restore")
	switch {
	case readErr == nil:
		outcome = string(restore.Phase)
		if restore.CompletedAt != nil {
			outcome += " at " + restore.CompletedAt.UTC().Format(time.RFC3339Nano)
		}
	case !errors.Is(readErr, store.ErrNotFound):
		outcome = "unknown (restore inventory unavailable)"
	}
	// Reserve room for the outcome even when the backend diagnostic is long.
	op.Message = boundedMessage(op.Message, 880) + "; initial restore: " + outcome
}

// preserveTransitions compares status and reason, not observation time or message.
func preserveTransitions(previous, next []domain.Condition) {
	for i := range next {
		for _, old := range previous {
			if old.Type == next[i].Type && old.Status == next[i].Status && old.Reason == next[i].Reason {
				next[i].LastTransitionTime = old.LastTransitionTime
				break
			}
		}
	}
}

// observedVersion never substitutes desired state for an unavailable observation.
// Catalog versions use numeric components, including vendor build suffixes.
func observedVersion(members []domain.MemberStatus) string {
	version := ""
	for _, member := range members {
		if member.Version != "" && (version == "" || olderVersion(member.Version, version)) {
			version = member.Version
		}
	}
	return version
}

func olderVersion(a, b string) bool {
	for a != "" || b != "" {
		left, restA := versionPart(a)
		right, restB := versionPart(b)
		if left != right {
			return left < right
		}
		a, b = restA, restB
	}
	return false
}

func versionPart(version string) (uint64, string) {
	i := 0
	for i < len(version) && (version[i] < '0' || version[i] > '9') {
		i++
	}
	var part uint64
	for i < len(version) && version[i] >= '0' && version[i] <= '9' {
		part = part*10 + uint64(version[i]-'0')
		i++
	}
	return part, version[i:]
}

// maintenanceBudget changes pause state only for work that actually reached its
// disruption gate. Cluster conditions alone are not imperative wait receipts.
// Cleanup and cancellation must always retain their bounded recovery budgets.
func (m *Manager) maintenanceBudget(op *domain.Operation, c *domain.Cluster, waiting bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := c.Ref().Key()
	if m.budgets == nil {
		m.budgets = make(map[string]map[string]operationBudget)
	}
	if m.budgets[key] == nil {
		m.budgets[key] = make(map[string]operationBudget)
	}
	budget := m.budgets[key][op.ID]
	paused := waiting && !op.IgnoreMaintenanceWindow && !op.CancelRequested && op.Step != failureCleanupStep
	if domain.IsConvergenceOperation(op.Kind) {
		paused = paused && op.TargetGeneration == c.Generation &&
			(c.MaintenanceOverrideGeneration == 0 || c.MaintenanceOverrideGeneration != c.Generation)
	} else {
		paused = paused && maintenance.Gated(op)
	}
	if paused || budget.paused || budget.observed.IsZero() {
		budget.observed = time.Now()
	}
	budget.paused = paused
	m.budgets[key][op.ID] = budget
}

// Provider health snapshots must not discard the disruption gate's wait receipt.
func preserveMaintenance(c *domain.Cluster, status *domain.ClusterStatus) {
	for _, condition := range status.Conditions {
		if condition.Type == "MaintenanceWindow" {
			return
		}
	}
	for _, condition := range c.Status.Conditions {
		if condition.Type == "MaintenanceWindow" {
			status.Conditions = append(status.Conditions, condition)
			return
		}
	}
}
