// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package reconciler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

func initialOperationMatchesCluster(c *domain.Cluster, op *domain.Operation) bool {
	if op.ClusterIncarnation == "" {
		return true
	}
	created, err := strconv.ParseInt(op.ClusterIncarnation, 10, 64)
	return err == nil && created == c.CreatedAt.UnixNano()
}

func initialLifecycle(c *domain.Cluster) *domain.InitialDataSourceLifecycle {
	if c.Status.InitialDataSource == nil {
		c.Status.InitialDataSource = &domain.InitialDataSourceLifecycle{}
	}
	return c.Status.InitialDataSource
}

func (m *Manager) initialCreateFailureStep(op *domain.Operation, cause error) string {
	var field *domain.FieldError
	permanent := errors.As(cause, &field) || errors.Is(cause, engine.ErrInvalid) || errors.Is(cause, engine.ErrNotSupported) || errors.Is(cause, infra.ErrDeploymentRetriesExhausted)
	if !permanent {
		if op.Elapsed >= m.operationTimeout(op) {
			return domain.InitialRestoreTrackerDeadline
		}
		if op.Failures >= m.opts.OperationRetryLimit {
			return domain.InitialRestoreTrackerRetryBudget
		}
	}
	return domain.InitialRestoreCreateFailure
}

func (m *Manager) initialTrackerBudgetFailure(op *domain.Operation) bool {
	if op.Step == domain.InitialRestoreTrackerDeadline || op.Step == domain.InitialRestoreTrackerRetryBudget {
		return true
	}
	// Old controllers recorded only the budget message and counters. Do not
	// reinterpret a structured genuine failure from this controller as budget-only.
	return op.Step != domain.InitialRestoreCreateFailure &&
		(strings.HasPrefix(op.Message, "create exceeded its ") && op.Elapsed >= m.operationTimeout(op) ||
			strings.HasPrefix(op.Message, "create exhausted its retry budget (") && op.Failures >= m.opts.OperationRetryLimit)
}

func nativeInitialAbort(native *domain.Operation, r *domain.Restore) string {
	if r != nil && (r.Phase == domain.PhaseFailed || r.Phase == domain.PhaseCancelled) {
		return "native restore inventory recorded failure: " + r.Error
	}
	if native == nil {
		return ""
	}
	if native.CancelRequested || native.Phase == domain.PhaseCancelled {
		return "native cancellation was requested"
	}
	if native.Phase == domain.PhaseFailed {
		return "native restore failed: " + native.Message
	}
	if native.Step == failureCleanupStep || native.FailureCleanupStartedAt != nil {
		return "native failure compensation was entered"
	}
	var cp struct {
		Failure      string `json:"failure"`
		Cancelling   bool   `json:"cancelling"`
		Uncertain    bool   `json:"restoreConsistencyUncertain"`
		FenceUnknown bool   `json:"restoreFenceUnknown"`
	}
	if len(native.Checkpoint) > 0 {
		if err := json.Unmarshal(native.Checkpoint, &cp); err != nil {
			// Missing/unreadable progress is not positive abort evidence.
			// The native owner handles its decode error and bounded recovery.
			return ""
		}
	}
	if cp.Failure != "" {
		return "native restore recorded failure: " + cp.Failure
	}
	if cp.Cancelling || cp.Uncertain || cp.FenceUnknown {
		return "native cancellation/partial recovery or uncertain compensation was recorded"
	}
	return ""
}

// observeInitialDataSourceLifecycle is observation only. Its abort result must
// never prevent an already-running native operation from draining compensation.
// current supplies the just-returned checkpoint before its atomic store commit.
func (m *Manager) observeInitialDataSourceLifecycle(ctx context.Context, p engine.Provider, c *domain.Cluster, current *domain.Operation) error {
	if c.Spec.DataSource == nil {
		return nil
	}
	if state := c.Status.InitialDataSource; state != nil && state.AbortMessage != "" {
		return engine.Invalid("%s", state.AbortMessage)
	}
	for _, condition := range c.Status.Conditions {
		if condition.Type == "InitialDataSource" && condition.Reason == "RestoreAborted" && condition.Status == "False" {
			markInitialDataSourceAborted(c, errors.New(condition.Message))
			return engine.Invalid("%s", condition.Message)
		}
	}
	r, readErr := m.opts.Store.GetRestore(ctx, c.Ref(), "initial-restore")
	if readErr != nil && !errors.Is(readErr, store.ErrNotFound) {
		return readErr
	}
	var native *domain.Operation
	if readErr == nil && r.OperationID != "" {
		if current != nil && current.ID == r.OperationID {
			native = current
		} else {
			var err error
			native, err = m.opts.Store.GetOperation(ctx, r.OperationID)
			if err != nil && !errors.Is(err, store.ErrNotFound) {
				return err
			}
		}
	}
	if native != nil && ((native.Kind != domain.OperationRestore && native.Kind != "initial-restore") ||
		native.Cluster != c.Name || domain.NormalizedNamespace(native.Namespace) != c.Ref().Namespace || !initialOperationMatchesCluster(c, native)) {
		native = nil
	}
	if native != nil {
		initialLifecycle(c).NativeOperation = native.ID
	}
	if reason := nativeInitialAbort(native, r); reason != "" {
		owner := "initial restore"
		if native != nil {
			owner = native.ID
		} else if r != nil && r.OperationID != "" {
			owner = r.OperationID
		}
		markInitialDataSourceAborted(c, fmt.Errorf("initial restore operation %s: %s", owner, reason))
		return engine.Invalid("%s", c.Status.InitialDataSource.AbortMessage)
	}
	claim, claimErr := m.opts.Store.GetInitialDataSourceClaim(ctx, c.Ref())
	if claimErr != nil && !errors.Is(claimErr, store.ErrNotFound) {
		return claimErr
	}
	if claim != nil {
		created, parseErr := strconv.ParseInt(claim.ClusterIncarnation, 10, 64)
		if parseErr != nil || created != c.CreatedAt.UnixNano() || claim.ClaimedAt.IsZero() {
			claim = nil
		} else {
			state := initialLifecycle(c)
			state.ClaimOperation, state.ClaimedAt = claim.Operation, &claim.ClaimedAt
		}
	}
	if native == nil && engine.InitialDataSourceUnproven(c) && !c.Deleting && !c.Status.CleanupStarted && c.Status.Phase != domain.PhaseSuspended {
		return nil
	}
	applied := false
	var observationErr error
	if observer, ok := p.(engine.InitialDataSourceObserver); ok {
		applied, observationErr = observer.InitialDataSourceApplied(c)
	}
	if c.Deleting || c.Status.CleanupStarted || c.Status.Phase == domain.PhaseDeleting || c.Status.Phase == domain.PhaseSuspended {
		if !applied {
			markInitialDataSourceAborted(c, errors.New("target teardown began before initial restore completion"))
			return engine.Invalid("%s", c.Status.InitialDataSource.AbortMessage)
		}
	}
	operations, err := m.opts.Store.ListOperations(ctx, c.Ref())
	if err != nil {
		return err
	}
	for i := range operations {
		create := &operations[i]
		if create.Kind != domain.OperationCreate || !initialOperationMatchesCluster(c, create) || (create.Phase != domain.PhaseFailed && create.Phase != domain.PhaseCancelled) {
			continue
		}
		if create.Phase == domain.PhaseCancelled && strings.HasPrefix(create.Message, "Superseded by ") {
			continue
		}
		claimedBeforeFailure := native != nil && native.StartedAt != nil &&
			create.CompletedAt != nil && !native.StartedAt.After(*create.CompletedAt) &&
			(native.Phase == domain.PhaseRunning || native.Phase == domain.PhaseSucceeded)
		if claim != nil && claim.Operation == create.ID && create.CompletedAt != nil && !claim.ClaimedAt.After(*create.CompletedAt) {
			// PostgreSQL/Supabase publish bootstrap recovery before the later
			// bookkeeping Restore exists; Rabbit imports within Sync itself.
			claimedBeforeFailure = true
		}
		tracking := create.Phase == domain.PhaseFailed && m.initialTrackerBudgetFailure(create) && claimedBeforeFailure
		if state := c.Status.InitialDataSource; state != nil && state.TrackingFailure == create.ID {
			tracking = true
		}
		if !tracking {
			markInitialDataSourceAborted(c, fmt.Errorf("create operation %s ended %s before a continuing native restore could prove a tracking-only failure: %s; delete this target and create a new target from the preserved source", create.ID, create.Phase, create.Message))
			return engine.Invalid("%s", c.Status.InitialDataSource.AbortMessage)
		}
		state := initialLifecycle(c)
		state.TrackingFailure = create.ID
		if applied && !state.CompletionAudited {
			owner := state.NativeOperation
			if owner == "" {
				owner = "bootstrap/import claim owned by create " + state.ClaimOperation
			}
			event := domain.Event{Namespace: c.Namespace, Cluster: c.Name, Operation: create.ID, Actor: "reconciler", Type: "Normal", Reason: "InitialRestoreCompletedAfterTrackerFailure", Message: fmt.Sprintf("Initial native restore %s is durably complete although create tracker %s failed its observation budget; restored data is verified by the native completion marker and the create operation remains Failed", owner, create.ID)}
			if err := m.record(ctx, event); err != nil {
				return err
			}
			state.CompletionAudited = true
		}
	}
	// An unavailable observation is not proof of a genuine abort. Explicit
	// checkpoint/operation/teardown evidence owns that classification.
	return observationErr
}
