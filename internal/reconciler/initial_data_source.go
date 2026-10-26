// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package reconciler

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/redact"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

// initialDataSourceFailure prevents ordinary convergence from retrying an
// explicitly aborted create/import as a fresh, empty healthy database. Running
// imperative work and its compensation are resumed before synchronize calls us.
func (m *Manager) initialDataSourceFailure(ctx context.Context, p engine.Provider, c *domain.Cluster) error {
	if c.Spec.DataSource == nil {
		return nil
	}
	if err := m.observeInitialDataSourceLifecycle(ctx, p, c, nil); err != nil {
		return err
	}
	for _, condition := range c.Status.Conditions {
		if condition.Type == "InitialDataSource" && condition.Reason == "RestoreAborted" && condition.Status == "False" {
			return engine.Invalid("%s", condition.Message)
		}
	}
	if engine.InitialDataSourceUnproven(c) {
		return nil
	}
	if observer, ok := p.(engine.InitialDataSourceObserver); ok {
		applied, err := observer.InitialDataSourceApplied(c)
		if err != nil {
			return err
		}
		if applied {
			return nil
		}
	}
	restore, restoreErr := m.opts.Store.GetRestore(ctx, c.Ref(), "initial-restore")
	if restoreErr != nil && !errors.Is(restoreErr, store.ErrNotFound) {
		return restoreErr
	}
	if restoreErr == nil && (restore.Phase == domain.PhaseFailed || restore.Phase == domain.PhaseCancelled) {
		return engine.Invalid("Initial data source restore operation %s ended %s: %s; the target cannot become Ready without completed recovery", restore.OperationID, restore.Phase, restore.Error)
	}
	operations, err := m.opts.Store.ListOperations(ctx, c.Ref())
	if err != nil {
		return err
	}
	for _, op := range operations {
		if !initialOperationMatchesCluster(c, &op) {
			continue
		}
		if op.Kind == domain.OperationCreate && (op.Phase == domain.PhaseFailed || op.Phase == domain.PhaseCancelled) {
			if state := c.Status.InitialDataSource; state != nil && state.TrackingFailure == op.ID {
				continue
			}
			// Older controllers retired only the convergence tracker on PATCH.
			// Its still-live native restore retains the requested data obligation;
			// explicit cancellation/failure is never excused by this compatibility.
			if op.Phase == domain.PhaseCancelled && strings.HasPrefix(op.Message, "Superseded by ") && restoreErr == nil &&
				(restore.Phase == domain.PhasePending || restore.Phase == domain.PhaseRunning) {
				continue
			}
			if op.Phase == domain.PhaseCancelled && strings.HasPrefix(op.Message, "Superseded by ") && errors.Is(restoreErr, store.ErrNotFound) {
				// No native receipt exists, but the database may have been used
				// since the old PATCH. Preserve runtime management and exposure;
				// never replay initial recovery against that unknown data.
				message := fmt.Sprintf("Initial restore completion for historical create operation %s cannot be proven; runtime management and data-plane exposure remain active, but Ready is withheld and initial recovery will not be replayed. To recover the requested data, delete this target and create a new target from the preserved source.", op.ID)
				c.Status.Phase, c.Status.Message = domain.PhaseDegraded, message
				c.Status.Conditions = append(c.Status.Conditions, domain.Condition{Type: "InitialDataSource", Status: "False", Reason: "LegacyRestoreUnproven", Message: message})
				continue
			}
			return engine.Invalid("Initial data source restore is incomplete: create operation %s ended %s: %s; delete this target and create a new target from the preserved source", op.ID, op.Phase, op.Message)
		}
	}
	return nil
}

func markInitialDataSourceAborted(c *domain.Cluster, err error) {
	state := initialLifecycle(c)
	for _, condition := range c.Status.Conditions {
		if condition.Type == "InitialDataSource" && condition.Reason == "RestoreAborted" && condition.Status == "False" {
			state.AbortMessage = redact.Diagnostic(condition.Message)
			c.Status.Phase, c.Status.Message = domain.PhaseFailed, state.AbortMessage
			return
		}
	}
	message := state.AbortMessage
	if message == "" {
		message = redact.Diagnostic(fmt.Sprintf("Initial data source restore aborted: %v", err))
		state.AbortMessage = message
	}
	c.Status.Phase, c.Status.Message = domain.PhaseFailed, message
	condition := domain.Condition{Type: "InitialDataSource", Status: "False", Reason: "RestoreAborted", Message: message}
	for i := range c.Status.Conditions {
		if c.Status.Conditions[i].Type == condition.Type {
			c.Status.Conditions[i] = condition
			return
		}
	}
	c.Status.Conditions = append(c.Status.Conditions, condition)
}
