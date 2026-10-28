// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/maintenance"
)

// ApplyMaintenanceOverride copies the explicit request override into an operation
// before enqueueing it (or attaching it to a spec update with store.WithOperation).
// It never changes provider admission or disruption safety checks.
func ApplyMaintenanceOverride(ctx context.Context, op *domain.Operation) {
	op.IgnoreMaintenanceWindow = request(ctx).URL.Query().Get("ignoreMaintenanceWindow") == "true"
}

func validateMaintenance(policy *domain.MaintenancePolicy) *response {
	if err := maintenance.Validate(policy); err != nil {
		var field *domain.FieldError
		if errors.As(err, &field) {
			r := invalidField(field.Field, field.Reason, field.Message)
			return &r
		}
		r := fail(400, "invalid_request", err.Error())
		return &r
	}
	return nil
}

func (s *Server) maintenanceStatus(c *domain.Cluster) {
	c.Status.Maintenance = nil
	// Conditions are response projections too: clusterAPI receives a shallow
	// cluster copy, so never replace entries in the persisted observation's slice.
	conditionIndex := -1
	for i, condition := range c.Status.Conditions {
		if condition.Type == "MaintenanceWindow" {
			conditionIndex = i
			break
		}
	}
	if c.Spec.Maintenance == nil || len(c.Spec.Maintenance.Windows) == 0 {
		if conditionIndex >= 0 {
			c.Status.Conditions = slices.Delete(slices.Clone(c.Status.Conditions), conditionIndex, conditionIndex+1)
		}
		return
	}
	now := engine.RealClock{}.Now()
	if s.Context.Clock != nil {
		now = s.Context.Clock.Now()
	}
	open, end, next, since, err := maintenance.Observe(c.Spec.Maintenance, now)
	if err != nil {
		return
	}
	status := &domain.MaintenanceStatus{Open: open}
	if !end.IsZero() {
		status.CurrentWindowEnd = &end
	}
	if !next.IsZero() {
		status.NextWindowStart = &next
	}
	c.Status.Maintenance = status
	condition := domain.Condition{
		Type: "MaintenanceWindow", Status: "True", Reason: "Open",
		Message: "Maintenance window permits new disruptions", LastTransitionTime: since,
	}
	if !open {
		condition.Status, condition.Reason = "False", "Closed"
		condition.Message = "Maintenance window is closed; no next start is scheduled"
		if !next.IsZero() {
			condition.Message = fmt.Sprintf("Maintenance window is closed; next start %s", next.UTC().Format(time.RFC3339))
		}
	}
	if conditionIndex >= 0 {
		old := c.Status.Conditions[conditionIndex]
		// Keep an actual provider wait diagnostic, but not a stale Open receipt.
		if !open && old.Status == "False" && old.Reason == "WaitingForWindow" {
			condition.Reason = old.Reason
			condition.Message = "Waiting for maintenance window; no next start is scheduled"
			if !next.IsZero() {
				condition.Message = fmt.Sprintf("Waiting for maintenance window; next start %s", next.UTC().Format(time.RFC3339))
			}
		}
		c.Status.Conditions = slices.Clone(c.Status.Conditions)
		c.Status.Conditions[conditionIndex] = condition
	} else {
		c.Status.Conditions = append(slices.Clone(c.Status.Conditions), condition)
	}
}
