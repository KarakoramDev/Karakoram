// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"context"
	"errors"

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
	if c.Spec.Maintenance == nil || len(c.Spec.Maintenance.Windows) == 0 {
		return
	}
	now := engine.RealClock{}.Now()
	if s.Context.Clock != nil {
		now = s.Context.Clock.Now()
	}
	open, end, next, err := maintenance.Open(c.Spec.Maintenance, now)
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
}
