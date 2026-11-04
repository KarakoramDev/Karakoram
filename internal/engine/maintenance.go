// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package engine

import (
	"fmt"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/maintenance"
)

// DisruptionAllowed checks permission to start a NEW member disruption. Providers
// call it before crossing the stop/drain boundary, never to pause a member already
// beyond that boundary. Healing, scale-out, leaf certificates with at most seven
// days remaining, and agent transport certificate renewal are exempt: their paths
// must not call this gate. A desired-state override applies only to its generation;
// an imperative override belongs only to the current operation.
func (rc *Context) DisruptionAllowed() (allowed bool, nextStart time.Time) {
	c := rc.Cluster
	if rc.Operation != nil {
		if rc.Operation.IgnoreMaintenanceWindow {
			return true, time.Time{}
		}
	} else if c.MaintenanceOverrideGeneration != 0 && c.MaintenanceOverrideGeneration == c.Generation {
		return true, time.Time{}
	}
	open, _, next, err := maintenance.Open(c.Spec.Maintenance, deploymentNow(rc))
	if err != nil {
		return false, time.Time{}
	}
	return open, next
}

// WaitingForMaintenance reports a wait observed for this operation during
// the current invocation, never a persisted condition left by desired-state work.
func (rc *Context) WaitingForMaintenance(op *domain.Operation) bool {
	return op != nil && op.ID != "" && rc.maintenanceWaitOperation == op.ID
}

// WaitForWindow publishes MaintenanceWindow and returns a bounded requeue delay.
// Call before a new disruption; a zero RequeueAfter means work may proceed.
// Operation providers can return OpResult{RequeueAfter: result.RequeueAfter}.
func (rc *Context) WaitForWindow() Result {
	allowed, next := rc.DisruptionAllowed()
	rc.maintenanceWaitOperation = ""
	now := deploymentNow(rc)
	condition := domain.Condition{Type: "MaintenanceWindow", Status: "True", Reason: "Open", Message: "Maintenance window permits new disruptions", LastTransitionTime: now}
	result := Result{}
	if !allowed {
		if rc.Operation != nil {
			rc.maintenanceWaitOperation = rc.Operation.ID
		}
		condition.Status, condition.Reason = "False", "WaitingForWindow"
		condition.Message = "Waiting for maintenance window; no next start is scheduled"
		if !next.IsZero() {
			condition.Message = fmt.Sprintf("Waiting for maintenance window; next start %s", next.UTC().Format(time.RFC3339))
		}
		result.RequeueAfter = maintenance.Delay(now, next)
	}
	for i, old := range rc.Cluster.Status.Conditions {
		if old.Type == condition.Type {
			if old.Status == condition.Status && old.Reason == condition.Reason {
				condition.LastTransitionTime = old.LastTransitionTime
			}
			rc.Cluster.Status.Conditions[i] = condition
			return result
		}
	}
	rc.Cluster.Status.Conditions = append(rc.Cluster.Status.Conditions, condition)
	return result
}
