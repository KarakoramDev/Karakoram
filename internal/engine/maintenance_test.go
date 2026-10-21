// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package engine

import (
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestMaintenanceDisruptionGate(t *testing.T) {
	now := time.Date(2026, 1, 1, 1, 59, 0, 0, time.UTC)
	for _, tt := range []struct {
		name                 string
		generation, override int64
		operation            *domain.Operation
		open                 bool
	}{
		{name: "closed", generation: 2}, {name: "generation override", generation: 2, override: 2, open: true}, {name: "old override", generation: 3, override: 2}, {name: "operation override", generation: 3, operation: &domain.Operation{IgnoreMaintenanceWindow: true}, open: true}, {name: "desired override cannot override operation", generation: 2, override: 2, operation: &domain.Operation{}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			rc := &Context{Clock: observationClock{at: now}, Operation: tt.operation, Cluster: &domain.Cluster{Generation: tt.generation, MaintenanceOverrideGeneration: tt.override, Spec: domain.ClusterSpec{Maintenance: &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: "0 2 * * *", Duration: "1h"}}}}}}
			allowed, next := rc.DisruptionAllowed()
			if allowed != tt.open {
				t.Fatalf("allowed=%v", allowed)
			}
			result := rc.WaitForWindow()
			condition := rc.Cluster.Status.Conditions[0]
			if tt.open {
				if result.RequeueAfter != 0 || condition.Status != "True" || condition.Reason != "Open" {
					t.Fatal(result, condition)
				}
			} else {
				if !next.Equal(now.Add(time.Minute)) || result.RequeueAfter != time.Minute || condition.Status != "False" || condition.Reason != "WaitingForWindow" {
					t.Fatal(next, result, condition)
				}
			}
			rc.Clock = observationClock{at: now.Add(time.Minute)}
			if result = rc.WaitForWindow(); result.RequeueAfter != 0 || len(rc.Cluster.Status.Conditions) != 1 || rc.Cluster.Status.Conditions[0].Status != "True" {
				t.Fatal("window did not reopen", result, rc.Cluster.Status.Conditions)
			}
		})
	}
}

func TestMaintenanceWaitReceiptBelongsToOperationInvocation(t *testing.T) {
	rc := &Context{
		Clock: observationClock{at: time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC)},
		Cluster: &domain.Cluster{Spec: domain.ClusterSpec{Maintenance: &domain.MaintenancePolicy{
			Windows: []domain.MaintenanceWindow{{Schedule: "0 2 * * *", Duration: "1h"}},
		}}},
	}
	op := &domain.Operation{ID: "rollout", Kind: domain.OperationUpgradeAgents}
	rc.WaitForWindow()
	if rc.WaitingForMaintenance(op) {
		t.Fatal("desired-state wait became an imperative receipt")
	}
	rc.Operation = op
	rc.WaitForWindow()
	copy := *op
	if !rc.WaitingForMaintenance(&copy) {
		t.Fatal("nested provider checkpoint lost operation wait receipt")
	}
	if rc.WaitingForMaintenance(&domain.Operation{ID: "backup"}) {
		t.Fatal("wait receipt leaked to another operation")
	}
	fresh := &Context{Cluster: rc.Cluster}
	if fresh.WaitingForMaintenance(op) {
		t.Fatal("persisted cluster condition became an invocation receipt")
	}
	op.IgnoreMaintenanceWindow = true
	rc.WaitForWindow()
	if rc.WaitingForMaintenance(op) {
		t.Fatal("override retained a wait receipt")
	}
}
