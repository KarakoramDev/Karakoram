// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package reconciler

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

func TestTrackingDeadlineUsesCurrentPlacementAndRestoreOutcome(t *testing.T) {
	for _, kind := range []string{domain.OperationCreate, domain.OperationUpgrade, domain.OperationUpdate} {
		for _, outcome := range []string{"pending", "Succeeded"} {
			t.Run(kind+"/"+outcome, func(t *testing.T) {
				p := lifecycleProvider()
				m, st := newTestManager(t, p)
				ctx := t.Context()
				op := &domain.Operation{Kind: kind}
				c := &domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: "fake"}}
				if kind == domain.OperationCreate {
					c.Spec.DataSource = &domain.DataSource{}
				}
				if err := st.CreateCluster(store.WithOperation(ctx, op), c); err != nil {
					t.Fatal(err)
				}
				op.Elapsed = 3 * time.Hour
				op.LastError = "nomad: original host volume is not ready"
				if err := st.UpdateOperation(ctx, op); err != nil {
					t.Fatal(err)
				}
				observed := time.Date(2026, 10, 22, 1, 23, 17, 0, time.UTC)
				c.Status = domain.ClusterStatus{Phase: domain.PhaseCreating, Message: "Proxy unavailable", ObservedAt: observed, Conditions: []domain.Condition{{Type: "Placement/proxy", Status: "False", Reason: "BackendPlacementFailed", Message: `proxy: dimension "cpu" exhausted on 3 nodes`, LastTransitionTime: observed.Add(-time.Hour)}}}
				if outcome == "Succeeded" {
					completed := observed.Add(-20 * time.Minute)
					if err := st.CreateRestore(ctx, &domain.Restore{Name: "initial-restore", Cluster: c.Name, Phase: domain.PhaseSucceeded, CompletedAt: &completed}); err != nil {
						t.Fatal(err)
					}
				}
				if err := m.trackingOperations(ctx, p, c, nil, true); err != nil {
					t.Fatal(err)
				}
				got, err := st.GetOperation(ctx, op.ID)
				if err != nil {
					t.Fatal(err)
				}
				if got.Phase != domain.PhaseFailed || !strings.Contains(got.Message, `dimension "cpu" exhausted`) || !strings.Contains(got.Message, "last observed at 2026-10-22T01:23:17Z") || strings.Contains(got.Message, "original host volume") {
					t.Fatalf("wrong terminal diagnosis: %+v", got)
				}
				if kind == domain.OperationCreate {
					if !strings.Contains(got.Message, "initial restore: "+outcome) || got.Step != domain.InitialRestoreTrackerDeadline {
						t.Fatalf("restore outcome/classification lost: %+v", got)
					}
				} else if strings.Contains(got.Message, "initial restore:") {
					t.Fatal(got.Message)
				}
			})
		}
	}
}

func TestDeadlineBlockerSurvivesRestartAndNewErrorSupersedesIt(t *testing.T) {
	m, _ := newTestManager(t, lifecycleProvider())
	op := &domain.Operation{Kind: domain.OperationUpdate}
	observed := time.Date(2026, 10, 22, 1, 0, 0, 0, time.UTC)
	m.observeOperationBlocker(op, "proxy placement unavailable", observed)
	raw, err := json.Marshal(op)
	if err != nil {
		t.Fatal(err)
	}
	var restored domain.Operation
	if err := json.Unmarshal(raw, &restored); err != nil {
		t.Fatal(err)
	}
	restored.Elapsed = 3 * time.Hour
	if !m.operationFailure(&restored, nil) || !strings.Contains(restored.Message, "last observed at 2026-10-22T01:00:00Z") {
		t.Fatal(restored)
	}
	restored.Phase = domain.PhaseRunning
	if !m.operationFailure(&restored, errors.New("new backend failure")) || !strings.Contains(restored.Message, "new backend failure") || strings.Contains(restored.Message, "proxy placement") {
		t.Fatal(restored)
	}
}

func TestDeadlineDiagnosticSanitizedBoundedAndLegacyTimeUnknown(t *testing.T) {
	m, _ := newTestManager(t, lifecycleProvider())
	op := &domain.Operation{Kind: domain.OperationCreate, Elapsed: 3 * time.Hour, LastError: "old blocker"}
	if !m.operationFailure(op, nil) || !strings.Contains(op.Message, "last observation time unknown: old blocker") {
		t.Fatal(op)
	}
	if !m.operationFailure(op, errors.New("mongodb://user:password@example.com/path?token=supersecret "+strings.Repeat("界", 1000))) {
		t.Fatal(op)
	}
	if len(op.Message) > 1024 || strings.Contains(op.Message, "password") || strings.Contains(op.Message, "supersecret") || op.BlockingObservedAt == nil {
		t.Fatal(op.Message)
	}
}
