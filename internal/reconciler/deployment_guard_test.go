// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package reconciler

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func TestDeploymentExhaustionIsScheduledNotFailureBackoff(t *testing.T) {
	exhausted := &infra.DeploymentExhaustedError{Service: "member", Version: "version", Attempts: 3, LastError: "build failed"}
	if scheduledPlatformWait(exhausted) == nil || scheduledPlatformWait(infra.ErrDeploymentRetriesExhausted) == nil {
		t.Fatal("terminal deployment entered transient failure backoff")
	}
	if scheduledPlatformWait(errors.Join(exhausted, errors.New("observation failed"))) != nil {
		t.Fatal("terminal fence hid observation error")
	}
}

func TestDeploymentExhaustionFailsTrackingWithoutRetryBudget(t *testing.T) {
	m, _ := newTestManager(t, lifecycleProvider())
	op := &domain.Operation{Kind: domain.OperationCreate, Phase: domain.PhaseRunning, Failures: 2, Elapsed: time.Minute}
	cause := &infra.DeploymentExhaustedError{Service: "member", Version: "version", Attempts: 3, LastError: "build failed"}
	if !m.operationFailure(op, cause) {
		t.Fatal("tracking operation remained active")
	}
	if op.Phase != domain.PhaseFailed || op.Failures != 2 || op.Elapsed != time.Minute || op.CompletedAt == nil || op.BlockingObservedAt == nil || op.BlockingMessage != cause.Error() || !strings.Contains(op.Message, cause.Error()) || !strings.HasPrefix(op.Message, "last observed at ") {
		t.Fatal(op)
	}
}

func TestEndpointRepairExhaustionFailsTrackingWithoutRetryBudget(t *testing.T) {
	m, _ := newTestManager(t, lifecycleProvider())
	op := &domain.Operation{Kind: domain.OperationUpdate, Phase: domain.PhaseRunning}
	cause := &infra.EndpointRepairExhaustedError{Service: "proxy", Attempts: 3}
	if !m.operationFailure(op, cause) || op.Phase != domain.PhaseFailed || op.CompletedAt == nil || op.Failures != 0 {
		t.Fatalf("endpoint exhaustion left tracking operation running: %+v", op)
	}
	if !strings.Contains(op.Message, "repair the Swarm private DNS alias/VIP") || !strings.Contains(op.Message, "automatic repair is stopped") {
		t.Fatalf("endpoint exhaustion omitted operator action: %s", op.Message)
	}
}

func TestEndpointRepairExhaustionPreservesNewerFailure(t *testing.T) {
	historical := domain.Condition{Type: "PublishedEndpoint", Status: "False", Reason: "PublishedEndpointRepairExhausted", Message: "previous endpoint repair exhausted", LastTransitionTime: time.Now().Add(-time.Hour)}
	for _, tc := range []struct {
		reason string
		phase  domain.Phase
	}{
		{"PublicPortInUse", domain.PhaseDegraded},
		{"HealthObservationFailed", domain.PhaseDegraded},
		{"HealthObservationFailed", domain.PhaseReady},
	} {
		t.Run(string(tc.phase)+"/"+tc.reason, func(t *testing.T) {
			current := domain.Condition{Type: "Ready", Status: "False", Reason: tc.reason, Message: "new failure", LastTransitionTime: time.Now()}
			status := domain.ClusterStatus{Phase: tc.phase, Message: current.Message, Conditions: []domain.Condition{current}}
			preservePublishedEndpointFailure([]domain.Condition{historical}, &status)
			if status.Phase != tc.phase || status.Message != current.Message || len(status.Conditions) != 2 {
				t.Fatalf("historical endpoint failure overwrote current failure: %+v", status)
			}
			ready, endpoint := status.Conditions[0], status.Conditions[1]
			if ready.Status != current.Status || ready.Reason != current.Reason || ready.Message != current.Message || !ready.LastTransitionTime.Equal(current.LastTransitionTime) {
				t.Fatalf("current readiness failure was overwritten: %+v", ready)
			}
			if endpoint.Type != historical.Type || endpoint.Reason != historical.Reason || endpoint.Message != historical.Message || !endpoint.LastTransitionTime.Equal(historical.LastTransitionTime) {
				t.Fatalf("historical endpoint condition was lost: %+v", endpoint)
			}
		})
	}
}

func TestDeploymentWaitPreservesOuterRetryAfter(t *testing.T) {
	at := time.Now().Add(time.Hour)
	err := &infra.RetryAfterError{Cause: &engine.PlatformRetryWait{RetryAt: at.Add(-time.Minute), Message: "waiting for previous platform deployment to finish"}, At: at}
	wait := scheduledPlatformWait(err)
	if wait == nil || !wait.RetryAt.Equal(at) {
		t.Fatalf("platform constraint dropped: %+v", wait)
	}
}

func TestJoinedDeploymentWaitUsesLatestBackendDeadline(t *testing.T) {
	at := time.Now().Add(time.Hour)
	backendWait := &infra.DeploymentWaitError{Reason: "DeploymentSlot", Key: "service/process/RUNNING", Detail: "occupied", RetryAt: at}
	converted := &engine.PlatformRetryWait{RetryAt: at.Add(-time.Minute), Message: backendWait.Error(), Cause: backendWait}
	err := errors.Join(converted, &infra.DeploymentExhaustedError{Service: "sibling"}, &infra.RetryAfterError{Cause: &infra.DeploymentWaitError{Reason: "PreviousDeployment"}, At: at.Add(time.Hour)})
	wait := scheduledPlatformWait(err)
	if wait == nil || !wait.RetryAt.Equal(at.Add(time.Hour)) || !infra.RetryAt(err).Equal(at.Add(time.Hour)) {
		t.Fatalf("most restrictive deadline lost: wait=%+v backend=%v", wait, infra.RetryAt(err))
	}
}

func TestTypedDeploymentWaitDoesNotConsumeOperationBudget(t *testing.T) {
	m, _ := newTestManager(t, lifecycleProvider())
	op := &domain.Operation{Kind: domain.OperationCreate, Phase: domain.PhaseRunning, Failures: 2}
	at := time.Now().Add(time.Minute)
	cause := &infra.DeploymentWaitError{Reason: "DeploymentSlot", Key: "application/queued", Detail: "build queue occupied", RetryAt: at}
	m.operationFailure(op, cause)
	if op.Failures != 2 || scheduledPlatformWait(cause) == nil {
		t.Fatalf("platform backpressure consumed budget: %+v", op)
	}
}
