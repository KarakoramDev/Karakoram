// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package psmdb

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

// Wrap the package's existing backend so unrelated deployment and observation
// behavior remains real fixture behavior rather than a guard-specific mock.
type deploymentGuardBackend struct {
	infra.Backend
	target    string
	pendingID string
	epochs    []int64
	siblings  map[string]bool
	mu        sync.Mutex
	observed  map[string]bool
}

func (b *deploymentGuardBackend) exhausted() error {
	return &infra.DeploymentExhaustedError{Service: b.target, Version: "guard-version", Attempts: 3, LastError: "platform deployment failed"}
}

func (b *deploymentGuardBackend) Deploy(ctx context.Context, name string, d infra.Deployment) (infra.Op, error) {
	if b.target == "" {
		b.target = name
	}
	if name == b.target {
		b.epochs = append(b.epochs, d.RetryEpoch)
		return infra.Op{}, b.exhausted()
	}
	b.siblings[name] = true
	return b.Backend.Deploy(ctx, name, d)
}

func (b *deploymentGuardBackend) OpStatus(ctx context.Context, op infra.Op) (infra.OpState, error) {
	if b.pendingID != "" && op.ID == b.pendingID {
		return infra.OpState{}, b.exhausted()
	}
	return b.Backend.OpStatus(ctx, op)
}

func (b *deploymentGuardBackend) Containers(ctx context.Context, name string) ([]infra.Container, error) {
	b.mu.Lock()
	b.observed[name] = true
	b.mu.Unlock()
	return b.Backend.Containers(ctx, name)
}

type deploymentGuardEvents struct{ events []domain.Event }

func (e *deploymentGuardEvents) Record(_ context.Context, event domain.Event) error {
	e.events = append(e.events, event)
	return nil
}

func TestDeploymentGuardExhaustionAndEpochRearm(t *testing.T) {
	p, rc, _, _ := setup(t)
	rc.Cluster.Spec.Components.Engine.Replicas = 3
	events := &deploymentGuardEvents{}
	rc.Events = events
	backend := &deploymentGuardBackend{Backend: rc.Backend, siblings: map[string]bool{}, observed: map[string]bool{}}
	rc.Backend = backend
	reconcile := func() {
		t.Helper()
		if _, err := p.Sync(t.Context(), rc); err != nil && !errors.Is(err, infra.ErrDeploymentRetriesExhausted) {
			t.Fatalf("sync: %v", err)
		}
		status, err := p.Status(t.Context(), rc)
		if err != nil {
			t.Fatal(err)
		}
		rc.Cluster.Status = status
	}
	for range 60 {
		reconcile()
	}
	if backend.target == "" || len(backend.epochs) != 1 {
		t.Fatalf("exhausted target replayed: target=%q epochs=%v", backend.target, backend.epochs)
	}
	if backend.epochs[0] != 0 {
		t.Fatalf("initial epoch=%d", backend.epochs[0])
	}
	failure, ok := rc.Cluster.Status.DeploymentFailures[backend.target]
	if !ok || len(rc.Cluster.Status.DeploymentFailures) != 1 || failure.Attempts != 3 || failure.Version != "guard-version" || failure.RetryEpoch != 0 {
		t.Fatalf("missing exact terminal record: %+v", rc.Cluster.Status.DeploymentFailures)
	}
	conditions := 0
	for _, condition := range rc.Cluster.Status.Conditions {
		if condition.Reason != "DeploymentRetriesExhausted" {
			continue
		}
		conditions++
		if condition.Status != "False" || !strings.Contains(condition.Message, backend.target) || !strings.Contains(condition.Message, "karactl clusters retry-deployments "+rc.Cluster.Name) || !strings.Contains(condition.Message, "--namespace") {
			t.Errorf("non-actionable terminal condition: %+v", condition)
		}
	}
	if conditions != 1 {
		t.Fatalf("want one exhausted condition, got %d: %+v", conditions, rc.Cluster.Status.Conditions)
	}
	warnings := 0
	for _, event := range events.events {
		if event.Reason == "DeploymentRetriesExhausted" {
			warnings++
		}
	}
	if warnings != 1 {
		t.Fatalf("terminal evidence recorded %d times", warnings)
	}
	if len(backend.siblings) == 0 {
		t.Fatal("exhausted member blocked sibling deployment")
	}
	backend.mu.Lock()
	observedSibling := false
	for name := range backend.siblings {
		if backend.observed[name] {
			observedSibling = true
		}
	}
	backend.mu.Unlock()
	if !observedSibling {
		t.Fatal("exhausted member blocked sibling observation")
	}
	rc.Cluster.Spec.RetryEpoch = 7
	for range 60 {
		reconcile()
		if len(backend.epochs) > 1 {
			break
		}
	}
	if len(backend.epochs) != 2 || backend.epochs[1] != 7 {
		t.Fatalf("operator rearm did not submit new epoch: %v", backend.epochs)
	}
	for range 5 {
		reconcile()
	}
	if len(backend.epochs) != 2 {
		t.Fatalf("new exhausted epoch replayed: %v", backend.epochs)
	}
	if failure := rc.Cluster.Status.DeploymentFailures[backend.target]; failure.RetryEpoch != 7 {
		t.Fatalf("rearmed failure not persisted: %+v", failure)
	}
}

func TestDeploymentGuardAdmissionWaitDoesNotScheduleFailure(t *testing.T) {
	_, rc, _, _ := setup(t)
	target := &pending{Action: "deploy", Member: "demom0"}
	s := &internalState{
		Pending:         target,
		Retry:           target,
		PlatformRetries: map[string]*platformRetry{platformRetryKey(target): {}},
	}
	wait := &engine.PlatformRetryWait{Message: "deployment admission is occupied"}
	err := s.failPendingCause(rc, errors.Join(errors.New("deploy"), wait))
	var got *engine.PlatformRetryWait
	if !errors.As(err, &got) || got != wait {
		t.Fatalf("admission wait lost: %v", err)
	}
	if s.Pending != nil || s.Retry != nil || len(s.PlatformRetries) != 0 {
		t.Fatalf("admission wait scheduled failure: pending=%+v retry=%+v failures=%+v", s.Pending, s.Retry, s.PlatformRetries)
	}
}
