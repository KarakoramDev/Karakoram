// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package elastic

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

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
	p, rc, _, _ := coreSetup(t, 3)
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
	p, rc, _, _ := coreSetup(t, 1)
	target := &pending{Action: "deploy", Member: "demon0"}
	s := &state{
		Pending:         target,
		Retry:           target,
		PlatformRetries: map[string]*platformRetry{retryKey(target): {}},
	}
	wait := &engine.PlatformRetryWait{Message: "deployment admission is occupied"}
	err := p.failPendingCause(t.Context(), rc, s, target, errors.Join(errors.New("deploy"), wait))
	var got *engine.PlatformRetryWait
	if !errors.As(err, &got) || got != wait {
		t.Fatalf("admission wait lost: %v", err)
	}
	if s.Pending != nil || s.Retry != nil || len(s.PlatformRetries) != 0 {
		t.Fatalf("admission wait scheduled failure: pending=%+v retry=%+v failures=%+v", s.Pending, s.Retry, s.PlatformRetries)
	}
}

func TestDeploymentGuardUpgradeExhaustionPreservesRollingOwner(t *testing.T) {
	p, rc, base, _ := coreSetup(t, 3)
	s := coreConverge(t, p, rc, base)
	target := s.Members[0].Name
	backend := &deploymentGuardBackend{
		Backend:   rc.Backend,
		target:    target,
		pendingID: "version:upgrade-guard",
	}
	rc.Backend = backend
	s.Rolling = &rolling{
		Reason:  "upgrade",
		Version: rc.Cluster.Spec.Version,
		Current: target,
		Order:   []string{target, s.Members[1].Name},
		Step:    "rejoin",
	}
	v := &pending{Action: "upgrade", Member: target, Version: rc.Cluster.Spec.Version, Op: infra.Op{ID: backend.pendingID, Kind: "deploy"}}
	s.Pending, s.Retry = v, v
	s.PlatformRetries = map[string]*platformRetry{retryKey(v): {}}
	_, err := p.observePending(t.Context(), rc, s)
	if !errors.Is(err, infra.ErrDeploymentRetriesExhausted) {
		t.Fatalf("upgrade lost terminal exhaustion: %v", err)
	}
	if s.Rolling == nil || s.Rolling.Current != target || s.Rolling.Step != "rejoin" {
		t.Fatalf("exhausted upgrade lost its disruption owner/rejoin checkpoint: %+v", s.Rolling)
	}
	if s.Pending != nil || s.Retry == nil || len(s.PlatformRetries) != 0 {
		t.Fatalf("exhausted upgrade lost recovery action: pending=%+v retry=%+v failures=%+v", s.Pending, s.Retry, s.PlatformRetries)
	}
	failure, ok := rc.Cluster.Status.DeploymentFailures[target]
	if !ok || failure.Version != "guard-version" || failure.Attempts != 3 {
		t.Fatalf("exhausted upgrade lost durable terminal evidence: %+v", rc.Cluster.Status.DeploymentFailures)
	}
}

func TestRollingRecoveryRetainsActionAcrossWaitAndExhaustion(t *testing.T) {
	p, rc, base, es := coreSetup(t, 3)
	s := coreConverge(t, p, rc, base)
	target := s.Members[0]
	delete(base.nodes, target.Name)
	es.voters = []string{s.Members[0].Name, s.Members[1].Name, s.Members[2].Name}
	s.Rolling = &rolling{Reason: "upgrade", Version: rc.Cluster.Spec.Version, Current: target.Name, Order: []string{target.Name}, Step: "rejoin"}
	v := &pending{Action: "upgrade", Member: target.Name, Version: rc.Cluster.Spec.Version}
	at := now(rc).Add(time.Hour)
	wait := &engine.PlatformRetryWait{RetryAt: at, Message: "backend retry wait"}
	if err := p.failPendingCause(t.Context(), rc, s, v, wait); err != wait {
		t.Fatal(err)
	}
	if s.Retry == nil || s.PlatformRetries[retryKey(v)] == nil || s.PlatformRetries[retryKey(v)].Failures != 0 {
		t.Fatalf("wait erased recovery or consumed failure budget: %+v", s)
	}
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	s, _ = load(rc.Cluster)
	before := base.mutations
	_, _, err := p.retryPending(t.Context(), rc, s)
	var scheduled *engine.PlatformRetryWait
	if !errors.As(err, &scheduled) || !scheduled.RetryAt.Equal(at) || base.mutations != before {
		t.Fatalf("backend deadline not honored: %v", err)
	}
	backend := &deploymentGuardBackend{Backend: base, target: target.Name}
	rc.Backend = backend
	s.PlatformRetries[retryKey(v)].RetryAfter = now(rc).Add(-time.Second)
	_, _, err = p.retryPending(t.Context(), rc, s)
	if !errors.Is(err, infra.ErrDeploymentRetriesExhausted) || s.Retry == nil || len(backend.epochs) != 1 {
		t.Fatalf("absent target recovery lost at exhaustion: %v %+v", err, s.Retry)
	}
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	s, _ = load(rc.Cluster)
	_, _, err = p.retryPending(t.Context(), rc, s)
	if err != nil || len(backend.epochs) != 1 || s.Retry == nil {
		t.Fatalf("exhausted recovery replayed: %v %v", err, backend.epochs)
	}
	rc.Cluster.Spec.RetryEpoch++
	_, _, err = p.retryPending(t.Context(), rc, s)
	if !errors.Is(err, infra.ErrDeploymentRetriesExhausted) || len(backend.epochs) != 2 || backend.epochs[1] != 1 || s.Rolling.Current != target.Name || s.Rolling.Step != "rejoin" {
		t.Fatalf("operator rearm lost same-owner recovery: %v %v %+v", err, backend.epochs, s.Rolling)
	}
}
