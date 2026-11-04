// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package supabase

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine/enginetest"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

// Wrap the package's existing backend so unrelated deployment and observation
// behavior remains real fixture behavior rather than a guard-specific mock.
type deploymentGuardBackend struct {
	infra.Backend
	target            string
	pendingID         string
	epochs            []int64
	siblings          map[string]bool
	mu                sync.Mutex
	observed          map[string]bool
	resizeCalls       int
	resizePending     bool
	deployedResources []infra.Resources
}

func (b *deploymentGuardBackend) exhausted() error {
	return &infra.DeploymentExhaustedError{Service: b.target, Version: "guard-version", Attempts: 3, LastError: "platform deployment failed"}
}

func (b *deploymentGuardBackend) Deploy(ctx context.Context, name string, d infra.Deployment) (infra.Op, error) {
	if b.target == "" {
		b.target = name
	}
	if name == b.target {
		service, err := b.Backend.GetService(ctx, name)
		if err != nil {
			return infra.Op{}, err
		}
		b.deployedResources = append(b.deployedResources, service.Resources)
		b.epochs = append(b.epochs, d.RetryEpoch)
		return infra.Op{}, b.exhausted()
	}
	b.siblings[name] = true
	return b.Backend.Deploy(ctx, name, d)
}

func (b *deploymentGuardBackend) OpStatus(ctx context.Context, op infra.Op) (infra.OpState, error) {
	if op.ID == "guard-resize" {
		return infra.OpState{Done: !b.resizePending}, nil
	}
	if b.pendingID != "" && op.ID == b.pendingID {
		return infra.OpState{}, b.exhausted()
	}
	return b.Backend.OpStatus(ctx, op)
}

func (b *deploymentGuardBackend) SetResources(ctx context.Context, name string, resources infra.Resources) (infra.Op, error) {
	op, err := b.Backend.SetResources(ctx, name, resources)
	if name == b.target && err == nil {
		b.resizeCalls++
		return infra.Op{ID: "guard-resize", Kind: "resources"}, nil
	}
	return op, err
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
	p, rc, original := storageDeployFailureFixture(t, infra.OpState{Status: "PREPARING_RUNTIME"})
	events := &deploymentGuardEvents{}
	rc.Events = events
	backend := &deploymentGuardBackend{Backend: rc.Backend, siblings: map[string]bool{}, observed: map[string]bool{}, target: original.member, pendingID: "version:storage-version", epochs: []int64{0}}
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
	// Add new desired work only after Storage has become durably fenced.
	reconcile()
	meta := rc.Cluster.Spec.Components.Extra["meta"]
	meta.Replicas = 1
	rc.Cluster.Spec.Components.Extra["meta"] = meta
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
	backend.mu.Lock()
	observedSibling := false
	for name := range backend.observed {
		if name != backend.target {
			observedSibling = true
		}
	}
	backend.mu.Unlock()
	if !observedSibling {
		t.Fatal("exhausted Storage component blocked sibling observation")
	}
	if !backend.siblings[rc.Cluster.Name+codes["meta"]] {
		t.Fatal("exhausted Storage component blocked new Meta deployment")
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

func TestDeploymentGuardDisabledTargetClearsFailure(t *testing.T) {
	p, rc, original := storageDeployFailureFixture(t, infra.OpState{Status: "PREPARING_RUNTIME"})
	backend := &deploymentGuardBackend{
		Backend:   rc.Backend,
		target:    original.member,
		pendingID: "version:storage-version",
		epochs:    []int64{0},
		siblings:  map[string]bool{},
		observed:  map[string]bool{},
	}
	rc.Backend = backend
	if _, err := p.Sync(t.Context(), rc); !errors.Is(err, infra.ErrDeploymentRetriesExhausted) {
		t.Fatalf("expected terminal Storage deployment: %v", err)
	}
	if _, ok := rc.Cluster.Status.DeploymentFailures[backend.target]; !ok {
		t.Fatal("fixture did not record Storage exhaustion")
	}
	storage := rc.Cluster.Spec.Components.Extra["storage"]
	storage.Replicas = 0
	rc.Cluster.Spec.Components.Extra["storage"] = storage
	for range 60 {
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
		status, err := p.Status(t.Context(), rc)
		if err != nil {
			t.Fatal(err)
		}
		rc.Cluster.Status = status
	}
	s, err := loadState(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	if s.Members["storage"] != nil {
		t.Fatalf("disabled Storage member was not retired: %+v", s.Members["storage"])
	}
	if _, ok := rc.Cluster.Status.DeploymentFailures[backend.target]; ok {
		t.Fatalf("confirmed target deletion retained terminal failure: %+v", rc.Cluster.Status.DeploymentFailures)
	}
	for _, condition := range rc.Cluster.Status.Conditions {
		if condition.Type == "Deployment/"+backend.target {
			t.Fatalf("confirmed target deletion retained terminal condition: %+v", condition)
		}
	}
	if len(backend.epochs) != 1 {
		t.Fatalf("disabling exhausted Storage replayed deployment: %v", backend.epochs)
	}
}

func TestExhaustedComponentRearmAppliesResourcesBeforeDeployment(t *testing.T) {
	p, rc, original := storageDeployFailureFixture(t, infra.OpState{Status: "PREPARING_RUNTIME"})
	backend := &deploymentGuardBackend{
		Backend: rc.Backend, target: original.member,
		pendingID: "version:storage-version", epochs: []int64{0},
		siblings: map[string]bool{}, observed: map[string]bool{}, resizePending: true,
	}
	rc.Backend = backend
	reconcile := func() {
		t.Helper()
		if _, err := p.Sync(t.Context(), rc); err != nil && !errors.Is(err, infra.ErrDeploymentRetriesExhausted) {
			t.Fatal(err)
		}
		status, err := p.Status(t.Context(), rc)
		if err != nil {
			t.Fatal(err)
		}
		rc.Cluster.Status = status
	}
	reconcile()
	if _, ok := rc.Cluster.Status.DeploymentFailures[backend.target]; !ok {
		t.Fatal("fixture did not exhaust the initial deployment")
	}
	desired := rc.Cluster.Spec.Components.Extra["storage"]
	desired.Resources.MinRAM *= 2
	desired.Resources.MaxRAM *= 2
	rc.Cluster.Spec.Components.Extra["storage"] = desired
	for range 5 {
		reconcile()
	}
	if backend.resizeCalls != 0 || len(backend.epochs) != 1 {
		t.Fatalf("resource correction bypassed the fence: resizes=%d epochs=%v", backend.resizeCalls, backend.epochs)
	}
	rc.Cluster.Spec.RetryEpoch = 1
	for range 60 {
		reconcile()
		if backend.resizeCalls > 0 {
			break
		}
	}
	if backend.resizeCalls != 1 || len(backend.epochs) != 1 {
		t.Fatalf("rearm did not resize first: resizes=%d epochs=%v", backend.resizeCalls, backend.epochs)
	}
	for range 5 {
		reconcile()
	}
	if len(backend.epochs) != 1 || backend.resizeCalls != 1 {
		t.Fatal("deployment submitted or resize replayed before resource completion")
	}
	backend.resizePending = false
	for range 60 {
		reconcile()
		if len(backend.epochs) > 1 {
			break
		}
	}
	if len(backend.epochs) != 2 || backend.epochs[1] != 1 || len(backend.deployedResources) != 1 || backend.deployedResources[0] != desired.Resources {
		t.Fatalf("rearmed deployment used stale capacity: epochs=%v resources=%+v", backend.epochs, backend.deployedResources)
	}
}

func TestRuntimeRecoveryPreservesAcceptedResourcesInClosedWindow(t *testing.T) {
	p, rc, base := setupProvider(t)
	convergeProvider(t, p, rc)
	before, err := loadState(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	m := before.Members["gateway"]
	accepted := m.Resources
	desired := rc.Cluster.Spec.Components.Extra["gateway"]
	desired.Resources.MaxCPU++
	rc.Cluster.Spec.Components.Extra["gateway"] = desired
	rc.Cluster.Generation++
	delete(base.services, m.Name)
	enginetest.AssertRuntimeRecoveryPreservesResources(t, rc, m.Name, accepted, accepted, func() error {
		_, err := p.Sync(t.Context(), rc)
		return err
	}, func() (infra.Resources, bool, error) {
		s, err := loadState(rc.Cluster)
		if err != nil {
			return infra.Resources{}, false, err
		}
		current := s.Members["gateway"]
		return current.Resources, current.Stage == "joined" && s.Pending == nil, nil
	})
}
