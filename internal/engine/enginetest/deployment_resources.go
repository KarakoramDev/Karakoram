// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package enginetest

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

// AssertDeploymentResourceRearm wraps the existing fixture backend. The first
// deployment exhausts its budget; a corrected RAM target must remain fenced
// until re-arm, then its resource operation must finish before deployment.
func AssertDeploymentResourceRearm(t *testing.T, rc *engine.Context, sync func() error, correct func() infra.Resources) {
	t.Helper()
	b := &exhaustedResourceBackend{Backend: rc.Backend, pending: true}
	rc.Backend = b
	reconcile := func() {
		t.Helper()
		if err := sync(); err != nil && !errors.Is(err, infra.ErrDeploymentRetriesExhausted) {
			t.Fatal(err)
		}
	}
	for range 80 {
		reconcile()
		if len(b.epochs) > 0 {
			break
		}
	}
	if len(b.epochs) != 1 {
		t.Fatal("initial deployment was not exhausted")
	}
	desired := correct()
	for range 5 {
		reconcile()
	}
	if b.resizes != 0 || len(b.epochs) != 1 {
		t.Fatal("resource correction bypassed the exhaustion fence")
	}
	rc.Cluster.Spec.RetryEpoch++
	for range 80 {
		reconcile()
		if b.resizes > 0 {
			break
		}
	}
	if b.resizes != 1 || len(b.epochs) != 1 {
		t.Fatalf("rearm did not resize first: resizes=%d epochs=%v", b.resizes, b.epochs)
	}
	for range 5 {
		reconcile()
	}
	if b.resizes != 1 || len(b.epochs) != 1 {
		t.Fatal("pending resource update was replayed or bypassed")
	}
	b.pending = false
	for range 80 {
		reconcile()
		if len(b.epochs) > 1 {
			break
		}
	}
	if len(b.epochs) != 2 || b.epochs[1] != rc.Cluster.Spec.RetryEpoch || b.resources[1].MinRAM != desired.MinRAM || b.resources[1].MaxRAM != desired.MaxRAM {
		t.Fatalf("rearmed deployment used stale capacity: epochs=%v resources=%+v", b.epochs, b.resources)
	}
}

type exhaustedResourceBackend struct {
	infra.Backend
	target    string
	epochs    []int64
	resources []infra.Resources
	resizes   int
	pending   bool
}

func (b *exhaustedResourceBackend) Deploy(ctx context.Context, name string, d infra.Deployment) (infra.Op, error) {
	if b.target == "" {
		b.target = name
	}
	if name != b.target {
		return b.Backend.Deploy(ctx, name, d)
	}
	service, err := b.Backend.GetService(ctx, name)
	if err != nil {
		return infra.Op{}, err
	}
	b.epochs = append(b.epochs, d.RetryEpoch)
	b.resources = append(b.resources, service.Resources)
	return infra.Op{}, &infra.DeploymentExhaustedError{Service: name, Attempts: 4, LastError: "runtime preparation failed"}
}

func (b *exhaustedResourceBackend) SetResources(ctx context.Context, name string, resources infra.Resources) (infra.Op, error) {
	op, err := b.Backend.SetResources(ctx, name, resources)
	if name == b.target && err == nil {
		b.resizes++
		return infra.Op{ID: "corrected-resources", Kind: "resources"}, nil
	}
	return op, err
}

func (b *exhaustedResourceBackend) OpStatus(ctx context.Context, op infra.Op) (infra.OpState, error) {
	if op.ID == "corrected-resources" {
		return infra.OpState{Done: !b.pending}, nil
	}
	return b.Backend.OpStatus(ctx, op)
}

// AssertRuntimeRecoveryPreservesResources checks that runtime-only recovery
// restores accepted capacity while a desired resource change waits for a window.
// The caller removes the runtime, supplies its accepted bounds, and observes its
// engine-owned resource/stage checkpoint after each real fixture Sync.
func AssertRuntimeRecoveryPreservesResources(t *testing.T, rc *engine.Context, member string, accepted, runtimeAccepted infra.Resources, sync func() error, observe func() (infra.Resources, bool, error)) {
	t.Helper()
	now := time.Now().UTC()
	if rc.Clock != nil {
		now = rc.Clock.Now().UTC()
	}
	rc.Cluster.Spec.Maintenance = &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: fmt.Sprintf("0 0 1 %d *", int(now.Month())%12+1), Duration: "1h"}}}
	b := &recoveryResourceBackend{Backend: rc.Backend, member: member}
	rc.Backend = b
	settled := 0
	for range 100 {
		if err := sync(); err != nil {
			t.Fatal(err)
		}
		resources, ready, err := observe()
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(resources, accepted) || b.resizes != 0 {
			t.Fatalf("recovery applied unadmitted desired resources: accepted=%+v actual=%+v resizes=%d", accepted, resources, b.resizes)
		}
		if ready && len(b.deployments) > 0 {
			settled++
			if settled < 5 {
				continue
			}
			service, err := rc.Backend.GetService(t.Context(), member)
			if err != nil || service == nil || !reflect.DeepEqual(service.Resources, runtimeAccepted) {
				t.Fatalf("recovered runtime did not retain accepted capacity: %v", err)
			}
			for _, deployed := range b.deployments {
				if !reflect.DeepEqual(deployed, runtimeAccepted) {
					t.Fatal("recovery deployed with unadmitted resources")
				}
			}
			return
		}
	}
	t.Fatal("closed maintenance window blocked accepted runtime recovery")
}

type recoveryResourceBackend struct {
	infra.Backend
	member      string
	resizes     int
	deployments []infra.Resources
}

func (b *recoveryResourceBackend) SetResources(ctx context.Context, name string, r infra.Resources) (infra.Op, error) {
	if name == b.member {
		b.resizes++
	}
	return b.Backend.SetResources(ctx, name, r)
}

func (b *recoveryResourceBackend) Deploy(ctx context.Context, name string, d infra.Deployment) (infra.Op, error) {
	if name == b.member {
		service, err := b.Backend.GetService(ctx, name)
		if err != nil {
			return infra.Op{}, err
		}
		b.deployments = append(b.deployments, service.Resources)
	}
	return b.Backend.Deploy(ctx, name, d)
}
