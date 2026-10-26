// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type coldReadinessAgents struct{ engine.AgentDialer }

func (d coldReadinessAgents) Dial(ctx context.Context, ref domain.ClusterRef, name string) (*agentapi.Client, error) {
	// Simulate a 1.5s cold namespace/TLS establishment plus 1s response.
	timer := time.NewTimer(2500 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C:
		return d.AgentDialer.Dial(ctx, ref, name)
	}
}

func TestObservationColdAgentAndSlowPlatformFitsSweep(t *testing.T) {
	p, rc, f, _ := coreSetup(t, 3)
	coreConverge(t, p, rc, f)
	rc.Clock = engine.RealClock{}
	rc.Agents = coldReadinessAgents{rc.Agents}
	rc.Backend = &delayedStatusBackend{Backend: rc.Backend}
	status, err := engine.ObserveStatus(t.Context(), p, rc)
	if err != nil || status.Phase != domain.PhaseReady || status.ObservedAt.IsZero() {
		t.Fatalf("healthy cold Elasticsearch member lost fresh readiness: %+v, %v", status, err)
	}
}

type delayedStatusBackend struct {
	infra.Backend
	inventory, containers, services atomic.Int32
}

func waitStatusBackend(ctx context.Context) error {
	timer := time.NewTimer(1500 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *delayedStatusBackend) ProjectInfo(ctx context.Context) (infra.Project, error) {
	if err := waitStatusBackend(ctx); err != nil {
		return infra.Project{}, err
	}
	return b.Backend.ProjectInfo(ctx)
}
func (b *delayedStatusBackend) ListServices(ctx context.Context, prefix string) ([]infra.Service, error) {
	b.inventory.Add(1)
	if err := waitStatusBackend(ctx); err != nil {
		return nil, err
	}
	return b.Backend.ListServices(ctx, prefix)
}
func (b *delayedStatusBackend) GetService(ctx context.Context, name string) (*infra.Service, error) {
	b.services.Add(1)
	if err := waitStatusBackend(ctx); err != nil {
		return nil, err
	}
	return b.Backend.GetService(ctx, name)
}
func (b *delayedStatusBackend) Containers(ctx context.Context, name string) ([]infra.Container, error) {
	b.containers.Add(1)
	if err := waitStatusBackend(ctx); err != nil {
		return nil, err
	}
	return b.Backend.Containers(ctx, name)
}

func TestObservationSlowPlatformInventoryFitsSweep(t *testing.T) {
	p, rc, f, _ := coreSetup(t, 5)
	s := coreConverge(t, p, rc, f)
	backend := &delayedStatusBackend{Backend: rc.Backend}
	rc.Backend = backend
	status, err := engine.ObserveStatus(t.Context(), p, rc)
	if err != nil || status.Phase != domain.PhaseReady || status.ObservedAt.IsZero() {
		t.Fatal("healthy slow inventory exhausted the sweep", status, err)
	}
	if backend.inventory.Load() != 1 || backend.services.Load() != 0 || backend.containers.Load() != int32(len(s.Members)) {
		t.Fatal("platform observations were duplicated", backend.inventory.Load(), backend.services.Load(), backend.containers.Load(), len(s.Members))
	}
}
