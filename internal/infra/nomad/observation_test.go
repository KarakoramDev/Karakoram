// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package nomad

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	model "github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type inventoryProvider struct {
	engine.Provider
	names []string
}

func (p inventoryProvider) Status(ctx context.Context, rc *engine.Context) (model.ClusterStatus, error) {
	inventory := engine.ObservePlatform(ctx, rc.Backend, rc.Cluster.Name, p.names)
	if err := errors.Join(inventory.ProjectErr, inventory.ServicesErr); err != nil {
		return model.ClusterStatus{}, err
	}
	services := map[string]infra.Service{}
	for _, s := range inventory.Services {
		services[s.Hostname] = s
	}
	status := model.ClusterStatus{Phase: model.PhaseReady}
	for i, name := range p.names {
		healthy := services[name].Status == infra.StatusRunning && inventory.Members[i].Err == nil && len(inventory.Members[i].Containers) > 0
		for _, c := range inventory.Members[i].Containers {
			healthy = healthy && (c.Status == infra.ContainerRunning || c.Status == infra.ContainerActive)
		}
		status.Members = append(status.Members, model.MemberStatus{Name: name, Health: healthy, Agent: model.AgentStatus{Healthy: healthy, LastSeen: time.Now()}})
		if !healthy {
			status.Phase = model.PhaseDegraded
		}
	}
	return status, nil
}

func TestObservationRealBackendLatencyFitsSweep(t *testing.T) {
	f := newFakeNomad(t)
	b := fakeBackend(t, f, nil)
	var names []string
	for i := range 3 {
		name, volume := fmt.Sprint("runtime", i), fmt.Sprint("volume", i)
		importFake(t, b, volumeSpec(volume), runtimeSpec(name, volume))
		deployFake(t, b, name, volume)
		names = append(names, name)
	}
	// Keep the production five-second sweep and normal-build latency intact.
	// Race instrumentation adds HTTP/JSON overhead to an almost full sweep;
	// halve only the synthetic delay to leave room for that instrumentation.
	latency := 600 * time.Millisecond
	if raceEnabled {
		latency /= 2
	}
	f.mu.Lock()
	f.latency = latency
	startCalls := len(f.calls)
	f.mu.Unlock()
	status, err := engine.ObserveStatus(t.Context(), inventoryProvider{names: names}, &engine.Context{Backend: b, Cluster: &model.Cluster{Name: "runtime"}})
	if err != nil || status.Phase != model.PhaseReady || status.ObservedAt.IsZero() {
		t.Fatal("healthy delayed platform could not complete a fresh sweep", status, err)
	}
	f.mu.Lock()
	requests := len(f.calls) - startCalls
	f.mu.Unlock()
	// Even under -race, serializing these delayed requests must not fit:
	// reducing the delay must not turn this into a trivial health check.
	if serialized := time.Duration(requests) * latency; serialized <= engine.ObservationSweepTimeout {
		t.Fatalf("%d requests at %s would fit serially in the %s sweep", requests, latency, engine.ObservationSweepTimeout)
	}
}

func TestObservationParallelAllocationHydration(t *testing.T) {
	f := newFakeNomad(t)
	b := fakeBackend(t, f, nil)
	spec := runtimeSpec("runtime", "")
	spec.MinContainers, spec.MaxContainers = 9, 9
	importFake(t, b, spec)
	deployFake(t, b, "runtime", "")
	f.mu.Lock()
	f.latency = 600 * time.Millisecond
	f.projectAllocations = true
	f.mu.Unlock()
	ctx, cancel := context.WithTimeout(t.Context(), engine.ObservationSweepTimeout)
	defer cancel()
	containers, err := b.Containers(ctx, "runtime")
	if err != nil || len(containers) != 9 {
		t.Fatal("allocation hydration exhausted the sweep", len(containers), err)
	}
	for _, container := range containers {
		if container.IPv4 == "" {
			t.Fatal("CNI allocation detail was not hydrated", container)
		}
	}
}
