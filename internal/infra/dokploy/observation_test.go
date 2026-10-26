// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package dokploy

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	model "github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

// inventoryProvider exercises the production shared sweep with real backend
// HTTP observations; native/agent health is held healthy by this fixture.
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
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	var names []string
	for i := range 3 {
		name, volume := fmt.Sprint("runtime", i), fmt.Sprint("volume", i)
		importFake(t, b, volumeSpec(volume), runtimeSpec(name, volume))
		deployFake(t, b, name, volume)
		names = append(names, name)
	}
	f.mu.Lock()
	f.latency = 600 * time.Millisecond
	f.mu.Unlock()
	status, err := engine.ObserveStatus(t.Context(), inventoryProvider{names: names}, &engine.Context{Backend: b, Cluster: &model.Cluster{Name: "runtime"}})
	if err != nil || status.Phase != model.PhaseReady || status.ObservedAt.IsZero() {
		t.Fatal("healthy delayed platform could not complete a fresh sweep", status, err)
	}
}

func TestConcurrentObservationSerializesRestoration(t *testing.T) {
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	imported := importFake(t, b, runtimeSpec("runtime", ""))
	deployFake(t, b, "runtime", "")
	removeFakeService(f, imported.Services["runtime"])
	before := fakeCalls(f, "application.reload")
	backends := []*Backend{b, fakeBackend(t, f, nil), fakeBackend(t, f, nil)}
	var calls sync.WaitGroup
	failures := make(chan error, len(backends))
	for _, backend := range backends {
		calls.Go(func() { _, err := backend.ListServices(t.Context(), "runtime"); failures <- err })
	}
	calls.Wait()
	close(failures)
	for err := range failures {
		if err != nil {
			t.Fatal(err)
		}
	}
	if got := fakeCalls(f, "application.reload") - before; got != 1 {
		t.Fatal("concurrent observation submitted restoration more than once", got)
	}
}
