// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"fmt"
	"maps"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

// idleSyncBackend counts the complete Backend surface, including local metadata
// calls. An exact map comparison also rejects any unexpected mutation. The
// embedded backend exposes no optional interfaces in the MySQL/Postgres fixtures.
type idleSyncBackend struct {
	infra.Backend
	mu       sync.Mutex
	calls    map[string]int
	trace    []string
	proxy    string
	replicas int
}

func (b *idleSyncBackend) note(method string, names ...string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.calls[method]++
	call := method
	if len(names) != 0 {
		call += "/" + names[0]
	}
	b.trace = append(b.trace, call)
}
func (b *idleSyncBackend) take() map[string]int {
	b.mu.Lock()
	defer b.mu.Unlock()
	calls := maps.Clone(b.calls)
	clear(b.calls)
	return calls
}

func (b *idleSyncBackend) takeTrace() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	trace := b.trace
	b.trace = nil
	return trace
}

// Observation workers can interleave; assert exact hostname counts, not order.
func assertIdleTargets(t *testing.T, trace []string, want map[string]int) {
	t.Helper()
	got := map[string]int{}
	for _, call := range trace {
		if strings.Contains(call, "/") {
			got[call]++
		}
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("hostname calls=%v, want %v", got, want)
	}
}

func (b *idleSyncBackend) Name() string {
	b.note("Name")
	return b.Backend.Name()
}

func (b *idleSyncBackend) Capabilities() infra.Capabilities {
	b.note("Capabilities")
	return b.Backend.Capabilities()
}

func (b *idleSyncBackend) Layout() infra.Layout {
	b.note("Layout")
	return b.Backend.Layout()
}

func (b *idleSyncBackend) ProjectInfo(ctx context.Context) (infra.Project, error) {
	b.note("ProjectInfo")
	return b.Backend.ProjectInfo(ctx)
}

func (b *idleSyncBackend) ImportServices(ctx context.Context, specs []infra.ServiceSpec) (infra.Op, error) {
	b.note("ImportServices")
	return b.Backend.ImportServices(ctx, specs)
}

func (b *idleSyncBackend) GetService(ctx context.Context, name string) (*infra.Service, error) {
	b.note("GetService", name)
	return b.Backend.GetService(ctx, name)
}

func (b *idleSyncBackend) ListServices(ctx context.Context, prefix string) ([]infra.Service, error) {
	b.note("ListServices")
	return b.Backend.ListServices(ctx, prefix)
}

func (b *idleSyncBackend) DeleteService(ctx context.Context, name string) (infra.Op, error) {
	b.note("DeleteService")
	return b.Backend.DeleteService(ctx, name)
}

func (b *idleSyncBackend) DeleteServiceByID(ctx context.Context, name, id string) (infra.Op, error) {
	b.note("DeleteServiceByID")
	return b.Backend.DeleteServiceByID(ctx, name, id)
}

func (b *idleSyncBackend) Deploy(ctx context.Context, name string, d infra.Deployment) (infra.Op, error) {
	b.note("Deploy")
	return b.Backend.Deploy(ctx, name, d)
}

func (b *idleSyncBackend) SetEnvSecrets(ctx context.Context, name string, vars map[string]string) error {
	b.note("SetEnvSecrets")
	return b.Backend.SetEnvSecrets(ctx, name, vars)
}

func (b *idleSyncBackend) SetEnvVars(ctx context.Context, name string, vars map[string]string) error {
	b.note("SetEnvVars")
	return b.Backend.SetEnvVars(ctx, name, vars)
}

func (b *idleSyncBackend) Start(ctx context.Context, name string) (infra.Op, error) {
	b.note("Start")
	return b.Backend.Start(ctx, name)
}

func (b *idleSyncBackend) Stop(ctx context.Context, name string) (infra.Op, error) {
	b.note("Stop")
	return b.Backend.Stop(ctx, name)
}

func (b *idleSyncBackend) Restart(ctx context.Context, name string) (infra.Op, error) {
	b.note("Restart")
	return b.Backend.Restart(ctx, name)
}

func (b *idleSyncBackend) SetResources(ctx context.Context, name string, r infra.Resources) (infra.Op, error) {
	b.note("SetResources")
	return b.Backend.SetResources(ctx, name, r)
}

func (b *idleSyncBackend) SetContainerCount(ctx context.Context, name string, min, max int) (infra.Op, error) {
	b.note("SetContainerCount")
	return b.Backend.SetContainerCount(ctx, name, min, max)
}

func (b *idleSyncBackend) EnsurePublicPort(ctx context.Context, name string, port infra.PublicPort) error {
	b.note("EnsurePublicPort")
	return b.Backend.EnsurePublicPort(ctx, name, port)
}

func (b *idleSyncBackend) ListPublicPorts(ctx context.Context) ([]infra.PublicPortRoute, error) {
	b.note("ListPublicPorts")
	return b.Backend.ListPublicPorts(ctx)
}

func (b *idleSyncBackend) DeletePublicPorts(ctx context.Context, name string) error {
	b.note("DeletePublicPorts")
	return b.Backend.DeletePublicPorts(ctx, name)
}

func (b *idleSyncBackend) Containers(ctx context.Context, name string) ([]infra.Container, error) {
	b.note("Containers", name)
	if name == b.proxy && b.replicas > 0 {
		instances := make([]infra.Container, b.replicas)
		for i := range instances {
			host := fmt.Sprintf("%s-instance-%d", name, i)
			instances[i] = infra.Container{ID: host, Hostname: host, Status: infra.ContainerRunning, PlacementGroup: host + "-host"}
		}
		return instances, nil
	}
	return b.Backend.Containers(ctx, name)
}

func (b *idleSyncBackend) Logs(ctx context.Context, name string, q infra.LogQuery) ([]infra.LogLine, error) {
	b.note("Logs")
	return b.Backend.Logs(ctx, name, q)
}

func (b *idleSyncBackend) OpStatus(ctx context.Context, op infra.Op) (infra.OpState, error) {
	b.note("OpStatus")
	return b.Backend.OpStatus(ctx, op)
}

// TestIdleSyncBackendProfile profiles a deep reconcile, not a steady availability
// renewal: Sync and the following Status each get fresh pass-local observations.
func TestIdleSyncBackendProfile(t *testing.T) {
	p, rc, base := setup(t, "group")
	proxy := rc.Cluster.Spec.Components.Extra["proxy"]
	proxy.Replicas = 3
	rc.Cluster.Spec.Components.Extra["proxy"] = proxy
	b := &idleSyncBackend{Backend: base, calls: map[string]int{}, proxy: "demomx", replicas: 3}
	rc.Backend = b
	converge(t, p, rc, base)
	base.apply(t, p, rc)

	before, err := p.Status(t.Context(), rc)
	if err != nil || before.Phase != domain.PhaseReady {
		t.Fatalf("fixture not healthy: phase=%s err=%v", before.Phase, err)
	}
	rc.Cluster.Status = before
	b.take()
	b.takeTrace()
	result, err := p.Sync(t.Context(), rc)
	if err != nil {
		t.Fatal(err)
	}
	syncCalls := b.take()
	syncTrace := b.takeTrace()
	status, err := p.Status(t.Context(), rc)
	if err != nil || status.Phase != domain.PhaseReady {
		t.Fatalf("post-Sync status: phase=%s err=%v", status.Phase, err)
	}
	statusCalls := b.take()
	statusTrace := b.takeTrace()
	t.Logf("idle Sync trace=%v; fresh Status trace=%v", syncTrace, statusTrace)
	assertIdleTargets(t, syncTrace, map[string]int{
		"GetService/demom0": 1, "GetService/demom1": 1, "GetService/demom2": 1, "GetService/demomx": 1,
		"GetService/demom0v": 2, "GetService/demom1v": 2, "GetService/demom2v": 2,
		"Containers/demom0": 1, "Containers/demom1": 1, "Containers/demom2": 1, "Containers/demomx": 2,
	})
	assertIdleTargets(t, statusTrace, map[string]int{
		"GetService/demom0": 1, "GetService/demom1": 1, "GetService/demom2": 1, "GetService/demomx": 1,
		"Containers/demom0": 1, "Containers/demom1": 1, "Containers/demom2": 1, "Containers/demomx": 2,
	})
	t.Logf("idle Sync=%v; fresh Status=%v; RequeueAfter=%s; Steady=%t", syncCalls, statusCalls, result.RequeueAfter, result.Steady)
	wantSync := map[string]int{"Capabilities": 4, "Containers": 5, "GetService": 10, "Layout": 6}
	wantStatus := map[string]int{"Containers": 5, "GetService": 4, "Layout": 3}
	if !reflect.DeepEqual(syncCalls, wantSync) {
		t.Errorf("idle Sync calls=%v, want %v", syncCalls, wantSync)
	}
	if !reflect.DeepEqual(statusCalls, wantStatus) {
		t.Errorf("fresh Status calls=%v, want %v", statusCalls, wantStatus)
	}
	if !result.Steady || result.RequeueAfter != 0 {
		t.Errorf("idle result=%+v, want Steady with no requeue", result)
	}
}
