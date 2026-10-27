// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

// Package enginetest contains shared, provider-neutral reconciliation assertions.
package enginetest

import (
	"context"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/infra/zerops"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

// IdleCycles deliberately reloads pointers on every reconciliation, rather than
// comparing an in-memory checkpoint with the object that originally created it.
const IdleCycles = 20

// RecordingBackend preserves a fixture's observations and mutation semantics,
// while exposing the real Zerops layout/capabilities and recording ALL mutation
// submissions, including redundant requests and requests that fail.
type RecordingBackend struct {
	infra.Backend
	mu    sync.Mutex
	calls []string
}

func (*RecordingBackend) Name() string                     { return "zerops" }
func (*RecordingBackend) Capabilities() infra.Capabilities { return (&zerops.Backend{}).Capabilities() }
func (*RecordingBackend) Layout() infra.Layout             { return (&zerops.Backend{}).Layout() }
func (b *RecordingBackend) record(method, service string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.calls = append(b.calls, method+":"+service)
}
func (b *RecordingBackend) Calls() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]string(nil), b.calls...)
}
func (b *RecordingBackend) ImportServices(ctx context.Context, specs []infra.ServiceSpec) (infra.Op, error) {
	b.record("ImportServices", "")
	return b.Backend.ImportServices(ctx, specs)
}
func (b *RecordingBackend) DeleteService(ctx context.Context, name string) (infra.Op, error) {
	b.record("DeleteService", name)
	return b.Backend.DeleteService(ctx, name)
}
func (b *RecordingBackend) DeleteServiceByID(ctx context.Context, name, id string) (infra.Op, error) {
	b.record("DeleteServiceByID", name)
	return b.Backend.DeleteServiceByID(ctx, name, id)
}
func (b *RecordingBackend) Deploy(ctx context.Context, name string, d infra.Deployment) (infra.Op, error) {
	b.record("Deploy", name)
	return b.Backend.Deploy(ctx, name, d)
}
func (b *RecordingBackend) SetEnvSecrets(ctx context.Context, name string, env map[string]string) error {
	b.record("SetEnvSecrets", name)
	return b.Backend.SetEnvSecrets(ctx, name, env)
}
func (b *RecordingBackend) SetEnvVars(ctx context.Context, name string, env map[string]string) error {
	b.record("SetEnvVars", name)
	return b.Backend.SetEnvVars(ctx, name, env)
}
func (b *RecordingBackend) Start(ctx context.Context, name string) (infra.Op, error) {
	b.record("Start", name)
	return b.Backend.Start(ctx, name)
}
func (b *RecordingBackend) Stop(ctx context.Context, name string) (infra.Op, error) {
	b.record("Stop", name)
	return b.Backend.Stop(ctx, name)
}
func (b *RecordingBackend) Restart(ctx context.Context, name string) (infra.Op, error) {
	b.record("Restart", name)
	return b.Backend.Restart(ctx, name)
}
func (b *RecordingBackend) SetResources(ctx context.Context, name string, r infra.Resources) (infra.Op, error) {
	b.record("SetResources", name)
	return b.Backend.SetResources(ctx, name, r)
}
func (b *RecordingBackend) SetContainerCount(ctx context.Context, name string, min, max int) (infra.Op, error) {
	b.record("SetContainerCount", name)
	return b.Backend.SetContainerCount(ctx, name, min, max)
}
func (b *RecordingBackend) EnsurePublicPort(ctx context.Context, name string, port infra.PublicPort) error {
	b.record("EnsurePublicPort", name)
	return b.Backend.EnsurePublicPort(ctx, name, port)
}
func (b *RecordingBackend) DeletePublicPorts(ctx context.Context, name string) error {
	b.record("DeletePublicPorts", name)
	return b.Backend.DeletePublicPorts(ctx, name)
}
func (b *RecordingBackend) SetSubdomainAccess(ctx context.Context, name string, enabled bool) (infra.Op, error) {
	b.record("SetSubdomainAccess", name)
	return b.Backend.(infra.SubdomainAccess).SetSubdomainAccess(ctx, name, enabled)
}
func (b *RecordingBackend) SubdomainURLs(ctx context.Context, name string) ([]string, error) {
	return b.Backend.(infra.SubdomainAccess).SubdomainURLs(ctx, name)
}

// AssertIdle drives ordinary Sync plus the shared availability observation to
// Ready, persists through the real SQL store and JSON decoder after EVERY cycle,
// then requires twenty further Ready cycles without an infrastructure mutation.
// Applied generation is checked separately: a healthy but unconverged fixture
// cannot accidentally pass. Positive provider samples must satisfy the same
// freshness contract as production, not just return Ready from raw Status.
// apply models agent configuration acceptance, not provider reconciliation.
func AssertIdle(t *testing.T, p engine.Provider, rc *engine.Context, apply func()) {
	t.Helper()
	backend := &RecordingBackend{Backend: rc.Backend}
	rc.Backend = backend
	// Use an isolated real store for the round trip; keep the fixture's secret and
	// dependency stores intact, including its native/agent observation handlers.
	seal, err := secrets.New(strings.Repeat("x", 32))
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(filepath.Join(t.TempDir(), "idle.db"), seal)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	if rc.Cluster.Generation == 0 {
		rc.Cluster.Generation = 1
	}
	bounds := func(component *domain.ComponentSpec) {
		if component.Replicas == 0 {
			return
		}
		// Exercise distinct autoscaling bounds as well as a nonnil optional
		// pointer on every deployed component, including config servers/routers.
		start := max(1, component.Resources.MinCPU)
		component.Resources.StartCPU = &start
		component.Resources.MaxCPU = max(component.Resources.MaxCPU, start+1)
	}
	bounds(&rc.Cluster.Spec.Components.Engine)
	bounds(&rc.Cluster.Spec.Components.ConfigServer)
	bounds(&rc.Cluster.Spec.Components.Router)
	for name, component := range rc.Cluster.Spec.Components.Extra {
		bounds(&component)
		rc.Cluster.Spec.Components.Extra[name] = component
	}
	if err = st.CreateCluster(t.Context(), rc.Cluster); err != nil {
		t.Fatal(err)
	}
	desired := rc.Cluster.Spec
	roundTrip := func() {
		t.Helper()
		if err := st.UpdateClusterState(t.Context(), rc.Cluster); err != nil {
			t.Fatal(err)
		}
		loaded, err := st.GetCluster(t.Context(), rc.Cluster.Ref())
		if err != nil {
			t.Fatal(err)
		}
		// Existing fake-agent handlers retain this pointer, just as they retain the
		// runtime identity across a control-plane restart.
		*rc.Cluster = *loaded
		if !reflect.DeepEqual(rc.Cluster.Spec, desired) {
			t.Fatal("reconciliation changed the desired specification")
		}
	}
	cycle := func() bool {
		t.Helper()
		rc.SyncWait = ""
		result, err := p.Sync(t.Context(), rc)
		if err != nil {
			t.Fatalf("Sync: %v", err)
		}
		if result.ConvergenceError != nil {
			t.Fatal(result.ConvergenceError)
		}
		if apply != nil {
			apply()
		}
		status, err := engine.ObserveStatus(t.Context(), p, rc)
		if err != nil {
			t.Fatalf("Status: %v", err)
		}
		// Match reconciler publication: Sync owns the applied-generation checkpoint.
		status.ObservedGeneration = rc.Cluster.Status.ObservedGeneration
		if result.RequeueAfter <= 0 {
			status.ObservedGeneration = rc.Cluster.Generation
		}
		rc.Cluster.Status = status
		ready := status.Phase == domain.PhaseReady && status.ObservedGeneration == rc.Cluster.Generation
		roundTrip()
		return ready
	}
	converged := false
	for range 300 {
		if cycle() {
			converged = true
			break
		}
	}
	if !converged {
		t.Fatalf("fixture did not converge: status=%+v internal=%s", rc.Cluster.Status, rc.Cluster.Internal)
	}
	before := len(backend.Calls())
	if before == 0 {
		t.Fatal("fixture did not exercise provisioning before becoming Ready")
	}
	for i := range IdleCycles {
		if !cycle() {
			t.Fatalf("idle cycle %d lost convergence: %+v", i, rc.Cluster.Status)
		}
		if calls := backend.Calls()[before:]; len(calls) > 0 {
			t.Fatalf("idle cycle %d submitted provider mutations: %v", i, calls)
		}
	}
	t.Logf("%d persisted Ready cycles; zero provider mutations", IdleCycles)
}
