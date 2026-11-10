// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package reconciler

import (
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	infrafake "github.com/KarakoramDev/Karakoram/internal/infra/fake"
	"testing"
	"time"
)

func TestSteadyConfigBaseline(t *testing.T) {
	p := &steadyProvider{fakeProvider: &fakeProvider{}}
	m, st, c := steadyFixture(t, p)
	snapshot := infra.PlatformSnapshot(t.Name() + "|target")
	m.opts.Namespaces = engine.StaticNamespaces{DefaultBackend: &steadySnapshotBackend{Backend: infrafake.New(infrafake.Options{}), snapshot: snapshot}}
	w := &worker{trigger: make(chan struct{}, 1)}
	w.steady.Store(&steadyState{generation: c.Generation})
	m.workers[c.Ref().Key()] = w
	c.Status.ObservedGeneration = c.Generation
	c.Status.ObservedAt = time.Now()
	c.Status.ObservationEpoch = m.opts.Context.ObservationEpoch
	if !m.SteadyConfigBaseline(t.Context(), c) {
		t.Fatal("current ready baseline rejected")
	}
	for _, change := range []func(){
		func() { c.Generation++ },
		func() { c.Deleting = true },
		func() { c.Status.Phase = domain.PhaseDegraded },
		func() { c.Status.ObservedAt = time.Now().Add(-2 * engine.ReadinessMaxAge) },
		func() { c.Status.ObservationEpoch = "previous-control-plane" },
		func() { w.reconciling.Store(true) },
		func() { w.steady.Store(nil) },
	} {
		original := *c
		change()
		if m.SteadyConfigBaseline(t.Context(), c) {
			t.Fatal("changed evidence retained configuration baseline")
		}
		*c = original
		w.reconciling.Store(false)
		w.steady.Store(&steadyState{generation: c.Generation})
	}
	if err := st.EnqueueOperation(t.Context(), &domain.Operation{Cluster: c.Name, Kind: "backup", Phase: domain.PhasePending}); err != nil {
		t.Fatal(err)
	}
	if m.SteadyConfigBaseline(t.Context(), c) {
		t.Fatal("pending operation retained configuration baseline")
	}
}
