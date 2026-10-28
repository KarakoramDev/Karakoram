// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package reconciler

import (
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

func TestCreateTrackingWaitsForReadyAfterObservedGeneration(t *testing.T) {
	m, st := newTestManager(t, lifecycleProvider())
	op := &domain.Operation{Kind: domain.OperationCreate}
	c := &domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: "fake"}}
	if err := st.CreateCluster(store.WithOperation(t.Context(), op), c); err != nil {
		t.Fatal(err)
	}
	c.Status.ObservedGeneration = c.Generation
	for _, message := range []string{"Availability observation unavailable or stale; inspect member reachability", "broker unavailable", ""} {
		c.Status.Phase, c.Status.Message = domain.PhaseDegraded, message
		if err := m.trackingOperations(t.Context(), nil, c, nil, true, ""); err != nil {
			t.Fatal(err)
		}
		got, err := st.GetOperation(t.Context(), op.ID)
		if err != nil || got.Phase != domain.PhaseRunning || got.CompletedAt != nil {
			t.Fatalf("create succeeded without Ready: %+v, %v", got, err)
		}
	}
	c.Status.Phase, c.Status.Message = domain.PhaseReady, ""
	if err := m.trackingOperations(t.Context(), nil, c, nil, true, ""); err != nil {
		t.Fatal(err)
	}
	got, err := st.GetOperation(t.Context(), op.ID)
	if err != nil || got.Phase != domain.PhaseSucceeded || got.CompletedAt == nil {
		t.Fatalf("fresh Ready did not complete create: %+v, %v", got, err)
	}
}
