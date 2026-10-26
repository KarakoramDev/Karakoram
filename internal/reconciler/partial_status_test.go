// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package reconciler

import (
	"errors"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

func TestPartialStatusErrorPublishesSafetyConditions(t *testing.T) {
	for _, phase := range []domain.Phase{domain.PhaseDegraded, domain.PhaseReady, ""} {
		t.Run(string(phase), func(t *testing.T) {
			p := &operationProvider{statusFn: func(_ *engine.Context) (domain.ClusterStatus, error) {
				return domain.ClusterStatus{Phase: phase, Conditions: []domain.Condition{{Type: "Quorum", Status: "False", Reason: "QuorumLost"}}}, errors.New("backup observation unavailable")
			}}
			m, st := newTestManager(t, p)
			c := &domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: "fake"}, Status: domain.ClusterStatus{Phase: domain.PhaseReady, ObservedGeneration: 1}}
			if err := st.CreateCluster(t.Context(), c); err != nil {
				t.Fatal(err)
			}
			c.Generation = 2
			rc := m.opts.Context
			rc.Cluster = c
			if _, err := m.synchronize(t.Context(), p, &rc, nil); err == nil {
				t.Fatal("observation error suppressed")
			}
			if c.Status.Phase != domain.PhaseDegraded {
				t.Fatalf("failed observation retained Ready: %+v", c.Status)
			}
			if phase != "" && (len(c.Status.Conditions) == 0 || c.Status.Conditions[0].Type != "Quorum" || c.Status.Conditions[0].Status != "False") {
				t.Fatalf("safety snapshot discarded: %+v", c.Status)
			}
			if c.Status.ObservedGeneration != 1 || !strings.Contains(c.Status.Message, "backup observation unavailable") {
				t.Fatalf("incorrect observation accounting: %+v", c.Status)
			}
		})
	}
}
