// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package neo4j

import (
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestInterruptedFirstCreateRetriesConfiguration(t *testing.T) {
	p, rc, _, a := setup(t)
	a.bootstrapFailures = 2
	sawFailure := false
	for range 40 {
		_, err := p.Sync(t.Context(), rc)
		if err != nil && a.reloads == 0 {
			t.Fatal(err)
		}
		if a.configurationError != "" {
			sawFailure = true
			status, err := p.Status(t.Context(), rc)
			if err != nil {
				t.Fatal(err)
			}
			if status.Phase == domain.PhaseReady || a.config.ConfigVersion != 0 {
				t.Fatal("interrupted bootstrap published readiness or applied configuration")
			}
			// Recreate the controller; only durable cluster state may drive replay.
			p = New(Options{ControlPlaneURL: "https://control.example.test"})
		}
		state, err := load(rc.Cluster)
		if err != nil {
			t.Fatal(err)
		}
		if state.Member != nil && state.Member.Stage == "ready" {
			break
		}
	}
	status, err := p.Status(t.Context(), rc)
	if err != nil {
		t.Fatal(err)
	}
	if !sawFailure || a.reloads < 3 || status.Phase != domain.PhaseReady || a.config.ConfigVersion == 0 {
		t.Fatalf("bootstrap did not retry to readiness: reloads=%d status=%+v", a.reloads, status)
	}
}
