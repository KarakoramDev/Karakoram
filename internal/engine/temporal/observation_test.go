// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package temporal

import (
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/temporal/tspec"
)

func TestNativeReadyObservationRenewsAvailabilityLease(t *testing.T) {
	for _, topology := range []string{"combined", "split"} {
		t.Run(topology, func(t *testing.T) {
			p, rc, _, agents := fixture(t, topology)
			converge(t, p, rc)
			rc.ObservationEpoch = "current-process"
			for range 3 {
				status, err := engine.ObserveStatus(t.Context(), p, rc)
				if err != nil || status.Phase != domain.PhaseReady || status.ObservationEpoch != rc.ObservationEpoch || len(status.Members) == 0 {
					t.Fatal("fresh native readiness was rejected by the shared availability fence", status, err)
				}
				for _, member := range status.Members {
					if !member.Health || !member.Agent.Healthy || !member.Agent.LastSeen.Equal(rc.Clock.Now()) {
						t.Fatal("positive readiness lacks generic observation evidence", member)
					}
					if member.Agent.Version != "agent-version" || member.Agent.Commit != "agent-commit" || member.Agent.Build != "agent-build" {
						t.Fatalf("agent executable identity lost: %+v", member.Agent)
					}
				}
				rc.Cluster.Status = status
				rc.Clock.(*testClock).now = rc.Clock.Now().Add(time.Second)
			}
			stale := engine.FreshStatus(rc.Cluster.Status, rc.Clock.Now().Add(engine.ReadinessMaxAge))
			for _, member := range stale.Members {
				if member.Health || member.Agent.Healthy {
					t.Fatal("expired observation retained positive health", member)
				}
			}
			agents.unready = tspec.UI
			status, err := engine.ObserveStatus(t.Context(), p, rc)
			if err != nil || status.Phase == domain.PhaseReady {
				t.Fatal("unready component retained Ready", status, err)
			}
			for _, member := range status.Members {
				if member.Set == tspec.UI && member.Health {
					t.Fatal("native-unready UI was healthy")
				}
			}
			agents.server.Close()
			status, _ = engine.ObserveStatus(t.Context(), p, rc)
			if status.Phase == domain.PhaseReady {
				t.Fatal("unreachable agents retained Ready")
			}
			for _, member := range status.Members {
				if member.Health || member.Agent.Healthy {
					t.Fatal("unreachable agent retained health", member)
				}
			}
		})
	}
}
