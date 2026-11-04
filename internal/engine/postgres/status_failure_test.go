// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"context"
	"errors"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type failedStatusBackend struct {
	infra.Backend
	containers bool
}

func (b failedStatusBackend) GetService(ctx context.Context, name string) (*infra.Service, error) {
	if !b.containers {
		return nil, errors.New("runtime observation unavailable")
	}
	return b.Backend.GetService(ctx, name)
}
func (b failedStatusBackend) Containers(ctx context.Context, name string) ([]infra.Container, error) {
	if b.containers {
		return nil, errors.New("container observation unavailable")
	}
	return b.Backend.Containers(ctx, name)
}

func TestStatusFailureRetainsEstablishedLifecycleAndMemberIdentities(t *testing.T) {
	for _, containers := range []bool{false, true} {
		t.Run(map[bool]string{false: "runtime", true: "containers"}[containers], func(t *testing.T) {
			p, rc, b := setup(t, "ha")
			s := converge(t, p, rc)
			ready, err := p.Status(t.Context(), rc)
			if err != nil || ready.Phase != domain.PhaseReady {
				t.Fatalf("initial status: %+v %v", ready, err)
			}
			ready.ObservedGeneration = rc.Cluster.Generation
			rc.Cluster.Status = ready
			before := string(rc.Cluster.Internal)
			rc.Backend = failedStatusBackend{Backend: b, containers: containers}
			for _, availability := range []bool{false, true} {
				rc.AvailabilityOnly = availability
				status, err := engine.ObserveStatus(t.Context(), p, rc)
				if err == nil {
					t.Fatal("failed platform observation succeeded")
				}
				if status.Phase != domain.PhaseDegraded || len(status.Members) != len(s.Members) {
					t.Fatalf("failed established observation reset lifecycle/inventory: phase=%s members=%d want Degraded/%d", status.Phase, len(status.Members), len(s.Members))
				}
				for i, member := range status.Members {
					if member.Name != s.Members[i].Name || member.Health || member.Agent.Healthy {
						t.Fatalf("member identity/unknown health lost: %+v", member)
					}
				}
				if !status.ObservedAt.IsZero() || status.ObservationEpoch != "" || string(rc.Cluster.Internal) != before {
					t.Fatal("failed observation renewed lease or changed provider checkpoint")
				}
			}
		})
	}
}
