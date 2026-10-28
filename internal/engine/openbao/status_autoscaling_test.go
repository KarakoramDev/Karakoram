// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package openbao

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type autoscalingStatusBackend struct {
	infra.Backend
	services []infra.Service
}

func (b autoscalingStatusBackend) ListServices(context.Context, string) ([]infra.Service, error) {
	return b.services, nil
}

func TestStatusUsesOnlyEffectiveServiceAutoscaling(t *testing.T) {
	p, rc, _, _ := setup(t)
	ready(t, p, rc)
	initial, err := p.Status(t.Context(), rc)
	if err != nil || len(initial.Members) == 0 {
		t.Fatalf("initial status: %+v, %v", initial, err)
	}
	backend := rc.Backend
	start := 2.0
	effective := domain.Resources{CPUMode: "SHARED", MinCPU: 1, MaxCPU: 4, StartCPU: &start, MinRAM: 2, MaxRAM: 8, MinDisk: 5, MaxDisk: 10}
	reservations := domain.Resources{CPUMode: "SHARED", MinCPU: 1, MaxCPU: 1, MinRAM: 1, MaxRAM: 1, MinDisk: 1, MaxDisk: 3}
	for _, platform := range []string{"nomad", "dokploy", "zerops"} {
		t.Run(platform, func(t *testing.T) {
			var want *domain.Resources
			if platform == "zerops" {
				want = &effective
			}
			services := make([]infra.Service, 0, len(initial.Members))
			for _, m := range initial.Members {
				services = append(services, infra.Service{ID: m.Name, Hostname: m.Name, Kind: infra.KindRuntime, Resources: reservations, Autoscaling: want})
			}
			rc.Backend = autoscalingStatusBackend{Backend: backend, services: services}
			status, err := p.Status(t.Context(), rc)
			if err != nil {
				t.Fatal(err)
			}
			if len(status.Members) != len(initial.Members) {
				t.Fatalf("members=%+v", status.Members)
			}
			for _, m := range status.Members {
				if !reflect.DeepEqual(m.Autoscaling, want) {
					t.Fatalf("%s autoscaling=%+v, want %+v", m.Name, m.Autoscaling, want)
				}
				encoded, err := json.Marshal(m)
				if err != nil {
					t.Fatal(err)
				}
				var public map[string]json.RawMessage
				if err := json.Unmarshal(encoded, &public); err != nil {
					t.Fatal(err)
				}
				if _, present := public["autoscaling"]; present != (want != nil) {
					t.Fatalf("autoscaling presence: %s", encoded)
				}
			}
		})
	}
}
