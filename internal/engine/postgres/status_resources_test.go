// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"reflect"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestStatusReportsEffectiveRuntimeResources(t *testing.T) {
	for _, topology := range []string{"single", "ha"} {
		t.Run(topology, func(t *testing.T) {
			p, rc, b := setup(t, topology)
			s := converge(t, p, rc)
			start := float64(2)
			effective := domain.Resources{CPUMode: "SHARED", MinCPU: 1, MaxCPU: 3, StartCPU: &start, MinRAM: 2, MaxRAM: 4, MinDisk: 3, MaxDisk: 5}
			b.genericOverrides = map[string]agentapi.Status{}
			for _, m := range s.Members {
				// Deliberately differ from both desired spec and persisted member resources.
				b.services[m.Name].Resources = effective
				b.services[m.Name].Autoscaling = &effective
				b.genericOverrides[m.Name] = agentapi.Status{ConfigVersion: m.ConfigVersion, Resources: &agentapi.ContainerResources{CPUCores: 3, MemoryBytes: 3 << 30, RootDiskBytes: 4 << 30}}
			}
			check := func(want domain.ObservedResources) {
				t.Helper()
				status, err := p.Status(t.Context(), rc)
				if err != nil {
					t.Fatal(err)
				}
				for _, m := range status.Members {
					if m.Autoscaling == nil || !reflect.DeepEqual(*m.Autoscaling, effective) {
						t.Fatalf("%s autoscaling=%+v, want platform bounds %+v", m.Name, m.Autoscaling, effective)
					}
					if m.Resources == nil || *m.Resources != want {
						t.Fatalf("%s resources=%+v, want %+v", m.Name, m.Resources, want)
					}
				}
			}
			check(domain.ObservedResources{CPUCores: 3, MemoryGB: 3, DiskGB: 4})
			for name, st := range b.genericOverrides {
				st.Resources = nil // Older agent: effective platform bounds are the fallback.
				b.genericOverrides[name] = st
			}
			check(domain.ObservedResources{CPUCores: 2, MemoryGB: 2, DiskGB: 3})
			rc.Agents = nil // Unreachable agents must not erase known platform resources.
			check(domain.ObservedResources{CPUCores: 2, MemoryGB: 2, DiskGB: 3})
			for _, m := range s.Members {
				delete(b.services, m.Name)
			}
			status, err := p.Status(t.Context(), rc)
			if err != nil {
				t.Fatal(err)
			}
			for _, m := range status.Members {
				if m.Autoscaling != nil || m.Resources != nil {
					t.Fatalf("%s invents effective resources for absent runtime: %+v", m.Name, m)
				}
			}
		})
	}
}

type reservationStatusBackend struct {
	*fakeBackend
	name string
}

func (b *reservationStatusBackend) Name() string { return b.name }

func TestStatusOmitsAutoscalingForSchedulerReservations(t *testing.T) {
	for _, backend := range []string{"nomad", "dokploy"} {
		t.Run(backend, func(t *testing.T) {
			p, rc, b := setup(t, "ha")
			s := converge(t, p, rc)
			rc.Backend = &reservationStatusBackend{fakeBackend: b, name: backend}
			b.genericOverrides = map[string]agentapi.Status{}
			for _, m := range s.Members {
				// Nomad/Dokploy expose reservations/limits, not autoscaling.
				b.services[m.Name].Resources = domain.Resources{MinCPU: 1, MaxCPU: 2, MinRAM: 2, MaxRAM: 4, MinDisk: 3, MaxDisk: 5}
				b.genericOverrides[m.Name] = agentapi.Status{ConfigVersion: m.ConfigVersion, Resources: &agentapi.ContainerResources{CPUCores: 2, MemoryBytes: 3 << 30, RootDiskBytes: 4 << 30}}
			}
			for _, observation := range []string{"live", "legacy", "unreachable"} {
				t.Run(observation, func(t *testing.T) {
					if observation == "legacy" {
						for name, st := range b.genericOverrides {
							st.Resources = nil
							b.genericOverrides[name] = st
						}
					}
					if observation == "unreachable" {
						rc.Agents = nil
					}
					status, err := p.Status(t.Context(), rc)
					if err != nil {
						t.Fatal(err)
					}
					for _, m := range status.Members {
						if m.Autoscaling != nil {
							t.Fatalf("%s reports scheduler reservations as autoscaling: %+v", m.Name, m.Autoscaling)
						}
						if observation == "live" {
							want := domain.ObservedResources{CPUCores: 2, MemoryGB: 3, DiskGB: 4}
							if m.Resources == nil || *m.Resources != want {
								t.Fatalf("%s loses live allocation: %+v", m.Name, m.Resources)
							}
						} else if m.Resources != nil {
							t.Fatalf("%s invents allocation from scheduler reservations: %+v", m.Name, m.Resources)
						}
					}
				})
			}
		})
	}
}
