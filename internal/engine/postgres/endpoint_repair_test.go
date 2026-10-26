// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type endpointRepairBackend struct {
	*exposureInstanceBackend
	repairs   int
	acks      int
	repairErr error
}

func (b *endpointRepairBackend) RepairPublishedEndpoint(_ context.Context, _ string, reachable bool) (bool, error) {
	if reachable {
		b.acks++
		return false, nil
	}
	b.repairs++
	return b.repairErr == nil, b.repairErr
}

func TestPublishedProxyRepairPrecedesLogicalReadinessBarrier(t *testing.T) {
	for _, replicas := range []int{1, 2, 3} {
		for _, scenario := range []string{"matching-published", "healthy-endpoint", "exhausted", "unpublished", "unhealthy-task", "empty-inventory"} {
			t.Run(fmt.Sprintf("%d/%s", replicas, scenario), func(t *testing.T) {
				p, rc, base := setup(t, "single")
				exposure := &exposureBackend{fakeBackend: base, routes: map[string]infra.PublicPort{}}
				rc.Backend = exposure
				rc.Cluster.Spec.Exposure = domain.Exposure{Public: scenario != "unpublished", IPType: "IPV4"}
				s := converge(t, p, rc)
				rc.Cluster.Spec.Components.Extra["proxy"] = domain.ComponentSpec{Replicas: replicas, Resources: rc.Cluster.Spec.Components.Extra["proxy"].Resources}
				for _, m := range s.Members {
					if m.Role == "proxy" {
						m.Replicas = replicas
					}
				}
				if err := save(rc.Cluster, s); err != nil {
					t.Fatal(err)
				}
				instance := newProxyInstanceFixture(t, s.ConfigVersion)
				logical := newProxyInstanceFixtureTLS(t, s.ConfigVersion, instance.server.TLS)
				instances := []infra.Container{{ID: "allocation-a", Hostname: "proxy-a", Status: infra.ContainerRunning}}
				instanceMap := map[string]*proxyInstanceFixture{"proxy-a": instance}
				for i := 1; i < replicas; i++ {
					name := fmt.Sprintf("proxy-%d", i)
					instances = append(instances, infra.Container{ID: name, Hostname: name, Status: infra.ContainerRunning})
					instanceMap[name] = newProxyInstanceFixtureTLS(t, s.ConfigVersion, instance.server.TLS)
				}
				if scenario == "unhealthy-task" {
					instance.running = false
				}
				if scenario == "empty-inventory" {
					instances = nil
				}
				backend := &endpointRepairBackend{exposureInstanceBackend: &exposureInstanceBackend{exposureBackend: exposure, instances: instances, family: infra.AddressFamilyIPv4}}
				rc.Backend = backend
				agents := exposureInstanceAgents{AgentDialer: rc.Agents, instances: instanceMap, logical: logical, unreachable: scenario != "healthy-endpoint"}
				rc.Agents = agents
				if scenario == "exhausted" {
					backend.repairErr = &infra.EndpointRepairExhaustedError{Service: "demopx", Attempts: 3}
					_, err := p.Sync(t.Context(), rc)
					var exhausted *infra.EndpointRepairExhaustedError
					if !errors.As(err, &exhausted) {
						t.Fatalf("repair exhaustion was hidden by Sync: %v", err)
					}
					return
				}
				if scenario == "healthy-endpoint" {
					if _, err := p.Sync(t.Context(), rc); err != nil {
						t.Fatal(err)
					}
					if backend.repairs != 0 || backend.acks == 0 {
						t.Fatalf("healthy logical endpoint repaired: repairs=%d acks=%d", backend.repairs, backend.acks)
					}
					return
				}
				rc.Cluster.Generation++
				if _, err := p.Sync(t.Context(), rc); err != nil {
					t.Fatal(err)
				}
				want := 0
				if scenario == "matching-published" {
					want = 1
				}
				if backend.repairs != want || backend.acks != 0 {
					t.Fatalf("repair conditions bypassed: repairs=%d want=%d acks=%d", backend.repairs, want, backend.acks)
				}
				if rc.Cluster.Status.ObservedGeneration == rc.Cluster.Generation {
					t.Fatal("repair submission acknowledged generation without a successful logical probe")
				}
				if scenario != "matching-published" {
					return
				}
				status, err := p.Status(t.Context(), rc)
				if err != nil || status.Phase == domain.PhaseReady {
					t.Fatalf("platform repair submission falsely acknowledged readiness: %+v err=%v", status, err)
				}
				// Only a fresh successful logical probe may acknowledge recovery.
				agents.unreachable = false
				rc.Agents = agents
				if _, err := p.Sync(t.Context(), rc); err != nil {
					t.Fatal(err)
				}
				if backend.acks == 0 {
					t.Fatal("successful logical probe did not acknowledge repair")
				}
			})
		}
	}
}
