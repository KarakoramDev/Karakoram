// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type proxyUpdateBackend struct {
	*proxyInventoryBackend
	serving       bool
	excluded      bool
	legacy        bool
	countCalls    int
	resourceCalls int
}

func (b *proxyUpdateBackend) Capabilities() infra.Capabilities {
	c := b.fakeBackend.Capabilities()
	c.ReplicaScalePreservesServing = b.serving
	c.ResourceUpdatePreservesServing = b.serving
	return c
}
func (b *proxyUpdateBackend) ResourceUpdatePreservesServingFor(_, _ infra.Resources) bool {
	return !b.excluded
}
func (b *proxyUpdateBackend) ProxyUpdatesPreserveServing(context.Context, string) (bool, error) {
	return !b.legacy, nil
}
func (b *proxyUpdateBackend) SetContainerCount(ctx context.Context, name string, min, max int) (infra.Op, error) {
	b.countCalls++
	return b.fakeBackend.SetContainerCount(ctx, name, min, max)
}
func (b *proxyUpdateBackend) SetResources(ctx context.Context, name string, r infra.Resources) (infra.Op, error) {
	if name == "demopx" {
		b.resourceCalls++
	}
	return b.fakeBackend.SetResources(ctx, name, r)
}

func TestProxyUpdateMaintenanceCapabilities(t *testing.T) {
	for _, action := range []string{"up", "down", "resources", "excluded-resources", "legacy-up", "legacy-resources"} {
		for _, serving := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/serving=%t", action, serving), func(t *testing.T) {
				p, rc, inventory, s, first, second := replicatedProxyFixture(t)
				b := &proxyUpdateBackend{proxyInventoryBackend: inventory, serving: serving, excluded: action == "excluded-resources", legacy: strings.HasPrefix(action, "legacy-")}
				rc.Backend = b
				m := s.find("demopx")
				desired := rc.Cluster.Spec.Components.Extra["proxy"]
				m.Replicas = 2
				desired.Replicas = 2
				switch strings.TrimPrefix(action, "legacy-") {
				case "up":
					desired.Replicas = 3
				case "down":
					desired.Replicas = 1
				default:
					desired.Resources.MinCPU++
				}
				rc.Cluster.Spec.Components.Extra["proxy"] = desired
				rc.Cluster.Generation++
				rc.Cluster.Spec.Maintenance = &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: fmt.Sprintf("0 0 1 %d *", int(time.Now().Month())%12+1), Duration: "1h"}}}
				if err := save(rc.Cluster, s); err != nil {
					t.Fatal(err)
				}
				syncOnce := func() {
					t.Helper()
					if _, err := p.Sync(t.Context(), rc); err != nil {
						t.Fatal(err)
					}
					current, err := load(rc.Cluster)
					if err != nil {
						t.Fatal(err)
					}
					first.applied(current.ConfigVersion)
					second.applied(current.ConfigVersion)
				}
				for range 12 {
					syncOnce()
					if b.countCalls+b.resourceCalls > 0 {
						break
					}
				}
				allowed := serving && action != "down" && action != "excluded-resources" && !b.legacy
				if got := b.countCalls+b.resourceCalls > 0; got != allowed {
					t.Fatalf("update issued=%t want=%t wait=%s", got, allowed, rc.SyncWait)
				}
				if !allowed {
					rc.Cluster.MaintenanceOverrideGeneration = rc.Cluster.Generation
					for range 12 {
						syncOnce()
						if b.countCalls+b.resourceCalls > 0 {
							break
						}
					}
					if b.countCalls+b.resourceCalls != 1 {
						t.Fatal("window override did not admit update", rc.SyncWait)
					}
				}
			})
		}
	}
}

func TestProxyUpdateCompletionRequiresEveryInstanceAndReplays(t *testing.T) {
	for _, action := range []string{"scale-up", "scale-down", "resize"} {
		for _, failure := range []string{"config", "stopped", "writer", "missing-instance"} {
			t.Run(action+"/"+failure, func(t *testing.T) {
				p, rc, b, s, _, late := replicatedProxyFixture(t)
				m := s.find("demopx")
				beforeResources := m.Resources
				beforeReplicas := 1
				if action == "scale-down" {
					beforeReplicas = 3
				}
				m.Replicas = beforeReplicas
				q := &pending{Action: "scale", Member: m.Name, Replicas: 2, Resources: m.Resources, Op: infra.Op{ID: "completed"}}
				if action == "resize" {
					q.Action = "resize"
					q.Resources.MinCPU++
					m.Replicas = 2
					beforeReplicas = 2
				}
				desired := rc.Cluster.Spec.Components.Extra["proxy"]
				desired.Replicas = 2
				rc.Cluster.Spec.Components.Extra["proxy"] = desired
				s.Pending = q
				switch failure {
				case "config":
					late.applied(m.ConfigVersion - 1)
				case "stopped":
					late.running = false
				case "writer":
					late.backends = nil
				case "missing-instance":
					b.instances = b.instances[:1]
				}
				if err := save(rc.Cluster, s); err != nil {
					t.Fatal(err)
				}
				for range 2 {
					var err error
					s, err = load(rc.Cluster)
					if err != nil {
						t.Fatal(err)
					}
					if busy, err := p.observePending(t.Context(), rc, s); err != nil || !busy {
						t.Fatal(busy, err)
					}
					m = s.find("demopx")
					if s.Pending == nil || m.Replicas != beforeReplicas || !reflect.DeepEqual(m.Resources, beforeResources) {
						t.Fatal("backend completion committed an unready proxy", s.Pending, m.Replicas, m.Resources)
					}
					if err := save(rc.Cluster, s); err != nil {
						t.Fatal(err)
					}
				}
				late.applied(m.ConfigVersion)
				late.running = true
				late.backends = []pgspec.ProxyBackend{{Host: "demop0", Hostgroup: 10, Status: "ONLINE"}}
				b.instances = []infra.Container{{ID: "a", Hostname: "proxy-a", Status: infra.ContainerRunning}, {ID: "b", Hostname: "proxy-b", Status: infra.ContainerRunning}}
				if _, err := p.observePending(t.Context(), rc, s); err != nil {
					t.Fatal(err)
				}
				if s.Pending != nil || m.Replicas != 2 || (action == "resize" && !reflect.DeepEqual(m.Resources, q.Resources)) {
					t.Fatal("ready target did not commit")
				}
			})
		}
	}
}

func TestProxySupersedingScaleRespectsLegacyMaintenance(t *testing.T) {
	p, rc, inventory, s, _, _ := replicatedProxyFixture(t)
	b := &proxyUpdateBackend{proxyInventoryBackend: inventory, serving: true, legacy: true}
	rc.Backend = b
	m := s.find("demopx")
	m.Replicas = 2
	desired := rc.Cluster.Spec.Components.Extra["proxy"]
	desired.Replicas = 2
	rc.Cluster.Spec.Components.Extra["proxy"] = desired
	rc.Cluster.Spec.Maintenance = &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: fmt.Sprintf("0 0 1 %d *", int(time.Now().Month())%12+1), Duration: "1h"}}}
	s.Pending = &pending{Action: "scale", Member: m.Name, Replicas: 3, Op: infra.Op{ID: "older-count"}}
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	var err error
	s, err = load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.observePending(t.Context(), rc, s); err != nil {
		t.Fatal(err)
	}
	if b.countCalls != 0 || s.Pending == nil || s.Pending.Replicas != 3 {
		t.Fatal("superseding legacy scale bypassed maintenance")
	}
	rc.Cluster.MaintenanceOverrideGeneration = rc.Cluster.Generation
	if _, err := p.observePending(t.Context(), rc, s); err != nil {
		t.Fatal(err)
	}
	if b.countCalls != 1 || s.Pending == nil || s.Pending.Replicas != 2 {
		t.Fatal("override did not admit superseding scale")
	}
}
