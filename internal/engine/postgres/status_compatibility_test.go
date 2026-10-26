// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func TestLegacyHealthFieldsDoNotGateReadiness(t *testing.T) {
	for _, topology := range []string{"single", "ha"} {
		for _, backup := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/backup=%t", topology, backup), func(t *testing.T) {
				p, rc, b := setup(t, topology)
				s := converge(t, p, rc)
				if backup {
					rc.Cluster.Spec.Backup.Storage = "archive-store"
				}
				b.statusOverrides = map[string]pgspec.StatusResponse{}
				for _, m := range s.data() {
					role := "standalone"
					if topology == "ha" {
						role = "replica"
						if m.Name == "demop0" {
							role = "primary"
						}
					}
					// Previous-release wire shape: no health capabilities, no
					// slot/WAL/archive-known flags and no local DCS health flag.
					payload := fmt.Sprintf(`{"role":%q,"running":true,"streaming":true,"version":%q,"systemId":"123456789","timeline":1,"walPosition":"1/100","etcdMemberId":%q}`, role, rc.Cluster.Spec.Version, m.Name+"-etcd")
					var legacy pgspec.StatusResponse
					if err := json.Unmarshal([]byte(payload), &legacy); err != nil {
						t.Fatal(err)
					}
					b.statusOverrides[m.Name] = legacy
				}
				status, err := p.Status(t.Context(), rc)
				if err != nil {
					t.Fatal(err)
				}
				if status.Phase != domain.PhaseReady {
					t.Fatalf("legacy readiness regressed: %+v", status)
				}
				unsupported := []string{"ReplicationSlots"}
				if topology == "ha" {
					unsupported = append(unsupported, "EtcdQuorum", "EtcdMembers", "Replication", "Failover")
				}
				if backup {
					unsupported = append(unsupported, "WALArchive")
				}
				for _, kind := range unsupported {
					condition := healthCondition(status, kind)
					if condition.Status != "Unknown" || condition.Reason != "Unsupported" {
						t.Errorf("%s fabricated an observation for a legacy member: %+v", kind, condition)
					}
				}
				if condition := healthCondition(status, "CredentialRecovery"); condition.Status != "False" || condition.Reason != "LegacyAgent" {
					t.Fatalf("legacy credential recovery capability was not explained: %+v", condition)
				}
			})
		}
	}
}

func TestAdvertisedHealthProbeFailureDegrades(t *testing.T) {
	for _, fault := range []string{"slots", "wal", "archive", "etcd", "distance"} {
		t.Run(fault, func(t *testing.T) {
			topology := "single"
			if fault == "etcd" || fault == "distance" {
				topology = "ha"
			}
			p, rc, b := setup(t, topology)
			converge(t, p, rc)
			name := "demop0"
			if fault == "distance" {
				name = "demop1"
			}
			observed, err := live(t.Context(), rc, name)
			if err != nil {
				t.Fatal(err)
			}
			switch fault {
			case "slots":
				observed.SlotStatusKnown = false
			case "wal":
				rc.Cluster.Spec.Backup.Storage = "archive-store"
				observed.WALStatusKnown = false
			case "archive":
				rc.Cluster.Spec.Backup.Storage = "archive-store"
				observed.ArchiveStatusKnown = false
			case "etcd":
				observed.EtcdHealthy = false
			case "distance":
				observed.SystemID = ""
			}
			b.statusOverrides = map[string]pgspec.StatusResponse{name: observed}
			status, err := p.Status(t.Context(), rc)
			if err != nil {
				t.Fatal(err)
			}
			if status.Phase != domain.PhaseDegraded || healthCondition(status, "Ready").Reason != "HealthObservationFailed" {
				t.Fatalf("capable member failed probe was treated as unsupported: %+v", status)
			}
		})
	}
}

func TestProxyStatusUsesIndependentInstanceBudgets(t *testing.T) {
	for _, scenario := range []struct {
		name      string
		instances int
		hung      int
	}{
		{"four", 4, -1},
		{"four-plus-canary", 5, -1},
		{"hung-first-plus-canary", 5, 0},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			p, rc, b := setup(t, "ha")
			s := converge(t, p, rc)
			component := rc.Cluster.Spec.Components.Extra["proxy"]
			component.Replicas = 4
			rc.Cluster.Spec.Components.Extra["proxy"] = component
			s.find("demopx").Replicas = 4
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			inventory := &proxyInventoryBackend{fakeBackend: b}
			instances := map[string]*proxyInstanceFixture{}
			generic, native := make([]atomic.Int32, scenario.instances), make([]atomic.Int32, scenario.instances)
			for i := range scenario.instances {
				name := fmt.Sprintf("proxy-%c", 'a'+i)
				instance := newProxyInstanceFixture(t, s.ConfigVersion)
				original := instance.server.Config.Handler
				instance.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					switch r.URL.Path {
					case "/v1/status":
						generic[i].Add(1)
					case "/v1/postgres/status":
						native[i].Add(1)
					}
					if i == scenario.hung && r.URL.Path == "/v1/postgres/status" {
						var request struct{}
						_ = json.NewDecoder(r.Body).Decode(&request)
						<-r.Context().Done()
						return
					}
					timer := time.NewTimer(300 * time.Millisecond)
					defer timer.Stop()
					select {
					case <-r.Context().Done():
						return
					case <-timer.C:
					}
					original.ServeHTTP(w, r)
				})
				instances[name] = instance
				inventory.instances = append(inventory.instances, infra.Container{ID: name, Hostname: name, Status: infra.ContainerRunning})
			}
			rc.Backend = inventory
			rc.Agents = proxyInstanceAgents{AgentDialer: rc.Agents, instances: instances}
			start := time.Now()
			rc.Clock = engine.RealClock{}
			status, err := engine.ObserveStatus(t.Context(), p, rc)
			if err != nil {
				t.Fatal(err)
			}
			if scenario.hung < 0 && status.Phase != domain.PhaseReady {
				t.Fatalf("healthy proxy sweep exhausted a shared deadline: %+v", status)
			}
			if scenario.hung >= 0 && status.Phase != domain.PhaseDegraded {
				t.Fatalf("hung proxy was not unhealthy: %+v", status)
			}
			if len(status.Members) != len(s.Members) || status.ObservedAt.IsZero() {
				t.Fatalf("proxy timeout discarded the sweep: %+v", status)
			}
			for _, member := range status.Members {
				if !member.Agent.Healthy {
					t.Fatalf("native timeout disconnected reachable agent: %+v", member)
				}
				if member.Role != "proxy" && !member.Health {
					t.Fatalf("proxy timeout invalidated healthy database evidence: %+v", member)
				}
				if scenario.hung >= 0 && member.Role == "proxy" && (member.Health || !strings.Contains(member.Agent.Message, "proxy-a") || !strings.Contains(member.Agent.Message, "restore")) {
					t.Fatalf("missing affected-instance diagnosis: %+v", member)
				}
			}
			if elapsed := time.Since(start); elapsed > 3*engine.ObservationTimeout {
				t.Fatalf("hung-instance sweep was not bounded: %s", elapsed)
			}
			for i := range scenario.instances {
				want := int32(1)
				if scenario.hung >= 0 {
					want = 2 // ObserveStatus retries the complete degraded sweep once.
				}
				nativeWant := want
				if i == 0 {
					// This fixture's logical-service DNS selects instance A;
					// readiness also probes that advertised endpoint separately.
					nativeWant += want
				}
				if generic[i].Load() != want || native[i].Load() != nativeWant {
					t.Errorf("instance %d not independently visited: generic=%d native=%d", i, generic[i].Load(), native[i].Load())
				}
			}
		})
	}
}
