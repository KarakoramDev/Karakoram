// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package supabase

import (
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"github.com/KarakoramDev/Karakoram/internal/engine/supabase/sbspec"
	"github.com/KarakoramDev/Karakoram/internal/infra/fake"
)

func TestCreateHAWithDeploymentAdmission(t *testing.T) {
	for _, backend := range []string{"zerops", "dokploy"} {
		for _, members := range []int{3, 5} {
			for _, cap := range []int{1, 2} {
				t.Run(fmt.Sprintf("%s/members%d/cap%d", backend, members, cap), func(t *testing.T) {
					p, rc, b := setupProvider(t)
					rc.Cluster.Spec.Topology.Type = "ha"
					rc.Cluster.Spec.Components.Engine.Replicas = members
					server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						name := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
						config, err := p.MemberConfig(r.Context(), rc, name)
						if err != nil {
							http.Error(w, "config pending", http.StatusServiceUnavailable)
							return
						}
						switch r.URL.Path {
						case "/v1/status":
							json.NewEncoder(w).Encode(agentapi.Status{ConfigVersion: config.ConfigVersion})
						case "/v1/postgres/status":
							role := "replica"
							if name == "demop0" {
								role = "primary"
							}
							if strings.HasSuffix(name, "px") {
								role = "proxy"
							}
							status := pgspec.StatusResponse{Running: true, Role: role, Leader: "demop0", Streaming: role == "replica", EtcdHealthy: true, Version: rc.Cluster.Spec.Version, Timeline: 1, SystemID: "123456789", WALPosition: "1/100", SlotStatusKnown: true, WALStatusKnown: true, ArchiveStatusKnown: true}
							if role == "proxy" {
								status.ProxyBackends = []pgspec.ProxyBackend{{Host: "demop0", Hostgroup: 10, Status: "ONLINE"}}
							}
							json.NewEncoder(w).Encode(status)
						case "/v1/supabase/status":
							json.NewEncoder(w).Encode(sbspec.Status{Ready: true, GatewayUpstreamHosts: b.gatewayHosts})
						default:
							json.NewEncoder(w).Encode(pgspec.ActionResponse{OK: true})
						}
					}))
					defer server.Close()
					rc.Agents = testAgents{url: server.URL, ca: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})}
					peers := make([]string, members)
					for i := range members {
						peers[i] = fmt.Sprintf("demop%d", i)
					}
					admission := &fake.Admission{Backend: b, Limit: cap, Peers: peers, LegacyNative: os.Getenv("KARAKORAM_TEST_LEGACY_NATIVE_READINESS") == "1"}
					rc.Backend = admission
					for range 300 {
						admission.Advance()
						if _, err := p.Sync(t.Context(), rc); err != nil {
							t.Fatal(err)
						}
						status, err := p.Status(t.Context(), rc)
						if err != nil {
							t.Fatal(err)
						}
						if status.Phase == domain.PhaseReady && rc.Cluster.Status.ObservedGeneration == rc.Cluster.Generation {
							if len(admission.Submitted) < members || admission.Peak > cap || len(rc.Cluster.Status.DeploymentFailures) != 0 {
								t.Fatalf("invalid completion: submitted=%d peak=%d failures=%v", len(admission.Submitted), admission.Peak, rc.Cluster.Status.DeploymentFailures)
							}
							if !admission.Ready() {
								t.Fatalf("model not natively converged: probes=%v", admission.ProbeErrors)
							}
							return
						}
					}
					t.Fatalf("admission bootstrap did not converge: commands=%v probes=%v", admission.Commands, admission.ProbeErrors)
				})
			}
		}
	}
}

func TestFrontendDeploymentRetainsNativeReadinessWithHealthyDependencies(t *testing.T) {
	p, rc, _ := setupProvider(t)
	rc.Cluster.Status = domain.ClusterStatus{ObservedAt: time.Now(), ObservedGeneration: 1, Members: []domain.MemberStatus{{Name: "demop0", Health: true, Agent: domain.AgentStatus{Healthy: true}}}}
	m := &componentMember{Name: "demosd", Component: "studio"}
	spec, err := p.runtimeSpec(rc, m)
	if err != nil {
		t.Fatal(err)
	}
	if spec.Readiness.Args[2] != "--engine=supabase" || m.NativeReadiness == nil || !*m.NativeReadiness {
		t.Fatal("frontend lost native overlap readiness", spec.Readiness)
	}
	rc.Cluster.Status = domain.ClusterStatus{}
	raw, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	var restored componentMember
	if err := json.Unmarshal(raw, &restored); err != nil {
		t.Fatal(err)
	}
	retry, err := p.runtimeSpec(rc, &restored)
	if err != nil || fmt.Sprint(retry.Readiness.Args) != fmt.Sprint(spec.Readiness.Args) {
		t.Fatal("restart changed probe", retry.Readiness, err)
	}
}

func TestMigrationApplyFailureIsVisibleInComponentStatus(t *testing.T) {
	p, rc, _ := setupProvider(t)
	convergeProvider(t, p, rc)
	state, err := loadState(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	diagnostic := "migration exited with status 1 while database unavailable"
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(agentapi.Status{ConfigVersion: 1 << 30, Error: diagnostic})
	}))
	defer server.Close()
	rc.Agents = testAgents{url: server.URL, ca: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})}
	status, err := componentStatus(t.Context(), rc, state.Members["gateway"])
	if err != nil {
		t.Fatal(err)
	}
	if status.Ready || !strings.Contains(status.Message, diagnostic) {
		t.Fatal("migration error hidden from component status", status)
	}
}
