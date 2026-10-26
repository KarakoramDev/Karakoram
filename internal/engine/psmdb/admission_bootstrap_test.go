// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package psmdb

import (
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/infra/fake"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestCreateReplicaSetWithDeploymentAdmission(t *testing.T) {
	for _, backend := range []string{"zerops", "dokploy"} {
		for _, members := range []int{3, 5} {
			for _, cap := range []int{1, 2} {
				t.Run(fmt.Sprintf("%s/members%d/cap%d", backend, members, cap), func(t *testing.T) {
					p, rc, b, _ := setup(t)
					rc.Cluster.Spec.Components.Engine.Replicas = members
					rc.Cluster.Spec.TLS.Mode = "disabled"
					s, _ := load(rc.Cluster)
					plan(rc.Cluster, s)
					if err := p.ensureSecrets(t.Context(), rc, s); err != nil {
						t.Fatal(err)
					}
					if err := save(rc.Cluster, s); err != nil {
						t.Fatal(err)
					}
					native := nativeHealingFixture(t, rc, s)
					native.command = func(bson.Raw) bson.M { return bson.M{"ok": 1} }
					server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						switch r.URL.Path {
						case "/v1/status":
							current, _ := load(rc.Cluster)
							json.NewEncoder(w).Encode(agentapi.Status{ConfigVersion: current.ConfigVersion, EngineVersion: rc.Cluster.Spec.Version, Processes: map[string]agentapi.ProcessStatus{"mongod": {State: "running"}}})
						case "/v1/bootstrap":
							var request agentapi.BootstrapRequest
							if err := json.NewDecoder(r.Body).Decode(&request); err != nil || len(request.Members) != members {
								t.Errorf("invalid bootstrap membership: count=%d err=%v", len(request.Members), err)
							}
							json.NewEncoder(w).Encode(agentapi.BootstrapResponse{Initialized: true, UserCreated: true})
						default:
							json.NewEncoder(w).Encode(agentapi.ActionResponse{Success: true})
						}
					}))
					defer server.Close()
					rc.Agents = testDialer{server.URL, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})}
					peers := make([]string, members)
					for i := range members {
						peers[i] = fmt.Sprintf("demom%d", i)
					}
					admission := &fake.Admission{Backend: b, Limit: cap, Peers: peers, LegacyNative: os.Getenv("KARAKORAM_TEST_LEGACY_NATIVE_READINESS") == "1"}
					rc.Backend = admission
					for range 250 {
						admission.Advance()
						if _, err := p.sync(t.Context(), rc, p.prepareDisruption, healthyTestReplication); err != nil {
							t.Fatal(err)
						}
						current, _ := load(rc.Cluster)
						if current.Phase == "ready" && current.Pending == nil {
							if len(admission.Submitted) != members || admission.Peak > cap || len(rc.Cluster.Status.DeploymentFailures) != 0 {
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

func TestRouterDeploymentRetainsNativeReadinessWithHealthyDependencies(t *testing.T) {
	p, rc, _, _ := setup(t)
	rc.Cluster.Status = domain.ClusterStatus{ObservedAt: time.Now(), ObservedGeneration: 1, Members: []domain.MemberStatus{{Name: "democ0", Health: true, Agent: domain.AgentStatus{Healthy: true}}}}
	m := &member{Name: "demor", Role: agentapi.RoleRouter}
	spec := p.runtimeSpec(rc, m, rc.Cluster.Spec.Version)
	if spec.Readiness.Args[2] != "--engine=psmdb" || m.NativeReadiness == nil || !*m.NativeReadiness {
		t.Fatal("router lost native overlap readiness", spec.Readiness)
	}
	rc.Cluster.Status = domain.ClusterStatus{}
	raw, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	var restored member
	if err := json.Unmarshal(raw, &restored); err != nil {
		t.Fatal(err)
	}
	retry := p.runtimeSpec(rc, &restored, rc.Cluster.Spec.Version)
	if fmt.Sprint(retry.Readiness.Args) != fmt.Sprint(spec.Readiness.Args) {
		t.Fatal("restart changed probe", retry.Readiness)
	}
}
