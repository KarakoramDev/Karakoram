// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package rabbitmq

import (
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine/rabbitmq/rmqspec"
)

func TestStatusProcessDiagnosticsPreserveAgentConnectivity(t *testing.T) {
	for _, tc := range []struct {
		name, state, diagnostic, wantState, reason string
		brokerUnavailable                          bool
	}{
		{name: "paused-native-healthy", state: "paused", wantState: "paused", reason: "ProcessPaused"},
		{name: "paused-native-unavailable", state: "paused", wantState: "paused", reason: "ProcessPaused", brokerUnavailable: true},
		{name: "errored", state: "error", diagnostic: "supervised child failed", wantState: "error", reason: "ProcessError"},
		{name: "running-with-error", state: "running", diagnostic: "supervised child failed", wantState: "error", reason: "ProcessError"},
		{name: "stopped", state: "stopped", wantState: "DOWN"},
		{name: "healthy", state: "running", wantState: "RUNNING"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, rc, f, a := setup(t)
			rc.Cluster.Spec.Components.Engine.Replicas = 1
			ready(t, p, rc)
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/v1/status":
					_ = json.NewEncoder(w).Encode(agentapi.Status{ConfigVersion: 100, Processes: map[string]agentapi.ProcessStatus{"rabbitmq-server": {State: tc.state, Error: tc.diagnostic}}})
				case "/v1/rabbitmq/observe":
					if tc.brokerUnavailable {
						http.Error(w, "broker unavailable", http.StatusServiceUnavailable)
						return
					}
					// Deliberately positive native evidence must not hide a child diagnostic.
					_ = json.NewEncoder(w).Encode(rmqspec.StatusResponse{Version: rc.Cluster.Spec.Version, RunningNodes: []string{"rabbit@demom0"}})
				default:
					t.Errorf("observation attempted unexpected RPC %s", r.URL.Path)
					http.NotFound(w, r)
				}
			}))
			defer server.Close()
			rc.Agents = coldObservationAgents{options: agentapi.ClientOptions{BaseURL: server.URL, Token: "test-token", CAPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})}}
			mutations, actions := f.mutations, len(a.log.snapshot())
			status, err := p.Status(t.Context(), rc)
			if err != nil {
				t.Fatal(err)
			}
			if len(status.Members) != 1 {
				t.Fatalf("members: %+v", status.Members)
			}
			m := status.Members[0]
			wantHealthy := tc.name == "healthy"
			if !m.Agent.Healthy || m.Agent.LastSeen.IsZero() || m.Role != "broker" || m.State != tc.wantState || m.Health != wantHealthy {
				t.Fatalf("process diagnostic lost member truth: %+v", m)
			}
			wantPhase := domain.PhaseDegraded
			wantCondition := "False"
			if wantHealthy {
				wantPhase, wantCondition = domain.PhaseReady, "True"
			}
			if status.Phase != wantPhase {
				t.Fatalf("phase=%s, want %s", status.Phase, wantPhase)
			}
			assertLifecycleCondition(t, status, "Healthy", wantCondition)
			processConditions := 0
			for _, c := range status.Conditions {
				if !strings.HasPrefix(c.Type, "Process/") {
					continue
				}
				processConditions++
				if c.Type != "Process/demom0/rabbitmq-server" || c.Reason != tc.reason || c.Status != "False" || c.LastTransitionTime.IsZero() {
					t.Fatalf("process condition: %+v", c)
				}
				if tc.reason == "ProcessPaused" && (!strings.Contains(c.Message, "investigate") || !strings.Contains(c.Message, "SIGCONT")) {
					t.Fatalf("pause recovery guidance missing: %+v", c)
				}
			}
			wantCount := 0
			if tc.reason != "" {
				wantCount = 1
			}
			if processConditions != wantCount {
				t.Fatalf("process conditions=%d, want %d: %+v", processConditions, wantCount, status.Conditions)
			}
			healthy, err := p.healthy(t.Context(), rc, state(t, rc).Members[0])
			if tc.brokerUnavailable {
				if err == nil || healthy {
					t.Fatalf("unavailable broker passed reconcile readiness: %v, %v", healthy, err)
				}
			} else if err != nil || healthy != wantHealthy {
				t.Fatalf("reconcile readiness=%v, %v; want %v", healthy, err, wantHealthy)
			}
			if f.mutations != mutations || len(a.log.snapshot()) != actions {
				t.Fatal("process observations mutated runtime")
			}
		})
	}
}
