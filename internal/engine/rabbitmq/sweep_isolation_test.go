// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package rabbitmq

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/rabbitmq/rmqspec"
)

type sweepIsolationAgents struct{ options agentapi.ClientOptions }

func (d sweepIsolationAgents) Dial(_ context.Context, _ domain.ClusterRef, name string) (*agentapi.Client, error) {
	options := d.options
	options.Token = name
	return agentapi.NewClient(options)
}

func TestSweepHungNativePreservesHealthyPeers(t *testing.T) {
	p, rc, _, _ := setup(t)
	rc.Cluster.Spec.Components.Engine.Replicas = 3
	ready(t, p, rc)
	members := ordered(state(t, rc))
	nodes := make([]string, len(members))
	for i, m := range members {
		nodes[i] = "rabbit@" + m.Name
	}
	hung := members[0].Name
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		name := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		switch r.URL.Path {
		case "/v1/status":
			_ = json.NewEncoder(w).Encode(agentapi.Status{Processes: map[string]agentapi.ProcessStatus{"rabbitmq-server": {State: "running"}}})
		case "/v1/rabbitmq/observe":
			var request struct{}
			_ = json.NewDecoder(r.Body).Decode(&request)
			if name == hung {
				<-r.Context().Done()
				return
			}
			_ = json.NewEncoder(w).Encode(rmqspec.StatusResponse{Version: rc.Cluster.Spec.Version, RunningNodes: nodes})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	rc.Clock = engine.RealClock{}
	rc.Agents = sweepIsolationAgents{options: agentapi.ClientOptions{BaseURL: server.URL, CAPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})}}
	status, err := engine.ObserveStatus(t.Context(), p, rc)
	if err != nil || status.ObservedAt.IsZero() || status.Phase == domain.PhaseReady {
		t.Fatalf("hung native sweep lost snapshot or retained Ready: %+v %v", status, err)
	}
	if len(status.Members) != len(members) {
		t.Fatalf("native timeout removed members: got %d want %d", len(status.Members), len(members))
	}
	for _, m := range status.Members {
		if !m.Agent.Healthy || m.Agent.LastSeen.Before(status.ObservedAt) {
			t.Fatalf("reachable agent lost connectivity: %+v", m)
		}
		if m.Name == hung {
			if m.Health || !strings.Contains(m.Agent.Message, "rabbitmq-diagnostics/cluster_status") {
				t.Fatalf("missing native diagnosis: %+v", m)
			}
		} else if !m.Health || m.Version != rc.Cluster.Spec.Version || m.State == "UNKNOWN" {
			t.Fatalf("healthy peer lost native evidence: %+v", m)
		}
	}
	for _, c := range status.Conditions {
		if c.Type == "Ready" && c.Status == "True" {
			t.Fatal("false Ready", c)
		}
	}
}
