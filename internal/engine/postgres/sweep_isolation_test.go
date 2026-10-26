// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func TestSweepHungNativePreservesHealthyPeers(t *testing.T) {
	p, rc, _ := setup(t, "ha")
	s := converge(t, p, rc)
	statuses := make(map[string]pgspec.StatusResponse)
	for _, m := range s.data() {
		st, err := live(t.Context(), rc, m.Name)
		if err != nil {
			t.Fatal(err)
		}
		statuses[m.Name] = st
	}
	hung := s.data()[0].Name
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		name := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		switch r.URL.Path {
		case "/v1/status":
			_ = json.NewEncoder(w).Encode(agentapi.Status{ConfigVersion: s.ConfigVersion})
		case "/v1/postgres/status":
			var request struct{}
			_ = json.NewDecoder(r.Body).Decode(&request)
			if name == hung {
				<-r.Context().Done()
				return
			}
			_ = json.NewEncoder(w).Encode(statuses[name])
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	rc.Clock = engine.RealClock{}
	rc.Agents = coldObservationAgents{options: agentapi.ClientOptions{BaseURL: server.URL, CAPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})}}
	status, err := engine.ObserveStatus(t.Context(), p, rc)
	if err != nil || status.ObservedAt.IsZero() || status.Phase == domain.PhaseReady {
		t.Fatalf("hung native sweep lost snapshot or retained Ready: %+v %v", status, err)
	}
	if len(status.Members) != len(s.Members) {
		t.Fatalf("native timeout removed members: got %d want %d", len(status.Members), len(s.Members))
	}
	for _, m := range status.Members {
		if m.Set != "postgres" {
			continue
		}
		if !m.Agent.Healthy || m.Agent.LastSeen.Before(status.ObservedAt) {
			t.Fatalf("reachable agent lost connectivity: %+v", m)
		}
		if m.Name == hung {
			if m.Health || !strings.Contains(m.Agent.Message, "restore observations") {
				t.Fatalf("missing native diagnosis: %+v", m)
			}
		} else if !m.Health || m.Role != statuses[m.Name].Role || m.Version != statuses[m.Name].Version {
			t.Fatalf("healthy peer lost native evidence: %+v", m)
		}
	}
	for _, c := range status.Conditions {
		if c.Type == "Ready" && c.Status == "True" {
			t.Fatal("false Ready", c)
		}
	}
}

func TestSweepHungDataPreservesColdProxyEvidence(t *testing.T) {
	p, rc, backend := setup(t, "ha")
	s := converge(t, p, rc)
	statuses := make(map[string]pgspec.StatusResponse)
	for _, m := range s.data() {
		st, err := live(t.Context(), rc, m.Name)
		if err != nil {
			t.Fatal(err)
		}
		statuses[m.Name] = st
		if st.Role == "primary" {
			statuses["demopx"] = pgspec.StatusResponse{Running: true, ProxyBackends: []pgspec.ProxyBackend{{Host: m.Name, Hostgroup: 10, Status: "ONLINE"}}}
		}
	}
	hung := s.data()[len(s.data())-1].Name
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		name := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if r.URL.Path == "/v1/postgres/status" {
			var request struct{}
			_ = json.NewDecoder(r.Body).Decode(&request)
			if name == hung {
				<-r.Context().Done()
				return
			}
		}
		if err := waitObservationLatency(r.Context(), 1500*time.Millisecond); err != nil {
			return
		}
		switch r.URL.Path {
		case "/v1/status":
			_ = json.NewEncoder(w).Encode(agentapi.Status{ConfigVersion: s.ConfigVersion})
		case "/v1/postgres/status":
			_ = json.NewEncoder(w).Encode(statuses[name])
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	rc.Backend = &proxyInventoryBackend{fakeBackend: backend, instances: []infra.Container{{ID: "cold-proxy", Hostname: "proxy-a", Status: infra.ContainerRunning}}}
	rc.Agents = coldObservationAgents{options: agentapi.ClientOptions{
		BaseURL: server.URL, CAPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}),
		DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
			if err := waitObservationLatency(ctx, 1500*time.Millisecond); err != nil {
				return nil, err
			}
			return (&net.Dialer{}).DialContext(ctx, network, server.Listener.Addr().String())
		},
	}}
	rc.Clock = engine.RealClock{}
	status, err := engine.ObserveStatus(t.Context(), p, rc)
	if err != nil || status.Phase == domain.PhaseReady || status.ObservedAt.IsZero() || len(status.Members) != len(s.Members) {
		t.Fatalf("hung data observation invalidated snapshot: %+v %v", status, err)
	}
	for _, m := range status.Members {
		if !m.Agent.Healthy || m.Agent.LastSeen.Before(status.ObservedAt) {
			t.Fatalf("reachable agent lost connectivity: %+v", m)
		}
		if m.Name != hung && !m.Health {
			t.Fatalf("hung data member consumed healthy cold peer's native budget: %+v", m)
		}
	}
}
