// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package psmdb

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type sweepRouterBackend struct{ infra.Backend }

func (b sweepRouterBackend) Containers(ctx context.Context, name string) ([]infra.Container, error) {
	if name == "demor" {
		return []infra.Container{{ID: "hung-router", Hostname: "router-a", Status: infra.ContainerRunning}}, nil
	}
	return b.Backend.Containers(ctx, name)
}

type sweepRouterAgents struct {
	engine.AgentDialer
	options agentapi.ClientOptions
}

func (d sweepRouterAgents) Dial(ctx context.Context, ref domain.ClusterRef, name string) (*agentapi.Client, error) {
	if name == "demor" {
		return agentapi.NewClient(d.options)
	}
	return d.AgentDialer.Dial(ctx, ref, name)
}

func TestSweepHungNativePreservesHealthyPeers(t *testing.T) {
	t.Run("legacy-native", func(t *testing.T) { testSweepHungRouter(t, false) })
	t.Run("embedded-native", func(t *testing.T) { testSweepHungRouter(t, true) })
}

func testSweepHungRouter(t *testing.T, embedded bool) {
	t.Helper()
	p, rc, _ := writableReadinessFixture(t)
	s, err := load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	s.Members = append(s.Members, &member{Name: "demor", Set: "router", Role: agentapi.RoleRouter, Containers: 1})
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if embedded {
			<-r.Context().Done()
			return
		}
		_ = json.NewEncoder(w).Encode(agentapi.Status{Processes: map[string]agentapi.ProcessStatus{"mongos": {State: "running"}}})
	}))
	defer server.Close()
	var nativeCalls atomic.Int32
	rc.Agents = sweepRouterAgents{AgentDialer: rc.Agents, options: agentapi.ClientOptions{
		BaseURL: server.URL, Token: "demor", CAPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}),
		DialInstanceContext: func(ctx context.Context, network, logical, instance string) (net.Conn, error) {
			if strings.HasSuffix(logical, ":27017") {
				nativeCalls.Add(1)
				<-ctx.Done()
				return nil, ctx.Err()
			}
			return (&net.Dialer{}).DialContext(ctx, network, server.Listener.Addr().String())
		},
	}}
	rc.Backend = sweepRouterBackend{rc.Backend}
	rc.Clock = engine.RealClock{}
	status, err := engine.ObserveStatus(t.Context(), p, rc)
	if err != nil || status.ObservedAt.IsZero() || status.Phase == domain.PhaseReady {
		t.Fatalf("hung native sweep lost snapshot or retained Ready: %+v %v", status, err)
	}
	if len(status.Members) != len(s.Members) {
		t.Fatalf("native timeout removed members: got %d want %d", len(status.Members), len(s.Members))
	}
	if !embedded && nativeCalls.Load() == 0 {
		t.Fatal("native endpoint was not exercised")
	}
	for _, m := range status.Members {
		if !(embedded && m.Name == "demor") && (!m.Agent.Healthy || m.Agent.LastSeen.Before(status.ObservedAt)) {
			t.Fatalf("native timeout disconnected reachable agent: %+v", m)
		}
		if m.Name == "demor" {
			diagnosis := "authenticated metadata probe"
			if embedded {
				diagnosis = "deadline"
			}
			if m.Health || !strings.Contains(m.Agent.Message, diagnosis) {
				t.Fatalf("missing native diagnosis: %+v", m)
			}
		} else if !m.Health || m.Optime.IsZero() || m.State == "UNKNOWN" {
			t.Fatalf("healthy peer lost native evidence: %+v", m)
		}
	}
	for _, c := range status.Conditions {
		if (c.Type == "Ready" || c.Type == "Healthy") && c.Status == "True" {
			t.Fatal("false readiness", c)
		}
	}
}
