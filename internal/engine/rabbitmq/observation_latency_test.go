// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package rabbitmq

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/rabbitmq/rmqspec"
)

type coldObservationAgents struct{ options agentapi.ClientOptions }

func (d coldObservationAgents) Dial(context.Context, domain.ClusterRef, string) (*agentapi.Client, error) {
	return agentapi.NewClient(d.options)
}

func waitObservationLatency(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestObservationColdTLSAndSlowBrokerFitsSweep(t *testing.T) {
	p, rc, _, _ := setup(t)
	rc.Cluster.Spec.Components.Engine.Replicas = 3
	ready(t, p, rc)
	rc.Clock = engine.RealClock{}
	nodes := []string{}
	for _, m := range ordered(state(t, rc)) {
		nodes = append(nodes, "rabbit@"+m.Name)
	}
	var brokerUnavailable atomic.Bool
	var managementCalls atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		delay := time.Second
		if r.URL.Path == "/v1/rabbitmq/observe" {
			delay = 1500 * time.Millisecond // Includes the local management HTTP calls.
		}
		if err := waitObservationLatency(r.Context(), delay); err != nil {
			return
		}
		switch r.URL.Path {
		case "/v1/status":
			_ = json.NewEncoder(w).Encode(agentapi.Status{Processes: map[string]agentapi.ProcessStatus{"rabbitmq-server": {State: "running"}}})
		case "/v1/rabbitmq/observe":
			if brokerUnavailable.Load() {
				http.Error(w, "broker unavailable", http.StatusServiceUnavailable)
				return
			}
			_ = json.NewEncoder(w).Encode(rmqspec.StatusResponse{Version: rc.Cluster.Spec.Version, RunningNodes: nodes})
		default:
			managementCalls.Add(1)
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	rc.Agents = coldObservationAgents{options: agentapi.ClientOptions{
		BaseURL: server.URL, Token: "test-token", CAPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}),
		DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
			// Every transport starts cold, including the independent engine
			// transport; do not let cached clients hide connection setup cost.
			if err := waitObservationLatency(ctx, 1500*time.Millisecond); err != nil {
				return nil, err
			}
			return (&net.Dialer{}).DialContext(ctx, network, address)
		},
	}}
	rc.Backend = &delayedStatusBackend{Backend: rc.Backend}
	status, err := engine.ObserveStatus(t.Context(), p, rc)
	if err != nil || status.Phase != domain.PhaseReady || status.ObservedAt.IsZero() {
		t.Fatalf("cold healthy broker could not establish fresh readiness: %+v, %v", status, err)
	}
	for _, member := range status.Members {
		if !member.Health || !member.Agent.Healthy || member.Agent.LastSeen.Before(status.ObservedAt) {
			t.Fatalf("positive member claim was not fresh: %+v", member)
		}
	}
	if managementCalls.Load() != 0 {
		t.Fatal("status unexpectedly used the CLI status RPC or a mutation")
	}
	// A completed failed native probe must still withdraw readiness even
	// when the generic agent and platform inventory remain healthy.
	brokerUnavailable.Store(true)
	status, err = engine.ObserveStatus(t.Context(), p, rc)
	if err != nil || status.Phase == domain.PhaseReady {
		t.Fatalf("unavailable broker retained Ready: %+v, %v", status, err)
	}
	for _, member := range status.Members {
		if member.Health {
			t.Fatalf("unavailable broker retained positive health: %+v", member)
		}
	}
}

func TestBrokerObservationFallsBackOnlyForLegacyAgent(t *testing.T) {
	for _, code := range []int{http.StatusNotFound, http.StatusUnauthorized, http.StatusServiceUnavailable} {
		t.Run(http.StatusText(code), func(t *testing.T) {
			p, rc, _, _ := setup(t)
			var legacy atomic.Int32
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/v1/rabbitmq/observe" {
					http.Error(w, "observation unavailable", code)
					return
				}
				if r.URL.Path == "/v1/rabbitmq/status" {
					legacy.Add(1)
					_ = json.NewEncoder(w).Encode(rmqspec.StatusResponse{Version: "4.3.6"})
					return
				}
				http.NotFound(w, r)
			}))
			defer server.Close()
			rc.Agents = coldObservationAgents{options: agentapi.ClientOptions{
				BaseURL: server.URL, Token: "test-token", CAPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}),
			}}
			status, err := p.brokerStatus(t.Context(), rc, "demom0")
			if code == http.StatusNotFound {
				if err != nil || status.Version != "4.3.6" || legacy.Load() != 1 {
					t.Fatalf("old agent lost its existing status protocol: %+v, %v, legacy=%d", status, err, legacy.Load())
				}
			} else if err == nil || legacy.Load() != 0 {
				t.Fatalf("failed fresh HTTP probe was hidden by CLI fallback: %+v, %v, legacy=%d", status, err, legacy.Load())
			}
		})
	}
}

func TestFormationAllowsColdCLIWithoutManagedHTTPUser(t *testing.T) {
	p, rc, _, _ := setup(t)
	var httpObservation atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/status":
			_ = json.NewEncoder(w).Encode(agentapi.Status{Processes: map[string]agentapi.ProcessStatus{"rabbitmq-server": {State: "running"}}})
		case "/v1/rabbitmq/status":
			// Formation uses CLI before HTTP credentials exist, but its read
			// must complete inside the short observation budget.
			if err := waitObservationLatency(r.Context(), 500*time.Millisecond); err != nil {
				return
			}
			_ = json.NewEncoder(w).Encode(rmqspec.StatusResponse{Version: "4.3.6", RunningNodes: []string{"rabbit@demom0"}})
		case "/v1/rabbitmq/observe":
			httpObservation.Add(1)
			http.Error(w, "managed HTTP user does not exist before bootstrap/join", http.StatusUnauthorized)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	rc.Agents = coldObservationAgents{options: agentapi.ClientOptions{
		BaseURL: server.URL, Token: "test-token", CAPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}),
	}}
	healthy, err := p.healthy(t.Context(), rc, &member{Name: "demom0", Stage: "deployed"})
	if err != nil || !healthy || httpObservation.Load() != 0 {
		t.Fatalf("bounded CLI formation failed or required an uncreated HTTP user: healthy=%v, %v, http=%d", healthy, err, httpObservation.Load())
	}
}

func TestSyncReadinessUsesOneConcurrentNativeProbePerMember(t *testing.T) {
	p, rc, _, _ := setup(t)
	ready(t, p, rc)
	nodes := []string{}
	for _, m := range state(t, rc).Members {
		nodes = append(nodes, "rabbit@"+m.Name)
	}
	var calls atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/status":
			_ = json.NewEncoder(w).Encode(agentapi.Status{ConfigVersion: 100, Processes: map[string]agentapi.ProcessStatus{"rabbitmq-server": {State: "running"}}})
		case "/v1/rabbitmq/observe":
			calls.Add(1)
			if err := waitObservationLatency(r.Context(), 100*time.Millisecond); err != nil {
				return
			}
			_ = json.NewEncoder(w).Encode(rmqspec.StatusResponse{Version: "4.3.6", RunningNodes: nodes})
		default:
			t.Errorf("readiness performed unexpected RPC %s", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	rc.Agents = coldObservationAgents{options: agentapi.ClientOptions{BaseURL: server.URL, Token: "test-token", CAPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})}}
	ctx, cancel := context.WithTimeout(t.Context(), 400*time.Millisecond)
	defer cancel()
	_, err := p.Sync(ctx, rc)
	if err != nil || state(t, rc).Phase != "ready" || calls.Load() != 3 {
		t.Fatalf("readiness serialized or duplicated native probes: count=%d err=%v phase=%s", calls.Load(), err, state(t, rc).Phase)
	}
}

func TestMemberHealthProbesIndependentEndpointsConcurrently(t *testing.T) {
	p, rc, _, _ := setup(t)
	var calls atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := waitObservationLatency(r.Context(), 100*time.Millisecond); err != nil {
			return
		}
		calls.Add(1)
		switch r.URL.Path {
		case "/v1/status":
			_ = json.NewEncoder(w).Encode(agentapi.Status{ConfigVersion: 10, Processes: map[string]agentapi.ProcessStatus{"rabbitmq-server": {State: "running"}}})
		case "/v1/rabbitmq/observe":
			_ = json.NewEncoder(w).Encode(rmqspec.StatusResponse{Version: "4.3.6", RunningNodes: []string{"rabbit@demom0"}})
		default:
			t.Errorf("unexpected health RPC %s", r.URL.Path)
		}
	}))
	defer server.Close()
	rc.Agents = coldObservationAgents{options: agentapi.ClientOptions{BaseURL: server.URL, Token: "test-token", CAPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})}}
	ctx, cancel := context.WithTimeout(t.Context(), 175*time.Millisecond)
	defer cancel()
	_, healthy, err := p.memberHealth(ctx, rc, &member{Name: "demom0", Stage: "ready", ConfigVersion: 10})
	if err != nil || !healthy || calls.Load() != 2 {
		t.Fatalf("independent probes consumed serial budgets: healthy=%v calls=%d err=%v", healthy, calls.Load(), err)
	}
}

func TestRemovalChecksSurvivorsWithinOneProbeBudget(t *testing.T) {
	p, rc, _, _ := setup(t)
	ready(t, p, rc)
	s := state(t, rc)
	retiring := s.find("demom2")
	retiring.Removing, retiring.RemoveStep, retiring.GrowIndex = true, "grow", 2
	var coldProbes atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Only the two initial survivor pairs are cold. Subsequent queue
		// administration selects a healthy seed independently of this gate.
		if coldProbes.Add(1) <= 4 {
			if err := waitObservationLatency(r.Context(), 100*time.Millisecond); err != nil {
				return
			}
		}
		switch r.URL.Path {
		case "/v1/status":
			_ = json.NewEncoder(w).Encode(agentapi.Status{ConfigVersion: 100, Processes: map[string]agentapi.ProcessStatus{"rabbitmq-server": {State: "running"}}})
		case "/v1/rabbitmq/observe":
			_ = json.NewEncoder(w).Encode(rmqspec.StatusResponse{Version: "4.3.6", RunningNodes: []string{"rabbit@demom0", "rabbit@demom1", "rabbit@demom2"}})
		default:
			t.Errorf("unexpected removal RPC %s", r.URL.Path)
		}
	}))
	defer server.Close()
	rc.Agents = coldObservationAgents{options: agentapi.ClientOptions{BaseURL: server.URL, Token: "test-token", CAPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})}}
	ctx, cancel := context.WithTimeout(t.Context(), 175*time.Millisecond)
	defer cancel()
	if err := p.removeStep(ctx, rc, s, retiring); err != nil || retiring.RemoveStep != "streams" {
		t.Fatalf("survivor gates serialized independent reads: checkpoint=%s err=%v", retiring.RemoveStep, err)
	}
}

func TestAvailableSeedsProbeAllMembersConcurrently(t *testing.T) {
	p, rc, _, _ := setup(t)
	ready(t, p, rc)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := waitObservationLatency(r.Context(), 100*time.Millisecond); err != nil {
			return
		}
		switch r.URL.Path {
		case "/v1/status":
			_ = json.NewEncoder(w).Encode(agentapi.Status{ConfigVersion: 100, Processes: map[string]agentapi.ProcessStatus{"rabbitmq-server": {State: "running"}}})
		case "/v1/rabbitmq/observe":
			_ = json.NewEncoder(w).Encode(rmqspec.StatusResponse{Version: "4.3.6", RunningNodes: []string{"rabbit@demom0", "rabbit@demom1", "rabbit@demom2"}})
		default:
			t.Errorf("unexpected seed RPC %s", r.URL.Path)
		}
	}))
	defer server.Close()
	rc.Agents = coldObservationAgents{options: agentapi.ClientOptions{BaseURL: server.URL, Token: "test-token", CAPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})}}
	ctx, cancel := context.WithTimeout(t.Context(), 175*time.Millisecond)
	defer cancel()
	seeds := p.availableMembers(ctx, rc, state(t, rc))
	if len(seeds) != 3 {
		t.Fatalf("serial seed probes consumed another member's deadline: got %d seeds", len(seeds))
	}
	for i, seed := range seeds {
		if seed.Name != []string{"demom0", "demom1", "demom2"}[i] {
			t.Fatalf("parallel seed completion changed deterministic preference: %s", seed.Name)
		}
	}
}

func TestRollingWaitProbesConfigAndMembershipConcurrently(t *testing.T) {
	p, rc, _, _ := setup(t)
	ready(t, p, rc)
	s := state(t, rc)
	m := s.find("demom0")
	m.ChangeAction, m.ChangeStep = "restart", "wait"
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := waitObservationLatency(r.Context(), 100*time.Millisecond); err != nil {
			return
		}
		switch r.URL.Path {
		case "/v1/status":
			_ = json.NewEncoder(w).Encode(agentapi.Status{ConfigVersion: 100, Processes: map[string]agentapi.ProcessStatus{"rabbitmq-server": {State: "running"}}})
		case "/v1/rabbitmq/observe":
			_ = json.NewEncoder(w).Encode(rmqspec.StatusResponse{Version: "4.3.6", RunningNodes: []string{"rabbit@demom0", "rabbit@demom1", "rabbit@demom2"}})
		default:
			t.Errorf("unexpected rolling RPC %s", r.URL.Path)
		}
	}))
	defer server.Close()
	rc.Agents = coldObservationAgents{options: agentapi.ClientOptions{BaseURL: server.URL, Token: "test-token", CAPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})}}
	ctx, cancel := context.WithTimeout(t.Context(), 175*time.Millisecond)
	defer cancel()
	if _, err := p.changeStep(ctx, rc, s, m); err != nil || m.ChangeStep != "revive" {
		t.Fatalf("rolling wait serialized independent safety observations: checkpoint=%s err=%v", m.ChangeStep, err)
	}
}

func TestStatusDoesNotProbeUnprovisionedAgents(t *testing.T) {
	p, rc, _, _ := setup(t)
	ready(t, p, rc)
	s := state(t, rc)
	s.Phase = "import"
	for _, m := range s.Members {
		m.Stage = "planned"
	}
	if err := s.save(rc.Cluster); err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		http.Error(w, "runtime has not been provisioned", http.StatusServiceUnavailable)
	}))
	defer server.Close()
	rc.Agents = coldObservationAgents{options: agentapi.ClientOptions{BaseURL: server.URL, Token: "test-token", CAPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})}}
	status, err := p.Status(t.Context(), rc)
	if err != nil || calls.Load() != 0 || status.Phase == domain.PhaseReady {
		t.Fatalf("unprovisioned member observation waited for an agent that cannot establish readiness: calls=%d phase=%s err=%v", calls.Load(), status.Phase, err)
	}
	for _, m := range status.Members {
		if m.Health || m.Agent.Healthy || !m.Agent.LastSeen.IsZero() {
			t.Fatalf("skipped observation invented member availability: %+v", m)
		}
	}
	for _, m := range s.Members {
		m.Stage = "imported"
	}
	if err := s.save(rc.Cluster); err != nil {
		t.Fatal(err)
	}
	status, err = p.Status(t.Context(), rc)
	if err != nil || calls.Load() != 0 || status.Phase == domain.PhaseReady {
		t.Fatalf("imported member without an observed deployment was probed or declared Ready: calls=%d phase=%s err=%v", calls.Load(), status.Phase, err)
	}
}
