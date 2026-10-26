// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package rabbitmq

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"syscall"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

type rollingColdAgents struct {
	engine.AgentDialer
	member string
}

func (a rollingColdAgents) Dial(ctx context.Context, ref domain.ClusterRef, member string) (*agentapi.Client, error) {
	if member == a.member {
		return nil, &url.Error{Op: "Post", URL: "https://" + member + ":9443/v1/reload", Err: &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}}
	}
	return a.AgentDialer.Dial(ctx, ref, member)
}

func TestRollingReplacementColdAgentUsesObservationCadence(t *testing.T) {
	p, rc, _, agents := setup(t)
	ready(t, p, rc)
	s := state(t, rc)
	m := ordered(s)[0]
	beginChange(rc, s, m, "resources")
	m.ChangeStep = "reload"
	if err := s.save(rc.Cluster); err != nil {
		t.Fatal(err)
	}
	rc.Agents = rollingColdAgents{AgentDialer: agents, member: m.Name}
	// r15 m1's first agent configuration was 172 seconds after Nomad
	// reported running. Keep the checkpoint and diagnostics on every sample;
	// exponential backoff must not turn these samples into 60-second gaps.
	for elapsed := time.Duration(0); elapsed < 172*time.Second; elapsed += engine.ReadinessInterval {
		result, err := p.Sync(context.Background(), rc)
		if err != nil {
			t.Fatalf("startup entered exponential error backoff at %s: %v", elapsed, err)
		}
		if !errors.Is(result.ConvergenceError, syscall.ECONNREFUSED) || result.RequeueAfter != engine.ReadinessInterval {
			t.Fatalf("startup diagnostic/cadence lost: %+v", result)
		}
		if got := state(t, rc).find(m.Name).ChangeStep; got != "reload" {
			t.Fatalf("advanced unavailable member: %s", got)
		}
	}
	rc.Agents = agents
	if _, err := p.Sync(context.Background(), rc); err != nil {
		t.Fatal(err)
	}
	if got := state(t, rc).find(m.Name).ChangeStep; got != "start" {
		t.Fatalf("recovered agent did not advance: %s", got)
	}
}

func TestRollingReplacementBootingBrokerUsesObservationCadence(t *testing.T) {
	p, rc, _, agents := setup(t)
	ready(t, p, rc)
	s := state(t, rc)
	m := ordered(s)[0]
	beginChange(rc, s, m, "resources")
	m.ChangeStep = "wait"
	if err := s.save(rc.Cluster); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/rabbitmq/observe" {
			http.Error(w, "management listener not ready", http.StatusServiceUnavailable)
			return
		}
		if r.URL.Path == "/v1/status" {
			_ = json.NewEncoder(w).Encode(agentapi.Status{ConfigVersion: m.ConfigVersion, Processes: map[string]agentapi.ProcessStatus{"rabbitmq-server": {State: "running"}}})
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()
	rc.Agents = coldObservationAgents{options: agentapi.ClientOptions{
		BaseURL: server.URL, Token: "test-token",
		CAPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}),
	}}
	// The measured m1/m2 native start-to-listener interval is ~13 seconds.
	for elapsed := time.Duration(0); elapsed < 13*time.Second; elapsed += engine.ReadinessInterval {
		result, err := p.Sync(context.Background(), rc)
		var response *agentapi.HTTPError
		if err != nil || !errors.As(result.ConvergenceError, &response) || response.StatusCode != 503 || result.RequeueAfter != engine.ReadinessInterval {
			t.Fatalf("boot observation entered error backoff: %+v, %v", result, err)
		}
		if got := state(t, rc).find(m.Name).ChangeStep; got != "wait" {
			t.Fatalf("advanced broker without membership: %s", got)
		}
	}
	rc.Agents = agents
	if _, err := p.Sync(context.Background(), rc); err != nil {
		t.Fatal(err)
	}
	if got := state(t, rc).find(m.Name).ChangeStep; got != "revive" {
		t.Fatalf("recovered broker did not pass membership gate: %s", got)
	}
}

func TestRollingStartupClassificationRetainsRealFailures(t *testing.T) {
	observe := func(code int) error { return &agentapi.HTTPError{StatusCode: code, Action: "/v1/rabbitmq/observe"} }
	for _, tt := range []struct {
		step    string
		err     error
		pending bool
	}{
		{"reload", syscall.ECONNREFUSED, true},
		{"wait", observe(503), true},
		{"wait", observe(401), false},
		{"wait", observe(500), false},
		{"reload", observe(503), false},
		{"stop", syscall.ECONNREFUSED, false},
		{"drain", observe(503), false},
		{"reload", context.DeadlineExceeded, false},
		{"wait", errors.Join(observe(503), context.DeadlineExceeded), false},
		{"wait", errors.Join(observe(503)), true},
	} {
		if got := rollingStartupPending(tt.step, tt.err); got != tt.pending {
			t.Fatalf("%s %v: got %v", tt.step, tt.err, got)
		}
	}
}
