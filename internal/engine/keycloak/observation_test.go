// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package keycloak

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

func TestNativeReadyObservationRenewsAvailabilityLease(t *testing.T) {
	p, rc, _, _ := setup(t)
	rc.Backend = idleBackend{rc.Backend.(*backendFake)}
	converge(t, p, rc)
	at := time.Date(2026, 10, 27, 22, 0, 0, 0, time.UTC)
	rc.Clock = fixedClock{at: at}
	rc.ObservationEpoch = "current-process"
	reachable := true
	nativeReady := true
	target := engine.CurrentAgentBuild()
	identity := agentapi.AgentBuild{Version: target.Version, Commit: target.Commit, Build: target.Build}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !reachable {
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
			return
		}
		switch r.URL.Path {
		case "/v1/status":
			s, _ := load(rc.Cluster)
			json.NewEncoder(w).Encode(agentapi.Status{Agent: identity, ConfigVersion: s.ConfigVersion})
		case "/v1/keycloak/status":
			json.NewEncoder(w).Encode(map[string]bool{"ready": nativeReady})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	rc.Agents = idleAgents{server}
	status, err := engine.ObserveStatus(t.Context(), p, rc)
	if err != nil || status.Phase != domain.PhaseReady || status.ObservationEpoch != rc.ObservationEpoch || len(status.Members) != 1 {
		t.Fatal("healthy native sample was rejected by the shared freshness fence", status, err)
	}
	member := status.Members[0]
	if !member.Health || !member.Agent.Healthy || !member.Agent.LastSeen.Equal(at) {
		t.Fatal("positive agent observation lacks a fresh sample timestamp", member)
	}
	for _, build := range []agentapi.AgentBuild{identity, {Version: "old-version", Commit: "old-commit", Build: "old-build"}, {}} {
		identity = build
		observed, err := engine.ObserveStatus(t.Context(), p, rc)
		if err != nil {
			t.Fatal(err)
		}
		agent := observed.Members[0].Agent
		if agent.Version != build.Version || agent.Commit != build.Commit || agent.Build != build.Build {
			t.Fatal("generic executable identity lost", agent, build)
		}
		if (len(observed.Agents.Unknown) == 1) != (build.Build == "") ||
			(len(observed.Agents.Outdated) == 1) != (build.Build != "" && build.Build != target.Build) {
			t.Fatal("incorrect agent fleet classification", observed.Agents, build)
		}
	}
	if got := engine.FreshStatus(status, at.Add(engine.ReadinessMaxAge+time.Second)); got.Members[0].Health || got.Members[0].Agent.Healthy {
		t.Fatal("expired sample remained available")
	}
	rc.Cluster.Status = status
	nativeReady = false
	status, err = engine.ObserveStatus(t.Context(), p, rc)
	if err != nil || status.Phase == domain.PhaseReady || status.Members[0].Health || !status.Members[0].Agent.Healthy {
		t.Fatal("native unready must remain distinct from an unreachable agent", status, err)
	}
	reachable = false
	status, _ = engine.ObserveStatus(t.Context(), p, rc)
	if status.Phase == domain.PhaseReady || status.Members[0].Health || status.Members[0].Agent.Healthy {
		t.Fatal("unreachable agent retained availability", status)
	}
}
