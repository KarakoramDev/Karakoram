// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"encoding/json"
	"encoding/pem"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/elastic/agentspec"
)

func TestPipelineConfigurationFailureTerminatesConvergenceAndRemainsRepairable(t *testing.T) {
	p, rc, f, _ := coreSetup(t, 1)
	component := rc.Cluster.Spec.Components.Extra[kindLogstash]
	component.Replicas = 1
	rc.Cluster.Spec.Components.Extra[kindLogstash] = component
	coreConverge(t, p, rc, f)
	observation := agentspec.Readiness{Component: "logstash", ConfigVersion: 1 << 30, ConfigurationError: true, Message: `Logstash pipeline "broken" configuration failed: LogStash::ConfigurationError at line 1, column 8`}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/status":
			_ = json.NewEncoder(w).Encode(agentapi.Status{ConfigVersion: 1 << 30, Processes: map[string]agentapi.ProcessStatus{"elasticsearch": {State: "running"}, "logstash": {State: "running"}}})
		case "/v1/elasticsearch/readiness":
			_ = json.NewEncoder(w).Encode(observation)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	rc.Agents = coreAgentDialer{server.URL, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})}
	// ErrInvalid is the reconciler's permanent-error contract: create/update tracking
	// fails on this observation, instead of spending the ordinary readiness deadline.
	for _, stage := range []string{stageJoined, stageDeployed} {
		s, err := load(rc.Cluster)
		if err != nil {
			t.Fatal(err)
		}
		s.find("demols").Stage = stage
		if err := save(rc.Cluster, s); err != nil {
			t.Fatal(err)
		}
		if _, err := p.Sync(t.Context(), rc); !errors.Is(err, engine.ErrInvalid) || !strings.Contains(err.Error(), "ConfigurationError") {
			t.Fatalf("configuration failure at %s did not return a terminal convergence error: %v", stage, err)
		}
	}
	status, err := p.Status(t.Context(), rc)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, condition := range status.Conditions {
		if condition.Type == "ComponentsReady" && condition.Status == "False" && strings.Contains(condition.Message, "ConfigurationError at line 1, column 8") {
			found = true
		}
	}
	if !found || !strings.Contains(status.Message, "ConfigurationError") {
		t.Fatalf("pipeline diagnostics missing: %+v", status)
	}
	observation.ConfigurationError, observation.Ready, observation.Message = false, true, ""
	for range 2 {
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal("repair failed", err)
		}
	}
	s, err := load(rc.Cluster)
	if err != nil || s.Phase != "ready" {
		t.Fatalf("repaired pipeline remained blocked: %+v %v", s, err)
	}
}
