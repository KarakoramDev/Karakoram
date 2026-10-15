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

const brokenPipeline = `input {`
const workingPipeline = `input { http { port => 8080 } } output { stdout {} }`

func setTestPipeline(t *testing.T, rc *engine.Context, config string) {
	t.Helper()
	options := Options{}
	options.Logstash.Pipelines = []agentspec.Pipeline{{ID: "main", Config: config}}
	raw, err := json.Marshal(options)
	if err != nil {
		t.Fatal(err)
	}
	rc.Cluster.Spec.EngineOptions = raw
}

// Readiness follows only the protected payload published to the member, not the
// desired spec. Repair cannot pass until the new pipeline actually reaches it.
func pipelineReadinessFixture(t *testing.T, config string) (*Provider, *engine.Context, *coreInfra) {
	t.Helper()
	p, rc, f, _ := coreSetup(t, 1)
	component := rc.Cluster.Spec.Components.Extra[kindLogstash]
	component.Replicas = 1
	rc.Cluster.Spec.Components.Extra[kindLogstash] = component
	setTestPipeline(t, rc, config)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/status":
			_ = json.NewEncoder(w).Encode(agentapi.Status{ConfigVersion: 1 << 30, Processes: map[string]agentapi.ProcessStatus{"elasticsearch": {State: "running"}, "logstash": {State: "running"}}})
		case "/v1/elasticsearch/readiness":
			raw, err := secret(r.Context(), rc, "members/demols/restart-spec")
			var spec agentspec.Spec
			if err != nil || json.Unmarshal(raw, &spec) != nil || spec.Logstash == nil {
				http.Error(w, "configuration not published", http.StatusServiceUnavailable)
				return
			}
			ready := len(spec.Logstash.Pipelines) == 1 && spec.Logstash.Pipelines[0].Config != brokenPipeline
			observation := agentspec.Readiness{Component: "logstash", ConfigVersion: 1 << 30, Ready: ready, ConfigurationError: !ready}
			if !ready {
				observation.Message = `Logstash pipeline "main" configuration failed: LogStash::ConfigurationError at line 1, column 8`
			}
			_ = json.NewEncoder(w).Encode(observation)
		case "/v1/reload":
			_ = json.NewEncoder(w).Encode(map[string]bool{"success": true})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	rc.Agents = coreAgentDialer{server.URL, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})}
	return p, rc, f
}

func waitForPipelineFailure(t *testing.T, p *Provider, rc *engine.Context) bool {
	t.Helper()
	sawRejoin := false
	for range 100 {
		s, err := load(rc.Cluster)
		if err != nil {
			t.Fatal(err)
		}
		sawRejoin = sawRejoin || s.Rolling != nil && s.Rolling.Step == "rejoin"
		result, err := p.Sync(t.Context(), rc)
		if err != nil {
			t.Fatalf("component failure blocked reconciliation: %v", err)
		}
		if result.ConvergenceError != nil {
			if !errors.Is(result.ConvergenceError, engine.ErrInvalid) || !strings.Contains(result.ConvergenceError.Error(), "ConfigurationError") {
				t.Fatal(result.ConvergenceError)
			}
			return sawRejoin
		}
	}
	t.Fatal("pipeline failure never terminated generation tracking")
	return false
}

func TestPipelineConfigurationFailureTerminatesConvergenceAndRemainsRepairable(t *testing.T) {
	for _, scenario := range []string{"initial-deployment", "joined-config-edit"} {
		t.Run(scenario, func(t *testing.T) {
			initial := brokenPipeline
			if scenario == "joined-config-edit" {
				initial = workingPipeline
			}
			p, rc, f := pipelineReadinessFixture(t, initial)
			if scenario == "joined-config-edit" {
				coreConverge(t, p, rc, f)
				setTestPipeline(t, rc, brokenPipeline)
				rc.Cluster.Generation++
			}
			sawRejoin := waitForPipelineFailure(t, p, rc)
			if scenario == "joined-config-edit" && !sawRejoin {
				t.Fatal("config edit did not exercise rolling rejoin")
			}
			failed, err := load(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			oldHash := failed.find("demols").ConfigHash
			if failed.Rolling != nil || failed.find("demols").Stage != stageDeployed {
				t.Fatal("failed stateless rollout still owns rolling work", failed.Rolling)
			}
			for range 3 {
				result, err := p.Sync(t.Context(), rc)
				if err != nil || !errors.Is(result.ConvergenceError, engine.ErrInvalid) {
					t.Fatalf("repeated observation: %+v %v", result, err)
				}
			}
			warnings := 0
			for _, event := range rc.Events.(*coreEvents).events {
				if event.Reason == "ComponentConfigurationError" {
					warnings++
				}
			}
			if warnings != 1 {
				t.Fatalf("configuration warning repeated: %d", warnings)
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
			setTestPipeline(t, rc, workingPipeline)
			rc.Cluster.Generation++
			repaired := coreConverge(t, p, rc, f)
			if repaired.find("demols").ConfigHash == oldHash || repaired.find("demols").Stage != stageJoined {
				t.Fatal("repair did not republish the component configuration")
			}
			raw, err := secret(t.Context(), rc, "members/demols/restart-spec")
			var published agentspec.Spec
			if err != nil || json.Unmarshal(raw, &published) != nil || published.Logstash.Pipelines[0].Config != workingPipeline {
				t.Fatal("repaired pipeline was not published", err)
			}
		})
	}
}

func TestPipelineChangeReplansAnExistingRollingRejoin(t *testing.T) {
	p, rc, f := pipelineReadinessFixture(t, workingPipeline)
	coreConverge(t, p, rc, f)
	setTestPipeline(t, rc, brokenPipeline)
	rc.Cluster.Generation++
	for range 100 {
		s, err := load(rc.Cluster)
		if err != nil {
			t.Fatal(err)
		}
		if s.Rolling != nil && s.Rolling.Step == "rejoin" {
			oldHash := s.find("demols").ConfigHash
			setTestPipeline(t, rc, workingPipeline)
			rc.Cluster.Generation++
			result, err := p.Sync(t.Context(), rc)
			if err != nil || result.ConvergenceError != nil {
				t.Fatalf("repair observed stale configuration error: %+v %v", result, err)
			}
			s, err = load(rc.Cluster)
			if err != nil || s.Rolling == nil || s.Rolling.Step != "apply" {
				t.Fatalf("rejoin was not replanned: %+v %v", s, err)
			}
			repaired := coreConverge(t, p, rc, f)
			if repaired.find("demols").ConfigHash == oldHash {
				t.Fatal("rejoin did not publish repaired configuration")
			}
			return
		}
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
	}
	t.Fatal("rolling edit never reached rejoin")
}
