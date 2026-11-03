// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package keycloak

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

func TestAgentUpgradeReplayAndGates(t *testing.T) {
	for _, include := range []bool{false, true} {
		t.Run(map[bool]string{false: "outdated", true: "include-current"}[include], func(t *testing.T) {
			p, rc, bindings, secrets := setup(t)
			backend := rc.Backend.(*backendFake)
			rc.Backend = idleBackend{backend}
			build := "old"
			if include {
				build = engine.CurrentAgentBuild().Build
			}
			ready := true
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				s, _ := load(rc.Cluster)
				if r.URL.Path == "/v1/status" {
					json.NewEncoder(w).Encode(agentapi.Status{ConfigVersion: s.ConfigVersion, Agent: agentapi.AgentBuild{Build: build}})
					return
				}
				json.NewEncoder(w).Encode(map[string]bool{"ready": ready})
			}))
			defer server.Close()
			rc.Agents = idleAgents{server}
			converge(t, p, rc)
			if !reflect.DeepEqual(p.Describe().Operations, []string{domain.OperationUpgradeAgents}) {
				t.Fatal(p.Describe().Operations)
			}
			before := append([]byte(nil), rc.Cluster.Internal...)
			config := append([]byte(nil), secrets.values["keycloak/config/loginkc"]...)
			requests := len(bindings.requests)
			deploys := backend.deploys
			payload, _ := json.Marshal(engine.AgentUpgradePayload{TargetBuild: engine.CurrentAgentBuild().Build, IncludeCurrent: include})
			op := &domain.Operation{Kind: domain.OperationUpgradeAgents, Payload: payload}
			step := func() engine.OpResult {
				t.Helper()
				p = New(p.options)
				out, e := p.RunOperation(t.Context(), rc, op)
				if e != nil {
					t.Fatal(e)
				}
				op.Checkpoint = out.Checkpoint
				return out
			}
			step()
			var cp agentUpgradeCheckpoint
			json.Unmarshal(op.Checkpoint, &cp)
			if !reflect.DeepEqual(cp.Order, []string{"loginkc"}) {
				t.Fatal(cp.Order)
			}
			rc.Cluster.Spec.Maintenance = &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: "0 0 1 1 *", Duration: "1m"}}}
			step()
			if backend.deploys != deploys {
				t.Fatal("window bypassed")
			}
			rc.Cluster.Spec.Maintenance = nil
			step()
			if backend.deploys != deploys {
				t.Fatal("intent not persisted first")
			}
			step()
			if backend.deploys != deploys+1 {
				t.Fatal("missing deployment")
			}
			if !include && step().Done {
				t.Fatal("ignored exact target build")
			}
			ready = false
			if step().Done {
				t.Fatal("ignored native readiness")
			}
			build = engine.CurrentAgentBuild().Build
			if step().Done {
				t.Fatal("ignored native readiness with target build")
			}
			ready = true
			rc.Cluster.Spec.Maintenance = &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: "0 0 1 1 *", Duration: "1m"}}}
			if !step().Done || !step().Done {
				t.Fatal("in-flight replay did not complete")
			}
			if backend.deploys != deploys+1 || !bytes.Equal(before, rc.Cluster.Internal) || !bytes.Equal(config, secrets.values["keycloak/config/loginkc"]) || len(bindings.requests) != requests {
				t.Fatal("rollout changed applied state or binding")
			}
		})
	}
}

func TestAgentUpgradeBuildFenceAndSkip(t *testing.T) {
	p, rc, _, _ := setup(t)
	payload, _ := json.Marshal(engine.AgentUpgradePayload{TargetBuild: "different"})
	if _, err := p.RunOperation(t.Context(), rc, &domain.Operation{Kind: domain.OperationUpgradeAgents, Payload: payload}); err == nil {
		t.Fatal("missing build fence")
	}
	if _, err := p.RunOperation(t.Context(), rc, &domain.Operation{Kind: domain.OperationRestart}); err != engine.ErrNotSupported {
		t.Fatal(err)
	}
	backend := rc.Backend.(*backendFake)
	rc.Backend = idleBackend{backend}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s, _ := load(rc.Cluster)
		if r.URL.Path == "/v1/status" {
			json.NewEncoder(w).Encode(agentapi.Status{ConfigVersion: s.ConfigVersion, Agent: agentapi.AgentBuild{Build: engine.CurrentAgentBuild().Build}})
			return
		}
		json.NewEncoder(w).Encode(map[string]bool{"ready": true})
	}))
	defer server.Close()
	rc.Agents = idleAgents{server}
	converge(t, p, rc)
	deploys := backend.deploys
	payload, _ = json.Marshal(engine.AgentUpgradePayload{TargetBuild: engine.CurrentAgentBuild().Build})
	op := &domain.Operation{Kind: domain.OperationUpgradeAgents, Payload: payload}
	for range 2 {
		out, err := p.RunOperation(t.Context(), rc, op)
		if err != nil {
			t.Fatal(err)
		}
		op.Checkpoint = out.Checkpoint
	}
	if backend.deploys != deploys {
		t.Fatal("current agent redeployed")
	}
}
