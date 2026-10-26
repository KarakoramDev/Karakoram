// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/elastic/agentspec"
)

func remediationUnreadyComponent(t *testing.T, rc *engine.Context, name string) {
	t.Helper()
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/status":
			opsJSON(w, agentapi.Status{ConfigVersion: 1 << 30})
		case "/v1/elasticsearch/readiness":
			opsJSON(w, agentspec.Readiness{ConfigVersion: 1 << 30, Message: "Component process is not running"})
		default:
			opsJSON(w, map[string]bool{"success": true})
		}
	}))
	t.Cleanup(server.Close)
	rc.Agents = componentInstanceDialer{AgentDialer: rc.Agents, member: name, first: server, second: server}
}

func TestRestartAllowsDisablingUnreadyComponent(t *testing.T) {
	for _, kind := range []string{kindKibana, kindLogstash, kindAPM} {
		t.Run(kind, func(t *testing.T) {
			p, rc, f, _ := coreSetup(t, 1)
			desired := rc.Cluster.Spec.Components.Extra[kind]
			desired.Replicas = 1
			rc.Cluster.Spec.Components.Extra[kind] = desired
			s := coreConverge(t, p, rc, f)
			name := s.members(kind)[0].Name
			remediationUnreadyComponent(t, rc, name)
			op := &domain.Operation{ID: "disable-during-restart", Kind: domain.OperationRestart}
			lastMessage := ""
			advance := func() bool {
				t.Helper()
				result, err := p.RunOperation(t.Context(), rc, op)
				if err != nil {
					t.Fatal(err)
				}
				op.Checkpoint = result.Checkpoint
				lastMessage = result.Message
				// Match the reconciler's public provider convergence policy.
				if !result.Done && p.SyncDuringOperation(op) {
					if _, err := p.Sync(t.Context(), rc); err != nil {
						t.Fatal(err)
					}
				}
				return result.Done
			}
			waiting := false
			for range 60 {
				if advance() {
					t.Fatal("restart acknowledged an unready component")
				}
				s, _ = load(rc.Cluster)
				if s.Rolling != nil && s.Rolling.Current == name && s.Rolling.Step == "rejoin" {
					waiting = true
					break
				}
			}
			if !waiting || advance() {
				t.Fatal("restart did not wait for component readiness")
			}
			desired.Replicas = 0
			rc.Cluster.Spec.Components.Extra[kind] = desired
			rc.Cluster.Generation++
			generation := rc.Cluster.Status.ObservedGeneration
			done := false
			for range 30 {
				if advance() {
					done = true
					break
				}
			}
			if !done {
				t.Fatal("replicas:0 remained gated by restart component readiness")
			}
			if !strings.Contains(lastMessage, "disabled components skipped") {
				t.Fatal("restart completion did not distinguish skipped components", lastMessage)
			}
			if rc.Cluster.Status.ObservedGeneration != generation {
				t.Fatal("restart acknowledged the changed generation before Sync")
			}
			s = coreConverge(t, p, rc, f)
			if s.find(name) != nil || f.services[name] != nil {
				t.Fatal("disabled component was not deleted")
			}
		})
	}
}

func TestPublicRevocationPrecedesComponentReadiness(t *testing.T) {
	for _, kind := range []string{kindKibana, kindAPM} {
		for _, rollingAlready := range []bool{false, true} {
			label := kind + "/before-roll"
			if rollingAlready {
				label = kind + "/waiting-roll"
			}
			t.Run(label, func(t *testing.T) {
				p, rc, f, _ := coreSetup(t, 1)
				desired := rc.Cluster.Spec.Components.Extra[kind]
				desired.Replicas = 1
				rc.Cluster.Spec.Components.Extra[kind] = desired
				options := map[string]any{kind: map[string]any{"public": true}}
				setOptions := func() { rc.Cluster.Spec.EngineOptions, _ = json.Marshal(options) }
				setOptions()
				s := coreConverge(t, p, rc, f)
				name := s.members(kind)[0].Name
				if !s.find(name).Subdomain {
					t.Fatal("fixture did not publish route")
				}
				remediationUnreadyComponent(t, rc, name)
				componentOptions := options[kind].(map[string]any)
				if rollingAlready {
					if kind == kindKibana {
						componentOptions["settings"] = map[string]any{"server.name": "changed"}
					} else {
						componentOptions["apiKeys"] = false
					}
					setOptions()
					waiting := false
					for range 40 {
						if _, err := p.Sync(t.Context(), rc); err != nil {
							t.Fatal(err)
						}
						s, _ = load(rc.Cluster)
						if s.Rolling != nil && s.Rolling.Current == name && s.Rolling.Step == "rejoin" {
							waiting = true
							break
						}
					}
					if !waiting {
						t.Fatal("fixture did not reach component config-roll wait")
					}
				}
				componentOptions["public"] = false
				setOptions()
				rc.Cluster.Generation++
				generation := rc.Cluster.Status.ObservedGeneration
				before := len(f.calls)
				for range 3 {
					if _, err := p.Sync(t.Context(), rc); err != nil {
						t.Fatal(err)
					}
				}
				s, _ = load(rc.Cluster)
				if !slices.Contains(f.calls[before:], "subdomain:false:"+name) || s.find(name).Subdomain {
					t.Fatal("public route revocation remained gated by component config roll/readiness")
				}
				if rc.Cluster.Status.ObservedGeneration != generation || s.Phase == "ready" {
					t.Fatal("route revocation claimed readiness or acknowledged generation")
				}
			})
		}
	}
}

func TestPublicRevocationPreservesOwnershipAndReceipt(t *testing.T) {
	for _, foreign := range []bool{false, true} {
		label := "pending-receipt"
		if foreign {
			label = "foreign-service"
		}
		t.Run(label, func(t *testing.T) {
			p, rc, f, _ := coreSetup(t, 1)
			desired := rc.Cluster.Spec.Components.Extra[kindKibana]
			desired.Replicas = 1
			rc.Cluster.Spec.Components.Extra[kindKibana] = desired
			rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"kibana":{"public":true}}`)
			s := coreConverge(t, p, rc, f)
			name := s.members(kindKibana)[0].Name
			oldHash := s.find(name).ConfigHash
			remediationUnreadyComponent(t, rc, name)
			rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"kibana":{"public":false}}`)
			if foreign {
				f.services[name].ID = "replacement-not-owned"
				f.services[name].Ownership = nil
			}
			f.done = false
			before := f.mutations
			_, err := p.Sync(t.Context(), rc)
			if foreign {
				if err == nil || f.mutations != before {
					t.Fatalf("foreign service route mutated: err=%v mutations=%d", err, f.mutations-before)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			for range 3 {
				if _, err := p.Sync(t.Context(), rc); err != nil {
					t.Fatal(err)
				}
			}
			s, _ = load(rc.Cluster)
			if f.mutations != before+1 || s.Pending == nil || s.Pending.Action != "subdomain-off" ||
				!s.find(name).Subdomain || s.find(name).ConfigHash != oldHash || s.Rolling != nil {
				t.Fatal("revocation bypassed its receipt or allowed configuration changes before completion")
			}
			f.done = true
			if _, err := p.Sync(t.Context(), rc); err != nil {
				t.Fatal(err)
			}
			s, _ = load(rc.Cluster)
			if s.Pending != nil || s.find(name).Subdomain {
				t.Fatal("completed revocation receipt was not applied")
			}
		})
	}
}

func TestRestartReenabledSkippedComponentMustRestart(t *testing.T) {
	for _, reconstruct := range []bool{false, true} {
		label := "same-provider"
		if reconstruct {
			label = "reconstructed-provider"
		}
		t.Run(label, func(t *testing.T) {
			p, rc, f, _ := coreSetup(t, 1)
			for _, kind := range []string{kindKibana, kindLogstash} {
				desired := rc.Cluster.Spec.Components.Extra[kind]
				desired.Replicas = 1
				rc.Cluster.Spec.Components.Extra[kind] = desired
			}
			s := coreConverge(t, p, rc, f)
			name := s.members(kindKibana)[0].Name
			oldVersion := s.find(name).ConfigVersion
			healthyAgents := rc.Agents
			remediationUnreadyComponent(t, rc, name)
			op := &domain.Operation{ID: "reenable-during-restart", Kind: domain.OperationRestart}
			advance := func() bool {
				t.Helper()
				result, err := p.RunOperation(t.Context(), rc, op)
				if err != nil {
					t.Fatal(err)
				}
				op.Checkpoint = result.Checkpoint
				s, err = load(rc.Cluster)
				if err != nil {
					t.Fatal(err)
				}
				return result.Done
			}
			preparing := false
			for range 60 {
				if advance() {
					t.Fatal("restart completed before reaching Kibana")
				}
				if s.Rolling.Current == name && s.Rolling.Step == "apply" {
					preparing = true
					break
				}
			}
			if !preparing {
				t.Fatal("restart never reached Kibana apply")
			}
			desired := rc.Cluster.Spec.Components.Extra[kindKibana]
			desired.Replicas = 0
			rc.Cluster.Spec.Components.Extra[kindKibana] = desired
			if advance() || s.Rolling.Current == name {
				t.Fatal("disabled component did not leave the current restart step")
			}
			if slices.Contains(s.Rolling.Done, name) || !slices.Contains(s.Rolling.Skipped, name) {
				t.Fatal("disabled component was recorded as a completed restart")
			}
			if reconstruct {
				// Both the provider state and operation checkpoint survive the
				// control-plane process; no in-memory provider journal survives.
				internal := slices.Clone(rc.Cluster.Internal)
				rc.Cluster = cloneCluster(t, rc.Cluster)
				// Cluster.Internal is persisted separately, not in API JSON.
				rc.Cluster.Internal = internal
				p = New(p.options)
				raw, err := json.Marshal(op)
				if err != nil {
					t.Fatal(err)
				}
				op = &domain.Operation{}
				if err := json.Unmarshal(raw, op); err != nil {
					t.Fatal(err)
				}
			}
			desired.Replicas = 1
			rc.Cluster.Spec.Components.Extra[kindKibana] = desired
			for range 40 {
				if advance() {
					t.Fatal("restart completed with a re-enabled, never-restarted unready component")
				}
			}
			if s.Rolling.Current != name || s.Rolling.Step != "rejoin" || s.find(name).ConfigVersion <= oldVersion {
				t.Fatal("re-enabled component was not restarted and readiness-gated")
			}
			rc.Agents = healthyAgents
			for range 60 {
				if advance() {
					return
				}
			}
			t.Fatal("restart did not finish after re-enabled component recovered")
		})
	}
}
