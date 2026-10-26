// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine/elastic/agentspec"
)

func TestInitializingComponentResizePrecedesConfigurationAndReadiness(t *testing.T) {
	for _, kind := range []string{kindKibana, kindLogstash, kindAPM} {
		t.Run(kind, func(t *testing.T) {
			p, rc, f, _ := coreSetup(t, 1)
			desired := rc.Cluster.Spec.Components.Extra[kind]
			desired.Replicas = 1
			desired.Resources.MinRAM, desired.Resources.MaxRAM = 1, 1
			rc.Cluster.Spec.Components.Extra[kind] = desired
			s := coreConverge(t, p, rc, f)
			m := s.members(kind)[0]
			m.Stage, s.Phase = stageDeployed, "components"
			oldHash := m.ConfigHash
			oldHeap := m.HeapMB
			rc.Cluster.Status = domain.ClusterStatus{}
			rc.Cluster.Generation++
			desired.Resources.MinRAM, desired.Resources.MaxRAM = 2, 2
			rc.Cluster.Spec.Components.Extra[kind] = desired
			intent, err := p.desiredSpec(t.Context(), rc, s, m, m.Version)
			if err != nil {
				t.Fatal(err)
			}
			if m.HeapMB != oldHeap || m.ConfigHash != oldHash {
				t.Fatal("computing desired intent mutated applied configuration")
			}
			if kind != kindAPM && (intent.HeapMB == oldHeap || intent.RestartToken == oldHash) {
				t.Fatal("applied heap masked desired resource configuration")
			}
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			healthyAgents := rc.Agents
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/v1/status":
					_ = json.NewEncoder(w).Encode(agentapi.Status{ConfigVersion: 1 << 30})
				case "/v1/elasticsearch/readiness":
					_ = json.NewEncoder(w).Encode(agentspec.Readiness{Ready: false, ConfigVersion: 1 << 30, Message: "Component process is not running"})
				default:
					_ = json.NewEncoder(w).Encode(map[string]bool{"success": true})
				}
			}))
			defer server.Close()
			ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
			rc.Agents = coreAgentDialer{server.URL, ca}
			step := func() *state {
				t.Helper()
				if _, err := p.Sync(t.Context(), rc); err != nil {
					t.Fatal(err)
				}
				current, err := load(rc.Cluster)
				if err != nil {
					t.Fatal(err)
				}
				return current
			}
			s = step()
			if s.Pending == nil || s.Pending.Action != "resources" || s.Pending.Member != m.Name || !reflect.DeepEqual(s.Pending.Resources, runtimeResources(m, desired.Resources)) {
				t.Fatalf("unready component did not submit corrective resize: %+v", s.Pending)
			}
			if s.find(m.Name).ConfigHash != oldHash || s.find(m.Name).HeapMB != oldHeap || s.find(m.Name).Resources.MinRAM != 1 {
				t.Fatal("configuration or applied resources advanced before resize completion")
			}
			f.done = false
			mutations := f.mutations
			s = step()
			if f.mutations != mutations || s.Pending == nil || s.find(m.Name).ConfigHash != oldHash {
				t.Fatal("active resize receipt was bypassed")
			}
			f.done = true
			s = step()
			if s.Pending != nil || s.find(m.Name).Resources.MinRAM != 2 {
				t.Fatal("completed resize did not persist applied resources")
			}
			s = step()
			if kind != kindAPM && s.find(m.Name).ConfigHash == oldHash {
				t.Fatal("resized component did not receive desired configuration")
			}
			if s.find(m.Name).HeapMB != intent.HeapMB {
				t.Fatal("configuration publication did not activate desired heap")
			}
			s = step()
			if s.find(m.Name).Stage != stageDeployed || s.Phase == "ready" {
				t.Fatal("resource convergence falsely established application readiness")
			}
			rc.Agents = healthyAgents
			coreConverge(t, p, rc, f)
		})
	}
}

func TestActivatedComponentResizeRetainsHeapDuringRollingDeployment(t *testing.T) {
	for _, kind := range []string{kindKibana, kindLogstash, kindAPM} {
		t.Run(kind, func(t *testing.T) {
			p, rc, f, _ := coreSetup(t, 1)
			desired := rc.Cluster.Spec.Components.Extra[kind]
			desired.Replicas = 1
			desired.Resources.MinRAM, desired.Resources.MaxRAM = 1, 1
			rc.Cluster.Spec.Components.Extra[kind] = desired
			s := coreConverge(t, p, rc, f)
			m := s.members(kind)[0]
			oldHeap, oldHash := m.HeapMB, m.ConfigHash
			desired.Resources.MinRAM, desired.Resources.MaxRAM = 2, 2
			rc.Cluster.Spec.Components.Extra[kind] = desired
			for _, stage := range []string{stageJoined, stageDeployed} {
				m.Stage = stage
				if stage == stageDeployed {
					s.Rolling = &rolling{Reason: "upgrade", Current: m.Name, Step: "rejoin"}
				}
				intent, err := p.desiredSpec(t.Context(), rc, s, m, m.Version)
				if err != nil {
					t.Fatal(err)
				}
				if intent.HeapMB != oldHeap || intent.RestartToken != oldHash {
					t.Fatalf("%s resource-only intent changed activated configuration", stage)
				}
				if m.HeapMB != oldHeap || m.ConfigHash != oldHash || m.Resources.MinRAM != 1 {
					t.Fatalf("%s desired intent mutated applied fields", stage)
				}
			}
		})
	}
}
