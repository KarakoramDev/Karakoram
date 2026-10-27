// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package keycloak

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine/keycloak/kcspec"
)

func TestAppliedGenerationRequiresAcceptedLiveConfiguration(t *testing.T) {
	for _, observation := range []string{"absent", "stale", "agent-error"} {
		t.Run(observation, func(t *testing.T) {
			p, rc, _, _ := setup(t)
			rc.Cluster.Generation = 1
			rc.Backend = idleBackend{rc.Backend.(*backendFake)}
			accepted := false
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				s, _ := load(rc.Cluster)
				if r.URL.Path == "/v1/status" {
					status := agentapi.Status{ConfigVersion: s.ConfigVersion}
					if !accepted && observation == "stale" {
						status.ConfigVersion = 0
					}
					if !accepted && observation == "agent-error" {
						status.Error = "configuration rejected"
					}
					json.NewEncoder(w).Encode(status)
					return
				}
				json.NewEncoder(w).Encode(agentapi.ActionResponse{Success: true})
			}))
			t.Cleanup(server.Close)
			if observation != "absent" {
				rc.Agents = idleAgents{server}
			}
			for range 20 {
				if _, err := p.Sync(t.Context(), rc); err != nil {
					t.Fatal(err)
				}
				if rc.Cluster.Status.ObservedGeneration != 0 {
					t.Fatal("unaccepted configuration acknowledged")
				}
			}
			accepted = true
			rc.Agents = idleAgents{server}
			if _, err := p.Sync(t.Context(), rc); err != nil {
				t.Fatal(err)
			}
			if rc.Cluster.Status.ObservedGeneration != 1 {
				t.Fatal("accepted configuration was not acknowledged")
			}
		})
	}
}

func TestAppliedGenerationWaitsForDeferredApplicationConfiguration(t *testing.T) {
	for _, field := range []string{"hostname", "proxyHeaders"} {
		t.Run(field, func(t *testing.T) {
			p, rc, bindings, _ := setup(t)
			rc.Cluster.Generation = 1
			rc.Backend = idleBackend{rc.Backend.(*backendFake)}
			rc.Clock = fixedClock{at: time.Date(2026, time.January, 2, 12, 0, 0, 0, time.UTC)}
			var applied atomic.Int64
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/v1/status" {
					json.NewEncoder(w).Encode(agentapi.Status{ConfigVersion: applied.Load()})
					return
				}
				json.NewEncoder(w).Encode(agentapi.ActionResponse{Success: true})
			}))
			t.Cleanup(server.Close)
			rc.Agents = idleAgents{server}
			step := func() {
				t.Helper()
				result, err := p.Sync(t.Context(), rc)
				if err != nil {
					t.Fatal(err)
				}
				if result.RequeueAfter != 10*time.Second {
					t.Fatalf("health cadence changed: %s", result.RequeueAfter)
				}
			}
			accept := func() {
				t.Helper()
				s, err := load(rc.Cluster)
				if err != nil {
					t.Fatal(err)
				}
				applied.Store(s.ConfigVersion)
			}
			published := func() kcspec.Spec {
				t.Helper()
				s, err := load(rc.Cluster)
				if err != nil {
					t.Fatal(err)
				}
				config, err := p.MemberConfig(t.Context(), rc, s.Member)
				if err != nil {
					t.Fatal(err)
				}
				var spec kcspec.Spec
				if err := json.Unmarshal(config.EngineSpec, &spec); err != nil {
					t.Fatal(err)
				}
				return spec
			}
			for range 20 {
				step()
				accept()
			}
			if rc.Cluster.Status.ObservedGeneration != 1 {
				t.Fatal("initial generation did not converge")
			}
			old := published()
			options, err := ParseOptions(rc.Cluster.Spec.EngineOptions)
			if err != nil {
				t.Fatal(err)
			}
			if field == "hostname" {
				options.Hostname = "https://identity.example.test"
			} else {
				options.ProxyHeaders = "xforwarded"
			}
			rc.Cluster.Spec.EngineOptions, err = json.Marshal(options)
			if err != nil {
				t.Fatal(err)
			}
			rc.Cluster.Generation++
			rc.Cluster.Spec.Maintenance = &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: "0 0 1 2 *", Duration: "1h"}}}
			bindings.conn.CredentialGeneration++
			bindings.password = "rotated-test-password"
			for range 20 {
				step()
				accept()
				if rc.Cluster.Status.ObservedGeneration != 1 {
					t.Fatal("deferred application generation acknowledged")
				}
			}
			deferred := published()
			if deferred.Hostname != old.Hostname || deferred.ProxyHeaders != old.ProxyHeaders {
				t.Fatal("closed window published application change")
			}
			if deferred.CredentialGeneration != bindings.conn.CredentialGeneration {
				t.Fatal("deferred application change blocked exempt credential refresh")
			}
			rc.Clock = fixedClock{at: time.Date(2026, time.February, 1, 0, 1, 0, 0, time.UTC)}
			step()
			desired := published()
			if desired.Hostname != options.Hostname || desired.ProxyHeaders != options.ProxyHeaders {
				t.Fatal("open window did not publish desired application configuration")
			}
			if rc.Cluster.Status.ObservedGeneration != 1 {
				t.Fatal("new application configuration acknowledged before agent acceptance")
			}
			accept()
			step()
			if rc.Cluster.Status.ObservedGeneration != 2 {
				t.Fatal("accepted desired application configuration was not acknowledged")
			}
		})
	}
}
