// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package keycloak

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
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
