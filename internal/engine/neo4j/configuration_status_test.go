// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package neo4j

import (
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine/neo4j/neo4jspec"
)

func TestConfigurationApplyFailureDoesNotClaimNativeRejection(t *testing.T) {
	p, rc, _, a := setup(t)
	converge(t, p, rc)
	for _, failure := range []string{"Neo4j command derivation failed", "Neo4j credential recovery interrupted", "Neo4j recovery shutdown timed out"} {
		t.Run(failure, func(t *testing.T) {
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/v1/neo4j/readiness":
					_ = json.NewEncoder(w).Encode(neo4jspec.Readiness{Ready: true, ConfigurationError: failure, ConfigVersion: a.config.ConfigVersion, Version: a.spec.Version, AppliedRestartToken: a.spec.RestartToken})
				case "/v1/status":
					_ = json.NewEncoder(w).Encode(agentapi.Status{Error: failure})
				default:
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			defer server.Close()
			rc.Agents = testAgents{url: server.URL, ca: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})}
			status, err := p.Status(t.Context(), rc)
			if err != nil {
				t.Fatal(err)
			}
			if status.Phase == domain.PhaseReady || len(status.Members) != 1 || status.Members[0].Health {
				t.Fatalf("failed apply reported healthy: %+v", status)
			}
			found := false
			for _, condition := range status.Conditions {
				if condition.Type == "Ready" && condition.Status == "False" && condition.Reason == "ConfigurationApplyFailed" {
					found = true
					if strings.Contains(condition.Message, "rejected") || strings.Contains(condition.Message, failure) {
						t.Fatalf("condition claims rejection or leaks diagnostics: %+v", condition)
					}
				}
			}
			if !found {
				t.Fatalf("missing truthful apply condition: %+v", status.Conditions)
			}
		})
	}
}
