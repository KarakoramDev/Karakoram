// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package server

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/temporal"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	infrafake "github.com/KarakoramDev/Karakoram/internal/infra/fake"
)

func TestTemporalIPv6AgentCompatibilityHTTP(t *testing.T) {
	s, _, _ := temporalTestServer(t)
	p := temporal.New(temporal.Options{Bindings: temporalAdmissionBindings{}})
	registry, err := engine.NewRegistry(p)
	if err != nil {
		t.Fatal(err)
	}
	s.Registry = registry
	s.namespaces = engine.StaticNamespaces{DefaultBackend: infrafake.New(infrafake.Options{Layout: infra.Layout{PrivateAddressFamily: infra.AddressFamilyIPv6}})}
	cluster, err := s.Store.GetCluster(t.Context(), domain.ClusterRef{Name: "prod"})
	if err != nil {
		t.Fatal(err)
	}
	cluster.Internal = json.RawMessage(`{"temporal":{"members":{"server":{"name":"prodts","component":"server"}}}}`)
	if err := s.Store.UpdateClusterState(t.Context(), cluster); err != nil {
		t.Fatal(err)
	}
	const password = "withheld-temporal-test-password"
	envelope := agentapi.MemberConfig{Engine: "temporal", Member: "prodts", EngineSpec: json.RawMessage(`{"component":"server","ipv6":true,"persistence":{"password":"` + password + `"}}`)}
	raw, err := json.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Store.SetClusterSecret(t.Context(), cluster.Ref(), temporal.ConfigSecretPrefix+"prodts", raw); err != nil {
		t.Fatal(err)
	}
	if err := s.Store.SetAgentToken(t.Context(), &domain.AgentToken{Cluster: "prod", Member: "prodts", Token: "temporal-config-test-token"}); err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{"", "?capability=unknown", "?capability=" + agentapi.ConfigCapabilityTemporalIPv6, ""} {
		request := httptest.NewRequest("GET", "/internal/agent/v1/config"+query, nil)
		request.Header.Set("Authorization", "Bearer temporal-config-test-token")
		response := httptest.NewRecorder()
		s.InternalHandler().ServeHTTP(response, request)
		body := response.Body.String()
		if query == "?capability="+agentapi.ConfigCapabilityTemporalIPv6 {
			if response.Code != 200 || !strings.Contains(body, password) {
				t.Fatalf("capable agent configuration: status=%d", response.Code)
			}
			continue
		}
		if response.Code != 409 || !strings.Contains(body, "AgentUpgradeRequired") || !strings.Contains(body, agentapi.ConfigCapabilityTemporalIPv6) || !strings.Contains(body, "upgrade") {
			t.Fatalf("missing actionable compatibility diagnostic: %d %s", response.Code, body)
		}
		// v0.4.3 agents decode only the legacy "error" string, not the
		// public API error's message/details fields.
		var legacy agentapi.ErrorResponse
		if err := json.Unmarshal(response.Body.Bytes(), &legacy); err != nil {
			t.Fatal(err)
		}
		for _, required := range []string{"AgentUpgradeRequired", agentapi.ConfigCapabilityTemporalIPv6, "upgrade this member's Karakoram agent"} {
			if !strings.Contains(legacy.Error, required) {
				t.Fatalf("legacy agent lost diagnostic %q: %q", required, legacy.Error)
			}
		}
		for _, withheld := range []string{password, "engineSpec", "persistence", "temporal-config-test-token"} {
			if strings.Contains(body, withheld) {
				t.Fatalf("compatibility error exposed withheld payload field %q", withheld)
			}
		}
	}
}
