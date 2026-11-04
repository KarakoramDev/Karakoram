// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package server

import (
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
)

func TestPostgresAdministratorRotationAdmission(t *testing.T) {
	for _, capable := range []bool{false, true} {
		t.Run(map[bool]string{false: "old-agent", true: "transition-aware-agent"}[capable], func(t *testing.T) {
			s, _, token := testServer(t)
			p := postgres.New(postgres.Options{Store: s.Store})
			if err := s.Registry.Register(p); err != nil {
				t.Fatal(err)
			}
			agent := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/v1/postgres/status" {
					t.Errorf("unexpected action %s", r.URL.Path)
					w.WriteHeader(400)
					return
				}
				json.NewEncoder(w).Encode(pgspec.StatusResponse{Running: true, Role: "standalone", AdminRotation: capable})
			}))
			defer agent.Close()
			s.Context.Agents = usageDialer{url: agent.URL, ca: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: agent.Certificate().Raw})}
			c := &domain.Cluster{Name: "pgrotate", Spec: domain.ClusterSpec{Provider: "postgres"}, Internal: json.RawMessage(`{"postgres":{"members":[{"name":"pgrotatep0","role":"data","group":0}]}}`)}
			p.ApplyDefaults(&c.Spec)
			if err := s.Store.CreateCluster(t.Context(), c); err != nil {
				t.Fatal(err)
			}
			response := call(s, "POST", "/api/v1/clusters/pgrotate/system-users/postgres/rotate", "", token)
			want := 409
			if capable {
				want = 202
			}
			if response.Code != want {
				t.Fatalf("admission=%d want=%d body=%s", response.Code, want, response.Body.String())
			}
			if !capable && !strings.Contains(response.Body.String(), "upgrade-agents") {
				t.Fatal("compatibility refusal omitted upgrade guidance")
			}
			operations, err := s.Store.ListOperations(t.Context(), c.Ref())
			if err != nil {
				t.Fatal(err)
			}
			if capable && len(operations) != 1 || !capable && len(operations) != 0 {
				t.Fatal("unexpected queued rotation inventory")
			}
		})
	}
}
