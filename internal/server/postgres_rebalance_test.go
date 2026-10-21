// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/api"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres"
)

type rebalanceProvider struct{ fakeProvider }

func (*rebalanceProvider) Name() string { return "postgres" }
func (*rebalanceProvider) Describe() domain.ProviderInfo {
	return postgres.New(postgres.Options{}).Describe()
}

func TestRebalancePostgres(t *testing.T) {
	for _, tc := range []struct {
		name, provider, topology, body string
		status                         int
	}{
		{"default", "postgres", "distributed", `{}`, 202},
		{"strategy", "postgres", "distributed", `{"strategy":"by_shard_count"}`, 202},
		{"wrong provider", "fake", "distributed", `{}`, 400},
		{"single", "postgres", "single", `{}`, 409},
		{"ha", "postgres", "ha", `{}`, 409},
		{"unknown field", "postgres", "distributed", `{"unknown":1}`, 400},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, _, token := testServer(t)
			registry, err := engine.NewRegistry(&fakeProvider{}, &rebalanceProvider{})
			if err != nil {
				t.Fatal(err)
			}
			s.Registry = registry
			cluster := domain.Cluster{Name: "distdb", Spec: domain.ClusterSpec{Provider: tc.provider, Version: "8.0", Topology: domain.Topology{Type: tc.topology}}, Status: domain.ClusterStatus{Phase: domain.PhaseReady}}
			(&fakeProvider{}).ApplyDefaults(&cluster.Spec)
			if err = s.Store.CreateCluster(t.Context(), &cluster); err != nil {
				t.Fatal(err)
			}
			send := func() *httptest.ResponseRecorder {
				r := httptest.NewRequest("POST", "/api/v1/clusters/distdb/actions/rebalance", strings.NewReader(tc.body))
				r.Header.Set("Content-Type", "application/json")
				r.Header.Set("Authorization", "Bearer "+token)
				r.Header.Set("Idempotency-Key", "rebalance-replay")
				w := httptest.NewRecorder()
				testHandler(s).ServeHTTP(w, r)
				return w
			}
			w := send()
			if w.Code != tc.status {
				t.Fatalf("status=%d body=%s", w.Code, w.Body)
			}
			if tc.status != 202 {
				return
			}
			var accepted api.Operation
			if err = json.Unmarshal(w.Body.Bytes(), &accepted); err != nil {
				t.Fatal(err)
			}
			if string(accepted.Type) != domain.OperationRebalance || accepted.Id != w.Header().Get("Karakoram-Operation-Id") {
				t.Fatal(w.Body.String())
			}
			op, err := s.Store.GetOperation(t.Context(), accepted.Id)
			if err != nil {
				t.Fatal(err)
			}
			var payload domain.RebalanceParams
			if err = json.Unmarshal(op.Payload, &payload); err != nil {
				t.Fatal(err)
			}
			want := ""
			if tc.name == "strategy" {
				want = "by_shard_count"
			}
			if payload.Strategy != want {
				t.Fatalf("strategy=%q want=%q", payload.Strategy, want)
			}
			replay := send()
			if replay.Code != 202 || replay.Body.String() != w.Body.String() {
				t.Fatalf("replay=%d %s original=%s", replay.Code, replay.Body, w.Body)
			}
		})
	}
}
