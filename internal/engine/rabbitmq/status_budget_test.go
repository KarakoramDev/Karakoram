// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package rabbitmq

import (
	"context"
	"encoding/pem"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestPlacementGenericStatusBudget(t *testing.T) {
	p, rc, _, _ := setup(t)
	ready(t, p, rc)
	s, err := loadState(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	s.Members = s.Members[:1]
	if err := s.save(rc.Cluster); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/status" {
			t.Errorf("unexpected request %s", r.URL.Path)
			http.NotFound(w, r)
			return
		}
		<-r.Context().Done()
	}))
	defer server.Close()
	rc.Agents = coldObservationAgents{options: agentapi.ClientOptions{BaseURL: server.URL, Token: "test", CAPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})}}
	ctx, cancel := context.WithTimeout(t.Context(), 6*time.Second)
	defer cancel()
	start := time.Now()
	placement, err := p.Placement(ctx, rc)
	if err != nil {
		t.Fatal(err)
	}
	if len(placement.Groups["unknown"]) != 1 {
		t.Fatalf("placement=%+v; want unknown member", placement)
	}
	if elapsed := time.Since(start); elapsed > engine.ObservationTimeout+time.Second {
		t.Fatalf("status read took %s", elapsed)
	}
}
