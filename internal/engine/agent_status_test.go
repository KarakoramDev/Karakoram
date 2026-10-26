// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package engine

import (
	"context"
	"encoding/pem"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
)

func blockedStatusClient(t *testing.T) *agentapi.Client {
	t.Helper()
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/status" {
			t.Errorf("unexpected request: %s", r.URL.Path)
			http.NotFound(w, r)
			return
		}
		<-r.Context().Done()
	}))
	t.Cleanup(server.Close)
	client, err := agentapi.NewClient(agentapi.ClientOptions{
		BaseURL: server.URL, Token: "test-token",
		CAPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	return client
}

func TestObserveAgentStatusBudget(t *testing.T) {
	for _, name := range []string{"PostgreSQL", "PSMDB", "RabbitMQ", "Supabase", "Elasticsearch", "Component"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			client := blockedStatusClient(t)
			parent, cancel := context.WithTimeout(t.Context(), ObservationTimeout+2*time.Second)
			defer cancel()
			start := time.Now()
			_, err := ObserveAgentStatus(parent, client, name, "member0")
			if elapsed := time.Since(start); elapsed > ObservationTimeout+time.Second {
				t.Errorf("generic observation took %s", elapsed)
			}
			if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), name+" generic status read exceeded 4s budget: inspect member member0 responsiveness/connectivity") {
				t.Fatalf("err=%v", err)
			}
			if err := parent.Err(); err != nil {
				t.Fatalf("observation cancelled administration context: %v", err)
			}
		})
	}
}

func TestComponentGenericStatusBudget(t *testing.T) {
	t.Parallel()
	client := blockedStatusClient(t)
	ctx, cancel := context.WithTimeout(t.Context(), ObservationTimeout+2*time.Second)
	defer cancel()
	probe := componentInstanceProbe(ctx, "component0", 1, ComponentProbe{Ready: func(*agentapi.Client) (bool, error) {
		t.Fatal("native probe ran after unavailable generic status")
		return false, nil
	}}, nil)
	start := time.Now()
	ready, err := probe(client, "instance0")
	if elapsed := time.Since(start); elapsed > ObservationTimeout+time.Second {
		t.Errorf("component observation took %s", elapsed)
	}
	if ready || !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "inspect member component0 responsiveness/connectivity") {
		t.Fatalf("ready=%v err=%v", ready, err)
	}
}
