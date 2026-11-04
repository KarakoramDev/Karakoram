// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package agentapi

import (
	"context"
	"encoding/pem"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestReloadUsesEngineBudgetAndHonorsCancellation(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/reload" || !AuthenticateBearer(r, "token") {
			t.Error("unexpected reload request")
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	client, err := NewClient(ClientOptions{BaseURL: server.URL, Token: "token", CAPEM: ca, Timeout: time.Nanosecond, EngineTimeout: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if err := client.Reload(t.Context()); err != nil {
		t.Fatalf("reload used ordinary request budget: %v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := client.Reload(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("reload ignored caller cancellation: %v", err)
	}
}
