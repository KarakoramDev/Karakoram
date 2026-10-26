// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package agentapi

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRecoveryPreservesBoundedStructuredError(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "PBM initiation outcome unknown; reconcile before retry; member-token\n" + strings.Repeat("x", 1500)})
	}))
	defer server.Close()
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	client, err := NewClient(ClientOptions{BaseURL: server.URL, Token: "member-token", CAPEM: ca})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	_, err = client.PBM(context.Background(), PBMRequest{Command: "status"})
	if err == nil || !strings.Contains(err.Error(), "reconcile before retry") {
		t.Fatal("actionable diagnosis lost", err)
	}
	if len(err.Error()) > 1200 || strings.Contains(err.Error(), "member-token") || strings.Contains(err.Error(), "\n") {
		t.Fatal("unsafe error rendering", err)
	}
}

func TestRecoveryProcessIdentityKeepsLegacyRoute(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/processes/mongod/restart" || r.Header.Get("Idempotency-Key") != "operation/member/restart" {
			t.Error("process retry identity or legacy route lost", r.URL.Path, r.Header.Get("Idempotency-Key"))
		}
		var request ProcessAction
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil || request.RequestID != "operation/member/restart" {
			t.Error("request identity missing", request, err)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	client, err := NewClient(ClientOptions{BaseURL: server.URL, Token: "member-token", CAPEM: ca})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if err := client.Process(context.Background(), ProcessAction{Process: "mongod", Action: "restart", RequestID: "operation/member/restart"}); err != nil {
		t.Fatal(err)
	}
}
