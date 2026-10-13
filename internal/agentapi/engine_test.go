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

func TestEngineClient(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !AuthenticateBearer(r, "token") {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if r.Method != "POST" {
			t.Error(r.Method)
		}
		switch r.URL.Path {
		case "/v1/rabbitmq/definitions":
			if r.Header.Get("Content-Type") != "application/json" {
				t.Error(r.Header)
			}
			var req struct {
				Name string `json:"name"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Name != "test" {
				t.Error(req, err)
			}
			_ = json.NewEncoder(w).Encode(map[string]bool{"ok": true})
		case "/v1/rabbitmq/fail":
			w.WriteHeader(http.StatusConflict)
		case "/v1/rabbitmq/invalid":
			_, _ = w.Write([]byte("not JSON"))
		case "/v1/rabbitmq/x":
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Error(r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	client, err := NewClient(ClientOptions{BaseURL: server.URL, Token: "token", CAPEM: ca})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	var out struct {
		OK bool `json:"ok"`
	}
	ctx := context.Background()
	if err := client.Engine(ctx, "rabbitmq", "definitions", map[string]string{"name": "test"}, &out); err != nil || !out.OK {
		t.Fatal(out, err)
	}
	if err := client.Engine(ctx, "rabbitmq", "fail", struct{}{}, nil); err == nil || !strings.Contains(err.Error(), "HTTP 409") {
		t.Fatal(err)
	}
	if err := client.Engine(ctx, "rabbitmq", "invalid", nil, &out); err == nil {
		t.Fatal("invalid response accepted")
	}
	if err := client.Engine(ctx, "rabbitmq", "x", struct{}{}, nil); err != nil {
		t.Fatal(err)
	}
	bad, err := NewClient(ClientOptions{BaseURL: server.URL, Token: "wrong", CAPEM: ca})
	if err != nil {
		t.Fatal(err)
	}
	defer bad.Close()
	if err := bad.Engine(ctx, "rabbitmq", "definitions", struct{}{}, &out); err == nil || !strings.Contains(err.Error(), "HTTP 401") {
		t.Fatal(err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if err := client.Engine(cancelled, "rabbitmq", "definitions", struct{}{}, nil); err == nil {
		t.Fatal("cancelled request accepted")
	}
}
func TestProcessNameSyntax(t *testing.T) {
	var calls int
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !AuthenticateBearer(r, "token") {
			t.Error("missing bearer")
		}
		calls++
		if !strings.HasPrefix(r.URL.Path, "/v1/processes/") {
			t.Error(r.URL.Path)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	client, err := NewClient(ClientOptions{BaseURL: server.URL, Token: "token", CAPEM: ca})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	for _, name := range []string{"mongod", "mongos", "pbm-agent", "rabbitmq", "elasticsearch", "worker-01", "ab", "a" + strings.Repeat("0", 31)} {
		if err := client.Process(context.Background(), ProcessAction{Process: name, Action: "start"}); err != nil {
			t.Fatal(name, err)
		}
	}
	before := calls
	for _, name := range []string{"", "a", "1worker", "Worker", "worker_name", "../shell", "worker/start", "a" + strings.Repeat("0", 32)} {
		if err := client.Process(context.Background(), ProcessAction{Process: name, Action: "start"}); err == nil {
			t.Fatal(name)
		}
	}
	if err := client.Process(context.Background(), ProcessAction{Process: "worker", Action: "exec"}); err == nil {
		t.Fatal("invalid action")
	}
	if calls != before {
		t.Fatal("invalid syntax reached server")
	}
}
