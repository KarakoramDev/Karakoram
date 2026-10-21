// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package mcp

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/auth"
	sdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

func TestLegacySessionPrincipalBinding(t *testing.T) {
	h, err := New(Options{Mode: "readonly", Authenticate: func(r *http.Request) (*auth.Principal, error) {
		switch r.Header.Get("Authorization") {
		case "Bearer alice":
			return &auth.Principal{Account: "alice", TokenID: "a"}, nil
		case "Bearer bob":
			return &auth.Principal{Account: "bob", TokenID: "b"}, nil
		}
		return nil, fmt.Errorf("unauthorized")
	}, Allowed: func(context.Context, *auth.Principal, string) bool { return true }, REST: http.NotFoundHandler()})
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"test","version":"1"}}}`))
	r.Header.Set("Authorization", "Bearer alice")
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Accept", "application/json, text/event-stream")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	id := w.Header().Get("Mcp-Session-Id")
	if w.Code != 200 || id == "" {
		t.Fatalf("initialize: %d %s", w.Code, w.Body.String())
	}
	r = httptest.NewRequest(http.MethodDelete, "/mcp", nil)
	r.Header.Set("Authorization", "Bearer bob")
	r.Header.Set("Mcp-Session-Id", id)
	w = httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 404 {
		t.Fatalf("session hijack returned %d", w.Code)
	}
	r = httptest.NewRequest(http.MethodDelete, "/mcp", nil)
	r.Header.Set("Authorization", "Bearer alice")
	r.Header.Set("Mcp-Session-Id", id)
	r.Header.Set("MCP-Protocol-Version", "2025-06-18")
	w = httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusNoContent {
		t.Fatalf("session delete: %d", w.Code)
	}
}
func TestLegacyToolListRechecksAuthorization(t *testing.T) {
	var allowed atomic.Bool
	allowed.Store(true)
	h, err := New(Options{Mode: "readwrite", Authenticate: func(*http.Request) (*auth.Principal, error) {
		return &auth.Principal{Account: "admin", TokenID: "test"}, nil
	}, Allowed: func(context.Context, *auth.Principal, string) bool { return allowed.Load() }, REST: http.NotFoundHandler()})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(h)
	t.Cleanup(server.Close)
	session := connectClient(t, server, "2025-11-25", nil)
	first, err := session.ListTools(context.Background(), nil)
	if err != nil || len(first.Tools) == 0 {
		t.Fatalf("initial list: %#v %v", first, err)
	}
	allowed.Store(false)
	second, err := session.ListTools(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Tools) != 0 {
		t.Fatalf("stale grants advertised %d tools", len(second.Tools))
	}
}
func TestToolsetsNarrowDiscoveryAndCalls(t *testing.T) {
	server, _ := testEndpoint(t, "readwrite", "admin", http.NotFoundHandler(), nil)
	client := sdk.NewClient(&sdk.Implementation{Name: "test", Version: "1"}, nil)
	session, err := client.Connect(context.Background(), &sdk.StreamableClientTransport{Endpoint: server.URL + "/mcp?toolsets=cLuStErS", HTTPClient: &http.Client{Transport: bearerTransport{http.DefaultTransport, "testing"}}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	list, err := session.ListTools(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(list.Tools) == 0 {
		t.Fatal("empty cluster toolset")
	}
	for _, tool := range list.Tools {
		if tool.Name == "version_get" || tool.Name == "wait" {
			t.Fatalf("unexpected tool %s", tool.Name)
		}
	}
	if _, err := session.CallTool(context.Background(), &sdk.CallToolParams{Name: "version_get", Arguments: map[string]any{}}); err == nil {
		t.Fatal("call escaped selected toolsets")
	}
}
