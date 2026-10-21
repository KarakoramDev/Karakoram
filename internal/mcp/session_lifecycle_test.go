// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package mcp

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

const legacyInitialize = `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"test","version":"1"}}}`

func legacyPost(h *Handler, body string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(body))
	r.Header.Set("Authorization", "Bearer testing")
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Accept", "application/json, text/event-stream")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}
func closeLegacy(h *Handler, id string) {
	r := httptest.NewRequest(http.MethodDelete, "/mcp", nil)
	r.Header.Set("Authorization", "Bearer testing")
	r.Header.Set("Mcp-Session-Id", id)
	r.Header.Set("MCP-Protocol-Version", "2025-06-18")
	h.ServeHTTP(httptest.NewRecorder(), r)
}
func TestFailedInitializationsReleaseReservations(t *testing.T) {
	server, _ := testEndpoint(t, "readonly", "admin", http.NotFoundHandler(), nil)
	h := server.Config.Handler.(*Handler)
	for i := range 260 {
		body := `{`
		if i%2 == 0 {
			body = `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":42}}`
		}
		legacyPost(h, body)
		h.mu.Lock()
		count := len(h.sessions)
		h.mu.Unlock()
		if count != 0 {
			t.Fatalf("attempt %d retained %d reservations", i, count)
		}
	}
	w := legacyPost(h, legacyInitialize)
	id := w.Header().Get("Mcp-Session-Id")
	if w.Code != 200 || id == "" {
		t.Fatalf("legitimate initialize: %d %s", w.Code, w.Body.String())
	}
	closeLegacy(h, id)
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.sessions) != 0 {
		t.Fatal("terminated session binding retained")
	}
}
func TestLegacyCapacityAndExpiry(t *testing.T) {
	server, _ := testEndpoint(t, "readonly", "admin", http.NotFoundHandler(), nil)
	h := server.Config.Handler.(*Handler)
	h.mu.Lock()
	for i := range 256 {
		h.sessions[fmt.Sprint(i)] = sessionBinding{principal: "admin\x00test", touched: time.Now()}
	}
	h.mu.Unlock()
	if w := legacyPost(h, legacyInitialize); w.Code != http.StatusServiceUnavailable {
		t.Fatalf("capacity returned %d", w.Code)
	}
	h.mu.Lock()
	for id, b := range h.sessions {
		b.touched = time.Now().Add(-31 * time.Minute)
		h.sessions[id] = b
	}
	h.mu.Unlock()
	w := legacyPost(h, legacyInitialize)
	id := w.Header().Get("Mcp-Session-Id")
	if w.Code != 200 || id == "" {
		t.Fatalf("expired bindings block initialization: %d %s", w.Code, w.Body.String())
	}
	closeLegacy(h, id)
}
