// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package mcp

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestOutputSchemaWireOptIn(t *testing.T) {
	for _, mode := range []string{"readonly", "readwrite"} {
		for _, enabled := range []bool{false, true, false} {
			t.Run(fmt.Sprintf("%s/enabled=%t", mode, enabled), func(t *testing.T) {
				server, _ := testEndpoint(t, mode, "admin", http.NotFoundHandler(), nil, enabled)
				body := `{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{"_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientInfo":{"name":"schema-test","version":"1"},"io.modelcontextprotocol/clientCapabilities":{}}}}`
				req := httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(body))
				req.Header.Set("Authorization", "Bearer testing")
				req.Header.Set("Content-Type", "application/json")
				req.Header.Set("Accept", "application/json, text/event-stream")
				req.Header.Set("MCP-Protocol-Version", "2026-07-28")
				req.Header.Set("Mcp-Method", "tools/list")
				response := httptest.NewRecorder()
				server.Config.Handler.ServeHTTP(response, req)
				if response.Code != http.StatusOK {
					t.Fatalf("HTTP %d: %s", response.Code, response.Body.String())
				}
				payload := response.Body.String()
				if strings.HasPrefix(response.Header().Get("Content-Type"), "text/event-stream") {
					_, data, found := strings.Cut(payload, "data: ")
					if !found {
						t.Fatalf("missing SSE data: %s", payload)
					}
					payload, _, _ = strings.Cut(data, "\n")
				}
				var wire struct {
					Result struct {
						Tools []map[string]json.RawMessage `json:"tools"`
					} `json:"result"`
				}
				if err := json.Unmarshal([]byte(payload), &wire); err != nil {
					t.Fatal(err)
				}
				if len(wire.Result.Tools) == 0 {
					t.Fatal("empty tool inventory")
				}
				schemas := 0
				for _, tool := range wire.Result.Tools {
					if schema, present := tool["outputSchema"]; present {
						schemas++
						if !enabled || string(schema) == "null" {
							t.Errorf("%s: unexpected outputSchema: %s", tool["name"], schema)
						}
					}
				}
				if enabled && schemas == 0 {
					t.Fatal("opt-in publishes no schemas")
				}
			})
		}
	}
}
