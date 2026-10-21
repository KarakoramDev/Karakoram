// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/api"
	sdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

func TestMCPRootLevelOperations(t *testing.T) {
	s, _, token := testServer(t)
	server := httptest.NewServer(s.Handler())
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	client := sdk.NewClient(&sdk.Implementation{Name: "test", Version: "1"}, nil)
	session, err := client.Connect(ctx, &sdk.StreamableClientTransport{Endpoint: server.URL + "/mcp", HTTPClient: &http.Client{Transport: mcpBearerTransport{token}}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	for _, id := range []string{"getHealth", "getPrometheusMetrics"} {
		result, err := session.CallTool(ctx, &sdk.CallToolParams{Name: api.OperationAgent[id].Tool, Arguments: map[string]any{}})
		if err != nil || result.IsError || result.StructuredContent == nil {
			t.Fatalf("%s: %#v %v", id, result, err)
		}
	}
}
