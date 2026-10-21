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
	"github.com/KarakoramDev/Karakoram/internal/domain"
	sdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

func TestMCPRestrictedTokenDiscovery(t *testing.T) {
	s, _, _ := testServer(t)
	s.MCPMode = "readwrite"
	var err error
	s.mcpHandler, err = s.newMCPHandler()
	if err != nil {
		t.Fatal(err)
	}
	token, _, err := s.Auth.CreateToken(context.Background(), "admin", "restricted MCP", nil, &domain.TokenRestrictions{ReadOnly: true, Namespaces: []string{"default"}, Permissions: []string{"clusters:read"}})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(s.Handler())
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	client := sdk.NewClient(&sdk.Implementation{Name: "restricted", Version: "1"}, nil)
	session, err := client.Connect(ctx, &sdk.StreamableClientTransport{Endpoint: server.URL + "/mcp", HTTPClient: &http.Client{Transport: mcpBearerTransport{token}}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	list, err := session.ListTools(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, tool := range list.Tools {
		found = found || tool.Name == api.OperationAgent["getCluster"].Tool
		for _, id := range []string{"deleteCluster", "getClusterConnection", "getSettings"} {
			if tool.Name == api.OperationAgent[id].Tool {
				t.Fatalf("restriction leaked %s", id)
			}
		}
	}
	if !found {
		t.Fatal("authorized cluster read missing")
	}
	result, err := session.CallTool(ctx, &sdk.CallToolParams{Name: api.OperationAgent["getCluster"].Tool, Arguments: map[string]any{"cluster": "prod"}})
	if err != nil || result.IsError {
		t.Fatalf("authorized read failed: %#v %v", result, err)
	}
	if _, err := session.CallTool(ctx, &sdk.CallToolParams{Name: api.OperationAgent["deleteCluster"].Tool, Arguments: map[string]any{"cluster": "prod"}}); err == nil {
		t.Fatal("restricted write was not unknown tool")
	}
}

func TestMCPInvalidModeRejected(t *testing.T) {
	if err := (Config{MCP: "dangerously-enabled"}).Validate(); err == nil {
		t.Fatal("invalid MCP mode accepted")
	}
}
