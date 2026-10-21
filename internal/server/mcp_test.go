// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/api"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	sdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

type mcpBearerTransport struct{ token string }

func (b mcpBearerTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	r.Header.Set("Authorization", "Bearer "+b.token)
	return http.DefaultTransport.RoundTrip(r)
}

func TestMCPPublicMountAndAudit(t *testing.T) {
	s, sealer, token := testServer(t)
	s.MCPMode = "readwrite"
	s.MCPSealer = sealer
	var err error
	s.mcpHandler, err = s.newMCPHandler()
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(s.Handler())
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	client := sdk.NewClient(&sdk.Implementation{Name: "must-not-enter-actor", Version: "1"}, &sdk.ClientOptions{ElicitationHandler: func(context.Context, *sdk.ElicitRequest) (*sdk.ElicitResult, error) {
		return &sdk.ElicitResult{Action: "accept", Content: map[string]any{"name": "prod"}}, nil
	}})
	session, err := client.Connect(ctx, &sdk.StreamableClientTransport{Endpoint: server.URL + "/mcp", HTTPClient: &http.Client{Transport: mcpBearerTransport{token}}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	result, err := session.CallTool(ctx, &sdk.CallToolParams{Name: api.OperationAgent["getCluster"].Tool, Arguments: map[string]any{"cluster": "prod"}})
	if err != nil || result.IsError || result.StructuredContent == nil {
		t.Fatalf("read: %#v %v", result, err)
	}
	result, err = session.CallTool(ctx, &sdk.CallToolParams{Name: api.OperationAgent["deleteCluster"].Tool, Arguments: map[string]any{"cluster": "prod"}})
	if err != nil || result.IsError {
		t.Fatalf("delete: %#v %v", result, err)
	}
	cluster, err := s.Store.GetCluster(ctx, domain.ClusterRef{Namespace: "default", Name: "prod"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(cluster.RequestedBy, " (mcp)") || strings.Contains(cluster.RequestedBy, "must-not-enter-actor") {
		t.Fatalf("wrong durable actor %q", cluster.RequestedBy)
	}
	for _, reason := range []string{"MCPConfirmationAccepted", "ClusterDeleteRequested"} {
		events := auditEvents(t, s, reason)
		if len(events) != 1 || !strings.HasSuffix(events[0].Actor, " (mcp)") || strings.Contains(events[0].Actor, "must-not-enter-actor") {
			t.Fatalf("incorrect %s audit: %#v", reason, events)
		}
	}
}
func TestMCPModesAndRoleDiscovery(t *testing.T) {
	s, _, admin := testServer(t)
	ctx := context.Background()
	if _, err := s.Auth.CreateAccount(ctx, domain.Account{Name: "viewer", Roles: []string{"role:viewer"}}, "viewer-test-password"); err != nil {
		t.Fatal(err)
	}
	viewer, _, err := s.Auth.CreateToken(ctx, "viewer", "MCP viewer", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		mode, token string
		write       bool
	}{{"readonly", admin, false}, {"readwrite", admin, true}, {"readwrite", viewer, false}} {
		t.Run(test.mode+"/"+test.token[:3], func(t *testing.T) {
			s.MCPMode = test.mode
			s.mcpHandler, err = s.newMCPHandler()
			if err != nil {
				t.Fatal(err)
			}
			server := httptest.NewServer(s.Handler())
			defer server.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			client := sdk.NewClient(&sdk.Implementation{Name: "test", Version: "1"}, nil)
			session, err := client.Connect(ctx, &sdk.StreamableClientTransport{Endpoint: server.URL + "/mcp", HTTPClient: &http.Client{Transport: mcpBearerTransport{test.token}}}, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer session.Close()
			list, err := session.ListTools(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			write := false
			for _, tool := range list.Tools {
				write = write || tool.Name == api.OperationAgent["deleteCluster"].Tool
			}
			if write != test.write {
				t.Fatalf("write listed=%v expected %v", write, test.write)
			}
		})
	}
	s.MCPMode = "off"
	s.mcpHandler, err = s.newMCPHandler()
	if err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(`{}`)))
	if w.Code != 404 {
		t.Fatalf("off returned %d", w.Code)
	}
}
