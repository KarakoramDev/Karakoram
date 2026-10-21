// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/api"
	"github.com/KarakoramDev/Karakoram/internal/auth"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	sdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

type bearerTransport struct {
	base  http.RoundTripper
	token string
}

func (b bearerTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	r.Header.Set("Authorization", "Bearer "+b.token)
	return b.base.RoundTrip(r)
}
func testEndpoint(t *testing.T, mode, role string, rest http.Handler, confirm *Confirmer) (*httptest.Server, *sdk.ClientSession) {
	t.Helper()
	h, err := New(Options{Mode: mode, PublicURL: "https://control.example", Authenticate: func(r *http.Request) (*auth.Principal, error) {
		if r.Header.Get("Authorization") != "Bearer testing" {
			return nil, fmt.Errorf("unauthorized")
		}
		return &auth.Principal{Account: role, TokenID: "test"}, nil
	}, Allowed: func(_ context.Context, p *auth.Principal, id string) bool {
		return p.Account == "admin" || api.OperationAgent[id].Risk == "read"
	}, REST: rest, Confirmer: confirm, SignIdempotency: func(v []byte) string { return fmt.Sprintf("%x", v) }})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(h)
	t.Cleanup(server.Close)
	return server, nil
}
func connectClient(t *testing.T, server *httptest.Server, version string, opts *sdk.ClientOptions) *sdk.ClientSession {
	t.Helper()
	client := sdk.NewClient(&sdk.Implementation{Name: "test", Version: "1"}, opts)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)
	session, err := client.Connect(ctx, &sdk.StreamableClientTransport{Endpoint: server.URL + "/mcp", HTTPClient: &http.Client{Transport: bearerTransport{http.DefaultTransport, "testing"}}}, &sdk.ClientSessionOptions{ProtocolVersion: version})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = session.Close() })
	return session
}
func TestProtocolErasAndModes(t *testing.T) {
	for _, version := range []string{"2026-07-28", "2025-11-25", "2025-06-18", "2025-03-26"} {
		for _, mode := range []string{"readonly", "readwrite"} {
			t.Run(version+"/"+mode, func(t *testing.T) {
				server, _ := testEndpoint(t, mode, "admin", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if !auth.IsMCPTraffic(r.Context()) {
						t.Error("missing trusted context")
					}
					w.Header().Set("Content-Type", "application/json")
					_, _ = w.Write([]byte(`{"items":[]}`))
				}), nil)
				session := connectClient(t, server, version, nil)
				list, err := session.ListTools(context.Background(), nil)
				if err != nil {
					t.Fatal(err)
				}
				found := false
				write := false
				last := ""
				for _, tool := range list.Tools {
					if tool.Name < last {
						t.Fatal("tools not sorted")
					}
					last = tool.Name
					found = found || tool.Name == api.OperationAgent["listClusters"].Tool
					write = write || tool.Name == api.OperationAgent["createCluster"].Tool
				}
				if !found {
					t.Fatal("read tool missing")
				}
				if write != (mode == "readwrite") {
					t.Fatal("mode filtering")
				}
				result, err := session.CallTool(context.Background(), &sdk.CallToolParams{Name: api.OperationAgent["listClusters"].Tool, Arguments: map[string]any{}})
				if err != nil || result.IsError || result.StructuredContent == nil {
					t.Fatalf("structured result: %#v %v", result, err)
				}
				if mode == "readonly" {
					if _, err := session.CallTool(context.Background(), &sdk.CallToolParams{Name: api.OperationAgent["createCluster"].Tool, Arguments: map[string]any{}}); err == nil {
						t.Fatal("write allowed")
					}
				}
			})
		}
	}
}
func TestRoleFiltering(t *testing.T) {
	server, _ := testEndpoint(t, "readwrite", "viewer", http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}), nil)
	session := connectClient(t, server, "2026-07-28", nil)
	list, err := session.ListTools(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, tool := range list.Tools {
		if !tool.Annotations.ReadOnlyHint {
			t.Fatal("viewer sees mutation")
		}
	}
}
func TestConfirmationExecution(t *testing.T) {
	for _, version := range []string{"2026-07-28", "2025-06-18"} {
		for _, action := range []string{"accept", "decline", "unsupported"} {
			t.Run(version+"/"+action, func(t *testing.T) {
				var executed atomic.Int32
				sealer, err := secrets.New(strings.Repeat("x", 32))
				if err != nil {
					t.Fatal(err)
				}
				gate := &Confirmer{Sign: sealer.MCPConfirmationHash}
				server, _ := testEndpoint(t, "readwrite", "admin", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { executed.Add(1); _, _ = w.Write([]byte(`{"ok":true}`)) }), gate)
				var options *sdk.ClientOptions
				if action != "unsupported" {
					options = &sdk.ClientOptions{ElicitationHandler: func(context.Context, *sdk.ElicitRequest) (*sdk.ElicitResult, error) {
						return &sdk.ElicitResult{Action: action, Content: map[string]any{"name": "prod"}}, nil
					}}
				}
				session := connectClient(t, server, version, options)
				result, err := session.CallTool(context.Background(), &sdk.CallToolParams{Name: api.OperationAgent["deleteCluster"].Tool, Arguments: map[string]any{"cluster": "prod"}})
				if err != nil {
					t.Fatal(err)
				}
				expected := int32(0)
				if action == "accept" {
					expected = 1
				}
				if executed.Load() != expected {
					t.Fatalf("executed %d, expected %d", executed.Load(), expected)
				}
				if result.IsError != (expected == 0) {
					t.Fatalf("unexpected result %#v", result)
				}
			})
		}
	}
}
func TestAuthenticationAndOrigin(t *testing.T) {
	server, _ := testEndpoint(t, "readonly", "admin", http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}), nil)
	for _, test := range []struct {
		origin, authorization, cookie string
		status                        int
	}{{"https://evil.example", "Bearer testing", "", 403}, {"", "", "karakoram_access=ignored", 401}} {
		r, _ := http.NewRequest(http.MethodPost, server.URL+"/mcp", strings.NewReader(`{}`))
		r.Header.Set("Origin", test.origin)
		r.Header.Set("Authorization", test.authorization)
		r.Header.Set("Cookie", test.cookie)
		res, err := http.DefaultClient.Do(r)
		if err != nil {
			t.Fatal(err)
		}
		_ = res.Body.Close()
		if res.StatusCode != test.status {
			t.Fatalf("status %d", res.StatusCode)
		}
		if test.status == 401 && res.Header.Get("WWW-Authenticate") != "Bearer" {
			t.Fatal("missing challenge")
		}
	}
}
func TestArgumentCanonicalizationAndIdempotency(t *testing.T) {
	var keys []string
	server, _ := testEndpoint(t, "readwrite", "admin", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		keys = append(keys, r.Header.Get("Idempotency-Key"))
		_, _ = w.Write([]byte(`{}`))
	}), nil)
	session := connectClient(t, server, "2026-07-28", nil)
	for range 2 {
		_, err := session.CallTool(context.Background(), &sdk.CallToolParams{Name: api.OperationAgent["createCluster"].Tool, Arguments: json.RawMessage(`{"body":{"name":"prod"}}`)})
		if err != nil {
			t.Fatal(err)
		}
	}
	if len(keys) != 2 || keys[0] == "" || keys[0] != keys[1] {
		t.Fatalf("unstable keys: %v", keys)
	}
}
