// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/api"
	"github.com/KarakoramDev/Karakoram/internal/datamcp"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/store"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type mountMcpUpstream struct{}

func (mountMcpUpstream) ListTools(context.Context) ([]*mcp.Tool, error) {
	return []*mcp.Tool{{Name: "search", InputSchema: map[string]any{"type": "object"}}, {Name: "doc_index", InputSchema: map[string]any{"type": "object"}}}, nil
}
func (mountMcpUpstream) CallTool(context.Context, string, map[string]any) (*mcp.CallToolResult, error) {
	return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: "result"}}}, nil
}
func (mountMcpUpstream) Close() error { return nil }

type mountMcpFactory struct{}

func (mountMcpFactory) Manifest(context.Context, *domain.Cluster) (string, error) {
	return "native-elasticsearch", nil
}
func (mountMcpFactory) Fingerprint(context.Context, *domain.Cluster, engine.DataAccessLevel) (string, error) {
	return "test", nil
}
func (mountMcpFactory) Open(context.Context, *domain.Cluster, engine.DataAccessLevel) (datamcp.Upstream, error) {
	return mountMcpUpstream{}, nil
}

type mountMcpBearer struct{ token string }

func (b mountMcpBearer) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	r.Header.Set("Authorization", "Bearer "+b.token)
	return http.DefaultTransport.RoundTrip(r)
}

func setupMountMcp(t *testing.T) (*Server, string, string) {
	t.Helper()
	s, _, unrestricted := testServer(t)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s.DataMcpUpstreams = mountMcpFactory{}
	if err := s.initDataMcp(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	ctx := context.Background()
	c, err := s.Store.GetCluster(ctx, domain.ClusterRef{Namespace: "default", Name: "prod"})
	if err != nil {
		t.Fatal(err)
	}
	c.Spec.DataMcp = &domain.DataMcpSpec{Enabled: true, Access: domain.DataMcpAccessReadWrite}
	if err := s.Store.UpdateClusterSpec(ctx, c.Ref(), c.Spec); err != nil {
		t.Fatal(err)
	}
	token, _, err := s.Auth.CreateToken(ctx, "admin", "data", nil, &domain.TokenRestrictions{Clusters: []string{"default/prod"}, Permissions: []string{"cluster-data:read", "cluster-data:write"}})
	if err != nil {
		t.Fatal(err)
	}
	return s, token, unrestricted
}

func TestDataMcpPublicMountAuthentication(t *testing.T) {
	s, token, unrestricted := setupMountMcp(t)
	handler := s.Handler()
	login := call(s, "POST", "/api/v1/auth/login", `{"username":"admin","password":"admin-test-password"}`, "")
	if login.Code != 200 {
		t.Fatalf("login: %d", login.Code)
	}
	cookieRequest := httptest.NewRequest("GET", "/mcp/clusters/default/prod", nil)
	for _, cookie := range login.Result().Cookies() {
		cookieRequest.AddCookie(cookie)
	}
	cookieResult := httptest.NewRecorder()
	handler.ServeHTTP(cookieResult, cookieRequest)
	if cookieResult.Code != 401 {
		t.Fatalf("session accepted: %d", cookieResult.Code)
	}
	for _, tc := range []struct {
		path, token, cookie string
		want                int
	}{
		{"/mcp/clusters/default/prod", "", "", 401},
		{"/mcp/clusters/default/prod", token, "session=browser", 401},
		{"/mcp/clusters/default/prod", unrestricted, "", 403},
		{"/mcp/clusters/prod", token, "", 404},
		{"/mcp/clusters/default/prod/extra", token, "", 404},
	} {
		r := httptest.NewRequest("POST", tc.path, strings.NewReader(`{}`))
		r.Header.Set("Authorization", "Bearer "+tc.token)
		if tc.cookie != "" {
			r.Header.Set("Cookie", tc.cookie)
		}
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		if w.Code != tc.want {
			t.Fatalf("%s: got %d: %s", tc.path, w.Code, w.Body.String())
		}
		if w.Header().Get("X-Request-ID") == "" {
			t.Fatal("missing request middleware")
		}
	}
	s.MCPMode = "off"
	r := httptest.NewRequest("POST", "/mcp/clusters/default/prod", nil)
	r.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	if w.Code != 404 {
		t.Fatalf("off: %d", w.Code)
	}
}

func TestDataMcpPublicToolsAndAudit(t *testing.T) {
	for _, mode := range []string{"readonly", "readwrite"} {
		t.Run(mode, func(t *testing.T) {
			s, token, _ := setupMountMcp(t)
			s.MCPMode = mode
			host := httptest.NewServer(s.Handler())
			defer host.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			client := mcp.NewClient(&mcp.Implementation{Name: "mount-test", Version: "1"}, nil)
			session, err := client.Connect(ctx, &mcp.StreamableClientTransport{Endpoint: host.URL + "/mcp/clusters/default/prod", HTTPClient: &http.Client{Transport: mountMcpBearer{token}}}, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer session.Close()
			tools, err := session.ListTools(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			names := map[string]bool{}
			for _, tool := range tools.Tools {
				names[tool.Name] = true
			}
			if !names["search"] || names["doc_index"] != (mode == "readwrite") {
				t.Fatalf("tools: %v", names)
			}
			if _, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "search", Arguments: map[string]any{"index": "logs", "query": map[string]any{}}}); err != nil {
				t.Fatal(err)
			}
			events, err := s.Store.ListEvents(ctx, store.EventFilter{})
			if err != nil {
				t.Fatal(err)
			}
			var auditID int64
			for _, e := range events {
				if e.Type == "Audit" && e.Resource == datamcp.RBACResource && strings.Contains(e.Actor, "admin (token: data [") {
					auditID = e.ID
				}
			}
			if auditID == 0 {
				t.Fatalf("missing persisted data audit: %+v", events)
			}
			for _, resource := range []string{"events", "audit"} {
				t.Run(resource, func(t *testing.T) {
					makeReader := func(name, object string) string {
						t.Helper()
						if _, err := s.Auth.CreateAccount(ctx, domain.Account{Name: name}, "audit-test-password"); err != nil {
							t.Fatal(err)
						}
						if err := s.RBAC.ReplacePolicy(ctx, s.RBAC.Policy()+"\np, user:"+name+", "+resource+", read, "+object+", allow\n"); err != nil {
							t.Fatal(err)
						}
						reader, _, err := s.Auth.CreateToken(ctx, name, "audit", nil, nil)
						if err != nil {
							t.Fatal(err)
						}
						return reader
					}
					reader := makeReader(resource+"-reader", "default/prod")
					for _, scope := range []string{"", "&namespace=default", "&cluster=prod", "&namespace=default&cluster=prod"} {
						w := call(s, "GET", "/api/v1/events?resource=cluster-data"+scope, "", reader)
						var list api.EventList
						if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &list) != nil || len(list.Items) != 1 || list.Items[0].Id != auditID {
							t.Fatalf("filtered gateway audit %q: %d %s", scope, w.Code, w.Body.String())
						}
					}
					other := makeReader(resource+"-other", "default/private")
					for _, denied := range []string{token, other} {
						for _, path := range []string{"/api/v1/events", "/api/v1/events/stream"} {
							w := call(s, "GET", path+"?namespace=default&cluster=prod&resource=cluster-data", "", denied)
							if w.Code != 403 {
								t.Fatalf("unauthorized audit filter: %d %s", w.Code, w.Body.String())
							}
						}
					}
					for _, tc := range []struct {
						token string
						want  bool
					}{{reader, true}, {other, false}} {
						w := call(s, "GET", "/api/v1/events", "", tc.token)
						var list api.EventList
						if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &list) != nil {
							t.Fatalf("unfiltered events: %d %s", w.Code, w.Body.String())
						}
						found := false
						for _, e := range list.Items {
							found = found || e.Id == auditID
						}
						if found != tc.want {
							t.Fatalf("unfiltered gateway audit visible=%t, want %t", found, tc.want)
						}
					}
				})
			}
		})
	}
}

func TestMcpModeValidation(t *testing.T) {
	s, _, _ := setupMountMcp(t)
	s.MCPMode = "invalid"
	if err := s.initDataMcp(); err == nil {
		t.Fatal("invalid server mode accepted")
	}
	c := Config{MCP: "invalid"}
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "KARAKORAM_MCP") {
		t.Fatalf("config error: %v", err)
	}
}

func TestMcpModeFromEnvironment(t *testing.T) {
	t.Setenv("hostname", "karakoram")
	t.Setenv("KARAKORAM_PUBLIC_URL", "https://example.test")
	t.Setenv("KARAKORAM_ADVERTISE_URL", "https://karakoram:9443")
	t.Setenv("KARAKORAM_MASTER_KEY", strings.Repeat("m", 32))
	t.Setenv("KARAKORAM_BACKEND", "zerops")
	t.Setenv("KARAKORAM_ZEROPS_TOKEN", "test-token")
	t.Setenv("KARAKORAM_ZEROPS_PROJECT_ID", "test-project")
	for _, mode := range []string{"", "off", "readonly", "readwrite", "invalid"} {
		t.Setenv("KARAKORAM_MCP", mode)
		c, err := ConfigFromEnv()
		if mode == "invalid" {
			if err == nil || !strings.Contains(err.Error(), "KARAKORAM_MCP") {
				t.Fatalf("invalid mode: %v", err)
			}
			continue
		}
		want := mode
		if want == "" {
			want = "readonly"
		}
		if err != nil || c.MCP != want {
			t.Fatalf("%q: mode %q, error %v", mode, c.MCP, err)
		}
	}
}
