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
	"github.com/KarakoramDev/Karakoram/internal/auth"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/oauth"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	sdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

func oauthFixtureGrant(t *testing.T, s *Server, id, owner, token string, expires time.Time) {
	t.Helper()
	now := time.Now().UTC()
	record := map[string]any{"id": id, "owner": owner, "clientId": "https://client.example/metadata", "clientName": "Test app", "clientHost": "client.example", "scopes": []string{"karakoram:read"}, "restrictions": domain.TokenRestrictions{ReadOnly: true, Namespaces: []string{"default"}}, "createdAt": now, "expiresAt": now.Add(24 * time.Hour), "tokens": []map[string]any{{"hash": secrets.Hash(token), "kind": "access", "expires": expires}}}
	if e := s.Store.OAuthPut(t.Context(), "grant", id, record); e != nil {
		t.Fatal(e)
	}
}
func enableTestOAuth(t *testing.T, s *Server) {
	t.Helper()
	s.OAuth = oauth.New(oauth.Options{Store: s.Store, Auth: s.Auth, PublicURL: s.PublicURL})
	var e error
	s.MCPMode = "readwrite"
	s.mcpHandler, e = s.newMCPHandler()
	if e != nil {
		t.Fatal(e)
	}
}
func oauthAPIRequest(s *Server, method, path, token string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, "https://localhost"+path, nil)
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	return w
}
func TestOAuthMCPAudienceRestrictionsAndExpiry(t *testing.T) {
	s, _, _ := testServer(t)
	enableTestOAuth(t, s)
	token := "kko_test-access"
	oauthFixtureGrant(t, s, "grant", "admin", token, time.Now().Add(time.Minute))
	w := oauthAPIRequest(s, "GET", "/api/v1/clusters", token)
	if w.Code != 401 {
		t.Fatalf("REST accepted OAuth: %d", w.Code)
	}
	server := httptest.NewServer(s.Handler())
	defer server.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	client := sdk.NewClient(&sdk.Implementation{Name: "untrusted-name", Version: "1"}, nil)
	session, e := client.Connect(ctx, &sdk.StreamableClientTransport{Endpoint: server.URL + "/mcp", HTTPClient: &http.Client{Transport: mcpBearerTransport{token}}}, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer session.Close()
	result, e := session.CallTool(ctx, &sdk.CallToolParams{Name: api.OperationAgent["getCluster"].Tool, Arguments: map[string]any{"cluster": "prod"}})
	if e != nil || result.IsError {
		t.Fatalf("OAuth read failed: %#v %v", result, e)
	}
	result, e = session.CallTool(ctx, &sdk.CallToolParams{Name: api.OperationAgent["deleteCluster"].Tool, Arguments: map[string]any{"cluster": "prod"}})
	if e == nil && !result.IsError {
		t.Fatal("read scope called write tool")
	}
	p, e := s.OAuth.Authenticate(func() *http.Request {
		r := httptest.NewRequest("POST", "https://localhost/mcp", nil)
		r.Header.Set("Authorization", "Bearer "+token)
		return r
	}())
	if e != nil || p.Actor() != "admin (mcp: client.example)" {
		t.Fatal("verified OAuth actor", e)
	}
	oauthFixtureGrant(t, s, "grant", "admin", token, time.Now().Add(-time.Minute))
	w = oauthAPIRequest(s, "POST", "/mcp", token)
	if w.Code != 401 || !strings.Contains(w.Header().Get("WWW-Authenticate"), `resource_metadata="https://localhost/.well-known/oauth-protected-resource/mcp"`) {
		t.Fatal("expired MCP token missing challenge", w.Code, w.Header())
	}
}
func TestOAuthGrantSelfServiceAndAdministratorRBAC(t *testing.T) {
	s, _, admin := testServer(t)
	enableTestOAuth(t, s)
	for _, name := range []string{"alice", "bob"} {
		if _, e := s.Auth.CreateAccount(t.Context(), domain.Account{Name: name}, "a-long-test-password"); e != nil {
			t.Fatal(e)
		}
		oauthFixtureGrant(t, s, name, name, "kko_"+name, time.Now().Add(time.Minute))
	}
	alice, _, e := s.Auth.CreateToken(t.Context(), "alice", "management", nil, nil)
	if e != nil {
		t.Fatal(e)
	}
	for _, test := range []struct {
		token string
		count int
	}{{alice, 1}, {admin, 2}} {
		w := oauthAPIRequest(s, "GET", "/api/v1/oauth-grants", test.token)
		var list api.OAuthGrantList
		if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &list) != nil || !list.Enabled || len(list.Items) != test.count {
			t.Fatalf("grant list status=%d body=%s", w.Code, w.Body.String())
		}
	}
	if w := oauthAPIRequest(s, "DELETE", "/api/v1/oauth-grants/bob", alice); w.Code != 403 {
		t.Fatal("revoked another user's grant", w.Code)
	}
	if w := oauthAPIRequest(s, "DELETE", "/api/v1/oauth-grants/alice", alice); w.Code != 204 {
		t.Fatal("own revoke denied", w.Code, w.Body.String())
	}
	if w := oauthAPIRequest(s, "DELETE", "/api/v1/oauth-grants/bob", admin); w.Code != 204 {
		t.Fatal("admin revoke denied", w.Code, w.Body.String())
	}
}
func TestOAuthDisabledRoutes(t *testing.T) {
	s, _, token := testServer(t)
	for _, path := range []string{"/oauth/authorize", "/oauth/token", "/oauth/revoke", "/oauth/register", "/.well-known/oauth-protected-resource", "/.well-known/oauth-protected-resource/mcp", "/.well-known/oauth-authorization-server"} {
		if w := oauthAPIRequest(s, "GET", path, ""); w.Code != 404 {
			t.Fatalf("disabled route %s: %d", path, w.Code)
		}
	}
	w := oauthAPIRequest(s, "GET", "/api/v1/oauth-grants", token)
	var list api.OAuthGrantList
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &list) != nil || list.Enabled {
		t.Fatal("disabled state not exposed")
	}
}

// OAuth access tokens are audience-bound to /mcp and die with their grant; an
// API token minted or extended through the connection would escape both.
func TestOAuthPrincipalCannotIssueAPITokens(t *testing.T) {
	s, _, admin := testServer(t)
	enableTestOAuth(t, s)
	token := "kko_token-issuer"
	oauthFixtureGrant(t, s, "issuer-grant", "admin", token, time.Now().Add(time.Minute))
	grant, err := s.OAuth.Get(t.Context(), "issuer-grant")
	if err != nil {
		t.Fatal(err)
	}
	grant.Restrictions = &domain.TokenRestrictions{AllowCredentialReveal: true}
	grant.Scopes = []string{"karakoram:read", "karakoram:write", "karakoram:credentials"}
	if err = s.Store.OAuthPut(t.Context(), "grant", grant.ID, grant); err != nil {
		t.Fatal(err)
	}
	call := func(method, path, bearer string, body any) *httptest.ResponseRecorder {
		raw, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		r := httptest.NewRequest(method, "https://localhost/api/v1"+path, strings.NewReader(string(raw)))
		if strings.HasPrefix(bearer, "kko_") {
			r = r.WithContext(auth.WithMCPTraffic(r.Context()))
		}
		r.Header.Set("Authorization", "Bearer "+bearer)
		r.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, r)
		return w
	}
	w := call("POST", "/api-tokens", admin, map[string]any{"name": "existing"})
	if w.Code != http.StatusCreated {
		t.Fatalf("fixture token: %d %s", w.Code, w.Body.String())
	}
	var existing api.CreatedAPIToken
	if err = json.Unmarshal(w.Body.Bytes(), &existing); err != nil {
		t.Fatal(err)
	}
	before, err := s.Auth.ListTokens(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	bounded := grant.ExpiresAt.Add(-30 * time.Second)
	for _, body := range []map[string]any{
		{"name": "bounded-child", "expiresAt": bounded, "restrictions": map[string]any{}},
		{"name": "data-child", "expiresAt": bounded, "restrictions": map[string]any{"permissions": []string{"cluster-data:read"}, "clusters": []string{"default/orders"}, "readOnly": true}},
	} {
		if w := call("POST", "/api-tokens", token, body); w.Code != http.StatusForbidden || !strings.Contains(w.Body.String(), "OAuth connections cannot create or change API tokens") {
			t.Fatalf("OAuth child creation: %d %s", w.Code, w.Body.String())
		}
	}
	if w := call("PUT", "/api-tokens/"+existing.Token.Id, token, map[string]any{"name": "renamed", "expiresAt": bounded}); w.Code != http.StatusForbidden {
		t.Fatalf("OAuth token replacement: %d %s", w.Code, w.Body.String())
	}
	after, err := s.Auth.ListTokens(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != len(before) {
		t.Fatalf("token count changed: %d -> %d", len(before), len(after))
	}
	for _, current := range after {
		if current.ID == existing.Token.Id && (current.Name != "existing" || current.ExpiresAt != nil) {
			t.Fatalf("existing token changed: %+v", current)
		}
	}
}
