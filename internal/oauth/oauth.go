// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
// Package oauth implements the embedded, MCP-audience authorization server.
package oauth

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"html/template"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/auth"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

var scopes = []string{"karakoram:read", "karakoram:write", "karakoram:credentials"}

type Options struct {
	Store       *store.Store
	Auth        *auth.Service
	PublicURL   string
	DCR         bool
	ClientHosts []string
	Resolve     func(context.Context, string) ([]netip.Addr, error)
	Dial        func(context.Context, string, string) (net.Conn, error)
}
type Service struct {
	o         Options
	mu        sync.Mutex
	clientsMu sync.Mutex
	clients   map[string]cachedClient
	now       func() time.Time
}
type credential struct {
	Hash     string    `json:"hash"`
	Kind     string    `json:"kind"`
	Expires  time.Time `json:"expires"`
	Consumed bool      `json:"consumed,omitempty"`
}
type Grant struct {
	ID           string                    `json:"id"`
	Owner        string                    `json:"owner"`
	ClientID     string                    `json:"clientId"`
	ClientName   string                    `json:"clientName"`
	ClientHost   string                    `json:"clientHost"`
	Scopes       []string                  `json:"scopes"`
	Restrictions *domain.TokenRestrictions `json:"restrictions"`
	CreatedAt    time.Time                 `json:"createdAt"`
	ExpiresAt    time.Time                 `json:"expiresAt"`
	LastUsedAt   *time.Time                `json:"lastUsedAt,omitempty"`
	Revoked      bool                      `json:"revoked"`
	CodeHash     string                    `json:"codeHash,omitempty"`
	CodeExpires  time.Time                 `json:"codeExpires"`
	Redirect     string                    `json:"redirect"`
	Challenge    string                    `json:"challenge"`
	Tokens       []credential              `json:"tokens,omitempty"`
}

func New(o Options) *Service {
	o.PublicURL = strings.TrimRight(o.PublicURL, "/")
	if o.Resolve == nil {
		o.Resolve = defaultResolve
	}
	if o.Dial == nil {
		o.Dial = (&net.Dialer{Timeout: 3 * time.Second}).DialContext
	}
	return &Service{o: o, clients: make(map[string]cachedClient), now: func() time.Time { return time.Now().UTC() }}
}
func secret(prefix string) (string, error) { v, e := secrets.RandomString(48); return prefix + v, e }
func (s *Service) ResourceMetadata() string {
	return s.o.PublicURL + "/.well-known/oauth-protected-resource/mcp"
}
func (s *Service) Mount(mux *http.ServeMux) {
	for _, p := range []string{"/.well-known/oauth-protected-resource", "/.well-known/oauth-protected-resource/mcp", "/.well-known/oauth-authorization-server", "/oauth/authorize", "/oauth/token", "/oauth/revoke", "/oauth/register"} {
		mux.Handle(p, s)
	}
}
func jsonResponse(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Pragma", "no-cache")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
func oauthError(w http.ResponseWriter, code string) {
	jsonResponse(w, 400, map[string]string{"error": code})
}
func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	switch r.URL.Path {
	case "/.well-known/oauth-protected-resource", "/.well-known/oauth-protected-resource/mcp":
		if r.Method != "GET" {
			w.WriteHeader(405)
			return
		}
		jsonResponse(w, 200, map[string]any{"resource": s.o.PublicURL + "/mcp", "authorization_servers": []string{s.o.PublicURL}, "scopes_supported": scopes, "bearer_methods_supported": []string{"header"}})
	case "/.well-known/oauth-authorization-server":
		if r.Method != "GET" {
			w.WriteHeader(405)
			return
		}
		v := map[string]any{"issuer": s.o.PublicURL, "authorization_endpoint": s.o.PublicURL + "/oauth/authorize", "token_endpoint": s.o.PublicURL + "/oauth/token", "revocation_endpoint": s.o.PublicURL + "/oauth/revoke", "response_types_supported": []string{"code"}, "grant_types_supported": []string{"authorization_code", "refresh_token"}, "code_challenge_methods_supported": []string{"S256"}, "token_endpoint_auth_methods_supported": []string{"none"}, "client_id_metadata_document_supported": true, "scopes_supported": scopes}
		v["revocation_endpoint_auth_methods_supported"] = []string{"none"}
		v["authorization_response_iss_parameter_supported"] = true
		if s.o.DCR {
			v["registration_endpoint"] = s.o.PublicURL + "/oauth/register"
		}
		jsonResponse(w, 200, v)
	case "/oauth/authorize":
		s.authorize(w, r)
	case "/oauth/register":
		if !s.o.DCR {
			http.NotFound(w, r)
			return
		}
		if r.Method != "POST" {
			w.WriteHeader(405)
			return
		}
		var c Client
		if json.NewDecoder(http.MaxBytesReader(w, r.Body, 5120)).Decode(&c) != nil || validateClient(c) != nil {
			oauthError(w, "invalid_client_metadata")
			return
		}
		id, e := secret("kkc_")
		if e != nil {
			w.WriteHeader(500)
			return
		}
		c.ID = id
		s.mu.Lock()
		e = s.o.Store.OAuthPut(r.Context(), "client", id, c)
		s.mu.Unlock()
		if e != nil {
			w.WriteHeader(500)
			return
		}
		jsonResponse(w, 201, c)
	case "/oauth/token", "/oauth/revoke":
		if r.Method != "POST" {
			w.WriteHeader(405)
			return
		}
		if strings.Split(r.Header.Get("Content-Type"), ";")[0] != "application/x-www-form-urlencoded" {
			oauthError(w, "invalid_request")
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, 8192)
		if r.ParseForm() != nil {
			oauthError(w, "invalid_request")
			return
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		if r.URL.Path == "/oauth/token" {
			s.token(w, r)
		} else {
			s.revoke(w, r)
		}
	default:
		http.NotFound(w, r)
	}
}
func (s *Service) audit(ctx context.Context, g *Grant, reason string) error {
	actor := store.Actor(ctx)
	if actor == "reconciler" {
		actor = g.Owner + " (mcp: " + g.ClientHost + ")"
	}
	return s.o.Store.AppendEvent(ctx, &domain.Event{Resource: "auth", Target: g.ID, Actor: actor, Type: "Normal", Reason: reason, Message: reason, CreatedAt: s.now()})
}
func (s *Service) active(ctx context.Context, g *Grant) bool {
	if g.Revoked || !g.ExpiresAt.After(s.now()) {
		return false
	}
	a, e := s.o.Store.GetAccount(ctx, g.Owner)
	return e == nil && !a.Disabled && !a.MustChangePassword && !g.CreatedAt.Before(a.CreatedAt) && !g.CreatedAt.Before(a.PasswordChangedAt)
}
func (s *Service) token(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	kind := r.PostForm.Get("grant_type")
	raw := r.PostForm.Get("code")
	if kind == "refresh_token" {
		raw = r.PostForm.Get("refresh_token")
	} else if kind != "authorization_code" {
		oauthError(w, "unsupported_grant_type")
		return
	}
	var g Grant
	hash := secrets.Hash(raw)
	if raw == "" || s.o.Store.OAuthFind(ctx, hash, &g) != nil || g.ClientID != r.PostForm.Get("client_id") || !s.active(ctx, &g) {
		oauthError(w, "invalid_grant")
		return
	}
	if resource := r.PostForm.Get("resource"); resource != "" && resource != s.o.PublicURL+"/mcp" {
		oauthError(w, "invalid_target")
		return
	}
	if requested := r.PostForm.Get("scope"); requested != "" {
		for _, v := range strings.Fields(requested) {
			if !slices.Contains(g.Scopes, v) {
				oauthError(w, "invalid_scope")
				return
			}
		}
	}
	if kind == "authorization_code" {
		verifier := r.PostForm.Get("code_verifier")
		sum := sha256.Sum256([]byte(verifier))
		if g.CodeHash != hash || !g.CodeExpires.After(s.now()) || g.Redirect != r.PostForm.Get("redirect_uri") || len(verifier) < 43 || len(verifier) > 128 || base64.RawURLEncoding.EncodeToString(sum[:]) != g.Challenge {
			oauthError(w, "invalid_grant")
			return
		}
		g.CodeHash = ""
	} else {
		found := false
		for i := range g.Tokens {
			t := &g.Tokens[i]
			if t.Hash != hash || t.Kind != "refresh" {
				continue
			}
			if t.Consumed {
				g.Revoked = true
				if s.o.Store.OAuthPut(ctx, "grant", g.ID, g) != nil || s.audit(ctx, &g, "OAuthRefreshReuseDetected") != nil {
					w.WriteHeader(500)
					return
				}
				oauthError(w, "invalid_grant")
				return
			}
			if !t.Expires.After(s.now()) {
				break
			}
			t.Consumed = true
			found = true
			break
		}
		if !found {
			oauthError(w, "invalid_grant")
			return
		}
	}
	access, e := secret("kko_")
	if e != nil {
		w.WriteHeader(500)
		return
	}
	refresh, e := secret("kkr_")
	if e != nil {
		w.WriteHeader(500)
		return
	}
	now := s.now()
	g.LastUsedAt = &now
	g.Tokens = slices.DeleteFunc(g.Tokens, func(t credential) bool { return t.Kind == "access" && !t.Expires.After(now) })
	g.Tokens = append(g.Tokens, credential{Hash: secrets.Hash(access), Kind: "access", Expires: now.Add(10 * time.Minute)}, credential{Hash: secrets.Hash(refresh), Kind: "refresh", Expires: g.ExpiresAt})
	if s.o.Store.OAuthPut(ctx, "grant", g.ID, g) != nil {
		w.WriteHeader(500)
		return
	}
	jsonResponse(w, 200, map[string]any{"access_token": access, "token_type": "Bearer", "expires_in": 600, "refresh_token": refresh, "scope": strings.Join(g.Scopes, " ")})
}
func (s *Service) revoke(w http.ResponseWriter, r *http.Request) {
	var g Grant
	if s.o.Store.OAuthFind(r.Context(), secrets.Hash(r.PostForm.Get("token")), &g) == nil && g.ClientID == r.PostForm.Get("client_id") {
		if s.revokeGrant(r.Context(), &g) != nil {
			w.WriteHeader(500)
			return
		}
	}
	w.WriteHeader(200)
}
func (s *Service) revokeGrant(ctx context.Context, g *Grant) error {
	g.Revoked = true
	if e := s.o.Store.OAuthPut(ctx, "grant", g.ID, g); e != nil {
		return e
	}
	return s.audit(ctx, g, "OAuthGrantRevoked")
}
func (s *Service) List(ctx context.Context) ([]Grant, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	raw, e := s.o.Store.OAuthList(ctx, "grant")
	if e != nil {
		return nil, e
	}
	result := make([]Grant, 0, len(raw))
	for _, v := range raw {
		var g Grant
		if e = json.Unmarshal(v, &g); e != nil {
			return nil, e
		}
		if !g.Revoked {
			g.CodeHash = ""
			g.Challenge = ""
			g.Tokens = nil
			result = append(result, g)
		}
	}
	return result, nil
}
func (s *Service) Get(ctx context.Context, id string) (*Grant, error) {
	var g Grant
	e := s.o.Store.OAuthGet(ctx, "grant", id, &g)
	return &g, e
}
func (s *Service) Revoke(ctx context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	g, e := s.Get(ctx, id)
	if e != nil {
		return e
	}
	return s.revokeGrant(ctx, g)
}

// Authenticate accepts only this server's access tokens. Call only from the MCP
// boundary; REST authentication intentionally has no knowledge of these tokens.
func (s *Service) Authenticate(r *http.Request) (*auth.Principal, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	parts := strings.Fields(r.Header.Get("Authorization"))
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") || !strings.HasPrefix(parts[1], "kko_") {
		return nil, auth.ErrUnauthenticated
	}
	hash := secrets.Hash(parts[1])
	var g Grant
	if s.o.Store.OAuthFind(r.Context(), hash, &g) != nil || !s.active(r.Context(), &g) {
		return nil, auth.ErrUnauthenticated
	}
	valid := false
	for _, t := range g.Tokens {
		if t.Hash == hash && t.Kind == "access" && t.Expires.After(s.now()) {
			valid = true
			break
		}
	}
	if !valid {
		return nil, auth.ErrUnauthenticated
	}
	a, e := s.o.Store.GetAccount(r.Context(), g.Owner)
	if e != nil {
		return nil, auth.ErrUnauthenticated
	}
	now := s.now()
	g.LastUsedAt = &now
	if s.o.Store.OAuthPut(r.Context(), "grant", g.ID, g) != nil {
		return nil, auth.ErrUnauthenticated
	}
	subjects := []string{"user:" + a.Name}
	for _, role := range a.Roles {
		subjects = append(subjects, "role:"+strings.TrimPrefix(role, "role:"))
	}
	return &auth.Principal{Account: a.Name, AccountCreatedAt: a.CreatedAt, Subjects: subjects, Via: "oauth", TokenID: g.ID, TokenName: g.ClientHost, OIDCIssuer: a.OIDCIssuer, Restrictions: auth.CloneRestrictions(g.Restrictions)}, nil
}

var consent = template.Must(template.New("consent").Parse(`<!doctype html><html><head><title>Connect app</title></head><body><h1>Connect {{.Client.Name}}</h1><p>Client: {{.Client.ID}}</p><p>Host: {{.Host}}</p><p>Redirect: {{.Redirect}}</p><p>Scopes: {{.Scopes}}</p><form method="post"><input type="hidden" name="csrf" value="{{.CSRF}}"><label>Namespace limit (comma separated; blank means all)<input name="namespaces"></label><button name="decision" value="approve">Approve</button><button name="decision" value="deny">Deny</button></form></body></html>`))

func (s *Service) authorize(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" && r.Method != "POST" {
		w.WriteHeader(405)
		return
	}
	w.Header().Set("X-Frame-Options", "DENY")
	w.Header().Set("Content-Security-Policy", "default-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'; form-action 'self'")
	q := r.URL.Query()
	client, e := s.client(r.Context(), q.Get("client_id"))
	if e != nil {
		http.Error(w, "Invalid client", 400)
		return
	}
	redirect := q.Get("redirect_uri")
	valid := false
	for _, registered := range client.Redirects {
		if redirectMatches(registered, redirect) {
			valid = true
		}
	}
	if !valid {
		http.Error(w, "Invalid redirect URI", 400)
		return
	}
	respond := func(key, value string) {
		u, _ := url.Parse(redirect)
		v := u.Query()
		v.Set(key, value)
		v.Set("iss", s.o.PublicURL)
		if state := q.Get("state"); state != "" {
			v.Set("state", state)
		}
		u.RawQuery = v.Encode()
		http.Redirect(w, r, u.String(), http.StatusSeeOther)
	}
	if q.Get("response_type") != "code" {
		respond("error", "unsupported_response_type")
		return
	}
	challenge := q.Get("code_challenge")
	decoded, e := base64.RawURLEncoding.DecodeString(challenge)
	if e != nil || len(decoded) != 32 || q.Get("code_challenge_method") != "S256" {
		respond("error", "invalid_request")
		return
	}
	if v := q.Get("resource"); v != "" && v != s.o.PublicURL+"/mcp" {
		respond("error", "invalid_target")
		return
	}
	requested := strings.Fields(q.Get("scope"))
	if len(requested) == 0 {
		requested = []string{"karakoram:read"}
	}
	for _, v := range requested {
		if !slices.Contains(scopes, v) {
			respond("error", "invalid_scope")
			return
		}
	}
	if r.Header.Get("Authorization") != "" {
		http.Error(w, "Browser session required", http.StatusUnauthorized)
		return
	}
	if r.Method == "POST" {
		if r.Header.Get("Origin") != s.o.PublicURL {
			http.Error(w, "Invalid origin", http.StatusForbidden)
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, 8192)
		if r.ParseForm() != nil {
			http.Error(w, "Invalid form", 400)
			return
		}
		r.Header.Set(auth.CSRFHeader, r.PostForm.Get("csrf"))
	}
	p, e := s.o.Auth.Authenticate(r)
	if e != nil || p == nil || p.MustChangePassword {
		if r.Method == "POST" {
			http.Error(w, "Session and CSRF required", http.StatusForbidden)
			return
		}
		http.Redirect(w, r, "/login?returnTo="+url.QueryEscape(r.URL.RequestURI()), http.StatusSeeOther)
		return
	}
	if p.CSRFToken == "" {
		http.Error(w, "Browser session required", http.StatusUnauthorized)
		return
	}
	host := "registered client"
	if u, e := url.Parse(client.ID); e == nil && u.Host != "" {
		host = u.Hostname()
	}
	if r.Method == "GET" {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_ = consent.Execute(w, map[string]any{"Client": client, "Host": host, "Redirect": redirect, "Scopes": strings.Join(requested, " "), "CSRF": p.CSRFToken})
		return
	}
	if r.PostForm.Get("decision") != "approve" {
		respond("error", "access_denied")
		return
	}
	restrictions := &domain.TokenRestrictions{ReadOnly: !slices.Contains(requested, "karakoram:write"), AllowCredentialReveal: slices.Contains(requested, "karakoram:credentials")}
	if raw := strings.TrimSpace(r.PostForm.Get("namespaces")); raw != "" {
		for _, v := range strings.Split(raw, ",") {
			v = strings.TrimSpace(v)
			if domain.ValidateNamespaceName(v) != nil {
				http.Error(w, "Invalid namespace limit", 400)
				return
			}
			restrictions.Namespaces = append(restrictions.Namespaces, v)
		}
	}
	restrictions, e = auth.NormalizeRestrictions(restrictions)
	if e != nil {
		http.Error(w, "Invalid restrictions", 400)
		return
	}
	code, e := secret("")
	if e != nil {
		w.WriteHeader(500)
		return
	}
	id, e := secret("")
	if e != nil {
		w.WriteHeader(500)
		return
	}
	now := s.now()
	g := Grant{ID: id, Owner: p.Account, ClientID: client.ID, ClientName: client.Name, ClientHost: host, Scopes: requested, Restrictions: restrictions, CreatedAt: now, ExpiresAt: now.Add(30 * 24 * time.Hour), CodeHash: secrets.Hash(code), CodeExpires: now.Add(time.Minute), Redirect: redirect, Challenge: challenge}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.o.Store.OAuthPut(r.Context(), "grant", id, g) != nil || s.audit(r.Context(), &g, "OAuthGrantIssued") != nil {
		w.WriteHeader(500)
		return
	}
	respond("code", code)
}
