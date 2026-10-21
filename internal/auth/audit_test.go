// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package auth

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

func authenticationEvents(t *testing.T, s *Service) []domain.Event {
	t.Helper()
	events, err := s.o.Store.ListEvents(t.Context(), store.EventFilter{Resource: "auth"})
	if err != nil {
		t.Fatal(err)
	}
	return events
}

func requireAuthenticationEvent(t *testing.T, event domain.Event, account, actor, reason string) {
	t.Helper()
	if event.ID == 0 || event.CreatedAt.IsZero() || event.Cluster != "" || event.Resource != "auth" || event.Target != account || event.Actor != actor || event.Reason != reason {
		t.Fatalf("unexpected authentication event: %+v", event)
	}
	kind := "Normal"
	if reason == "LoginFailed" {
		kind = "Warning"
	}
	if event.Type != kind {
		t.Fatalf("event type = %q, want %q", event.Type, kind)
	}
}

func TestPrincipalActor(t *testing.T) {
	for _, tc := range []struct {
		name      string
		principal Principal
		want      string
	}{
		{name: "local", principal: Principal{Account: "admin", Via: "session"}, want: "admin"},
		{name: "oidc", principal: Principal{Account: "sso-user", Via: "oidc", OIDCIssuer: "https://issuer.example"}, want: "sso-user (oidc: https://issuer.example)"},
		{name: "named token", principal: Principal{Account: "admin", Via: "token", TokenName: "deploy job", TokenID: "public-id"}, want: "admin (token: deploy job [public-id])"},
		{name: "unnamed token", principal: Principal{Account: "admin", Via: "token", TokenID: "public-id"}, want: "admin (token: [public-id])"},
		{name: "oidc token", principal: Principal{Account: "sso-user", Via: "token", OIDCIssuer: "https://issuer.example", TokenName: "deploy job", TokenID: "public-id"}, want: "sso-user (oidc: https://issuer.example) (token: deploy job [public-id])"},
		{name: "unnamed oidc token", principal: Principal{Account: "sso-user", Via: "token", OIDCIssuer: "https://issuer.example", TokenID: "public-id"}, want: "sso-user (oidc: https://issuer.example) (token: [public-id])"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.principal.Actor(); got != tc.want {
				t.Fatalf("Actor() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestAuthenticatedTokenActorsAreDistinct(t *testing.T) {
	s := setup(t)
	actors := make(map[string]bool)
	for range 2 {
		secret, token, err := s.CreateToken(t.Context(), "admin", "deployment", nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		r := request("GET", nil, "")
		r.Header.Set("Authorization", "Bearer "+secret)
		principal, err := s.Authenticate(r)
		if err != nil {
			t.Fatal(err)
		}
		want := "admin (token: deployment [" + token.ID + "])"
		if principal.TokenName != token.Name || principal.TokenID != token.ID || principal.Actor() != want || strings.Contains(principal.Actor(), secret) {
			t.Fatalf("token actor = %q, want %q", principal.Actor(), want)
		}
		if actors[principal.Actor()] {
			t.Fatal("different tokens produced the same actor")
		}
		actors[principal.Actor()] = true
		if _, err = s.Authenticate(r); err != nil {
			t.Fatal(err)
		}
		if len(authenticationEvents(t, s)) != 0 {
			t.Fatal("token authentication emitted a state-changing audit event")
		}
	}
}

func TestLocalLoginFailureAuditIsBoundedAndRateLimitFriendly(t *testing.T) {
	s := setup(t)
	s.verifyPassword = func(string, string) bool { return false }
	ctx := WithClientIP(t.Context(), "192.0.2.45")
	const attemptedPassword = "password-must-not-appear-in-audit"
	for _, username := range []string{"admin", "missing-user"} {
		if _, err := s.Login(ctx, httptest.NewRecorder(), username, attemptedPassword); !errors.Is(err, ErrUnauthenticated) {
			t.Fatal(err)
		}
	}
	events := authenticationEvents(t, s)
	if len(events) != 2 {
		t.Fatalf("failed login events = %d, want 2", len(events))
	}
	for i, username := range []string{"admin", "missing-user"} {
		requireAuthenticationEvent(t, events[i], username, username, "LoginFailed")
		encoded, err := json.Marshal(events[i])
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(encoded), attemptedPassword) || strings.Contains(string(encoded), "192.0.2.45") {
			t.Fatal("login audit included a password or IP address")
		}
	}
	for _, username := range []string{"", "bad\nidentity", strings.Repeat("a", 65)} {
		if _, err := s.Login(ctx, httptest.NewRecorder(), username, attemptedPassword); !errors.Is(err, ErrUnauthenticated) {
			t.Fatal(err)
		}
	}
	if len(authenticationEvents(t, s)) != 2 {
		t.Fatal("malformed identity was persisted in the audit log")
	}
	for range 4 {
		if _, err := s.Login(ctx, httptest.NewRecorder(), "admin", attemptedPassword); !errors.Is(err, ErrUnauthenticated) {
			t.Fatal(err)
		}
	}
	for range 3 {
		if _, err := s.Login(ctx, httptest.NewRecorder(), "admin", attemptedPassword); !errors.Is(err, ErrRateLimited) {
			t.Fatal(err)
		}
	}
	if len(authenticationEvents(t, s)) != 6 {
		t.Fatal("backoff denials emitted audit events")
	}
	s.loginWork <- struct{}{}
	s.loginWork <- struct{}{}
	if _, err := s.Login(ctx, httptest.NewRecorder(), "another-user", attemptedPassword); !errors.Is(err, ErrRateLimited) {
		t.Fatal(err)
	}
	if len(authenticationEvents(t, s)) != 6 {
		t.Fatal("admission denial emitted an audit event")
	}
}

func TestCookieLogoutAuditUsesRefreshFamilyIdentity(t *testing.T) {
	s := setup(t)
	cookies, info := login(t, s)
	events := authenticationEvents(t, s)
	if len(events) != 1 {
		t.Fatalf("login events = %d, want 1", len(events))
	}
	requireAuthenticationEvent(t, events[0], "admin", "admin", "LoginSucceeded")
	// Logout must recover the owner without an access cookie or principal, even
	// after access expiry, using only the authenticated refresh family.
	var refreshCookies []*http.Cookie
	for _, cookie := range cookies {
		if cookie.Name != AccessCookie {
			refreshCookies = append(refreshCookies, cookie)
		}
	}
	now := s.now().Add(s.o.AccessTTL + time.Second)
	s.now = func() time.Time { return now }
	if err := s.Logout(t.Context(), httptest.NewRecorder(), request("POST", refreshCookies, "")); !errors.Is(err, ErrCSRF) {
		t.Fatal(err)
	}
	if len(authenticationEvents(t, s)) != 1 {
		t.Fatal("failed CSRF logout emitted a success audit")
	}
	w := httptest.NewRecorder()
	if err := s.Logout(t.Context(), w, request("POST", refreshCookies, info.CSRFToken)); err != nil {
		t.Fatal(err)
	}
	events = authenticationEvents(t, s)
	if len(events) != 2 {
		t.Fatalf("login/logout events = %d, want 2", len(events))
	}
	requireAuthenticationEvent(t, events[1], "admin", "admin", "LogoutSucceeded")
	if len(w.Result().Cookies()) != 3 {
		t.Fatal("logout did not clear every local cookie")
	}
	for _, cookie := range w.Result().Cookies() {
		if cookie.MaxAge != -1 || cookie.Value != "" {
			t.Fatal("logout did not expire cookie", cookie.Name)
		}
	}
	if err := s.Logout(t.Context(), httptest.NewRecorder(), request("POST", refreshCookies, info.CSRFToken)); err != nil {
		t.Fatal(err)
	}
	if err := s.Logout(t.Context(), httptest.NewRecorder(), request("POST", nil, "")); err != nil {
		t.Fatal(err)
	}
	if len(authenticationEvents(t, s)) != 2 {
		t.Fatal("anonymous or already revoked logout created a misleading audit")
	}
}

func TestRefreshAuditUsesSessionOwner(t *testing.T) {
	s := setup(t)
	cookies, info := login(t, s)
	if _, err := s.Refresh(t.Context(), httptest.NewRecorder(), request("POST", cookies, info.CSRFToken)); err != nil {
		t.Fatal(err)
	}
	events := authenticationEvents(t, s)
	if len(events) != 2 {
		t.Fatalf("login/refresh events = %d, want 2", len(events))
	}
	requireAuthenticationEvent(t, events[1], "admin", "admin", "RefreshSessionSucceeded")
}

func TestOIDCAuthenticationAuditRequiresModeledIdentity(t *testing.T) {
	s := setup(t)
	provider := newIssuer(t)
	actor := "sso-user (oidc: " + provider.server.URL + ")"
	if err := s.UpdateSettings(t.Context(), OIDCSettings{IssuerURL: provider.server.URL, ClientID: "client", ClientSecret: "test-client-secret", AutoCreate: true, AllowedGroups: []string{"operators"}}); err != nil {
		t.Fatal(err)
	}
	if w := oidcCallback(t, s, provider, false); w.Code != http.StatusFound {
		t.Fatalf("OIDC login status = %d", w.Code)
	}
	events := authenticationEvents(t, s)
	if len(events) != 1 {
		t.Fatalf("OIDC login events = %d, want 1", len(events))
	}
	requireAuthenticationEvent(t, events[0], "sso-user", actor, "LoginSucceeded")
	provider.groups = []string{"outsiders"}
	if w := oidcCallback(t, s, provider, false); w.Code != http.StatusUnauthorized {
		t.Fatalf("OIDC group denial status = %d", w.Code)
	}
	events = authenticationEvents(t, s)
	if len(events) != 2 {
		t.Fatalf("OIDC events = %d, want 2", len(events))
	}
	requireAuthenticationEvent(t, events[1], "sso-user", actor, "LoginFailed")
	provider.groups = []string{"operators"}
	if w := oidcCallback(t, s, provider, true); w.Code != http.StatusUnauthorized {
		t.Fatalf("OIDC state denial status = %d", w.Code)
	}
	provider.badNonce = true
	if w := oidcCallback(t, s, provider, false); w.Code != http.StatusUnauthorized {
		t.Fatalf("OIDC nonce denial status = %d", w.Code)
	}
	provider.badNonce = false
	provider.username = "admin"
	if w := oidcCallback(t, s, provider, false); w.Code != http.StatusUnauthorized {
		t.Fatalf("OIDC local collision status = %d", w.Code)
	}
	if len(authenticationEvents(t, s)) != 2 {
		t.Fatal("unverified callback or local account collision was attributed to an account")
	}
	encoded, err := json.Marshal(events)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"test-client-secret", "stable-provider-subject", "test-access"} {
		if strings.Contains(string(encoded), secret) {
			t.Fatal("OIDC audit included credential or provider subject material")
		}
	}
}

func TestOIDCSessionAndTokenAuditsKeepIssuerScope(t *testing.T) {
	s := setup(t)
	provider := newIssuer(t)
	if err := s.UpdateSettings(t.Context(), OIDCSettings{IssuerURL: provider.server.URL, ClientID: "client", ClientSecret: "test-client-secret", AutoCreate: true}); err != nil {
		t.Fatal(err)
	}
	w := oidcCallback(t, s, provider, false)
	if w.Code != http.StatusFound {
		t.Fatalf("OIDC login status = %d", w.Code)
	}
	cookies := w.Result().Cookies()
	principal, err := s.Authenticate(request("GET", cookies, ""))
	if err != nil {
		t.Fatal(err)
	}
	actor := "sso-user (oidc: " + provider.server.URL + ")"
	if principal.OIDCIssuer != provider.server.URL || principal.Actor() != actor {
		t.Fatalf("OIDC principal actor = %q, want %q", principal.Actor(), actor)
	}
	csrfToken := principal.CSRFToken
	secret, token, err := s.CreateToken(t.Context(), "sso-user", "deployment", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	r := request("GET", nil, "")
	r.Header.Set("Authorization", "Bearer "+secret)
	tokenPrincipal, err := s.Authenticate(r)
	if err != nil {
		t.Fatal(err)
	}
	if want := actor + " (token: deployment [" + token.ID + "])"; tokenPrincipal.Actor() != want {
		t.Fatalf("OIDC token actor = %q, want %q", tokenPrincipal.Actor(), want)
	}
	// A failed local login records the bounded attempted name, not a claim to
	// have authenticated the OIDC identity with the same account name.
	if _, err = s.Login(t.Context(), httptest.NewRecorder(), "sso-user", password); !errors.Is(err, ErrUnauthenticated) {
		t.Fatal(err)
	}
	w = httptest.NewRecorder()
	if _, err = s.Refresh(t.Context(), w, request("POST", cookies, csrfToken)); err != nil {
		t.Fatal(err)
	}
	var refreshCookies []*http.Cookie
	for _, cookie := range w.Result().Cookies() {
		if cookie.Name != AccessCookie {
			refreshCookies = append(refreshCookies, cookie)
		}
	}
	if err = s.Logout(t.Context(), httptest.NewRecorder(), request("POST", refreshCookies, csrfToken)); err != nil {
		t.Fatal(err)
	}
	events := authenticationEvents(t, s)
	if len(events) != 4 {
		t.Fatalf("OIDC session audit events = %d, want 4", len(events))
	}
	requireAuthenticationEvent(t, events[0], "sso-user", actor, "LoginSucceeded")
	requireAuthenticationEvent(t, events[1], "sso-user", "sso-user", "LoginFailed")
	requireAuthenticationEvent(t, events[2], "sso-user", actor, "RefreshSessionSucceeded")
	requireAuthenticationEvent(t, events[3], "sso-user", actor, "LogoutSucceeded")
}
