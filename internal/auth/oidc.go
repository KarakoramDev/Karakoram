// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package auth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/store"
	"github.com/coreos/go-oidc/v3/oidc"
	"github.com/golang-jwt/jwt/v5"
	"golang.org/x/oauth2"
)

// OIDCSettings configures server-side authorization-code SSO. ClientSecret is
// write-only: empty on update retains it, and Settings always omits its value.
type OIDCSettings struct {
	IssuerURL     string   `json:"issuerURL"`
	ClientID      string   `json:"clientID"`
	ClientSecret  string   `json:"clientSecret,omitempty"`
	Scopes        []string `json:"scopes"`
	GroupsClaim   string   `json:"groupsClaim"`
	UsernameClaim string   `json:"usernameClaim"`
	AllowedGroups []string `json:"allowedGroups,omitempty"`
	AutoCreate    bool     `json:"autoCreate"`
}

func (s *Service) oidcSettings(ctx context.Context) (*OIDCSettings, error) {
	raw, err := s.o.Store.GetSetting(ctx, "auth/oidc")
	if errors.Is(err, store.ErrNotFound) {
		return &OIDCSettings{}, nil
	}
	if err != nil {
		return nil, err
	}
	var cfg OIDCSettings
	if err = json.Unmarshal(raw, &cfg); err != nil {
		return nil, err
	}
	if cfg.ClientSecret != "" {
		plain, err := s.o.Sealer.OpenFor("auth/oidc/client-secret", cfg.ClientSecret)
		if err != nil {
			return nil, err
		}
		cfg.ClientSecret = string(plain)
	}
	return &cfg, nil
}

// Settings reads one persisted snapshot without joining identity mutations.
// Public version discovery must remain available while a mutation waits to write.
func (s *Service) Settings(ctx context.Context) (*OIDCSettings, error) {
	cfg, err := s.oidcSettings(ctx)
	if cfg != nil {
		cfg.ClientSecret = ""
	}
	return cfg, err
}

// UpdateSettings validates and seals write-only OIDC credentials at rest.
func (s *Service) UpdateSettings(ctx context.Context, cfg OIDCSettings) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if cfg.IssuerURL != "" {
		u, err := url.Parse(cfg.IssuerURL)
		if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || cfg.ClientID == "" || (u.Scheme != "https" && !(u.Scheme == "http" && (u.Hostname() == "127.0.0.1" || u.Hostname() == "localhost" || u.Hostname() == "::1"))) {
			return fmt.Errorf("%w: OIDC requires a valid HTTPS issuer and client ID", ErrInvalid)
		}
	}
	if cfg.UsernameClaim == "" {
		cfg.UsernameClaim = "preferred_username"
	}
	if cfg.GroupsClaim == "" {
		cfg.GroupsClaim = "groups"
	}
	if len(cfg.Scopes) == 0 {
		cfg.Scopes = []string{"openid", "profile", "email"}
	}
	if cfg.ClientSecret == "" && cfg.IssuerURL != "" {
		old, err := s.oidcSettings(ctx)
		if err != nil {
			return err
		}
		if old.IssuerURL == cfg.IssuerURL && old.ClientID == cfg.ClientID {
			cfg.ClientSecret = old.ClientSecret
		}
	}
	if cfg.ClientSecret != "" {
		sealed, err := s.o.Sealer.SealFor("auth/oidc/client-secret", []byte(cfg.ClientSecret))
		if err != nil {
			return err
		}
		cfg.ClientSecret = sealed
	}
	raw, err := json.Marshal(cfg)
	if err != nil {
		return err
	}
	return s.o.Store.SetSetting(ctx, "auth/oidc", raw)
}
func (s *Service) oidcClient(ctx context.Context) (*OIDCSettings, *oidc.Provider, *oauth2.Config, error) {
	s.mu.Lock()
	cfg, err := s.oidcSettings(ctx)
	s.mu.Unlock()
	if err != nil {
		return nil, nil, nil, err
	}
	if cfg.IssuerURL == "" || s.o.PublicURL == "" {
		return nil, nil, nil, errors.New("OIDC is not configured")
	}
	provider, err := oidc.NewProvider(ctx, cfg.IssuerURL)
	if err != nil {
		return nil, nil, nil, err
	}
	scopes := []string{oidc.ScopeOpenID}
	for _, scope := range cfg.Scopes {
		if scope != oidc.ScopeOpenID {
			scopes = append(scopes, scope)
		}
	}
	config := &oauth2.Config{ClientID: cfg.ClientID, ClientSecret: cfg.ClientSecret, Endpoint: provider.Endpoint(), RedirectURL: strings.TrimRight(s.o.PublicURL, "/") + "/api/v1/auth/oidc/callback", Scopes: scopes}
	return cfg, provider, config, nil
}

const oidcCookie = "karakoram_oidc_state"

type oidcState struct {
	jwt.RegisteredClaims
	State      string `json:"state"`
	Nonce      string `json:"nonce"`
	Verifier   string `json:"verifier"`
	OIDCIssuer string `json:"oi"`
	ClientID   string `json:"ci"`
	ReturnTo   string `json:"returnTo,omitempty"`
}

// oidcHTTPError uses the same secret-safe error envelope as the management API.
func oidcHTTPError(w http.ResponseWriter, message string, status int) {
	code := "internal_error"
	if status == http.StatusUnauthorized {
		code = "unauthorized"
	} else if status == http.StatusServiceUnavailable {
		code = "unavailable"
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	}{code, message})
}

// oidcReturnPath only resumes the embedded OAuth consent flow. Parse and require
// the exact absolute-path spelling so encoded separators and network-path URLs
// cannot turn the callback into an open redirect.
func oidcReturnPath(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || !strings.HasPrefix(raw, "/oauth/authorize?") || u.IsAbs() || u.Host != "" || u.User != nil || u.Fragment != "" || u.Path != "/oauth/authorize" || u.RawPath != "" || strings.ContainsAny(raw, "\\\r\n") {
		return "/"
	}
	return raw
}

// OIDCLoginHandler sends state, nonce and S256 PKCE to the verified issuer. The
// signed ten-minute state cookie uses Lax only for the top-level SSO callback;
// local access/refresh/CSRF cookies remain SameSite Strict.
func (s *Service) OIDCLoginHandler(w http.ResponseWriter, r *http.Request) {
	cfg, _, config, err := s.oidcClient(r.Context())
	if err != nil {
		oidcHTTPError(w, "OIDC unavailable", http.StatusServiceUnavailable)
		return
	}
	state, err := random()
	if err != nil {
		oidcHTTPError(w, "authentication unavailable", 500)
		return
	}
	nonce, err := random()
	if err != nil {
		oidcHTTPError(w, "authentication unavailable", 500)
		return
	}
	verifier := oauth2.GenerateVerifier()
	now := s.now()
	claims := oidcState{RegisteredClaims: jwt.RegisteredClaims{Issuer: s.o.Issuer, Audience: jwt.ClaimStrings{"karakoram-oidc-state"}, IssuedAt: jwt.NewNumericDate(now), ExpiresAt: jwt.NewNumericDate(now.Add(10 * time.Minute))}, State: state, Nonce: nonce, Verifier: verifier, OIDCIssuer: cfg.IssuerURL, ClientID: cfg.ClientID}
	claims.ReturnTo = oidcReturnPath(r.URL.Query().Get("returnTo"))
	signed, err := jwt.NewWithClaims(jwt.SigningMethodEdDSA, claims).SignedString(s.key)
	if err != nil {
		oidcHTTPError(w, "authentication unavailable", 500)
		return
	}
	http.SetCookie(w, &http.Cookie{Name: oidcCookie, Value: signed, Path: "/api/v1/auth/oidc", HttpOnly: true, Secure: s.o.SecureCookies, SameSite: http.SameSiteLaxMode, MaxAge: 600, Expires: now.Add(10 * time.Minute)})
	http.Redirect(w, r, config.AuthCodeURL(state, oidc.Nonce(nonce), oauth2.S256ChallengeOption(verifier)), http.StatusFound)
}

// OIDCCallbackHandler verifies signature, issuer, audience, expiry, nonce and
// provider subject before provisioning. Existing local identities cannot be
// adopted by an OIDC issuer, even if their username matches.
func (s *Service) OIDCCallbackHandler(w http.ResponseWriter, r *http.Request) {
	http.SetCookie(w, &http.Cookie{Name: oidcCookie, Path: "/api/v1/auth/oidc", HttpOnly: true, Secure: s.o.SecureCookies, SameSite: http.SameSiteLaxMode, MaxAge: -1})
	auditPrincipal := Principal{}
	fail := func() {
		if auditPrincipal.Account != "" {
			if err := s.auditAuthentication(r.Context(), auditPrincipal, "LoginFailed", "OIDC login failed"); err != nil {
				oidcHTTPError(w, "authentication unavailable", http.StatusServiceUnavailable)
				return
			}
		}
		oidcHTTPError(w, "OIDC authentication failed", http.StatusUnauthorized)
	}
	c, err := r.Cookie(oidcCookie)
	if err != nil {
		fail()
		return
	}
	state := &oidcState{}
	_, err = jwt.ParseWithClaims(c.Value, state, func(t *jwt.Token) (interface{}, error) { return s.key.Public(), nil }, jwt.WithValidMethods([]string{"EdDSA"}), jwt.WithIssuer(s.o.Issuer), jwt.WithAudience("karakoram-oidc-state"), jwt.WithExpirationRequired(), jwt.WithIssuedAt(), jwt.WithTimeFunc(s.now))
	if err != nil || !equal(state.State, r.URL.Query().Get("state")) || r.URL.Query().Get("code") == "" {
		fail()
		return
	}
	cfg, provider, config, err := s.oidcClient(r.Context())
	if err != nil || cfg.IssuerURL != state.OIDCIssuer || cfg.ClientID != state.ClientID {
		fail()
		return
	}
	token, err := config.Exchange(r.Context(), r.URL.Query().Get("code"), oauth2.VerifierOption(state.Verifier))
	if err != nil {
		fail()
		return
	}
	raw, ok := token.Extra("id_token").(string)
	if !ok {
		fail()
		return
	}
	id, err := provider.Verifier(&oidc.Config{ClientID: cfg.ClientID}).Verify(r.Context(), raw)
	if err != nil || !equal(id.Nonce, state.Nonce) || id.Subject == "" {
		fail()
		return
	}
	var claims map[string]json.RawMessage
	if err = id.Claims(&claims); err != nil {
		fail()
		return
	}
	var username string
	if json.Unmarshal(claims[cfg.UsernameClaim], &username) != nil || domain.ValidateAccountName(username) != nil {
		fail()
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	a, err := s.o.Store.GetAccount(r.Context(), username)
	// Only a verified provider identity matching a persisted OIDC account can
	// attribute a failed callback; a local-name collision is not that account.
	if err == nil && a.OIDC && a.OIDCIssuer == cfg.IssuerURL && a.OIDCSubject == id.Subject {
		auditPrincipal = Principal{Account: a.Name, Via: "oidc", OIDCIssuer: a.OIDCIssuer}
	}
	var groups []string
	if raw, ok := claims[cfg.GroupsClaim]; ok {
		if json.Unmarshal(raw, &groups) != nil {
			fail()
			return
		}
	}
	allowed := len(cfg.AllowedGroups) == 0
	extra := make([]string, 0, len(groups))
	for _, g := range groups {
		if g == "" {
			continue
		}
		extra = append(extra, "group:"+g)
		for _, a := range cfg.AllowedGroups {
			if g == a {
				allowed = true
			}
		}
	}
	if !allowed {
		fail()
		return
	}
	if errors.Is(err, store.ErrNotFound) && cfg.AutoCreate {
		a = &domain.Account{Name: username, DisplayName: username, OIDC: true, OIDCIssuer: cfg.IssuerURL, OIDCSubject: id.Subject, CreatedAt: s.now()}
		err = s.o.Store.CreateAccount(r.Context(), a)
	}
	if err != nil || a.Disabled || !a.OIDC || a.OIDCIssuer != cfg.IssuerURL || a.OIDCSubject != id.Subject {
		fail()
		return
	}
	auditPrincipal = Principal{Account: a.Name, Via: "oidc", OIDCIssuer: a.OIDCIssuer}
	if _, err = s.issue(r.Context(), w, a, extra, "oidc"); err != nil {
		fail()
		return
	}
	http.Redirect(w, r, strings.TrimRight(s.o.PublicURL, "/")+oidcReturnPath(state.ReturnTo), http.StatusFound)
}
