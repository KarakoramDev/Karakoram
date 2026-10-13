// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package main

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

func TestIssuer(t *testing.T) {
	s, err := newIssuer("https://issuer.example", "https://karakoram.example/api/v1/auth/oidc/callback", "karakoram-e2e")
	if err != nil {
		t.Fatal(err)
	}
	handler := s.handler()
	for _, path := range []string{"/.well-known/openid-configuration", "/jwks", "/healthz"} {
		r := httptest.NewRecorder()
		handler.ServeHTTP(r, httptest.NewRequest("GET", path, nil))
		if r.Code != 200 || !json.Valid(r.Body.Bytes()) {
			t.Fatalf("%s: HTTP %d", path, r.Code)
		}
	}
	verifier := strings.Repeat("v", 43)
	hash := sha256.Sum256([]byte(verifier))
	q := url.Values{"client_id": {s.client}, "redirect_uri": {s.callback}, "response_type": {"code"}, "code_challenge_method": {"S256"}, "code_challenge": {base64.RawURLEncoding.EncodeToString(hash[:])}, "state": {"state"}, "nonce": {"nonce"}}
	authorize := func() string {
		t.Helper()
		r := httptest.NewRecorder()
		handler.ServeHTTP(r, httptest.NewRequest("GET", "/authorize?"+q.Encode(), nil))
		if r.Code != 302 {
			t.Fatalf("authorization HTTP %d", r.Code)
		}
		target, err := url.Parse(r.Header().Get("Location"))
		if err != nil {
			t.Fatal(err)
		}
		if target.Query().Get("state") != "state" {
			t.Fatal("state not preserved")
		}
		return target.Query().Get("code")
	}
	exchange := func(code, proof string) *httptest.ResponseRecorder {
		form := url.Values{"client_id": {s.client}, "redirect_uri": {s.callback}, "grant_type": {"authorization_code"}, "code": {code}, "code_verifier": {proof}}
		req := httptest.NewRequest("POST", "/token", strings.NewReader(form.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		r := httptest.NewRecorder()
		handler.ServeHTTP(r, req)
		return r
	}
	code := authorize()
	result := exchange(code, verifier)
	if result.Code != 200 {
		t.Fatalf("token HTTP %d", result.Code)
	}
	var body struct {
		IDToken string `json:"id_token"`
	}
	if err := json.Unmarshal(result.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	token, err := jwt.Parse(body.IDToken, func(_ *jwt.Token) (any, error) { return &s.key.PublicKey, nil }, jwt.WithValidMethods([]string{"RS256"}), jwt.WithIssuer(s.origin), jwt.WithAudience(s.client), jwt.WithExpirationRequired())
	if err != nil {
		t.Fatal(err)
	}
	claims := token.Claims.(jwt.MapClaims)
	if claims["nonce"] != "nonce" || claims["preferred_username"] != "oidctest" || len(claims["groups"].([]any)) != 1 {
		t.Fatal("missing test identity/nonce/groups")
	}
	if exchange(code, verifier).Code != 400 {
		t.Fatal("code replay accepted")
	}
	if exchange(authorize(), "wrong").Code != 400 {
		t.Fatal("wrong PKCE accepted")
	}
	expired := authorize()
	s.mu.Lock()
	entry := s.codes[expired]
	entry.expires = time.Now().Add(-time.Second)
	s.codes[expired] = entry
	s.mu.Unlock()
	if exchange(expired, verifier).Code != 400 {
		t.Fatal("expired code accepted")
	}
	q.Set("redirect_uri", "https://other.example/callback")
	r := httptest.NewRecorder()
	handler.ServeHTTP(r, httptest.NewRequest("GET", "/authorize?"+q.Encode(), nil))
	if r.Code != http.StatusBadRequest || r.Header().Get("Location") != "" {
		t.Fatal("unregistered redirect accepted")
	}
}
