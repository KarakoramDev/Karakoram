// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

// TEST ONLY: this issuer automatically authenticates every visitor as oidctest.
// Never use it for production authentication or bind its group to an admin role.
package main

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"log"
	"math/big"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

type authorization struct {
	nonce, challenge string
	expires          time.Time
}
type issuer struct {
	origin, callback, client string
	key                      *rsa.PrivateKey
	mu                       sync.Mutex
	codes                    map[string]authorization
}

func newIssuer(origin, callback, client string) (*issuer, error) {
	for _, raw := range []string{origin, callback} {
		u, err := url.Parse(raw)
		if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
			return nil, errors.New("issuer and callback must be HTTPS URLs")
		}
	}
	if client == "" {
		return nil, errors.New("client ID is required")
	}
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, err
	}
	return &issuer{origin: strings.TrimRight(origin, "/"), callback: callback, client: client, key: key, codes: make(map[string]authorization)}, nil
}

func jsonResponse(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(value)
}
func (s *issuer) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		jsonResponse(w, map[string]string{"status": "ok", "warning": "TEST ONLY: automatic login"})
	})
	mux.HandleFunc("GET /.well-known/openid-configuration", func(w http.ResponseWriter, r *http.Request) {
		jsonResponse(w, map[string]any{"issuer": s.origin, "authorization_endpoint": s.origin + "/authorize", "token_endpoint": s.origin + "/token", "jwks_uri": s.origin + "/jwks", "response_types_supported": []string{"code"}, "subject_types_supported": []string{"public"}, "id_token_signing_alg_values_supported": []string{"RS256"}, "token_endpoint_auth_methods_supported": []string{"none"}, "code_challenge_methods_supported": []string{"S256"}, "scopes_supported": []string{"openid", "profile", "email"}})
	})
	mux.HandleFunc("GET /jwks", func(w http.ResponseWriter, r *http.Request) {
		jsonResponse(w, map[string]any{"keys": []any{map[string]string{"kty": "RSA", "use": "sig", "kid": "test-only", "alg": "RS256", "n": base64.RawURLEncoding.EncodeToString(s.key.N.Bytes()), "e": base64.RawURLEncoding.EncodeToString(big.NewInt(int64(s.key.E)).Bytes())}}})
	})
	mux.HandleFunc("GET /authorize", s.authorize)
	mux.HandleFunc("POST /token", s.token)
	return mux
}
func (s *issuer) authorize(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	if q.Get("client_id") != s.client || q.Get("redirect_uri") != s.callback || q.Get("response_type") != "code" || q.Get("code_challenge_method") != "S256" || len(q.Get("code_challenge")) != 43 || q.Get("nonce") == "" || q.Get("state") == "" {
		http.Error(w, "invalid authorization request", http.StatusBadRequest)
		return
	}
	codeBytes := make([]byte, 32)
	if _, err := rand.Read(codeBytes); err != nil {
		http.Error(w, "random source unavailable", 500)
		return
	}
	code := base64.RawURLEncoding.EncodeToString(codeBytes)
	now := time.Now()
	s.mu.Lock()
	for key, v := range s.codes {
		if !now.Before(v.expires) {
			delete(s.codes, key)
		}
	}
	if len(s.codes) >= 1024 {
		s.mu.Unlock()
		http.Error(w, "too many pending test logins", 503)
		return
	}
	s.codes[code] = authorization{q.Get("nonce"), q.Get("code_challenge"), now.Add(time.Minute)}
	s.mu.Unlock()
	target, _ := url.Parse(s.callback)
	params := target.Query()
	params.Set("code", code)
	params.Set("state", q.Get("state"))
	target.RawQuery = params.Encode()
	w.Header().Set("Cache-Control", "no-store")
	http.Redirect(w, r, target.String(), http.StatusFound)
}
func (s *issuer) token(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 8192)
	if r.ParseForm() != nil {
		http.Error(w, "invalid form", 400)
		return
	}
	client := r.Form.Get("client_id")
	if id, _, ok := r.BasicAuth(); ok {
		client = id
	}
	if client != s.client || r.Form.Get("redirect_uri") != s.callback || r.Form.Get("grant_type") != "authorization_code" {
		http.Error(w, "invalid token request", 400)
		return
	}
	s.mu.Lock()
	code, ok := s.codes[r.Form.Get("code")]
	delete(s.codes, r.Form.Get("code"))
	s.mu.Unlock()
	hash := sha256.Sum256([]byte(r.Form.Get("code_verifier")))
	if !ok || !time.Now().Before(code.expires) || base64.RawURLEncoding.EncodeToString(hash[:]) != code.challenge {
		http.Error(w, "invalid code or PKCE", 400)
		return
	}
	now := time.Now()
	token := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{"iss": s.origin, "sub": "karakoram-test-subject", "aud": s.client, "iat": now.Unix(), "exp": now.Add(5 * time.Minute).Unix(), "nonce": code.nonce, "preferred_username": "oidctest", "email": "oidctest@example.invalid", "groups": []string{"karakoram-admins"}})
	token.Header["kid"] = "test-only"
	signed, err := token.SignedString(s.key)
	if err != nil {
		http.Error(w, "signing failed", 500)
		return
	}
	jsonResponse(w, map[string]any{"access_token": "test-only-not-an-api-token", "token_type": "Bearer", "expires_in": 300, "id_token": signed})
}
func main() {
	s, err := newIssuer(os.Getenv("OIDC_ISSUER_URL"), os.Getenv("OIDC_CALLBACK_URL"), "karakoram-e2e")
	if err != nil {
		log.Fatal(err)
	}
	log.Print("TEST ONLY OIDC issuer: automatic oidctest login, never use in production")
	server := &http.Server{Addr: ":8080", Handler: s.handler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 30 * time.Second}
	log.Fatal(server.ListenAndServe())
}
