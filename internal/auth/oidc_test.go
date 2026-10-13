// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package auth

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

type oidcCode struct{ Nonce, Challenge, Redirect string }
type issuer struct {
	server    *httptest.Server
	key       ed25519.PrivateKey
	mu        sync.Mutex
	codes     map[string]oidcCode
	username  string
	groups    []string
	badNonce  bool
	exchanges int
}

func newIssuer(t *testing.T) *issuer {
	t.Helper()
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	p := &issuer{key: key, codes: map[string]oidcCode{}, username: "sso-user", groups: []string{"operators"}}
	mux := http.NewServeMux()
	p.server = httptest.NewServer(mux)
	t.Cleanup(p.server.Close)
	jsonReply := func(w http.ResponseWriter, v interface{}) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(v)
	}
	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, r *http.Request) {
		jsonReply(w, map[string]interface{}{"issuer": p.server.URL, "authorization_endpoint": p.server.URL + "/authorize", "token_endpoint": p.server.URL + "/token", "jwks_uri": p.server.URL + "/jwks", "response_types_supported": []string{"code"}, "subject_types_supported": []string{"public"}, "id_token_signing_alg_values_supported": []string{"EdDSA"}})
	})
	mux.HandleFunc("/jwks", func(w http.ResponseWriter, r *http.Request) {
		jsonReply(w, map[string]interface{}{"keys": []map[string]string{{"kty": "OKP", "crv": "Ed25519", "kid": "test-key", "alg": "EdDSA", "use": "sig", "x": base64.RawURLEncoding.EncodeToString(p.key.Public().(ed25519.PublicKey))}}})
	})
	mux.HandleFunc("/authorize", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		if q.Get("code_challenge_method") != "S256" || q.Get("nonce") == "" || q.Get("state") == "" {
			http.Error(w, "missing PKCE/state/nonce", 400)
			return
		}
		code, err := random()
		if err != nil {
			t.Fatal(err)
		}
		p.mu.Lock()
		p.codes[code] = oidcCode{q.Get("nonce"), q.Get("code_challenge"), q.Get("redirect_uri")}
		p.mu.Unlock()
		u, err := url.Parse(q.Get("redirect_uri"))
		if err != nil {
			t.Fatal(err)
		}
		values := u.Query()
		values.Set("code", code)
		values.Set("state", q.Get("state"))
		u.RawQuery = values.Encode()
		http.Redirect(w, r, u.String(), http.StatusFound)
	})
	mux.HandleFunc("/token", func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			http.Error(w, "form", 400)
			return
		}
		client, secret, ok := r.BasicAuth()
		if !ok {
			client = r.Form.Get("client_id")
			secret = r.Form.Get("client_secret")
		}
		if client != "client" || secret != "test-client-secret" {
			http.Error(w, "client", http.StatusUnauthorized)
			return
		}
		p.mu.Lock()
		defer p.mu.Unlock()
		code, ok := p.codes[r.Form.Get("code")]
		if !ok {
			http.Error(w, "consumed code", 400)
			return
		}
		delete(p.codes, r.Form.Get("code"))
		digest := sha256.Sum256([]byte(r.Form.Get("code_verifier")))
		if base64.RawURLEncoding.EncodeToString(digest[:]) != code.Challenge || r.Form.Get("redirect_uri") != code.Redirect {
			http.Error(w, "PKCE", 400)
			return
		}
		nonce := code.Nonce
		if p.badNonce {
			nonce = "wrong-nonce"
		}
		claims := jwt.MapClaims{"iss": p.server.URL, "aud": "client", "sub": "stable-provider-subject", "exp": time.Now().Add(time.Minute).Unix(), "iat": time.Now().Unix(), "nonce": nonce, "preferred_username": p.username, "groups": p.groups}
		token := jwt.NewWithClaims(jwt.SigningMethodEdDSA, claims)
		token.Header["kid"] = "test-key"
		signed, err := token.SignedString(p.key)
		if err != nil {
			t.Fatal(err)
		}
		p.exchanges++
		jsonReply(w, map[string]interface{}{"access_token": "test-access", "token_type": "Bearer", "expires_in": 60, "id_token": signed})
	})
	return p
}
func oidcCallback(t *testing.T, s *Service, p *issuer, mutateState bool) *httptest.ResponseRecorder {
	t.Helper()
	loginW := httptest.NewRecorder()
	s.OIDCLoginHandler(loginW, httptest.NewRequest("GET", "https://karakoram.example/api/v1/auth/oidc/login", nil))
	if loginW.Code != 302 {
		t.Fatalf("login: %d %s", loginW.Code, loginW.Body.String())
	}
	authorize := httptest.NewRecorder()
	p.server.Config.Handler.ServeHTTP(authorize, httptest.NewRequest("GET", loginW.Header().Get("Location"), nil))
	if authorize.Code != 302 {
		t.Fatalf("authorize: %d %s", authorize.Code, authorize.Body.String())
	}
	callbackURL := authorize.Header().Get("Location")
	if mutateState {
		u, _ := url.Parse(callbackURL)
		q := u.Query()
		q.Set("state", "wrong")
		u.RawQuery = q.Encode()
		callbackURL = u.String()
	}
	r := httptest.NewRequest("GET", callbackURL, nil)
	for _, c := range loginW.Result().Cookies() {
		r.AddCookie(c)
	}
	w := httptest.NewRecorder()
	s.OIDCCallbackHandler(w, r)
	return w
}
func TestOIDCFlow(t *testing.T) {
	s := setup(t)
	p := newIssuer(t)
	cfg := OIDCSettings{IssuerURL: p.server.URL, ClientID: "client", ClientSecret: "test-client-secret", AutoCreate: true, AllowedGroups: []string{"operators"}}
	if err := s.UpdateSettings(t.Context(), cfg); err != nil {
		t.Fatal(err)
	}
	visible, err := s.Settings(t.Context())
	if err != nil || visible.ClientSecret != "" {
		t.Fatal("secret exposed", err)
	}
	raw, err := s.o.Store.GetSetting(t.Context(), "auth/oidc")
	if err != nil || strings.Contains(string(raw), "test-client-secret") || !strings.Contains(string(raw), "kk:v1:") {
		t.Fatal("OIDC secret not sealed")
	}
	cfg.ClientSecret = ""
	if err = s.UpdateSettings(t.Context(), cfg); err != nil {
		t.Fatal(err)
	}
	w := oidcCallback(t, s, p, false)
	if w.Code != 302 {
		t.Fatalf("callback: %d %s", w.Code, w.Body.String())
	}
	principal, err := s.Authenticate(request("GET", w.Result().Cookies(), ""))
	if err != nil || principal.Account != "sso-user" || principal.Via != "oidc" {
		t.Fatal(principal, err)
	}
	if !strings.Contains(strings.Join(principal.Subjects, ","), "group:operators") {
		t.Fatal("groups not mapped")
	}
	a, err := s.GetAccount(t.Context(), "sso-user")
	if err != nil || !a.OIDC {
		t.Fatal(a, err)
	}
	if _, err = s.Login(t.Context(), httptest.NewRecorder(), "sso-user", password); err == nil {
		t.Fatal("OIDC local login")
	}
	again := oidcCallback(t, s, p, false)
	if again.Code != 302 {
		t.Fatal("existing OIDC account rejected")
	}
	before := p.exchanges
	bad := oidcCallback(t, s, p, true)
	if bad.Code != 401 || p.exchanges != before {
		t.Fatal("state not checked before exchange")
	}
	p.badNonce = true
	bad = oidcCallback(t, s, p, false)
	if bad.Code != 401 {
		t.Fatal("nonce not checked")
	}
	p.badNonce = false
	p.groups = []string{"outsiders"}
	bad = oidcCallback(t, s, p, false)
	if bad.Code != 401 {
		t.Fatal("allowed groups not enforced")
	}
	p.groups = []string{"operators"}
	p.username = "admin"
	bad = oidcCallback(t, s, p, false)
	if bad.Code != 401 {
		t.Fatal("local admin account adopted")
	}
}
func TestOIDCAutoCreateDisabledAndSettingsValidation(t *testing.T) {
	s := setup(t)
	p := newIssuer(t)
	if err := s.UpdateSettings(t.Context(), OIDCSettings{IssuerURL: p.server.URL, ClientID: "client", ClientSecret: "test-client-secret", AutoCreate: false}); err != nil {
		t.Fatal(err)
	}
	w := oidcCallback(t, s, p, false)
	if w.Code != 401 {
		t.Fatal("autocreate disabled")
	}
	if err := s.UpdateSettings(t.Context(), OIDCSettings{IssuerURL: "http://not-loopback.example", ClientID: "client"}); err == nil {
		t.Fatal("insecure issuer")
	}
}
