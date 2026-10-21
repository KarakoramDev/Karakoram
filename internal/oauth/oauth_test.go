// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package oauth

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/auth"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

const origin = "https://karakoram.example"

type fixture struct {
	s        *Service
	cookies  []*http.Cookie
	csrf     string
	client   Client
	verifier string
	query    url.Values
}

func setup(t *testing.T) *fixture {
	t.Helper()
	seal, e := secrets.New(strings.Repeat("k", 32))
	if e != nil {
		t.Fatal(e)
	}
	st, e := store.Open(":memory:", seal)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = st.Close() })
	a, e := auth.New(auth.Options{Store: st, Sealer: seal, PublicURL: origin})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = a.Bootstrap(t.Context(), "temporary-test-password"); e != nil {
		t.Fatal(e)
	}
	if e = a.ChangePassword(t.Context(), "admin", "temporary-test-password", "a-long-test-password"); e != nil {
		t.Fatal(e)
	}
	w := httptest.NewRecorder()
	info, e := a.Login(t.Context(), w, "admin", "a-long-test-password")
	if e != nil {
		t.Fatal(e)
	}
	s := New(Options{Store: st, Auth: a, PublicURL: origin, DCR: true})
	c := Client{ID: "kkc_test", Name: "Test app", Redirects: []string{"http://127.0.0.1:3000/callback"}, AuthMethod: "none"}
	if e = st.OAuthPut(t.Context(), "client", c.ID, c); e != nil {
		t.Fatal(e)
	}
	verifier := strings.Repeat("v", 43)
	sum := sha256.Sum256([]byte(verifier))
	q := url.Values{"client_id": {c.ID}, "redirect_uri": {c.Redirects[0]}, "response_type": {"code"}, "code_challenge_method": {"S256"}, "code_challenge": {base64.RawURLEncoding.EncodeToString(sum[:])}, "state": {"test-state"}, "resource": {origin + "/mcp"}}
	return &fixture{s: s, cookies: w.Result().Cookies(), csrf: info.CSRFToken, client: c, verifier: verifier, query: q}
}
func (f *fixture) authorize(t *testing.T, method string, q url.Values, form url.Values, session bool) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(method, origin+"/oauth/authorize?"+q.Encode(), strings.NewReader(form.Encode()))
	if method == "POST" {
		r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		r.Header.Set("Origin", origin)
	}
	if session {
		for _, c := range f.cookies {
			r.AddCookie(c)
		}
	}
	w := httptest.NewRecorder()
	f.s.ServeHTTP(w, r)
	return w
}
func (f *fixture) code(t *testing.T) string {
	t.Helper()
	w := f.authorize(t, "POST", f.query, url.Values{"decision": {"approve"}, "csrf": {f.csrf}, "namespaces": {"default"}}, true)
	if w.Code != 303 {
		t.Fatalf("consent status %d: %s", w.Code, w.Body.String())
	}
	u, e := url.Parse(w.Header().Get("Location"))
	if e != nil || u.Query().Get("state") != "test-state" || u.Query().Get("code") == "" || u.Query().Get("iss") != origin {
		t.Fatal("invalid code response")
	}
	return u.Query().Get("code")
}
func (f *fixture) post(path string, form url.Values) *httptest.ResponseRecorder {
	r := httptest.NewRequest("POST", origin+path, strings.NewReader(form.Encode()))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()
	f.s.ServeHTTP(w, r)
	return w
}
func (f *fixture) exchange(code string) url.Values {
	return url.Values{"grant_type": {"authorization_code"}, "client_id": {f.client.ID}, "code": {code}, "redirect_uri": {f.client.Redirects[0]}, "code_verifier": {f.verifier}}
}
func accessRequest(token string) *http.Request {
	r := httptest.NewRequest("POST", origin+"/mcp", nil)
	r.Header.Set("Authorization", "Bearer "+token)
	return r
}
func tokenBody(t *testing.T, w *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	if w.Code != 200 {
		t.Fatalf("token status %d: %s", w.Code, w.Body.String())
	}
	if w.Header().Get("Cache-Control") != "no-store" || w.Header().Get("Pragma") != "no-cache" {
		t.Fatal("token response may be cached")
	}
	var body map[string]any
	if json.Unmarshal(w.Body.Bytes(), &body) != nil {
		t.Fatal("invalid JSON")
	}
	return body
}
func cloneValues(v url.Values) url.Values {
	result := make(url.Values, len(v))
	for key, values := range v {
		result[key] = append([]string(nil), values...)
	}
	return result
}
func TestCodeFlowAndRefreshReuse(t *testing.T) {
	f := setup(t)
	code := f.code(t)
	form := f.exchange(code)
	for _, field := range []string{"code_verifier", "redirect_uri", "client_id"} {
		bad := cloneValues(form)
		bad.Set(field, "wrong")
		if w := f.post("/oauth/token", bad); w.Code != 400 {
			t.Fatalf("accepted wrong %s", field)
		}
	}
	body := tokenBody(t, f.post("/oauth/token", form))
	access := body["access_token"].(string)
	refresh := body["refresh_token"].(string)
	if !strings.HasPrefix(access, "kko_") || !strings.HasPrefix(refresh, "kkr_") {
		t.Fatal("wrong token prefixes")
	}
	p, e := f.s.Authenticate(accessRequest(access))
	if e != nil || !p.Restrictions.ReadOnly || len(p.Restrictions.Namespaces) != 1 {
		t.Fatal("missing grant restrictions", e)
	}
	if _, e = f.s.o.Auth.Authenticate(accessRequest(access)); e == nil {
		t.Fatal("REST accepted OAuth token")
	}
	if w := f.post("/oauth/token", form); w.Code != 400 {
		t.Fatal("code replay accepted")
	}
	rotation := url.Values{"grant_type": {"refresh_token"}, "client_id": {f.client.ID}, "refresh_token": {refresh}}
	bad := cloneValues(rotation)
	bad.Set("scope", "karakoram:write")
	if w := f.post("/oauth/token", bad); w.Code != 400 {
		t.Fatal("scope escalation accepted")
	}
	next := tokenBody(t, f.post("/oauth/token", rotation))
	if next["refresh_token"] == refresh {
		t.Fatal("refresh did not rotate")
	}
	if w := f.post("/oauth/token", rotation); w.Code != 400 {
		t.Fatal("refresh replay accepted")
	}
	if _, e = f.s.Authenticate(accessRequest(next["access_token"].(string))); e == nil {
		t.Fatal("reuse did not revoke grant")
	}
}
func TestAuthorizeValidationAndConsent(t *testing.T) {
	f := setup(t)
	w := f.authorize(t, "GET", f.query, nil, false)
	if w.Code != 303 || !strings.HasPrefix(w.Header().Get("Location"), "/login?returnTo=") {
		t.Fatal("missing login return")
	}
	w = f.authorize(t, "GET", f.query, nil, true)
	if w.Code != 200 || !strings.Contains(w.Body.String(), "Test app") || w.Header().Get("X-Frame-Options") != "DENY" || w.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("missing consent/security headers")
	}
	if w = f.authorize(t, "POST", f.query, url.Values{"decision": {"approve"}}, true); w.Code != 403 {
		t.Fatal("missing CSRF accepted")
	}
	if w = f.authorize(t, "POST", f.query, url.Values{"decision": {"approve"}, "csrf": {f.csrf}}, false); w.Code != 403 {
		t.Fatal("missing session accepted")
	}
	for field, value := range map[string]string{"code_challenge_method": "plain", "resource": "https://other.example/mcp", "scope": "admin"} {
		q := cloneValues(f.query)
		q.Set(field, value)
		w = f.authorize(t, "GET", q, nil, true)
		if w.Code != 303 || !strings.Contains(w.Header().Get("Location"), "error=") {
			t.Fatalf("accepted %s", field)
		}
	}
	q := cloneValues(f.query)
	q.Set("redirect_uri", "https://attacker.example")
	w = f.authorize(t, "GET", q, nil, true)
	if w.Code != 400 || w.Header().Get("Location") != "" {
		t.Fatal("invalid redirect not local")
	}
	w = f.authorize(t, "POST", f.query, url.Values{"decision": {"deny"}, "csrf": {f.csrf}}, true)
	if !strings.Contains(w.Header().Get("Location"), "error=access_denied") {
		t.Fatal("deny ignored")
	}
}
func TestMetadataAndDCR(t *testing.T) {
	f := setup(t)
	for _, path := range []string{"/.well-known/oauth-protected-resource", "/.well-known/oauth-protected-resource/mcp", "/.well-known/oauth-authorization-server"} {
		w := httptest.NewRecorder()
		f.s.ServeHTTP(w, httptest.NewRequest("GET", origin+path, nil))
		if w.Code != 200 || !json.Valid(w.Body.Bytes()) {
			t.Fatal("metadata", path)
		}
		var metadata map[string]any
		if json.Unmarshal(w.Body.Bytes(), &metadata) != nil {
			t.Fatal("invalid metadata JSON")
		}
		if strings.HasSuffix(path, "oauth-authorization-server") {
			if metadata["issuer"] != origin || metadata["client_id_metadata_document_supported"] != true || metadata["token_endpoint"] != origin+"/oauth/token" || metadata["registration_endpoint"] != origin+"/oauth/register" {
				t.Fatal("incorrect authorization metadata")
			}
		} else if metadata["resource"] != origin+"/mcp" || len(metadata["authorization_servers"].([]any)) != 1 || len(metadata["scopes_supported"].([]any)) != 3 {
			t.Fatal("incorrect resource metadata")
		}
	}
	f.s.o.DCR = false
	w := f.post("/oauth/register", nil)
	if w.Code != 404 {
		t.Fatal("DCR enabled by default")
	}
	f.s.o.DCR = true
	r := httptest.NewRequest("POST", origin+"/oauth/register", strings.NewReader(`{"client_name":"App","redirect_uris":["http://127.0.0.1:4000/callback"],"token_endpoint_auth_method":"none"}`))
	w = httptest.NewRecorder()
	f.s.ServeHTTP(w, r)
	if w.Code != 201 {
		t.Fatal("registration", w.Code)
	}
}
func TestExpiryAndRevocation(t *testing.T) {
	for _, mode := range []string{"access", "grant", "revoke", "disabled"} {
		t.Run(mode, func(t *testing.T) {
			f := setup(t)
			body := tokenBody(t, f.post("/oauth/token", f.exchange(f.code(t))))
			access := body["access_token"].(string)
			switch mode {
			case "access":
				now := f.s.now()
				f.s.now = func() time.Time { return now.Add(11 * time.Minute) }
			case "grant":
				now := f.s.now()
				f.s.now = func() time.Time { return now.Add(31 * 24 * time.Hour) }
			case "revoke":
				w := f.post("/oauth/revoke", url.Values{"client_id": {f.client.ID}, "token": {access}})
				if w.Code != 200 {
					t.Fatal(w.Code)
				}
			case "disabled":
				a, e := f.s.o.Store.GetAccount(t.Context(), "admin")
				if e != nil {
					t.Fatal(e)
				}
				a.Disabled = true
				if e = f.s.o.Store.UpdateAccount(t.Context(), a); e != nil {
					t.Fatal(e)
				}
			}
			if _, e := f.s.Authenticate(accessRequest(access)); e == nil {
				t.Fatal("inactive token accepted")
			}
		})
	}
}
func TestClientPrivateAddresses(t *testing.T) {
	for _, ip := range []string{"127.0.0.1", "10.0.0.1", "169.254.169.254", "::1", "100.100.100.200", "168.63.129.16"} {
		t.Run(ip, func(t *testing.T) {
			s := New(Options{Resolve: func(context.Context, string) ([]netip.Addr, error) { return []netip.Addr{netip.MustParseAddr(ip)}, nil }, Dial: func(context.Context, string, string) (net.Conn, error) {
				t.Fatal("dialed forbidden address")
				return nil, nil
			}})
			if _, e := s.client(t.Context(), "https://client.example/metadata"); e == nil {
				t.Fatal("private metadata accepted")
			}
		})
	}
}
func TestLoopbackRedirect(t *testing.T) {
	if !redirectMatches("http://127.0.0.1:3000/cb", "http://127.0.0.1:4000/cb") || redirectMatches("http://127.0.0.1:3000/cb", "http://127.0.0.1:4000/other") || redirectMatches("https://client.example:3000/cb", "https://client.example:4000/cb") {
		t.Fatal("redirect matching")
	}
}

func TestMetadataHTTPClientRefusesRedirects(t *testing.T) {
	followed := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/metadata" {
			http.Redirect(w, r, "/private", http.StatusFound)
			return
		}
		followed = true
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	client := metadataHTTPClient(http.DefaultTransport)
	response, err := client.Get(server.URL + "/metadata")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusFound || followed {
		t.Fatal("metadata redirect followed")
	}
}

func TestCodeExpiration(t *testing.T) {
	f := setup(t)
	code := f.code(t)
	now := f.s.now()
	f.s.now = func() time.Time { return now.Add(61 * time.Second) }
	if w := f.post("/oauth/token", f.exchange(code)); w.Code != 400 {
		t.Fatal("expired authorization code accepted")
	}
}

func TestSlowMetadataFetchDoesNotBlockAuthentication(t *testing.T) {
	f := setup(t)
	token := tokenBody(t, f.post("/oauth/token", f.exchange(f.code(t))))["access_token"].(string)
	started, release, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	f.s.o.Resolve = func(context.Context, string) ([]netip.Addr, error) {
		calls.Add(1)
		close(started)
		<-release
		return nil, errors.New("synthetic resolution failure")
	}
	go func() {
		defer close(finished)
		w := httptest.NewRecorder()
		f.s.ServeHTTP(w, httptest.NewRequest("GET", origin+"/oauth/authorize?client_id=https%3A%2F%2Fslow.example%2Fmetadata", nil))
	}()
	<-started
	defer func() { close(release); <-finished }()
	authenticated := make(chan error, 1)
	go func() { _, err := f.s.Authenticate(accessRequest(token)); authenticated <- err }()
	select {
	case err := <-authenticated:
		if err != nil {
			t.Fatal("token authentication failed during metadata fetch", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("metadata fetch blocked credential lifecycle")
	}
	// An in-flight entry also bounds concurrent requests for the same slow ID.
	if _, err := f.s.client(t.Context(), "https://slow.example/metadata"); err == nil {
		t.Fatal("in-flight metadata accepted")
	}
	if calls.Load() != 1 {
		t.Fatal("duplicate slow metadata fetch")
	}
}

func TestMetadataFailureCacheExpires(t *testing.T) {
	s := New(Options{})
	calls := 0
	s.o.Resolve = func(context.Context, string) ([]netip.Addr, error) {
		calls++
		return nil, errors.New("synthetic resolution failure")
	}
	for range 2 {
		if _, err := s.client(t.Context(), "https://client.example/metadata"); err == nil {
			t.Fatal("failed metadata accepted")
		}
	}
	if calls != 1 {
		t.Fatal("failed metadata was not cached")
	}
	now := s.now()
	s.now = func() time.Time { return now.Add(6 * time.Second) }
	if _, err := s.client(t.Context(), "https://client.example/metadata"); err == nil || calls != 2 {
		t.Fatal("negative cache did not expire")
	}
}
