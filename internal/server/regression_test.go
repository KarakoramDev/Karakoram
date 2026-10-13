// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/api"
	"github.com/KarakoramDev/Karakoram/internal/auth"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine/psmdb"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
)

func TestLoginRejectsCrossOriginAndSimpleContentTypes(t *testing.T) {
	s, _, _ := testServer(t)
	for _, tc := range []struct {
		origin, site, content string
		status                int
	}{
		{"https://evil.example", "", "application/json", 403},
		{s.PublicURL, "cross-site", "application/json", 403},
		{"", "same-site", "application/json", 403},
		{"null", "", "application/json", 403},
		{s.PublicURL, "same-origin", "text/plain", 415},
		{s.PublicURL, "same-origin", "application/json", 200},
		{"", "", "application/json", 200},
	} {
		r := httptest.NewRequest("POST", "/api/v1/auth/login", strings.NewReader(`{"username":"admin","password":"admin-test-password"}`))
		r.Header.Set("Origin", tc.origin)
		r.Header.Set("Sec-Fetch-Site", tc.site)
		r.Header.Set("Content-Type", tc.content)
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, r)
		if w.Code != tc.status || (tc.status != 200 && len(w.Result().Cookies()) != 0) {
			t.Fatalf("%+v: %d %s", tc, w.Code, w.Body.String())
		}
	}
	for _, path := range []string{"refresh", "logout", "oidc/login"} {
		method := "POST"
		if path == "oidc/login" {
			method = "GET"
		}
		r := httptest.NewRequest(method, "/api/v1/auth/"+path, nil)
		r.Header.Set("Origin", "https://evil.example")
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, r)
		if w.Code != 403 || len(w.Result().Cookies()) != 0 {
			t.Fatal(path, w.Code, w.Body.String())
		}
	}
}

func TestLogoutAuthenticatesRefreshWithoutAccessCookie(t *testing.T) {
	s, _, _ := testServer(t)
	login := call(s, "POST", "/api/v1/auth/login", `{"username":"admin","password":"admin-test-password"}`, "")
	if login.Code != 200 {
		t.Fatal(login.Code, login.Body.String())
	}
	var session api.Session
	if err := json.Unmarshal(login.Body.Bytes(), &session); err != nil {
		t.Fatal(err)
	}
	cookies := login.Result().Cookies()
	r := httptest.NewRequest("POST", "/api/v1/auth/logout", nil)
	var refresh string
	for _, cookie := range cookies {
		if cookie.Name != auth.AccessCookie {
			r.AddCookie(cookie)
		}
		if cookie.Name == auth.RefreshCookie {
			refresh = cookie.Value
		}
	}
	r.Header.Set(auth.CSRFHeader, *session.CsrfToken)
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	if w.Code != 204 || len(w.Result().Cookies()) != 3 {
		t.Fatal(w.Code, w.Body.String())
	}
	id, _, _ := strings.Cut(refresh, ".")
	stored, err := s.Store.GetSession(t.Context(), id)
	if err != nil || !stored.Revoked {
		t.Fatal("refresh family not revoked", stored, err)
	}
	r = httptest.NewRequest("GET", "/api/v1/auth/me", nil)
	for _, cookie := range cookies {
		r.AddCookie(cookie)
	}
	w = httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	if w.Code != 401 {
		t.Fatal("revoked access remains usable", w.Code)
	}
	// CSRF remains mandatory without an access credential.
	login = call(s, "POST", "/api/v1/auth/login", `{"username":"admin","password":"admin-test-password"}`, "")
	r = httptest.NewRequest("POST", "/api/v1/auth/logout", nil)
	for _, cookie := range login.Result().Cookies() {
		if cookie.Name != auth.AccessCookie {
			r.AddCookie(cookie)
		}
	}
	w = httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	if w.Code != 403 {
		t.Fatal("refresh logout bypassed CSRF", w.Code)
	}
}

func TestOperationsOmittedScopeIncludesAuthorizedClusters(t *testing.T) {
	s, _, token := testServer(t)
	for _, cluster := range []string{"prod", "dev"} {
		if err := s.Store.EnqueueOperation(t.Context(), &domain.Operation{Cluster: cluster, Kind: "restart"}); err != nil {
			t.Fatal(err)
		}
	}
	for _, tc := range []struct {
		query string
		count int
	}{{"", 2}, {"?cluster=prod", 1}} {
		w := call(s, "GET", "/api/v1/operations"+tc.query, "", token)
		var result api.OperationList
		if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
			t.Fatal(err)
		}
		if w.Code != 200 || len(result.Items) != tc.count {
			t.Fatal(w.Code, w.Body.String())
		}
	}
}

type callbackStreamWriter struct {
	*httptest.ResponseRecorder
	flushed func()
}

func (w *callbackStreamWriter) Flush() {
	w.ResponseRecorder.Flush()
	if w.flushed != nil {
		w.flushed()
	}
}

func streamContext(t *testing.T, s *Server, token string) (context.Context, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	r := httptest.NewRequest("GET", "/api/v1/events/stream", nil).WithContext(ctx)
	r.Header.Set("Authorization", "Bearer "+token)
	return context.WithValue(ctx, requestKey{}, r), cancel
}

func TestSSEDrainsDurableEventsDespiteReversedPublication(t *testing.T) {
	s, _, token := testServer(t)
	ctx, cancel := streamContext(t, s, token)
	defer cancel()
	initial := domain.Event{Cluster: "prod", Type: "cluster.updated", Message: "ready"}
	if err := s.Store.AppendEvent(ctx, &initial); err != nil {
		t.Fatal(err)
	}
	w := &callbackStreamWriter{ResponseRecorder: httptest.NewRecorder()}
	published := false
	w.flushed = func() {
		body := w.Body.String()
		if strings.Contains(body, `"message":"ready"`) && !published {
			published = true
			first := domain.Event{Cluster: "prod", Type: "cluster.updated", Message: "first"}
			second := domain.Event{Cluster: "prod", Type: "cluster.updated", Message: "second"}
			if err := s.Store.AppendEvent(ctx, &first); err != nil {
				t.Fatal(err)
			}
			if err := s.Store.AppendEvent(ctx, &second); err != nil {
				t.Fatal(err)
			}
			s.Hub.Publish(second)
			s.Hub.Publish(first)
		}
		if strings.Contains(body, `"message":"second"`) {
			cancel()
		}
	}
	if err := s.stream(ctx).serve(w); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	body := w.Body.String()
	first, second := strings.Index(body, `"message":"first"`), strings.Index(body, `"message":"second"`)
	if first < 0 || second <= first {
		t.Fatal("durable events lost or reordered", body)
	}
}

func TestSSERevalidatesBeforeEveryEvent(t *testing.T) {
	for _, change := range []string{"disable", "delete", "password", "revoke-token", "expire-token", "revoke-session"} {
		t.Run(change, func(t *testing.T) {
			s, _, token := testServer(t)
			ctx, cancel := streamContext(t, s, token)
			defer cancel()
			var sessionID string
			if change == "revoke-session" {
				login := call(s, "POST", "/api/v1/auth/login", `{"username":"admin","password":"admin-test-password"}`, "")
				r := request(ctx).Clone(ctx)
				r.Header.Del("Authorization")
				for _, cookie := range login.Result().Cookies() {
					r.AddCookie(cookie)
					if cookie.Name == auth.RefreshCookie {
						sessionID, _, _ = strings.Cut(cookie.Value, ".")
					}
				}
				ctx = context.WithValue(ctx, requestKey{}, r)
			}
			initial := domain.Event{Cluster: "prod", Type: "cluster.updated", Message: "ready"}
			if err := s.Store.AppendEvent(ctx, &initial); err != nil {
				t.Fatal(err)
			}
			w := &callbackStreamWriter{ResponseRecorder: httptest.NewRecorder()}
			changed := false
			w.flushed = func() {
				if changed || !strings.Contains(w.Body.String(), `"message":"ready"`) {
					return
				}
				changed = true
				var err error
				switch change {
				case "disable":
					account, e := s.Auth.GetAccount(ctx, "admin")
					if e != nil {
						t.Fatal(e)
					}
					account.Disabled = true
					_, err = s.Auth.UpdateAccount(ctx, *account)
				case "delete":
					err = s.Auth.DeleteAccount(ctx, "admin")
				case "password":
					err = s.Auth.ResetPassword(ctx, "admin", "replacement-test-password")
				case "revoke-token", "expire-token":
					stored, e := s.Store.FindApiTokenByHash(ctx, secrets.Hash(token))
					if e != nil {
						t.Fatal(e)
					}
					if change == "revoke-token" {
						err = s.Auth.DeleteToken(ctx, stored.ID)
					} else {
						expired := time.Now().Add(-time.Second)
						stored.ExpiresAt = &expired
						_, err = s.Auth.UpdateToken(ctx, *stored)
					}
				case "revoke-session":
					session, e := s.Store.GetSession(ctx, sessionID)
					if e != nil {
						t.Fatal(e)
					}
					session.Revoked = true
					err = s.Store.UpdateSession(ctx, session)
				}
				if err != nil {
					t.Fatal(err)
				}
				next := domain.Event{Cluster: "prod", Type: "cluster.updated", Message: "must-not-deliver"}
				if err := s.Store.AppendEvent(ctx, &next); err != nil {
					t.Fatal(err)
				}
				s.Hub.Publish(next)
			}
			err := s.stream(ctx).serve(w)
			if !errors.Is(err, auth.ErrUnauthenticated) || strings.Contains(w.Body.String(), "must-not-deliver") {
				t.Fatalf("revoked stream delivered: %v %s", err, w.Body.String())
			}
		})
	}
}

func TestSSEUsesCurrentAccountSubjects(t *testing.T) {
	s, _, _ := testServer(t)
	if _, err := s.Auth.CreateAccount(t.Context(), domain.Account{Name: "reader", Roles: []string{"role:viewer"}}, "reader-test-password"); err != nil {
		t.Fatal(err)
	}
	if err := s.RBAC.ReplacePolicy(t.Context(), s.RBAC.Policy()+"\np, role:changed, events, read, dev, allow\n"); err != nil {
		t.Fatal(err)
	}
	token, _, err := s.Auth.CreateToken(t.Context(), "reader", "test", nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := streamContext(t, s, token)
	defer cancel()
	for _, event := range []domain.Event{
		{Cluster: "prod", Type: "cluster.updated", Message: "ready"},
		{Cluster: "prod", Type: "cluster.updated", Message: "must-not-deliver"},
		{Cluster: "dev", Type: "cluster.updated", Message: "sentinel"},
	} {
		if err := s.Store.AppendEvent(ctx, &event); err != nil {
			t.Fatal(err)
		}
	}
	w := &callbackStreamWriter{ResponseRecorder: httptest.NewRecorder()}
	changed := false
	w.flushed = func() {
		if strings.Contains(w.Body.String(), `"message":"sentinel"`) {
			cancel()
			return
		}
		if changed || !strings.Contains(w.Body.String(), `"message":"ready"`) {
			return
		}
		changed = true
		account, err := s.Auth.GetAccount(ctx, "reader")
		if err != nil {
			t.Fatal(err)
		}
		account.Roles = []string{"role:changed"}
		if _, err := s.Auth.UpdateAccount(ctx, *account); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.stream(ctx).serve(w); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if strings.Contains(w.Body.String(), "must-not-deliver") || !strings.Contains(w.Body.String(), "sentinel") {
		t.Fatal(w.Body.String())
	}
}

func TestInternalTLSRenewsDuringOperationAndPersists(t *testing.T) {
	s, sealer, _ := testServer(t)
	config, ca, err := internalTLS(t.Context(), s.Store, sealer, "cpnode")
	if err != nil {
		t.Fatal(err)
	}
	first, err := config.GetCertificate(&tls.ClientHelloInfo{})
	if err != nil {
		t.Fatal(err)
	}
	original := string(first.Certificate[0])
	first.Leaf.NotAfter = time.Now().Add(29 * 24 * time.Hour)
	renewed, err := config.GetCertificate(&tls.ClientHelloInfo{})
	if err != nil {
		t.Fatal(err)
	}
	if string(renewed.Certificate[0]) == original || !renewed.Leaf.NotAfter.After(time.Now().Add(30*24*time.Hour)) {
		t.Fatal("leaf not renewed during operation")
	}
	restarted, again, err := internalTLS(t.Context(), s.Store, sealer, "cpnode")
	if err != nil {
		t.Fatal(err)
	}
	persisted, err := restarted.GetCertificate(&tls.ClientHelloInfo{})
	if err != nil || again != ca || string(persisted.Certificate[0]) != string(renewed.Certificate[0]) {
		t.Fatal("renewed leaf or original CA not persisted", err)
	}
}

func TestShardedPresetPreservesTopology(t *testing.T) {
	provider := providerAPI(psmdb.New(psmdb.Options{}))
	for _, preset := range provider.Presets {
		if preset.Name != "sharded" {
			continue
		}
		spec := preset.Spec
		if spec.Topology.Type != "sharded" || spec.Topology.Shards == nil || *spec.Topology.Shards != 2 || spec.Components.ConfigServer == nil || spec.Components.Router == nil {
			t.Fatal(spec)
		}
		return
	}
	t.Fatal("sharded preset missing")
}

func TestMemberRolesMapToPublicEnum(t *testing.T) {
	for _, tc := range []struct{ role, set, state, want string }{
		{"mongos", "", "", "router"}, {"mongod-rs", "rs0", "PRIMARY", "primary"},
		{"secondary", "rs0", "SECONDARY", "secondary"}, {"primary", "cfg", "PRIMARY", "config-primary"},
		{"secondary", "cfg", "SECONDARY", "config-secondary"}, {"mongod-cfg", "", "PRIMARY", "config-primary"},
		{"recovering", "rs0", "RECOVERING", "unknown"}, {"mongod-rs", "rs0", "", "unknown"},
		{"arbiter", "rs0", "ARBITER", "arbiter"},
	} {
		member := (&Server{}).memberAPI("", domain.MemberStatus{Role: tc.role, Set: tc.set, State: tc.state})
		if string(member.Role) != tc.want || member.State != tc.state {
			t.Fatalf("%+v: %+v", tc, member)
		}
	}
}

func TestSSEClosesIdleStreamOnRevocation(t *testing.T) {
	s, _, token := testServer(t)
	ctx, cancel := streamContext(t, s, token)
	defer cancel()
	ctx, timeout := context.WithTimeout(ctx, 20*time.Second)
	defer timeout()
	initial := domain.Event{Cluster: "prod", Type: "cluster.updated", Message: "ready"}
	if err := s.Store.AppendEvent(ctx, &initial); err != nil {
		t.Fatal(err)
	}
	w := &callbackStreamWriter{ResponseRecorder: httptest.NewRecorder()}
	revoked := false
	w.flushed = func() {
		if revoked || !strings.Contains(w.Body.String(), `"message":"ready"`) {
			return
		}
		revoked = true
		token, err := s.Store.FindApiTokenByHash(ctx, secrets.Hash(token))
		if err != nil {
			t.Fatal(err)
		}
		if err := s.Auth.DeleteToken(ctx, token.ID); err != nil {
			t.Fatal(err)
		}
		// No publication wakes this stream: periodic revalidation must close it.
	}
	if err := s.stream(ctx).serve(w); !errors.Is(err, auth.ErrUnauthenticated) {
		t.Fatal("idle revoked stream not closed", err)
	}
}

func TestSSEClosesOnAccessExpiry(t *testing.T) {
	s, sealer, _ := testServer(t)
	service, err := auth.New(auth.Options{Store: s.Store, Sealer: sealer, AccessTTL: time.Second, RefreshTTL: time.Minute, PublicURL: s.PublicURL})
	if err != nil {
		t.Fatal(err)
	}
	s.Auth = service
	login := call(s, "POST", "/api/v1/auth/login", `{"username":"admin","password":"admin-test-password"}`, "")
	if login.Code != 200 {
		t.Fatal(login.Code, login.Body.String())
	}
	ctx, cancel := streamContext(t, s, "")
	defer cancel()
	r := request(ctx).Clone(ctx)
	r.Header.Del("Authorization")
	for _, cookie := range login.Result().Cookies() {
		r.AddCookie(cookie)
	}
	ctx = context.WithValue(ctx, requestKey{}, r)
	initial := domain.Event{Cluster: "prod", Type: "cluster.updated", Message: "ready"}
	if err := s.Store.AppendEvent(ctx, &initial); err != nil {
		t.Fatal(err)
	}
	w := &callbackStreamWriter{ResponseRecorder: httptest.NewRecorder()}
	expired := false
	w.flushed = func() {
		if expired || !strings.Contains(w.Body.String(), `"message":"ready"`) {
			return
		}
		expired = true
		time.Sleep(1100 * time.Millisecond)
		next := domain.Event{Cluster: "prod", Type: "cluster.updated", Message: "must-not-deliver"}
		if err := s.Store.AppendEvent(ctx, &next); err != nil {
			t.Fatal(err)
		}
		s.Hub.Publish(next)
	}
	if err := s.stream(ctx).serve(w); !errors.Is(err, auth.ErrUnauthenticated) {
		t.Fatal("expired access stream not closed", err)
	}
	if strings.Contains(w.Body.String(), "must-not-deliver") {
		t.Fatal(w.Body.String())
	}
}

func TestSSEFlushesInitialCommentBeforeAnyEvent(t *testing.T) {
	s, _, token := testServer(t)
	server := httptest.NewServer(s.Handler())
	defer server.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	r, err := http.NewRequestWithContext(ctx, "GET", server.URL+"/api/v1/events/stream", nil)
	if err != nil {
		t.Fatal(err)
	}
	r.Header.Set("Authorization", "Bearer "+token)
	response, err := server.Client().Do(r)
	if err != nil {
		t.Fatal("initial stream headers were buffered without events", err)
	}
	defer response.Body.Close()
	if response.StatusCode != 200 || response.Header.Get("Cache-Control") != "no-cache, no-transform" || response.Header.Get("X-Accel-Buffering") != "no" {
		t.Fatal(response.StatusCode, response.Header)
	}
	reader := bufio.NewReader(response.Body)
	comment, err := reader.ReadString('\n')
	if err != nil || !strings.HasPrefix(comment, ": connected") || len(comment) < 16<<10 {
		t.Fatal("initial padded comment was not flushed", len(comment), err)
	}
	blank, err := reader.ReadString('\n')
	if err != nil || blank != "\n" {
		t.Fatal("comment did not terminate before any event", blank, err)
	}
}

func TestSSEConnectionPaddingSurvivesCompression(t *testing.T) {
	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	if _, err := writer.Write([]byte(sseConnectedComment)); err != nil {
		t.Fatal(err)
	}
	for i := range 6 {
		comment := sseHeartbeatComments[i%len(sseHeartbeatComments)]
		if !strings.HasPrefix(comment, ": heartbeat ") || !strings.HasSuffix(comment, "\n\n") || strings.Contains(comment, "\nid:") {
			t.Fatal("heartbeat padding must remain a cursor-free SSE comment")
		}
		if _, err := writer.Write([]byte(comment)); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if compressed.Len() < 7*(8<<10) {
		t.Fatalf("transport padding compressed away: %d bytes", compressed.Len())
	}
}
