// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/api"
	"github.com/KarakoramDev/Karakoram/internal/auth"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

func auditEvents(t *testing.T, s *Server, reason string) []domain.Event {
	t.Helper()
	items, err := s.Store.ListEvents(context.Background(), store.EventFilter{Limit: 1000})
	if err != nil {
		t.Fatal(err)
	}
	var out []domain.Event
	for _, e := range items {
		if e.Reason == reason {
			out = append(out, e)
		}
	}
	return out
}

func TestAuditClusterDeleteCookieTokenAndOIDC(t *testing.T) {
	for _, via := range []string{"session", "token", "oidc"} {
		t.Run(via, func(t *testing.T) {
			s, _, token := testServer(t)
			var expected string
			switch via {
			case "session":
				login := call(s, "POST", "/api/v1/auth/login", `{"username":"admin","password":"admin-test-password"}`, "")
				if login.Code != 200 {
					t.Fatal(login.Code, login.Body.String())
				}
				var session api.Session
				if err := json.Unmarshal(login.Body.Bytes(), &session); err != nil {
					t.Fatal(err)
				}
				r := httptest.NewRequest("DELETE", "/api/v1/clusters/prod?keepVolumes=true&keepBackups=true", nil)
				for _, cookie := range login.Result().Cookies() {
					r.AddCookie(cookie)
				}
				r.Header.Set(auth.CSRFHeader, *session.CsrfToken)
				w := httptest.NewRecorder()
				s.Handler().ServeHTTP(w, r)
				if w.Code != 202 {
					t.Fatal(w.Code, w.Body.String())
				}
				expected = "admin"
			case "token":
				r := httptest.NewRequest("DELETE", "/api/v1/clusters/prod?keepVolumes=true&keepBackups=true", nil)
				r.Header.Set("Authorization", "Bearer "+token)
				p, err := s.Auth.Authenticate(r)
				if err != nil {
					t.Fatal(err)
				}
				expected = p.Actor()
				w := httptest.NewRecorder()
				s.Handler().ServeHTTP(w, r)
				if w.Code != 202 {
					t.Fatal(w.Code, w.Body.String())
				}
				if !strings.Contains(expected, "token: testing [") {
					t.Fatal("token identity missing", expected)
				}
			case "oidc":
				// Exercise dispatch with verified issuer metadata independently
				// of the external identity provider handshake.
				expected = "alice (oidc: https://issuer.example)"
				r := httptest.NewRequest("DELETE", "/api/v1/clusters/prod?keepVolumes=true&keepBackups=true", nil)
				ctx := context.WithValue(r.Context(), requestKey{}, r)
				ctx = context.WithValue(ctx, identityKey{}, &auth.Principal{Account: "alice", OIDCIssuer: "https://issuer.example", Via: "oidc", Subjects: []string{"role:admin"}})
				if got := s.handle(ctx, "DeleteCluster", input{Cluster: "prod"}); got.status != 202 {
					t.Fatal(got)
				}
			}
			events := auditEvents(t, s, "ClusterDeleteRequested")
			if len(events) != 1 || events[0].Actor != expected || events[0].Cluster != "prod" || events[0].Target != "prod" || events[0].Resource != "clusters" || !strings.Contains(events[0].Message, "keepVolumes=true, keepBackups=true") {
				t.Fatalf("unexpected delete audit: %+v", events)
			}
			c, err := s.Store.GetCluster(context.Background(), "prod")
			if err != nil || c.RequestedBy != expected {
				t.Fatalf("desired actor not durable: %+v %v", c, err)
			}
		})
	}
}

func TestAuditCredentialsRBACAndFailedLogin(t *testing.T) {
	s, _, token := testServer(t)
	if got := call(s, "GET", "/api/v1/clusters/prod/connection", "", token); got.Code != 200 {
		t.Fatal(got.Code, got.Body.String())
	}
	events := auditEvents(t, s, "ConnectionCredentialsRevealed")
	if len(events) != 1 || events[0].Actor == "" || events[0].Cluster != "prod" || events[0].Resource != "cluster-credentials" || strings.Contains(events[0].Message, "private-test-password") {
		t.Fatal(events)
	}
	body, err := json.Marshal(api.RBACPolicy{Policy: s.RBAC.Policy()})
	if err != nil {
		t.Fatal(err)
	}
	if got := call(s, "PUT", "/api/v1/rbac/policy", string(body), token); got.Code != 200 {
		t.Fatal(got.Code, got.Body.String())
	}
	events = auditEvents(t, s, "ReplaceRBACPolicySucceeded")
	if len(events) != 1 || events[0].Actor == "" || events[0].Resource != "rbac" || events[0].Cluster != "" {
		t.Fatal(events)
	}
	if got := call(s, "POST", "/api/v1/auth/login", `{"username":"attempted-user","password":"never-record-this-password"}`, ""); got.Code != 401 {
		t.Fatal(got.Code)
	}
	events = auditEvents(t, s, "LoginFailed")
	if len(events) != 1 || events[0].Actor != "attempted-user" || strings.Contains(events[0].Message, "never-record-this-password") {
		t.Fatal(events)
	}
}

func TestAuditVisibilityAndExactFilters(t *testing.T) {
	s, _, admin := testServer(t)
	ctx := context.Background()
	if _, err := s.Auth.CreateAccount(ctx, domain.Account{Name: "reader", Roles: []string{"role:viewer"}}, "reader-test-password"); err != nil {
		t.Fatal(err)
	}
	reader, _, err := s.Auth.CreateToken(ctx, "reader", "test", nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range []domain.Event{
		{Cluster: "prod", Resource: "clusters", Actor: "alice", Type: "Normal", Reason: "PhaseChanged", Message: "ready"},
		{Resource: "accounts", Target: "new-user", Actor: "alice", Type: "Normal", Reason: "CreateAccountSucceeded", Message: "account created"},
		{Resource: "rbac", Target: "policy", Actor: "bob", Type: "Normal", Reason: "ReplaceRBACPolicySucceeded", Message: "policy replaced"},
	} {
		if err := s.Store.AppendEvent(ctx, &e); err != nil {
			t.Fatal(err)
		}
	}
	fetch := func(path, token string) api.EventList {
		t.Helper()
		w := call(s, "GET", path, "", token)
		var list api.EventList
		if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &list) != nil {
			t.Fatal(w.Code, w.Body.String())
		}
		return list
	}
	if list := fetch("/api/v1/events", reader); len(list.Items) != 1 || value(list.Items[0].Cluster) != "prod" {
		t.Fatal("viewer sees audit events", list)
	}
	if list := fetch("/api/v1/events?actor=alice&resource=accounts", admin); len(list.Items) != 1 || value(list.Items[0].Target) != "new-user" {
		t.Fatal("exact filters", list)
	}
	policy := s.RBAC.Policy() + "\np, user:reader, audit, read, *, allow\n"
	if err := s.RBAC.ReplacePolicy(ctx, policy); err != nil {
		t.Fatal(err)
	}
	if list := fetch("/api/v1/events", reader); len(list.Items) != 3 {
		t.Fatal("explicit audit grant not effective", list)
	}
	if err := s.RBAC.ReplacePolicy(ctx, policy+"\np, user:reader, audit, read, *, deny\n"); err != nil {
		t.Fatal(err)
	}
	if list := fetch("/api/v1/events", reader); len(list.Items) != 1 {
		t.Fatal("deny override ignored", list)
	}
}

func TestAuditMutationsExcludeSecrets(t *testing.T) {
	for _, tc := range []struct {
		name, method, path, body, resource, target string
		status                                     int
	}{
		{"CreateAccountSucceeded", "POST", "/api/v1/accounts", `{"name":"new-user","password":"sensitive-test-password"}`, "accounts", "new-user", 201},
		{"CreateAPITokenSucceeded", "POST", "/api/v1/api-tokens", `{"name":"ci"}`, "api-tokens", "", 201},
		{"CreateDatabaseUserSucceeded", "POST", "/api/v1/clusters/prod/database-users", `{"username":"app","database":"app","roles":[],"password":"sensitive-test-password"}`, "database-users", "prod/app/app", 201},
		{"CreateDatabaseRoleSucceeded", "POST", "/api/v1/clusters/prod/database-roles", `{"name":"custom","database":"app","roles":[],"privileges":[]}`, "database-users", "prod/app/custom", 201},
		{"ReplaceSettingsSucceeded", "PUT", "/api/v1/settings", `{"oidc":{"enabled":false,"clientSecret":"sensitive-test-password"}}`, "settings", "oidc", 200},
		{"CreateBackupStorageSucceeded", "POST", "/api/v1/backup-storages", `{"name":"archive","type":"s3","endpointURL":"https://s3.example","region":"test","bucket":"archive","credentials":{"accessKeyId":"sensitive-test-password","secretAccessKey":"sensitive-test-password"}}`, "backup-storages", "archive", 201},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, _, token := testServer(t)
			w := call(s, tc.method, tc.path, tc.body, token)
			if w.Code != tc.status {
				t.Fatal(w.Code, w.Body.String())
			}
			events := auditEvents(t, s, tc.name)
			if len(events) != 1 || events[0].Actor == "" || events[0].Resource != tc.resource || events[0].Target == "" || (tc.target != "" && events[0].Target != tc.target) {
				t.Fatal(events)
			}
			data, err := json.Marshal(events[0])
			if err != nil || strings.Contains(string(data), "sensitive-test-password") || strings.Contains(string(data), token) {
				t.Fatal("audit contains secret", err)
			}
		})
	}
}

func TestAuditStreamVisibility(t *testing.T) {
	s, _, _ := testServer(t)
	if _, err := s.Auth.CreateAccount(t.Context(), domain.Account{Name: "viewer", Roles: []string{"role:viewer"}}, "viewer-test-password"); err != nil {
		t.Fatal(err)
	}
	token, _, err := s.Auth.CreateToken(t.Context(), "viewer", "stream", nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := streamContext(t, s, token)
	defer cancel()
	for _, e := range []domain.Event{
		{Resource: "rbac", Actor: "admin", Type: "Normal", Message: "private audit"},
		{Cluster: "prod", Resource: "clusters", Actor: "admin", Type: "Normal", Message: "public sentinel"},
	} {
		if err := s.Store.AppendEvent(ctx, &e); err != nil {
			t.Fatal(err)
		}
	}
	w := &callbackStreamWriter{ResponseRecorder: httptest.NewRecorder()}
	w.flushed = func() {
		if strings.Contains(w.Body.String(), `"message":"public sentinel"`) {
			cancel()
		}
	}
	_ = s.stream(ctx).serve(w)
	if strings.Contains(w.Body.String(), "private audit") || !strings.Contains(w.Body.String(), "public sentinel") {
		t.Fatal("stream exposed audit or omitted cluster event")
	}
}
