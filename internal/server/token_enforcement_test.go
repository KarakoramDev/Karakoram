// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/api"
	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func restrictedAdmin(t *testing.T, s *Server, restrictions *domain.TokenRestrictions) string {
	t.Helper()
	token, _, err := s.Auth.CreateToken(t.Context(), "admin", "restricted", nil, restrictions)
	if err != nil {
		t.Fatal(err)
	}
	return token
}

func TestRestrictedTokenRouteGates(t *testing.T) {
	s, _, _ := testServer(t)
	read := restrictedAdmin(t, s, &domain.TokenRestrictions{ReadOnly: true})
	noSecrets := restrictedAdmin(t, s, &domain.TokenRestrictions{})
	permissions := restrictedAdmin(t, s, &domain.TokenRestrictions{Permissions: []string{"clusters:read", "rbac:read"}})
	for _, tc := range []struct {
		token, method, path, body string
		status                    int
	}{
		{read, "GET", "/api/v1/clusters/prod", "", 200},
		{read, "POST", "/api/v1/clusters/prod/restart", "", 403},
		{read, "POST", "/api/v1/api-tokens", `{"name":"child"}`, 403},
		{read, "POST", "/api/v1/auth/logout", "", 403},
		{noSecrets, "GET", "/api/v1/clusters/prod/connection", "", 403},
		{noSecrets, "POST", "/api/v1/clusters/prod/postgres/roles", `{"name":"newapp"}`, 403},
		{noSecrets, "POST", "/api/v1/api-tokens", `{"name":"child"}`, 403},
		{permissions, "GET", "/api/v1/clusters/prod", "", 200},
		{permissions, "GET", "/api/v1/events", "", 403},
		{permissions, "GET", "/api/v1/accounts", "", 403},
	} {
		w := call(s, tc.method, tc.path, tc.body, tc.token)
		if w.Code != tc.status {
			t.Fatalf("%s %s: %d want %d: %s", tc.method, tc.path, w.Code, tc.status, w.Body.String())
		}
	}
	for _, tc := range []struct {
		object, action string
		allowed        bool
	}{{"default/prod", "read", true}, {"default/prod", "update", false}} {
		body, _ := json.Marshal(api.CanRequest{Resource: "clusters", Action: api.CanRequestAction(tc.action), Object: tc.object})
		w := call(s, "POST", "/api/v1/rbac/can", string(body), permissions)
		var got api.CanResponse
		if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
			t.Fatal(err)
		}
		if w.Code != 200 || got.Allowed != tc.allowed {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	w := call(s, "GET", "/api/v1/permissions", "", permissions)
	var got api.Permissions
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if w.Code != 200 || len(got.Permissions) == 0 {
		t.Fatal(w.Code, w.Body.String())
	}
	for _, p := range got.Permissions {
		if p.Effect == "allow" && (p.Action != "read" || p.Resource != "clusters" && p.Resource != "rbac") {
			t.Fatal("wider permission", p)
		}
	}
}

func TestRestrictedTokenScopesAndObservability(t *testing.T) {
	s, admin := namespaceTestServer(t)
	ctx := t.Context()
	if err := s.Store.CreateNamespace(ctx, &domain.Namespace{Name: "other", Phase: "Ready"}); err != nil {
		t.Fatal(err)
	}
	c, err := s.Store.GetCluster(ctx, domain.ClusterRef{Namespace: "default", Name: "prod"})
	if err != nil {
		t.Fatal(err)
	}
	c.Namespace = "other"
	if err = s.Store.CreateCluster(ctx, c); err != nil {
		t.Fatal(err)
	}
	for _, ns := range []string{"default", "other"} {
		if err = s.Context.Events.Record(ctx, domain.Event{Namespace: ns, Cluster: "prod", Type: "Normal", Message: ns + " event"}); err != nil {
			t.Fatal(err)
		}
		if err = s.Store.EnqueueOperation(ctx, &domain.Operation{Namespace: ns, Cluster: "prod", Kind: domain.OperationRestart}); err != nil {
			t.Fatal(err)
		}
	}
	token := restrictedAdmin(t, s, &domain.TokenRestrictions{Namespaces: []string{"default"}, Clusters: []string{"default/prod"}})
	for _, path := range []string{"/api/v1/clusters/prod?namespace=other", "/api/v1/settings", "/api/v1/auth/me", "/api/v1/permissions"} {
		if w := call(s, "GET", path, "", token); w.Code != 403 {
			t.Fatal(path, w.Code, w.Body.String())
		}
	}
	for _, path := range []string{"/api/v1/clusters", "/api/v1/events", "/api/v1/operations", "/api/v1/namespaces"} {
		w := call(s, "GET", path, "", token)
		if w.Code != 200 || strings.Contains(w.Body.String(), `"namespace":"other"`) || strings.Contains(w.Body.String(), `"name":"other"`) || strings.Contains(w.Body.String(), `"name":"dev"`) {
			t.Fatal(path, w.Code, w.Body.String())
		}
	}
	var operations api.OperationList
	w := call(s, "GET", "/api/v1/operations", "", admin)
	if err = json.Unmarshal(w.Body.Bytes(), &operations); err != nil {
		t.Fatal(err)
	}
	for _, op := range operations.Items {
		w = call(s, "GET", "/api/v1/operations/"+op.Id, "", token)
		want := 200
		if op.Namespace != nil && *op.Namespace != "default" {
			want = 403
		}
		if w.Code != want {
			t.Fatal(w.Code, w.Body.String())
		}
		if want == 403 {
			if w = call(s, "POST", "/api/v1/operations/"+op.Id+"/cancel", "", token); w.Code != 403 {
				t.Fatal(w.Code, w.Body.String())
			}
		}
	}
}

func TestRestrictedTokenIssuanceSubset(t *testing.T) {
	s, _, _ := testServer(t)
	token := restrictedAdmin(t, s, &domain.TokenRestrictions{Permissions: []string{"api-tokens:*"}, AllowCredentialReveal: true})
	for _, body := range []string{`{"name":"unrestricted"}`, `{"name":"wider","restrictions":{"allowCredentialReveal":true}}`, `{"name":"other","restrictions":{"permissions":["clusters:read"],"allowCredentialReveal":true}}`} {
		if w := call(s, "POST", "/api/v1/api-tokens", body, token); w.Code != 403 {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	w := call(s, "POST", "/api/v1/api-tokens", `{"name":"narrow","restrictions":{"readOnly":true,"permissions":["api-tokens:read"]}}`, token)
	if w.Code != 201 {
		t.Fatal(w.Code, w.Body.String())
	}
}

func TestRestrictedTokenReplayReauthorizes(t *testing.T) {
	s, _, _ := testServer(t)
	token, record, err := s.Auth.CreateToken(t.Context(), "admin", "replay", nil, &domain.TokenRestrictions{AllowCredentialReveal: true})
	if err != nil {
		t.Fatal(err)
	}
	body := `{"name":"child","restrictions":{"readOnly":true}}`
	first := idempotentCall(s, "POST", "/api/v1/api-tokens", body, token, "restriction-replay")
	if first.Code != 201 {
		t.Fatal(first.Code, first.Body.String())
	}
	stored, err := s.Store.GetApiToken(t.Context(), record.ID)
	if err != nil {
		t.Fatal(err)
	}
	stored.Restrictions.ReadOnly = true
	if err = s.Store.UpdateApiToken(t.Context(), stored); err != nil {
		t.Fatal(err)
	}
	replay := idempotentCall(s, "POST", "/api/v1/api-tokens", body, token, "restriction-replay")
	if replay.Code != 403 || replay.Header().Get("Idempotent-Replayed") != "" || strings.Contains(replay.Body.String(), `"secret"`) {
		t.Fatal(replay.Code, replay.Body.String())
	}
}

func TestRestrictedTokenSnapshotAndUsage(t *testing.T) {
	s, _, _ := testServer(t)
	token, record, err := s.Auth.CreateToken(t.Context(), "admin", "snapshot", nil, &domain.TokenRestrictions{Namespaces: []string{"default"}})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodGet, "/api/v1/clusters", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	p, err := s.Auth.Authenticate(req)
	if err != nil {
		t.Fatal(err)
	}
	p.Restrictions.Namespaces[0] = "other"
	if err = s.Store.TouchApiToken(t.Context(), record.ID, time.Now()); err != nil {
		t.Fatal(err)
	}
	again, err := s.Auth.Authenticate(req)
	if err != nil || again.Restrictions.Namespaces[0] != "default" {
		t.Fatal(again, err)
	}
}
