// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/neo4j"
)

type fakeNeo4jAccess struct {
	fakeProvider
	writes   int
	err      error
	password string
}

func (*fakeNeo4jAccess) Name() string { return "neo4j" }
func (p *fakeNeo4jAccess) ListUsers(context.Context, *engine.Context) ([]neo4j.User, error) {
	return []neo4j.User{{Name: "app", PasswordChangeRequired: false}}, p.err
}
func (p *fakeNeo4jAccess) GetUser(_ context.Context, _ *engine.Context, name string) (*neo4j.User, error) {
	return &neo4j.User{Name: name}, p.err
}
func (p *fakeNeo4jAccess) CreateUser(_ context.Context, _ *engine.Context, _, password string) (string, error) {
	p.writes++
	p.password = password
	if password != "" {
		return "", p.err
	}
	return "generated-once", p.err
}
func (p *fakeNeo4jAccess) SetUserPassword(ctx context.Context, rc *engine.Context, name, password string) (string, error) {
	return p.CreateUser(ctx, rc, name, password)
}
func (p *fakeNeo4jAccess) DeleteUser(context.Context, *engine.Context, string) error {
	p.writes++
	return p.err
}

func neo4jTestServer(t *testing.T) (*Server, string, *fakeNeo4jAccess) {
	t.Helper()
	s, token := namespaceTestServer(t)
	p := &fakeNeo4jAccess{}
	var err error
	s.Registry, err = engine.NewRegistry(p, &fakeProvider{})
	if err != nil {
		t.Fatal(err)
	}
	c, err := s.Store.GetCluster(t.Context(), domain.ClusterRef{Name: "prod"})
	if err != nil {
		t.Fatal(err)
	}
	c.Spec.Provider = "neo4j"
	if err := s.Store.UpdateClusterSpec(t.Context(), c.Ref(), c.Spec); err != nil {
		t.Fatal(err)
	}
	return s, token, p
}

func TestNeo4jSecretIdempotency(t *testing.T) {
	for _, tc := range []struct {
		method, suffix, body string
		status               int
	}{
		{"POST", "", `{"name":"app"}`, 201},
		{"POST", "/app/password", "", 200},
		{"POST", "/app/password", `{"password":"supplied-secret"}`, 200},
		{"DELETE", "/app", "", 204},
	} {
		t.Run(tc.method+tc.suffix+tc.body, func(t *testing.T) {
			s, token, p := neo4jTestServer(t)
			path := "/api/v1/clusters/prod/neo4j/users" + tc.suffix
			first := idempotentCall(s, tc.method, path, tc.body, token, "user-request")
			second := idempotentCall(s, tc.method, path+"?namespace=default", tc.body, token, "user-request")
			assertReplay(t, first, second, tc.status)
			if p.writes != 1 {
				t.Fatal("replay executed mutation", p.writes)
			}
			if tc.method == "POST" {
				if first.Header().Get("Cache-Control") != "no-store" {
					t.Fatal("cacheable password response")
				}
				if strings.Contains(first.Body.String(), "supplied-secret") {
					t.Fatal("supplied password echoed")
				}
				var out map[string]any
				if err := json.Unmarshal(first.Body.Bytes(), &out); err != nil {
					t.Fatal(err)
				}
				if tc.body == `{"password":"supplied-secret"}` {
					if out["generatedPassword"] != nil {
						t.Fatal("supplied password returned")
					}
				} else if out["generatedPassword"] != "generated-once" {
					t.Fatal("generated password missing")
				}
				different := `{"password":"different"}`
				if tc.suffix == "" {
					different = `{"name":"different"}`
				}
				assertIdempotencyConflict(t, idempotentCall(s, tc.method, path, different, token, "user-request"), domain.ReasonConflict)
			}
		})
	}
}

func TestNeo4jAccessErrorsAndEngineMismatch(t *testing.T) {
	s, token, p := neo4jTestServer(t)
	for _, tc := range []struct {
		err    error
		status int
	}{{engine.ErrNotFound, 404}, {engine.ErrConflict, 409}, {engine.ErrInvalid, 400}, {engine.ErrNotSupported, 409}} {
		p.err = tc.err
		w := call(s, "GET", "/api/v1/clusters/prod/neo4j/users/app", "", token)
		if w.Code != tc.status {
			t.Fatal(tc.err, w.Code, w.Body.String())
		}
	}
	p.err = nil
	c, err := s.Store.GetCluster(t.Context(), domain.ClusterRef{Name: "prod"})
	if err != nil {
		t.Fatal(err)
	}
	c.Spec.Provider = "fake"
	if err := s.Store.UpdateClusterSpec(t.Context(), c.Ref(), c.Spec); err != nil {
		t.Fatal(err)
	}
	if w := call(s, "GET", "/api/v1/clusters/prod/neo4j/users", "", token); w.Code != 409 {
		t.Fatal(w.Code, w.Body.String())
	}
}

func TestNeo4jMutationRequiresClusterUpdate(t *testing.T) {
	s, admin, p := neo4jTestServer(t)
	ctx := t.Context()
	if _, err := s.Auth.CreateAccount(ctx, domain.Account{Name: "user-manager"}, "test-manager-password"); err != nil {
		t.Fatal(err)
	}
	policy := s.RBAC.Policy() + "\np, user:user-manager, database-users, *, default/prod, allow\n"
	if err := s.RBAC.ReplacePolicy(ctx, policy); err != nil {
		t.Fatal(err)
	}
	delegated, _, err := s.Auth.CreateToken(ctx, "user-manager", "users", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ method, suffix, body string }{{"POST", "", `{"name":"app"}`}, {"POST", "/app/password", ""}, {"DELETE", "/app", ""}} {
		w := call(s, tc.method, "/api/v1/clusters/prod/neo4j/users"+tc.suffix, tc.body, delegated)
		if w.Code != http.StatusForbidden || p.writes != 0 {
			t.Fatal("administrative mutation allowed", w.Code)
		}
	}
	if w := call(s, "GET", "/api/v1/clusters/prod/neo4j/users", "", delegated); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	if w := call(s, "POST", "/api/v1/clusters/prod/neo4j/users", `{"name":"app"}`, admin); w.Code != 201 {
		t.Fatal(w.Code, w.Body.String())
	}
	if err := s.RBAC.ReplacePolicy(ctx, policy+"p, user:user-manager, clusters, update, default/prod, allow\n"); err != nil {
		t.Fatal(err)
	}
	path := "/api/v1/clusters/prod/neo4j/users/app/password"
	first := idempotentCall(s, "POST", path, "", delegated, "revoked-user")
	if first.Code != 200 {
		t.Fatal(first.Code, first.Body.String())
	}
	before := p.writes
	if err := s.RBAC.ReplacePolicy(ctx, policy); err != nil {
		t.Fatal(err)
	}
	replay := idempotentCall(s, "POST", path, "", delegated, "revoked-user")
	if replay.Code != 403 || p.writes != before || strings.Contains(replay.Body.String(), "generated-once") {
		t.Fatal("revoked administrative permission revealed cached credentials", replay.Code)
	}
}

func TestNeo4jRestrictedTokenDeniesPasswordReveal(t *testing.T) {
	s, _, p := neo4jTestServer(t)
	token, _, err := s.Auth.CreateToken(t.Context(), "admin", "no-reveal", nil, &domain.TokenRestrictions{AllowCredentialReveal: false})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ path, body string }{
		{"/api/v1/clusters/prod/neo4j/users", `{"name":"app"}`},
		{"/api/v1/clusters/prod/neo4j/users/app/password", ""},
	} {
		w := call(s, "POST", tc.path, tc.body, token)
		if w.Code != http.StatusForbidden || p.writes != 0 {
			t.Fatalf("restricted token allowed password mutation: %d %s", w.Code, w.Body.String())
		}
	}
}
