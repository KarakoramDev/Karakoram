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
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres"
)

type fakePostgresAccess struct {
	fakeProvider
	roles    []postgres.Role
	writes   int
	lastRole postgres.RoleWrite
	err      error
}

func (p *fakePostgresAccess) ListRoles(context.Context, *engine.Context) ([]postgres.Role, error) {
	return p.roles, p.err
}
func (p *fakePostgresAccess) GetRole(_ context.Context, _ *engine.Context, name string) (*postgres.Role, error) {
	for _, role := range p.roles {
		if role.Name == name {
			return &role, p.err
		}
	}
	return nil, engine.ErrNotFound
}
func (p *fakePostgresAccess) CreateRole(_ context.Context, _ *engine.Context, role postgres.RoleWrite) (string, error) {
	p.writes++
	p.lastRole = role
	return "generated-password-returned-once", p.err
}
func (p *fakePostgresAccess) UpdateRole(_ context.Context, _ *engine.Context, role postgres.RoleWrite) error {
	p.writes++
	p.lastRole = role
	return p.err
}
func (p *fakePostgresAccess) DeleteRole(context.Context, *engine.Context, string) error {
	p.writes++
	return p.err
}
func (p *fakePostgresAccess) RotateRolePassword(context.Context, *engine.Context, string) (string, error) {
	p.writes++
	return "rotated-password-returned-once", p.err
}
func (p *fakePostgresAccess) ListDatabases(context.Context, *engine.Context) ([]postgres.Database, error) {
	return []postgres.Database{{Name: "app", Owner: "custom", Encoding: "UTF8", Locale: "C.UTF-8", AllowConnections: true}}, p.err
}
func (p *fakePostgresAccess) GetDatabase(context.Context, *engine.Context, string) (*postgres.Database, error) {
	return &postgres.Database{Name: "app", Owner: "custom", Encoding: "UTF8", Locale: "C.UTF-8", AllowConnections: true}, p.err
}
func (p *fakePostgresAccess) CreateDatabase(context.Context, *engine.Context, postgres.Database) error {
	p.writes++
	return p.err
}
func (p *fakePostgresAccess) DeleteDatabase(context.Context, *engine.Context, string, bool) error {
	p.writes++
	return p.err
}
func (p *fakePostgresAccess) ListGrants(context.Context, *engine.Context, string) ([]postgres.Grant, error) {
	return []postgres.Grant{{Role: "custom", Database: "app", Privileges: []string{"CONNECT"}}}, p.err
}
func (p *fakePostgresAccess) Grant(context.Context, *engine.Context, postgres.Grant) error {
	p.writes++
	return p.err
}
func (p *fakePostgresAccess) Revoke(context.Context, *engine.Context, postgres.Grant) error {
	p.writes++
	return p.err
}

func postgresTestServer(t *testing.T) (*Server, string, *fakePostgresAccess) {
	t.Helper()
	s, token := namespaceTestServer(t)
	p := &fakePostgresAccess{
		roles: []postgres.Role{
			{Name: "custom", Login: true, ConnectionLimit: -1, MemberOf: []string{}},
			{Name: "privileged", Login: true, Superuser: true, ConnectionLimit: -1, MemberOf: []string{}},
			{Name: "indirect", Login: true, ConnectionLimit: -1, MemberOf: []string{"pg_execute_server_program"}},
		},
	}
	var err error
	s.Registry, err = engine.NewRegistry(p)
	if err != nil {
		t.Fatal(err)
	}
	return s, token, p
}

func TestPostgresRoleSecretIdempotency(t *testing.T) {
	s, token, p := postgresTestServer(t)
	for _, tc := range []struct {
		path, body string
		status     int
	}{
		{"/api/v1/clusters/prod/postgres/roles", `{"name":"newapp"}`, http.StatusCreated},
		{"/api/v1/clusters/prod/postgres/roles/custom/password", "", http.StatusOK},
	} {
		before := p.writes
		first := idempotentCall(s, http.MethodPost, tc.path, tc.body, token, "secret-once")
		second := idempotentCall(s, http.MethodPost, tc.path, tc.body, token, "secret-once")
		assertReplay(t, first, second, tc.status)
		if p.writes != before+1 || second.Header().Get("Cache-Control") != "no-store" {
			t.Fatal("secret response replay wrote again or is cacheable")
		}
		var out map[string]any
		if err := json.Unmarshal(first.Body.Bytes(), &out); err != nil || out["generatedPassword"] == nil || out["password"] != nil {
			t.Fatal("invalid one-time secret response", err)
		}
	}
	if !p.lastRole.Login || p.lastRole.ConnectionLimit != -1 {
		t.Fatal("role create did not default login/connection limit")
	}
}

func TestPostgresPrivilegedRoleRBAC(t *testing.T) {
	s, admin, p := postgresTestServer(t)
	ctx := t.Context()
	if _, err := s.Auth.CreateAccount(ctx, domain.Account{Name: "role-manager"}, "test-manager-password"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Auth.CreateAccount(ctx, domain.Account{Name: "cluster-manager"}, "test-cluster-manager-password"); err != nil {
		t.Fatal(err)
	}
	policy := s.RBAC.Policy() + "\np, user:role-manager, database-users, create, default/prod, allow\np, user:role-manager, database-users, update, default/prod, allow\n"
	policy += "p, user:cluster-manager, clusters, update, default/prod, allow\n"
	if err := s.RBAC.ReplacePolicy(ctx, policy); err != nil {
		t.Fatal(err)
	}
	delegated, _, err := s.Auth.CreateToken(ctx, "role-manager", "roles", nil)
	if err != nil {
		t.Fatal(err)
	}
	clusterManager, _, err := s.Auth.CreateToken(ctx, "cluster-manager", "clusters", nil)
	if err != nil {
		t.Fatal(err)
	}
	before := p.writes
	denied := call(s, "POST", "/api/v1/clusters/prod/postgres/roles", `{"name":"newapp","superuser":true}`, clusterManager)
	if denied.Code != http.StatusForbidden || p.writes != before {
		t.Fatal("additional cluster permission replaced the primary role-management permission")
	}
	for _, body := range []string{
		`{"name":"newapp","superuser":true}`,
		`{"name":"newapp","replication":true}`,
		`{"name":"newapp","bypassrls":true}`,
		`{"name":"newapp","memberOf":["privileged"]}`,
		`{"name":"newapp","memberOf":["indirect"]}`,
	} {
		before := p.writes
		w := call(s, "POST", "/api/v1/clusters/prod/postgres/roles", body, delegated)
		if w.Code != http.StatusForbidden || p.writes != before {
			t.Fatal("privileged role bypassed admin requirement", w.Code)
		}
		w = call(s, "POST", "/api/v1/clusters/prod/postgres/roles", body, admin)
		if w.Code != http.StatusCreated {
			t.Fatal("admin cannot create privileged role", w.Code, w.Body.String())
		}
	}
	for _, tc := range []struct{ method, path, body string }{
		{"PUT", "/api/v1/clusters/prod/postgres/roles/privileged", `{"name":"privileged","password":"password-overwrite"}`},
		{"PATCH", "/api/v1/clusters/prod/postgres/roles/privileged", `{"password":"password-overwrite"}`},
		{"POST", "/api/v1/clusters/prod/postgres/roles/privileged/password", ""},
	} {
		before := p.writes
		w := call(s, tc.method, tc.path, tc.body, delegated)
		if w.Code != http.StatusForbidden || p.writes != before {
			t.Fatal("existing privileged role password bypassed admin requirement", w.Code)
		}
	}
	w := call(s, "POST", "/api/v1/clusters/prod/postgres/roles", `{"name":"newapp","memberOf":["pg_monitor"]}`, delegated)
	if w.Code != http.StatusCreated {
		t.Fatal("predefined read-only role requires unexpected admin", w.Code, w.Body.String())
	}
	w = call(s, "GET", "/api/v1/clusters/prod/postgres/roles", "", delegated)
	if w.Code != http.StatusForbidden {
		t.Fatal("write grant incorrectly allows reads", w.Code)
	}
}

func TestPostgresPatchAndUnavailable(t *testing.T) {
	s, token, p := postgresTestServer(t)
	w := call(s, "PATCH", "/api/v1/clusters/prod/postgres/roles/custom", `{"createdb":true}`, token)
	if w.Code != http.StatusOK || !p.lastRole.Createdb || !p.lastRole.Login || p.lastRole.ConnectionLimit != -1 {
		t.Fatal("partial update replaced unspecified attributes", w.Code, w.Body.String())
	}
	w = call(s, "PUT", "/api/v1/clusters/prod/postgres/roles/custom", `{"name":"different"}`, token)
	if w.Code != http.StatusBadRequest {
		t.Fatal("path/body mismatch accepted", w.Code)
	}
	p.err = engine.ErrUnavailable
	w = call(s, "GET", "/api/v1/clusters/prod/postgres/roles", "", token)
	if w.Code != http.StatusServiceUnavailable || strings.Contains(w.Body.String(), "password") {
		t.Fatal("primary unavailability not mapped safely", w.Code)
	}
}
