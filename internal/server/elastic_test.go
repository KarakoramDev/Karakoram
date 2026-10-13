// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/api"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/elastic"
)

type fakeElasticAccess struct {
	fakeProvider
	err         error
	user        elastic.UserWrite
	role        elastic.AccessRole
	createRole  bool
	key         elastic.APIKeyCreate
	invalidated string
	namespace   string
}

func (f *fakeElasticAccess) ListUsers(_ context.Context, rc *engine.Context) ([]elastic.User, error) {
	f.namespace = rc.Cluster.Ref().Namespace
	return []elastic.User{{Username: "app", Roles: []string{"viewer"}, Enabled: true}}, f.err
}
func (f *fakeElasticAccess) GetUser(context.Context, *engine.Context, string) (*elastic.User, error) {
	return &elastic.User{Username: "app", Roles: []string{"viewer"}, Enabled: true}, f.err
}
func (f *fakeElasticAccess) CreateUser(_ context.Context, _ *engine.Context, u elastic.UserWrite) error {
	f.user = u
	return f.err
}
func (f *fakeElasticAccess) UpdateUser(_ context.Context, _ *engine.Context, u elastic.UserWrite) error {
	f.user = u
	return f.err
}
func (f *fakeElasticAccess) DeleteUser(context.Context, *engine.Context, string) error { return f.err }
func (f *fakeElasticAccess) ListRoles(context.Context, *engine.Context) ([]elastic.AccessRole, error) {
	return []elastic.AccessRole{{Name: "ingest", Role: elastic.Role{Cluster: []string{}, Indices: []elastic.IndexPrivileges{}, Applications: []elastic.ApplicationPrivileges{{Application: "apm", Privileges: []string{"event:write", "config_agent:read"}, Resources: []string{"*"}}}}}}, f.err
}
func (f *fakeElasticAccess) GetRole(ctx context.Context, rc *engine.Context, _ string) (*elastic.AccessRole, error) {
	roles, err := f.ListRoles(ctx, rc)
	return &roles[0], err
}
func (f *fakeElasticAccess) PutRole(_ context.Context, _ *engine.Context, r elastic.AccessRole, create bool) error {
	f.role = r
	f.createRole = create
	return f.err
}
func (f *fakeElasticAccess) DeleteRole(context.Context, *engine.Context, string) error { return f.err }
func (f *fakeElasticAccess) ListAPIKeys(context.Context, *engine.Context) ([]elastic.APIKey, error) {
	return []elastic.APIKey{{ID: "key", Name: "agent", Creation: 123, Username: "karakoram"}}, f.err
}
func (f *fakeElasticAccess) CreateAPIKey(_ context.Context, _ *engine.Context, k elastic.APIKeyCreate) (*elastic.APIKeyCreated, error) {
	f.key = k
	return &elastic.APIKeyCreated{ID: "key", Name: k.Name, APIKey: "one-time-secret", Encoded: "one-time-encoded"}, f.err
}
func (f *fakeElasticAccess) InvalidateAPIKey(_ context.Context, _ *engine.Context, id string) error {
	f.invalidated = id
	return f.err
}

func elasticTestServer(t *testing.T) (*Server, *fakeElasticAccess, string) {
	t.Helper()
	s, _, token := testServer(t)
	f := &fakeElasticAccess{}
	registry, err := engine.NewRegistry(f)
	if err != nil {
		t.Fatal(err)
	}
	s.Registry = registry
	return s, f, token
}
func TestElasticAccessHandlers(t *testing.T) {
	s, f, token := elasticTestServer(t)
	routes := []struct {
		method, path, body string
		status             int
	}{
		{"GET", "users", "", 200},
		{"GET", "users/app", "", 200},
		{"POST", "users", `{"username":"app","roles":["viewer"],"enabled":false,"fullName":"Analyst","email":"app@example.com","password":"private-password"}`, 201},
		{"PUT", "users/app", `{"username":"app","roles":["viewer"],"enabled":true}`, 200},
		{"DELETE", "users/app", "", 204},
		{"GET", "roles", "", 200},
		{"GET", "roles/ingest", "", 200},
		{"POST", "roles", `{"name":"ingest","cluster":[],"indices":[],"applications":[{"application":"apm","privileges":["event:write","config_agent:read"],"resources":["*"]}]}`, 201},
		{"PUT", "roles/ingest", `{"name":"ingest","cluster":[],"indices":[],"applications":[]}`, 200},
		{"DELETE", "roles/ingest", "", 204},
		{"GET", "api-keys", "", 200},
		{"POST", "api-keys", `{"name":"agent","expiration":"1d","roleDescriptors":{"apm":{"cluster":[],"indices":[],"applications":[{"application":"apm","privileges":["event:write"],"resources":["*"]}]}}}`, 201},
		{"DELETE", "api-keys/key", "", 204},
	}
	for _, r := range routes {
		t.Run(r.method+" "+r.path, func(t *testing.T) {
			w := call(s, r.method, "/api/v1/clusters/prod/elasticsearch/"+r.path, r.body, token)
			if w.Code != r.status {
				t.Fatal(w.Code, w.Body.String())
			}
			if r.path == "api-keys" && r.method == "POST" {
				if w.Header().Get("Cache-Control") != "no-store" {
					t.Fatal("key response cached")
				}
				var key api.ElasticAPIKeyCreated
				if err := json.Unmarshal(w.Body.Bytes(), &key); err != nil || key.ApiKey != "one-time-secret" {
					t.Fatal(key, err)
				}
				if f.key.RoleDescriptors["apm"].Applications[0].Application != "apm" {
					t.Fatal(f.key)
				}
			} else if strings.Contains(w.Body.String(), "private-password") || strings.Contains(w.Body.String(), "one-time-secret") {
				t.Fatal("secret leaked")
			}
			if r.path == "users" && r.method == "POST" && (f.user.Password != "private-password" || f.user.Enabled || f.user.FullName != "Analyst") {
				t.Fatal(f.user)
			}
			if r.path == "roles" && r.method == "POST" && (!f.createRole || len(f.role.Applications) != 1) {
				t.Fatal(f.role)
			}
			if r.path == "roles/ingest" && r.method == "PUT" && f.createRole {
				t.Fatal("update used create semantics")
			}
		})
	}
	if f.invalidated != "key" {
		t.Fatal(f.invalidated)
	}
	for _, reason := range []string{"CreateElasticUserSucceeded", "UpdateElasticUserSucceeded", "DeleteElasticUserSucceeded", "CreateElasticRoleSucceeded", "UpdateElasticRoleSucceeded", "DeleteElasticRoleSucceeded", "CreateElasticAPIKeySucceeded", "InvalidateElasticAPIKeySucceeded"} {
		events := auditEvents(t, s, reason)
		if len(events) != 1 || events[0].Actor == "" || events[0].Resource != "database-users" || !strings.HasPrefix(events[0].Target, "prod/") {
			t.Fatal(reason, events)
		}
		data, _ := json.Marshal(events)
		if strings.Contains(string(data), "private-password") || strings.Contains(string(data), "one-time-secret") {
			t.Fatal("audit leaked secret")
		}
	}
}
func TestElasticAccessErrorsAndValidation(t *testing.T) {
	s, f, token := elasticTestServer(t)
	for _, tc := range []struct {
		err  error
		code int
	}{{engine.ErrNotFound, 404}, {engine.ErrConflict, 409}, {engine.ErrInvalid, 400}, {errors.New("failure"), 500}} {
		f.err = tc.err
		w := call(s, "GET", "/api/v1/clusters/prod/elasticsearch/users", "", token)
		if w.Code != tc.code {
			t.Fatal(tc.code, w.Code, w.Body.String())
		}
	}
	f.err = nil
	for _, body := range []string{`{"username":"app","roles":[],"enabled":true}`, `{"username":"app","roles":[],"enabled":true,"password":"short"}`} {
		w := call(s, "POST", "/api/v1/clusters/prod/elasticsearch/users", body, token)
		if w.Code != 400 {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	if w := call(s, "PUT", "/api/v1/clusters/prod/elasticsearch/users/app", `{"username":"other","roles":[],"enabled":true}`, token); w.Code != 400 {
		t.Fatal(w.Code, w.Body.String())
	}
	if w := call(s, "PUT", "/api/v1/clusters/prod/elasticsearch/roles/ingest", `{"name":"other","cluster":[],"indices":[],"applications":[]}`, token); w.Code != 400 {
		t.Fatal(w.Code, w.Body.String())
	}
	plain, _, plainToken := testServer(t)
	if w := call(plain, "GET", "/api/v1/clusters/prod/elasticsearch/users", "", plainToken); w.Code != 409 {
		t.Fatal(w.Code, w.Body.String())
	}
	if w := call(s, "GET", "/api/v1/clusters/missing/elasticsearch/users", "", token); w.Code != 404 {
		t.Fatal(w.Code, w.Body.String())
	}
}
func TestElasticAccessRBACAndNamespace(t *testing.T) {
	s, f, admin := elasticTestServer(t)
	if _, err := s.Auth.CreateAccount(t.Context(), domain.Account{Name: "viewer", Roles: []string{"role:viewer"}}, "viewer-password"); err != nil {
		t.Fatal(err)
	}
	token, _, err := s.Auth.CreateToken(t.Context(), "viewer", "test", nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, method := range []string{http.MethodPost, http.MethodDelete} {
		path := "/api/v1/clusters/prod/elasticsearch/users"
		body := ""
		if method == http.MethodPost {
			body = `{"username":"app","roles":[],"enabled":true,"password":"private-password"}`
		}
		if method == http.MethodDelete {
			path += "/app"
		}
		if w := call(s, method, path, body, token); w.Code != 403 {
			t.Fatal(method, w.Code, w.Body.String())
		}
	}
	if w := call(s, "GET", "/api/v1/clusters/prod/elasticsearch/users", "", token); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	if w := call(s, "GET", "/api/v1/clusters/prod/elasticsearch/users?namespace=default", "", admin); w.Code != 200 || f.namespace != "default" {
		t.Fatal(w.Code, f.namespace, w.Body.String())
	}
	if w := call(s, "GET", "/api/v1/clusters/prod/elasticsearch/users?namespace=missing", "", admin); w.Code == 200 {
		t.Fatal("namespace ignored")
	}
}
func TestElasticMemberRoles(t *testing.T) {
	for _, role := range []string{"master", "data", "master,data", "kibana", "logstash", "apm"} {
		if got := memberRole(domain.MemberStatus{Role: role, State: "joined"}); string(got) != role {
			t.Fatal(role, got)
		}
	}
}
