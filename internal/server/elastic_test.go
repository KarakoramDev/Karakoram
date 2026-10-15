// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/api"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/elastic"
)

type fakeElasticAccess struct {
	fakeProvider
	err               error
	user              elastic.UserWrite
	role              elastic.AccessRole
	createRole        bool
	key               elastic.APIKeyCreate
	invalidated       string
	namespace         string
	invalidateContext func(context.Context) error
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
func (f *fakeElasticAccess) InvalidateAPIKey(ctx context.Context, _ *engine.Context, id string) error {
	f.invalidated = id
	if f.invalidateContext != nil {
		return f.invalidateContext(ctx)
	}
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

func TestElasticNativeMutationsReplayAndScopeIdempotency(t *testing.T) {
	for _, tc := range []struct {
		name, method, path, body, different string
		status                              int
	}{
		{"create user", "POST", "users", `{"username":"app","roles":["viewer"],"enabled":true,"password":"private-password"}`, `{"username":"app","roles":["viewer"],"enabled":false,"password":"private-password"}`, 201},
		{"update user", "PUT", "users/app", `{"username":"app","roles":["viewer"],"enabled":true}`, `{"username":"app","roles":["viewer"],"enabled":false}`, 200},
		{"create role", "POST", "roles", `{"name":"ingest","cluster":[],"indices":[],"applications":[]}`, `{"name":"ingest","cluster":["monitor"],"indices":[],"applications":[]}`, 201},
		{"update role", "PUT", "roles/ingest", `{"name":"ingest","cluster":[],"indices":[],"applications":[]}`, `{"name":"ingest","cluster":["monitor"],"indices":[],"applications":[]}`, 200},
		{"create API key", "POST", "api-keys", `{"name":"agent","roleDescriptors":{"reader":{"cluster":[],"indices":[],"applications":[]}}}`, `{"name":"other","roleDescriptors":{"reader":{"cluster":[],"indices":[],"applications":[]}}}`, 201},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, f, token := elasticTestServer(t)
			path := "/api/v1/clusters/prod/elasticsearch/" + tc.path
			first := idempotentCall(s, tc.method, path, tc.body, token, "native-request")
			// Any duplicate provider execution now fails instead of returning
			// the original object or one-time secret.
			f.err = engine.ErrConflict
			second := idempotentCall(s, tc.method, path, tc.body, token, "native-request")
			assertReplay(t, first, second, tc.status)
			normalized := idempotentCall(s, tc.method, path+"?namespace=default", tc.body, token, "native-request")
			if normalized.Code != tc.status || normalized.Header().Get("Idempotent-Replayed") != "true" {
				t.Fatal("default namespace scope changed", normalized.Code, normalized.Body.String())
			}
			assertIdempotencyConflict(t, idempotentCall(s, tc.method, path, tc.different, token, "native-request"), domain.ReasonConflict)
			isolated := idempotentCall(s, tc.method, path+"?namespace=missing", tc.body, token, "native-request")
			if isolated.Code != 404 || isolated.Header().Get("Idempotent-Replayed") != "" {
				t.Fatal("namespace shared an object or secret replay", isolated.Code, isolated.Body.String())
			}
			if tc.path == "api-keys" && (second.Header().Get("Cache-Control") != "no-store" || !strings.Contains(second.Body.String(), "one-time-secret")) {
				t.Fatal("API-key replay lost secret response protection", second.Header())
			}
		})
	}
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

func TestRestorePITRAdmissionUsesProviderCapability(t *testing.T) {
	for _, supported := range []bool{false, true} {
		name := "unsupported-elasticsearch"
		if supported {
			name = "supported-provider"
		}
		t.Run(name, func(t *testing.T) {
			s, _, token := testServer(t)
			info := (&fakeProvider{}).Describe()
			info.Capabilities = elastic.New(elastic.ProviderOptions{}).Describe().Capabilities
			wantStatus, wantRecords := http.StatusBadRequest, 0
			if supported {
				info.Capabilities = append(info.Capabilities, "pitr")
				wantStatus, wantRecords = http.StatusAccepted, 1
			}
			var err error
			s.Registry, err = engine.NewRegistry(&fakeProvider{info: &info})
			if err != nil {
				t.Fatal(err)
			}
			body := `{"name":"point-in-time","source":{"pitrTime":"2026-10-12T12:00:00Z"}}`
			w := call(s, http.MethodPost, "/api/v1/clusters/prod/restores", body, token)
			if w.Code != wantStatus {
				t.Fatal(w.Code, w.Body.String())
			}
			ref := domain.ClusterRef{Name: "prod"}
			restores, err := s.Store.ListRestores(t.Context(), ref)
			if err != nil || len(restores) != wantRecords {
				t.Fatal("unexpected restore inventory", restores, err)
			}
			ops, err := s.Store.ListOperations(t.Context(), ref)
			if err != nil || len(ops) != wantRecords {
				t.Fatal("unexpected restore queue", ops, err)
			}
		})
	}
}

func TestElasticAccessStructuredErrors(t *testing.T) {
	s, f, token := elasticTestServer(t)
	for _, tc := range []struct {
		name, method, path, body, field, resource, resourceName, reason, message string
		err                                                                      error
		status                                                                   int
	}{
		{name: "short password", method: "POST", path: "users", body: `{"username":"app","roles":["viewer"],"enabled":true,"password":"short"}`, field: "password", reason: domain.ReasonOutOfRange, message: "8", status: 400},
		{name: "unknown role", method: "POST", path: "users", body: `{"username":"app","roles":["missing"],"enabled":true,"password":"long-password"}`, err: engine.InvalidField("roles[0]", domain.ReasonNotFound, `roles[0]: Elasticsearch role "missing" does not exist; create or select an existing role first`), field: "roles[0]", reason: domain.ReasonNotFound, message: `"missing"`, status: 400},
		{name: "invalid privilege", method: "POST", path: "roles", body: `{"name":"app","cluster":["wrong"],"indices":[],"applications":[]}`, err: engine.InvalidField("cluster", domain.ReasonInvalid, "cluster contains an invalid Elasticsearch privilege"), field: "cluster", reason: domain.ReasonInvalid, message: "invalid Elasticsearch privilege", status: 400},
		{name: "licence", method: "POST", path: "roles", body: `{"name":"app","cluster":[],"indices":[],"applications":[]}`, err: engine.InvalidField("indices[0].query", domain.ReasonUnsupported, "indices[0].query requires an Elasticsearch licence tier that includes document- and field-level security"), field: "indices[0].query", reason: domain.ReasonUnsupported, message: "licence tier", status: 400},
		{name: "duplicate user", method: "POST", path: "users", body: `{"username":"app","roles":["viewer"],"enabled":true,"password":"long-password"}`, err: domain.Conflict("elasticsearchUser", "app", domain.ReasonAlreadyExists, engine.ErrConflict, `Elasticsearch user "app" already exists`), resource: "elasticsearchUser", resourceName: "app", reason: domain.ReasonAlreadyExists, message: `"app" already exists`, status: 409},
		{name: "duplicate role", method: "POST", path: "roles", body: `{"name":"app","cluster":[],"indices":[],"applications":[]}`, err: domain.Conflict("elasticsearchRole", "app", domain.ReasonAlreadyExists, engine.ErrConflict, `Elasticsearch role "app" already exists`), resource: "elasticsearchRole", resourceName: "app", reason: domain.ReasonAlreadyExists, message: `"app" already exists`, status: 409},
		{name: "protected elastic", method: "DELETE", path: "users/elastic", err: domain.Conflict("elasticsearchUser", "elastic", domain.ReasonProtected, engine.ErrInvalid, `Elasticsearch user "elastic" is protected`), resource: "elasticsearchUser", resourceName: "elastic", reason: domain.ReasonProtected, message: `"elastic" is protected`, status: 409},
		{name: "protected kibana", method: "GET", path: "users/kibana_system", err: domain.Conflict("elasticsearchUser", "kibana_system", domain.ReasonProtected, engine.ErrInvalid, `Elasticsearch user "kibana_system" is protected`), resource: "elasticsearchUser", resourceName: "kibana_system", reason: domain.ReasonProtected, message: `"kibana_system" is protected`, status: 409},
		{name: "protected role", method: "DELETE", path: "roles/superuser", err: domain.Conflict("elasticsearchRole", "superuser", domain.ReasonProtected, engine.ErrInvalid, `Elasticsearch role "superuser" is protected`), resource: "elasticsearchRole", resourceName: "superuser", reason: domain.ReasonProtected, message: `"superuser" is protected`, status: 409},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f.err = tc.err
			w := call(s, tc.method, "/api/v1/clusters/prod/elasticsearch/"+tc.path, tc.body, token)
			var failure api.Error
			if err := json.Unmarshal(w.Body.Bytes(), &failure); err != nil {
				t.Fatal(err)
			}
			if w.Code != tc.status || failure.Details == nil || !strings.Contains(failure.Message, tc.message) {
				t.Fatal(w.Code, w.Body.String())
			}
			details := *failure.Details
			if details["reason"] != tc.reason || (tc.field != "" && details["field"] != tc.field) ||
				(tc.resource != "" && (details["resource"] != tc.resource || details["name"] != tc.resourceName)) {
				t.Fatal(details)
			}
		})
	}
}

func TestElasticAPIKeyInvalidationSurvivesRequestCancellation(t *testing.T) {
	s, f, token := elasticTestServer(t)
	type contextKey struct{}
	parent, cancel := context.WithCancel(context.WithValue(t.Context(), contextKey{}, "retained"))
	defer cancel()
	f.invalidateContext = func(ctx context.Context) error {
		cancel()
		if ctx.Err() != nil || ctx.Value(contextKey{}) != "retained" {
			t.Fatal("request cancellation propagated or context values lost", ctx.Err())
		}
		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) <= 0 || time.Until(deadline) > 30*time.Second {
			t.Fatal("invalidation must have a bounded deadline", deadline)
		}
		return nil
	}
	r := httptest.NewRequest("DELETE", "/api/v1/clusters/prod/elasticsearch/api-keys/key", nil).WithContext(parent)
	r.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	if w.Code != 204 || f.invalidated != "key" || parent.Err() == nil {
		t.Fatal(w.Code, w.Body.String(), f.invalidated, parent.Err())
	}
}

func TestElasticAPIKeyInvalidationRepeatedRequests(t *testing.T) {
	s, f, token := elasticTestServer(t)
	for range 2 {
		w := call(s, "DELETE", "/api/v1/clusters/prod/elasticsearch/api-keys/key", "", token)
		if w.Code != 204 || f.invalidated != "key" {
			t.Fatal(w.Code, w.Body.String(), f.invalidated)
		}
	}
}
