// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package server

import (
	"context"
	"encoding/json"
	"github.com/KarakoramDev/Karakoram/internal/api"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/weaviate"
	"reflect"
	"strings"
	"testing"
)

type fakeWeaviateAccess struct {
	fakeProvider
	last  string
	roles []string
}

var _ weaviate.Access = (*fakeWeaviateAccess)(nil)

func (f *fakeWeaviateAccess) ListUsers(ctx context.Context, rc *engine.Context) ([]weaviate.User, error) {
	f.last = "ListUsers"
	return []weaviate.User{{Name: "app", UserType: "db_user", Active: true}}, nil
}
func (f *fakeWeaviateAccess) GetUser(ctx context.Context, rc *engine.Context, name string) (*weaviate.User, error) {
	f.last = "GetUser"
	return &weaviate.User{Name: name, UserType: "db_user", Active: true, Roles: f.roles}, nil
}
func (f *fakeWeaviateAccess) CreateUser(ctx context.Context, rc *engine.Context, v weaviate.UserCreate) (*weaviate.UserCreated, error) {
	f.last = "CreateUser"
	return &weaviate.UserCreated{User: weaviate.User{Name: v.Name, Roles: v.Roles, Active: true, UserType: "db_user"}, APIKey: "one-time"}, nil
}
func (f *fakeWeaviateAccess) UpdateUser(ctx context.Context, rc *engine.Context, name string, v weaviate.UserUpdate) error {
	f.last = "UpdateUser"
	return nil
}
func (f *fakeWeaviateAccess) DeleteUser(ctx context.Context, rc *engine.Context, name string) error {
	f.last = "DeleteUser"
	return nil
}
func (f *fakeWeaviateAccess) RotateUserKey(ctx context.Context, rc *engine.Context, name string) (string, error) {
	f.last = "RotateUserKey"
	return "one-time", nil
}
func (f *fakeWeaviateAccess) ListRoles(ctx context.Context, rc *engine.Context) ([]weaviate.Role, error) {
	f.last = "ListRoles"
	return []weaviate.Role{{Name: "custom", Permissions: []weaviate.Permission{{Action: "read_data"}}}}, nil
}
func (f *fakeWeaviateAccess) GetRole(ctx context.Context, rc *engine.Context, name string) (*weaviate.Role, error) {
	f.last = "GetRole"
	return &weaviate.Role{Name: name}, nil
}
func (f *fakeWeaviateAccess) CreateRole(ctx context.Context, rc *engine.Context, v weaviate.Role) error {
	f.last = "CreateRole"
	return nil
}
func (f *fakeWeaviateAccess) ReplaceRole(ctx context.Context, rc *engine.Context, v weaviate.Role) error {
	f.last = "ReplaceRole"
	return nil
}
func (f *fakeWeaviateAccess) DeleteRole(ctx context.Context, rc *engine.Context, name string) error {
	f.last = "DeleteRole"
	return nil
}
func TestWeaviateRoutes(t *testing.T) {
	s, _, token := testServer(t)
	f := &fakeWeaviateAccess{}
	var err error
	s.Registry, err = engine.NewRegistry(f)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		method, path, body, want string
		status                   int
	}{
		{"GET", "users", "", "ListUsers", 200},
		{"POST", "users", `{"name":"app","roles":["viewer"]}`, "CreateUser", 201},
		{"GET", "users/app", "", "GetUser", 200},
		{"PUT", "users/app", `{"roles":["viewer"],"active":false}`, "UpdateUser", 204},
		{"DELETE", "users/app", "", "DeleteUser", 204},
		{"POST", "users/app/rotate-key", "", "RotateUserKey", 200},
		{"GET", "roles", "", "ListRoles", 200},
		{"POST", "roles", `{"name":"app","permissions":[]}`, "CreateRole", 201},
		{"GET", "roles/app", "", "GetRole", 200},
		{"PUT", "roles/app", `{"name":"app","permissions":[]}`, "ReplaceRole", 200},
		{"DELETE", "roles/app", "", "DeleteRole", 204},
	} {
		t.Run(tc.want, func(t *testing.T) {
			f.last = ""
			w := call(s, tc.method, "/api/v1/clusters/prod/weaviate/"+tc.path, tc.body, token)
			if w.Code != tc.status || f.last != tc.want {
				t.Fatalf("%d %s dispatched %s", w.Code, w.Body.String(), f.last)
			}
			if tc.want == "CreateUser" || tc.want == "RotateUserKey" {
				if w.Header().Get("Cache-Control") != "no-store" || strings.Count(w.Body.String(), "one-time") != 1 {
					t.Fatal(w.Header(), w.Body.String())
				}
			}
		})
	}
}
func TestWeaviateWrongEngine(t *testing.T) {
	s, _, token := testServer(t)
	w := call(s, "GET", "/api/v1/clusters/prod/weaviate/users", "", token)
	if w.Code != 409 {
		t.Fatal(w.Code, w.Body.String())
	}
}
func TestWeaviatePermissionRoundTrip(t *testing.T) {
	original := `{"name":"custom","permissions":[{"action":"read_data","future":{"enabled":true},"data":{"collection":"Books","tenant":"*","object":"*","nativeFuture":17},"namespaces":{"namespace":"*","future":"value"}}]}`
	var body api.WeaviateRole
	if err := json.Unmarshal([]byte(original), &body); err != nil {
		t.Fatal(err)
	}
	native, err := weaviateRole(body)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(native)
	if err != nil {
		t.Fatal(err)
	}
	var want, got any
	json.Unmarshal([]byte(original), &want)
	json.Unmarshal(encoded, &got)
	if !reflect.DeepEqual(want, got) {
		t.Fatalf("%s", encoded)
	}
}
func TestWeaviateExtraGrant(t *testing.T) {
	s, _, _ := testServer(t)
	ctx := t.Context()
	f := &fakeWeaviateAccess{}
	s.Registry, _ = engine.NewRegistry(f)
	if _, err := s.Auth.CreateAccount(ctx, domain.Account{Name: "wv-manager"}, "integration-account-password"); err != nil {
		t.Fatal(err)
	}
	policy := s.RBAC.Policy() + "\np, user:wv-manager, database-users, create, default/prod, allow\np, user:wv-manager, database-users, update, default/prod, allow\n"
	if err := s.RBAC.ReplacePolicy(ctx, policy); err != nil {
		t.Fatal(err)
	}
	token, _, err := s.Auth.CreateToken(ctx, "wv-manager", "test", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		method, path, body string
		status             int
	}{
		{"POST", "users", `{"name":"app","roles":["viewer","read-only"]}`, 201},
		{"POST", "users", `{"name":"app","roles":["admin"]}`, 403},
		{"PUT", "users/app", `{"roles":["custom"]}`, 403},
		{"POST", "roles", `{"name":"app","permissions":[]}`, 403},
		{"PUT", "roles/app", `{"name":"app","permissions":[]}`, 403},
		{"DELETE", "roles/app", "", 403},
	} {
		f.last = ""
		w := call(s, tc.method, "/api/v1/clusters/prod/weaviate/"+tc.path, tc.body, token)
		if w.Code != tc.status {
			t.Fatal(tc, w.Code, w.Body.String())
		}
		if tc.status == 403 && f.last != "" {
			t.Fatal("unauthorized provider call", f.last)
		}
	}
	if err := s.RBAC.ReplacePolicy(ctx, policy+"p, user:wv-manager, clusters, update, default/prod, allow\n"); err != nil {
		t.Fatal(err)
	}
	w := call(s, "POST", "/api/v1/clusters/prod/weaviate/users", `{"name":"app","roles":["custom"]}`, token)
	if w.Code != 201 {
		t.Fatal(w.Code, w.Body.String())
	}
}

func TestWeaviateRotateKeyPrivileges(t *testing.T) {
	s, _, _ := testServer(t)
	ctx := t.Context()
	f := &fakeWeaviateAccess{}
	s.Registry, _ = engine.NewRegistry(f)
	if _, err := s.Auth.CreateAccount(ctx, domain.Account{Name: "wv-rotator"}, "integration-account-password"); err != nil {
		t.Fatal(err)
	}
	policy := s.RBAC.Policy() + "\np, user:wv-rotator, database-users, update, default/prod, allow\np, user:wv-rotator, database-users, delete, default/prod, allow\n"
	if err := s.RBAC.ReplacePolicy(ctx, policy); err != nil {
		t.Fatal(err)
	}
	token, _, err := s.Auth.CreateToken(ctx, "wv-rotator", "test", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		roles  []string
		status int
	}{
		{"admin", []string{"admin"}, 403},
		{"custom", []string{"viewer", "custom"}, 403},
		{"viewer", []string{"viewer"}, 200},
		{"read-only", []string{"viewer", "read-only"}, 200},
		{"no roles", nil, 200},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f.roles, f.last = tc.roles, ""
			w := call(s, "POST", "/api/v1/clusters/prod/weaviate/users/app/rotate-key", "", token)
			if w.Code != tc.status {
				t.Fatal(w.Code, w.Body.String())
			}
			want := "RotateUserKey"
			if tc.status == 403 {
				want = "GetUser"
			}
			if f.last != want {
				t.Fatalf("last provider call = %s, want %s", f.last, want)
			}
			if tc.status == 200 && w.Header().Get("Cache-Control") != "no-store" {
				t.Fatal("rotated key is cacheable")
			}
		})
	}
	f.roles = []string{"admin"}
	w := call(s, "DELETE", "/api/v1/clusters/prod/weaviate/users/app", "", token)
	if w.Code != 204 || f.last != "DeleteUser" {
		t.Fatal("privileged deletion must match PostgreSQL precedent", w.Code, w.Body.String(), f.last)
	}
	if err := s.RBAC.ReplacePolicy(ctx, policy+"p, user:wv-rotator, clusters, update, default/prod, allow\n"); err != nil {
		t.Fatal(err)
	}
	w = call(s, "POST", "/api/v1/clusters/prod/weaviate/users/app/rotate-key", "", token)
	if w.Code != 200 || f.last != "RotateUserKey" {
		t.Fatal(w.Code, w.Body.String(), f.last)
	}
}

func TestWeaviateAdditionalRBACInventory(t *testing.T) {
	for _, id := range []string{"createWeaviateUser", "updateWeaviateUser", "rotateWeaviateUserKey", "createWeaviateRole", "updateWeaviateRole", "deleteWeaviateRole"} {
		rule, exists := api.OperationAdditionalRBAC[id]
		if !exists || rule.Resource != "clusters" || rule.Action != "update" || rule.ObjectParam != "cluster" || rule.Public {
			t.Fatalf("incorrect additional permission for %s: %+v", id, rule)
		}
	}
	if _, exists := api.OperationAdditionalRBAC["deleteWeaviateUser"]; exists {
		t.Fatal("user deletion must not require privileged credential access")
	}
}

func TestWeaviateIdempotency(t *testing.T) {
	for _, tc := range []struct {
		method, path, body string
		status             int
		secret             bool
	}{
		{"POST", "users", `{"name":"app","roles":[]}`, 201, true},
		{"PUT", "users/app", `{"roles":[]}`, 204, false},
		{"POST", "users/app/rotate-key", "", 200, true},
		{"POST", "roles", `{"name":"app","permissions":[]}`, 201, false},
		{"PUT", "roles/app", `{"name":"app","permissions":[]}`, 200, false},
	} {
		t.Run(tc.path+"/"+tc.method, func(t *testing.T) {
			s, token := namespaceServer(t)
			f := &fakeWeaviateAccess{}
			s.Registry, _ = engine.NewRegistry(f)
			path := "/api/v1/clusters/prod/weaviate/" + tc.path
			first := idempotentCall(s, tc.method, path, tc.body, token, "wv-access")
			f.last = ""
			replayed := idempotentCall(s, tc.method, path+"?namespace=default", tc.body, token, "wv-access")
			assertReplay(t, first, replayed, tc.status)
			if f.last != "" {
				t.Fatal("replay reached provider", f.last)
			}
			if tc.secret && replayed.Header().Get("Cache-Control") != "no-store" {
				t.Fatal("replayed secret is cacheable")
			}
		})
	}
}

func TestWeaviateAuditResources(t *testing.T) {
	s, _, token := testServer(t)
	s.Registry, _ = engine.NewRegistry(&fakeWeaviateAccess{})
	for _, tc := range []struct{ family, body string }{
		{"users", `{"name":"app","roles":["viewer"]}`},
		{"roles", `{"name":"app","permissions":[]}`},
	} {
		w := call(s, "POST", "/api/v1/clusters/prod/weaviate/"+tc.family, tc.body, token)
		if w.Code != 201 {
			t.Fatal(w.Code, w.Body.String())
		}
		events := call(s, "GET", "/api/v1/events?resource=weaviate-"+tc.family+"&cluster=prod", "", token)
		if events.Code != 200 {
			t.Fatal(events.Code, events.Body.String())
		}
		var list struct {
			Items []api.Event `json:"items"`
		}
		if err := json.Unmarshal(events.Body.Bytes(), &list); err != nil {
			t.Fatal(err)
		}
		if len(list.Items) == 0 || strings.Contains(events.Body.String(), "one-time") {
			t.Fatal("missing audit event or leaked API key", events.Body.String())
		}
	}
}
