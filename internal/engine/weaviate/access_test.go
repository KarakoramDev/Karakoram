// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package weaviate

import (
	"encoding/json"
	"encoding/pem"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/weaviate/wvapi"
)

func accessSetup(t *testing.T, handler http.HandlerFunc) *coreFixture {
	t.Helper()
	f := coreSetup(t, 1)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer root-test-key" {
			t.Error("native access did not use root credential")
		}
		w.Header().Set("Content-Type", "application/json")
		handler(w, r)
	}))
	t.Cleanup(server.Close)
	for name, value := range map[string][]byte{secretCA: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), secretAdmin: []byte("root-test-key")} {
		if err := f.rc.Secrets.SetClusterSecret(t.Context(), f.rc.Cluster.Ref(), name, value); err != nil {
			t.Fatal(err)
		}
	}
	s := &state{Phase: "ready", Members: []*member{{Name: "127.0.0.1", Stage: "ready", TLSMode: "required"}}}
	if err := s.save(f.rc.Cluster); err != nil {
		t.Fatal(err)
	}
	f.rc.Dialer = coreDialer{address: server.Listener.Addr().String()}
	return f
}
func accessRespond(t *testing.T, w http.ResponseWriter, v any) {
	t.Helper()
	if err := json.NewEncoder(w).Encode(v); err != nil {
		t.Error(err)
	}
}
func TestAccessUserRoleReplacement(t *testing.T) {
	var mutations []string
	f := accessSetup(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/users/db/alice":
			accessRespond(t, w, wvapi.DBUser{UserID: "alice", DBUserType: "db_user", Active: true, Roles: []string{"stale"}})
		case "/v1/authz/users/alice/roles/db":
			accessRespond(t, w, []Role{{Name: "keep"}, {Name: "remove"}})
		case "/v1/authz/users/alice/revoke", "/v1/authz/users/alice/assign":
			var body struct {
				Roles    []string `json:"roles"`
				UserType string   `json:"userType"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Error(err)
			}
			if body.UserType != "db" {
				t.Error("wrong identity type")
			}
			mutations = append(mutations, r.URL.Path+":"+strings.Join(body.Roles, ","))
		case "/v1/users/db/alice/deactivate":
			mutations = append(mutations, "deactivate")
		default:
			t.Errorf("unexpected request %s", r.URL.Path)
			w.WriteHeader(500)
		}
	})
	active := false
	if err := f.p.UpdateUser(t.Context(), f.rc, "alice", UserUpdate{Roles: []string{"keep", "add", "add"}, Active: &active}); err != nil {
		t.Fatal(err)
	}
	want := []string{"/v1/authz/users/alice/revoke:remove", "/v1/authz/users/alice/assign:add", "deactivate"}
	if !reflect.DeepEqual(mutations, want) {
		t.Fatalf("mutations: %v", mutations)
	}
}
func TestAccessCreateRollbackAndSecretSanitization(t *testing.T) {
	deleted := false
	f := accessSetup(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == "POST" && r.URL.Path == "/v1/users/db/alice":
			accessRespond(t, w, wvapi.APIKey{APIKey: "new-secret-key"})
		case strings.HasSuffix(r.URL.Path, "/assign"):
			w.WriteHeader(422)
			accessRespond(t, w, map[string]string{"error": "new-secret-key root-test-key"})
		case r.Method == "DELETE" && r.URL.Path == "/v1/users/db/alice":
			deleted = true
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
		}
	})
	created, err := f.p.CreateUser(t.Context(), f.rc, UserCreate{Name: "alice", Roles: []string{"missing"}})
	if created != nil || !deleted || !errors.Is(err, engine.ErrInvalid) {
		t.Fatalf("rollback: created=%v deleted=%v err=%v", created, deleted, err)
	}
	if strings.Contains(err.Error(), "secret-key") {
		t.Fatal("native error exposed secret")
	}
}
func TestAccessRolePermissionReplacement(t *testing.T) {
	keep := Permission{Action: "read_data", Data: &wvapi.PermissionData{Collection: "Keep"}, Extra: map[string]json.RawMessage{"future": json.RawMessage(`{"scope":"preserved"}`)}}
	old := Permission{Action: "read_data", Data: &wvapi.PermissionData{Collection: "Old"}}
	added := Permission{Action: "read_data", Data: &wvapi.PermissionData{Collection: "New"}}
	var removed, assigned []Permission
	f := accessSetup(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			accessRespond(t, w, Role{Name: "custom", Permissions: []Permission{keep, old}})
			return
		}
		var body struct {
			Permissions []Permission `json:"permissions"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		switch r.URL.Path {
		case "/v1/authz/roles/custom/remove-permissions":
			removed = body.Permissions
		case "/v1/authz/roles/custom/add-permissions":
			assigned = body.Permissions
		default:
			t.Error("unexpected permission mutation")
		}
	})
	if err := f.p.ReplaceRole(t.Context(), f.rc, Role{Name: "custom", Permissions: []Permission{keep, added}}); err != nil {
		t.Fatal(err)
	}
	if len(removed) != 1 || len(assigned) != 1 || accessPermissionKey(removed[0]) != accessPermissionKey(old) || accessPermissionKey(assigned[0]) != accessPermissionKey(added) {
		t.Fatalf("incorrect diff: remove=%v add=%v", removed, assigned)
	}
}
func TestAccessProtectedObjectsAndValidation(t *testing.T) {
	p := New(Options{})
	for _, name := range []string{AdminUser, ClusterUser} {
		if err := p.DeleteUser(t.Context(), nil, name); !errors.Is(err, engine.ErrConflict) {
			t.Fatal(name, err)
		}
		if _, err := p.RotateUserKey(t.Context(), nil, name); !errors.Is(err, engine.ErrConflict) {
			t.Fatal(name, err)
		}
		if _, err := p.CreateUser(t.Context(), nil, UserCreate{Name: name}); !errors.Is(err, engine.ErrConflict) {
			t.Fatal(name, err)
		}
	}
	for _, name := range []string{"root", "admin", "viewer", "read-only", "operator_metadata_reader"} {
		if err := p.DeleteRole(t.Context(), nil, name); !errors.Is(err, engine.ErrConflict) {
			t.Fatal(name, err)
		}
		if err := p.ReplaceRole(t.Context(), nil, Role{Name: name}); !errors.Is(err, engine.ErrConflict) {
			t.Fatal(name, err)
		}
	}
	for _, name := range []string{"", "1bad", "bad/name", "bad:user", "é", strings.Repeat("a", 129)} {
		if err := accessUserName(name); !errors.Is(err, engine.ErrInvalid) {
			t.Fatal(name, err)
		}
	}
	for _, name := range []string{"", "1bad", "bad.name", "bad@role", strings.Repeat("a", 65)} {
		if err := accessRoleName(name); !errors.Is(err, engine.ErrInvalid) {
			t.Fatal(name, err)
		}
	}
	calls := 0
	f := accessSetup(t, func(w http.ResponseWriter, r *http.Request) {
		calls++
		accessRespond(t, w, wvapi.DBUser{UserID: "environment", DBUserType: "db_env_user"})
	})
	if err := f.p.DeleteUser(t.Context(), f.rc, "environment"); !errors.Is(err, engine.ErrConflict) || calls != 1 {
		t.Fatal("environment identity mutated", calls, err)
	}
}
func TestAccessErrorMapping(t *testing.T) {
	for _, tc := range []struct {
		status int
		want   error
	}{{404, engine.ErrNotFound}, {409, engine.ErrConflict}, {400, engine.ErrInvalid}, {422, engine.ErrInvalid}, {401, engine.ErrUnavailable}, {403, engine.ErrUnavailable}, {500, engine.ErrUnavailable}} {
		f := accessSetup(t, func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(tc.status)
			accessRespond(t, w, map[string]string{"error": "private-secret"})
		})
		_, err := f.p.GetUser(t.Context(), f.rc, "alice")
		if !errors.Is(err, tc.want) || strings.Contains(err.Error(), "private-secret") {
			t.Fatalf("status %d: %v", tc.status, err)
		}
	}
}
func TestAccessUserViewsAndFreshCredentials(t *testing.T) {
	f := accessSetup(t, func(w http.ResponseWriter, r *http.Request) {
		accessRespond(t, w, []wvapi.DBUser{{UserID: "alice", DBUserType: "db_user", Active: true, CreatedAt: ptrAccess("2026-10-21T12:00:00Z"), APIKeyFirstLetters: ptrAccess("secret-prefix")}, {UserID: "environment", DBUserType: "db_env_user"}})
	})
	users, err := f.p.ListUsers(t.Context(), f.rc)
	if err != nil || len(users) != 1 || users[0].CreatedAt == nil {
		t.Fatal(users, err)
	}
	raw, _ := json.Marshal(users)
	if strings.Contains(string(raw), "secret-prefix") || strings.Contains(string(raw), "apiKey") {
		t.Fatal("user view exposed key material")
	}
	if err := f.rc.Secrets.SetClusterSecret(t.Context(), f.rc.Cluster.Ref(), secretCA, []byte("invalid new CA")); err != nil {
		t.Fatal(err)
	}
	if _, err = f.p.ListUsers(t.Context(), f.rc); !errors.Is(err, engine.ErrUnavailable) {
		t.Fatal("native client reused stale CA", err)
	}
	s, _ := load(f.rc.Cluster)
	s.Suspended = true
	if err = s.save(f.rc.Cluster); err != nil {
		t.Fatal(err)
	}
	if _, err = f.p.ListRoles(t.Context(), f.rc); !errors.Is(err, engine.ErrUnavailable) {
		t.Fatal("suspended access", err)
	}
}
func ptrAccess(s string) *string { return &s }
