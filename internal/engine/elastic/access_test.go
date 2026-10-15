// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/elastic/esapi"
)

type accessSecrets struct {
	engine.SecretStore
	ca []byte
}

func (s accessSecrets) GetClusterSecret(_ context.Context, _ domain.ClusterRef, key string) ([]byte, error) {
	switch key {
	case "tls/ca.crt":
		return s.ca, nil
	case "users/karakoram":
		return []byte("admin-test-password"), nil
	}
	return nil, engine.ErrNotFound
}

type accessDialer struct{ address string }

func (d accessDialer) DialContext(ctx context.Context, network, _ string) (net.Conn, error) {
	return (&net.Dialer{}).DialContext(ctx, network, d.address)
}
func accessFixture(t *testing.T, h http.HandlerFunc) (*Provider, *engine.Context) {
	t.Helper()
	server := httptest.NewTLSServer(h)
	t.Cleanup(server.Close)
	c := &domain.Cluster{Name: "search", Spec: domain.ClusterSpec{Provider: "elasticsearch", Topology: domain.Topology{Type: "cluster"}}}
	if err := save(c, &state{Members: []*member{{Name: "example.com", Kind: kindNode, Stage: stageJoined}}}); err != nil {
		t.Fatal(err)
	}
	rc := &engine.Context{Cluster: c, Dialer: accessDialer{address: server.Listener.Addr().String()}, Secrets: accessSecrets{ca: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})}}
	return New(ProviderOptions{}), rc
}
func TestAccessUsersRolesAndManagedKeys(t *testing.T) {
	var mu sync.Mutex
	enabled := true
	users := map[string]esapi.User{"elastic": {Roles: []string{"superuser"}}, "karakoram_logstash": {}, "other_reserved": {Metadata: map[string]any{"_reserved": true}}, "existing": {Roles: []string{"viewer"}, Enabled: &enabled}}
	roles := map[string]esapi.Role{"superuser": {Metadata: map[string]any{"_reserved": true}}, "karakoram_monitor": {}, "custom": {Cluster: []string{"monitor"}}, "dynamic_reserved": {Metadata: map[string]any{"_reserved": true}}, "viewer": {Metadata: map[string]any{"_reserved": true}}}
	keys := []esapi.APIKeyInfo{{ID: "managed", Name: "app", Username: AdminUser, Metadata: map[string]any{"karakoram": true}}, {ID: "unmanaged", Username: AdminUser}, {ID: "other-owner", Username: "other", Metadata: map[string]any{"karakoram": true}}}
	calls := 0
	invalidated := ""
	var keyRequest esapi.APIKeyRequest
	p, rc := accessFixture(t, func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		calls++
		user, password, ok := r.BasicAuth()
		if !ok || user != AdminUser || password != "admin-test-password" {
			t.Error("admin authentication missing")
		}
		w.Header().Set("Content-Type", "application/json")
		write := func(v any) {
			if err := json.NewEncoder(w).Encode(v); err != nil {
				t.Error(err)
			}
		}
		for _, prefix := range []string{"/_security/user", "/_security/role"} {
			if r.URL.Path != prefix && !strings.HasPrefix(r.URL.Path, prefix+"/") {
				continue
			}
			name := strings.TrimPrefix(strings.TrimPrefix(r.URL.Path, prefix), "/")
			switch r.Method {
			case "GET":
				if prefix == "/_security/user" {
					if name == "" {
						write(users)
					} else if u, ok := users[name]; ok {
						write(map[string]esapi.User{name: u})
					} else {
						http.Error(w, `{"error":{"type":"not_found"}}`, 404)
					}
				} else {
					if name == "" {
						write(roles)
					} else if role, ok := roles[name]; ok {
						write(map[string]esapi.Role{name: role})
					} else {
						http.Error(w, `{"error":{"type":"not_found"}}`, 404)
					}
				}
			case "PUT":
				if prefix == "/_security/user" {
					var u esapi.User
					if err := json.NewDecoder(r.Body).Decode(&u); err != nil {
						t.Error(err)
					}
					users[name] = u
				} else {
					var role esapi.Role
					if err := json.NewDecoder(r.Body).Decode(&role); err != nil {
						t.Error(err)
					}
					roles[name] = role
				}
				write(map[string]bool{"created": true})
			case "DELETE":
				delete(users, name)
				delete(roles, name)
				write(map[string]bool{"found": true})
			default:
				t.Error(r.Method)
			}
			return
		}
		if r.URL.Path == "/_security/api_key" {
			switch r.Method {
			case "GET":
				if r.URL.Query().Get("owner") != "true" {
					t.Error("owner scope missing")
				}
				write(map[string]any{"api_keys": keys})
			case "POST":
				if err := json.NewDecoder(r.Body).Decode(&keyRequest); err != nil {
					t.Error(err)
				}
				write(esapi.APIKey{ID: "new", Name: keyRequest.Name, APIKey: "key-secret", Encoded: "encoded-secret", Expiration: 999})
			case "DELETE":
				var body struct {
					IDs []string `json:"ids"`
				}
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Error(err)
				}
				if len(body.IDs) != 1 {
					t.Error(body)
				} else {
					invalidated = body.IDs[0]
				}
				write(map[string]int{"error_count": 0})
			}
			return
		}
		t.Error(r.Method, r.URL.String())
		http.NotFound(w, r)
	})
	ctx := t.Context()
	list, err := p.ListUsers(ctx, rc)
	if err != nil || len(list) != 1 || list[0].Username != "existing" || !list[0].Enabled {
		t.Fatal(list, err)
	}
	if _, err := p.GetUser(ctx, rc, "elastic"); !errors.Is(err, engine.ErrInvalid) {
		t.Fatal(err)
	}
	u := UserWrite{User: User{Username: "analyst", Roles: []string{"viewer"}, FullName: "Analyst", Email: "analyst@example.com", Enabled: true}, Password: "test-password"}
	if err := p.CreateUser(ctx, rc, u); err != nil {
		t.Fatal(err)
	}
	if err := p.CreateUser(ctx, rc, u); !errors.Is(err, engine.ErrConflict) {
		t.Fatal(err)
	}
	got, err := p.GetUser(ctx, rc, u.Username)
	if err != nil || got.FullName != u.FullName || got.Email != u.Email {
		t.Fatal(got, err)
	}
	u.Password = ""
	u.Enabled = false
	if err := p.UpdateUser(ctx, rc, u); err != nil {
		t.Fatal(err)
	}
	got, err = p.GetUser(ctx, rc, u.Username)
	if err != nil || got.Enabled {
		t.Fatal(got, err)
	}
	if err := p.DeleteUser(ctx, rc, u.Username); err != nil {
		t.Fatal(err)
	}
	if err := p.UpdateUser(ctx, rc, u); !errors.Is(err, engine.ErrNotFound) {
		t.Fatal(err)
	}
	if err := p.DeleteUser(ctx, rc, u.Username); !errors.Is(err, engine.ErrNotFound) {
		t.Fatal(err)
	}
	roleList, err := p.ListRoles(ctx, rc)
	if err != nil || len(roleList) != 4 {
		t.Fatal(roleList, err)
	}
	builtin, err := p.GetRole(ctx, rc, "superuser")
	if err != nil || !builtin.Reserved {
		t.Fatal(builtin, err)
	}
	role := AccessRole{Name: "agent", Role: Role{Cluster: []string{}, Indices: []IndexPrivileges{{Names: []string{"logs-*"}, Privileges: []string{"read"}, AllowRestrictedIndices: true}}, Applications: []ApplicationPrivileges{{Application: "apm", Privileges: []string{"event:write", "config_agent:read"}, Resources: []string{"*"}}}}}
	if err := p.PutRole(ctx, rc, role, true); err != nil {
		t.Fatal(err)
	}
	if err := p.PutRole(ctx, rc, role, true); !errors.Is(err, engine.ErrConflict) {
		t.Fatal(err)
	}
	loaded, err := p.GetRole(ctx, rc, "agent")
	if err != nil || len(loaded.Applications) != 1 || loaded.Applications[0].Privileges[1] != "config_agent:read" || !loaded.Indices[0].AllowRestrictedIndices {
		t.Fatal(loaded, err)
	}
	if err := p.PutRole(ctx, rc, role, false); err != nil {
		t.Fatal(err)
	}
	if err := p.DeleteRole(ctx, rc, "agent"); err != nil {
		t.Fatal(err)
	}
	if err := p.PutRole(ctx, rc, role, false); !errors.Is(err, engine.ErrNotFound) {
		t.Fatal(err)
	}
	if err := p.DeleteRole(ctx, rc, "dynamic_reserved"); !errors.Is(err, engine.ErrInvalid) {
		t.Fatal(err)
	}
	inventory, err := p.ListAPIKeys(ctx, rc)
	if err != nil || len(inventory) != 1 || inventory[0].ID != "managed" {
		t.Fatal(inventory, err)
	}
	key, err := p.CreateAPIKey(ctx, rc, APIKeyCreate{Name: "agent", Expiration: "1d", RoleDescriptors: map[string]RoleDescriptor{"apm": role.Role}})
	if err != nil || key.APIKey != "key-secret" || key.Expiration != 999 {
		t.Fatal(key, err)
	}
	mu.Lock()
	if keyRequest.Metadata["karakoram"] != true || keyRequest.RoleDescriptors["apm"].Applications[0].Application != "apm" {
		t.Error(keyRequest)
	}
	mu.Unlock()
	if err := p.InvalidateAPIKey(ctx, rc, "unmanaged"); !errors.Is(err, engine.ErrNotFound) {
		t.Fatal(err)
	}
	if err := p.InvalidateAPIKey(ctx, rc, "other-owner"); !errors.Is(err, engine.ErrNotFound) {
		t.Fatal(err)
	}
	if err := p.InvalidateAPIKey(ctx, rc, "managed"); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	if invalidated != "managed" {
		t.Error(invalidated)
	}
	before := calls
	mu.Unlock()
	for _, name := range []string{"elastic", "kibana_system", "karakoram_custom"} {
		reserved := UserWrite{User: User{Username: name}, Password: "test-password"}
		for _, err := range []error{p.CreateUser(ctx, rc, reserved), p.UpdateUser(ctx, rc, reserved), p.DeleteUser(ctx, rc, name)} {
			if !errors.Is(err, engine.ErrInvalid) {
				t.Fatal(name, err)
			}
		}
	}
	for _, name := range []string{"superuser", "karakoram_custom"} {
		for _, err := range []error{p.PutRole(ctx, rc, AccessRole{Name: name}, true), p.PutRole(ctx, rc, AccessRole{Name: name}, false), p.DeleteRole(ctx, rc, name)} {
			if !errors.Is(err, engine.ErrInvalid) {
				t.Fatal(name, err)
			}
		}
	}
	mu.Lock()
	if calls != before {
		t.Error("protected identity reached ES")
	}
	mu.Unlock()
}
func TestAccessErrorClassificationAndPasswordValidation(t *testing.T) {
	for _, tc := range []struct {
		status int
		want   error
	}{{400, engine.ErrInvalid}, {422, engine.ErrInvalid}, {404, engine.ErrNotFound}, {409, engine.ErrConflict}} {
		if got := accessError(&esapi.Error{Status: tc.status, Reason: "request-secret"}); !errors.Is(got, tc.want) || strings.Contains(got.Error(), "request-secret") {
			t.Fatal(got)
		}
	}
	for _, u := range []UserWrite{{User: User{Username: "bad/name"}, Password: "test-password"}, {User: User{Username: "app"}, Password: "short"}, {User: User{Username: "app", Roles: []string{"bad/role"}}, Password: "test-password"}} {
		if err := validateUser(u, true); !errors.Is(err, engine.ErrInvalid) {
			t.Fatal(u, err)
		}
	}
	if err := validateUser(UserWrite{User: User{Username: "app", Roles: []string{"viewer"}}}, false); err != nil {
		t.Fatal(err)
	}
}

func TestUserRoleReferencesCheckedBeforeWrite(t *testing.T) {
	for _, create := range []bool{true, false} {
		for _, role := range []string{"missing", "viewer", "custom"} {
			t.Run(fmt.Sprintf("%t/%s", create, role), func(t *testing.T) {
				writes := 0
				p, rc := accessFixture(t, func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("Content-Type", "application/json")
					switch {
					case r.Method == "PUT":
						writes++
						fmt.Fprint(w, `{"created":true}`)
					case r.URL.Path == "/_security/role":
						fmt.Fprint(w, `{"viewer":{"metadata":{"_reserved":true}},"custom":{}}`)
					case create:
						http.Error(w, `{"error":{"type":"resource_not_found_exception","reason":"not found"}}`, 404)
					default:
						fmt.Fprint(w, `{"app":{"roles":["viewer"]}}`)
					}
				})
				u := UserWrite{User: User{Username: "app", Roles: []string{role}}, Password: "long-password"}
				err := p.putUser(t.Context(), rc, u, create)
				if role == "missing" {
					var field *engine.ValidationError
					if !errors.As(err, &field) || field.Field != "roles[0]" || !strings.Contains(field.Message, `"missing"`) || writes != 0 {
						t.Fatal(err, writes)
					}
				} else if err != nil || writes != 1 {
					t.Fatal(err, writes)
				}
			})
		}
	}
}

func TestNativeRoleWriteErrors(t *testing.T) {
	query := `{"term":{"tenant":"one"}}`
	grant := []string{"public.*"}
	for _, tc := range []struct {
		name, body, field, reason string
		status                    int
		index                     IndexPrivileges
	}{
		{"document licence", `{"error":{"root_cause":[{"type":"security_exception","reason":"current license is non-compliant for [field and document level security]"}],"type":"security_exception","reason":"current license is non-compliant for [field and document level security]","license.expired.feature":"field and document level security"},"status":403}`, "indices[0].query", domain.ReasonUnsupported, 403, IndexPrivileges{Query: &query}},
		{"field licence", `{"error":{"type":"security_exception","reason":"current license is non-compliant for [field and document level security]"},"status":403}`, "indices[0].fieldSecurity", domain.ReasonUnsupported, 403, IndexPrivileges{FieldSecurity: &FieldSecurity{Grant: &grant}}},
		{"cluster privilege", `{"error":{"type":"action_request_validation_exception","reason":"Validation Failed: 1: unknown cluster privilege [not_a_privilege];"},"status":400}`, "cluster", domain.ReasonInvalid, 400, IndexPrivileges{}},
		{"index privilege", `{"error":{"type":"illegal_argument_exception","reason":"unknown index privilege [not_a_privilege]. a privilege must be either one of the predefined fixed indices privileges [all,auto_configure,create,create_doc,create_index,cross_cluster_replication,cross_cluster_replication_internal,delete,delete_index,index,maintenance,manage,manage_data_stream_lifecycle,manage_failure_store,manage_follow_index,manage_ilm,manage_leader_index,monitor,none,read,read_cross_cluster,read_failure_store,view_index_metadata,write] or a pattern over one of the available index actions"},"status":400}`, "indices[0].privileges", domain.ReasonInvalid, 400, IndexPrivileges{Privileges: []string{"not_a_privilege"}}},
		{"application privilege", `{"error":{"type":"action_request_validation_exception","reason":"Validation Failed: 1: Application privilege names must match the pattern ^[a-zA-Z0-9][a-zA-Z0-9_.-]*$ (found [cluster:invalid]);"},"status":400}`, "applications", domain.ReasonInvalid, 400, IndexPrivileges{}},
		{"ordinary forbidden", `{"error":{"type":"security_exception","reason":"action [cluster:admin/xpack/security/role/put] is unauthorized for user [operator]"},"status":403}`, "", "", 403, IndexPrivileges{Query: &query}},
		{"unrelated licence document", `{"error":{"type":"security_exception","reason":"unauthorized to read document in [license] index"},"status":403}`, "", "", 403, IndexPrivileges{Query: &query}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, rc := accessFixture(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if r.Method == "GET" {
					fmt.Fprint(w, `{}`)
					return
				}
				w.WriteHeader(tc.status)
				fmt.Fprint(w, tc.body)
			})
			err := p.PutRole(t.Context(), rc, AccessRole{Name: "custom", Role: Role{Cluster: []string{"not_a_privilege"}, Indices: []IndexPrivileges{tc.index}}}, true)
			if tc.field == "" {
				var native *esapi.Error
				if !errors.As(err, &native) || native.Status != 403 {
					t.Fatal(err)
				}
				return
			}
			var field *engine.ValidationError
			if !errors.As(err, &field) || field.Field != tc.field || field.Reason != tc.reason || !strings.Contains(field.Message, tc.field) {
				t.Fatal(err)
			}
		})
	}
}

func TestAPIKeyLicenceErrorIdentifiesRoleDescriptor(t *testing.T) {
	query := `{"term":{"tenant":"one"}}`
	grant := []string{"public.*"}
	for _, tc := range []struct {
		name  string
		index IndexPrivileges
	}{
		{"query", IndexPrivileges{Query: &query}},
		{"fieldSecurity", IndexPrivileges{FieldSecurity: &FieldSecurity{Grant: &grant}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, rc := accessFixture(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusForbidden)
				fmt.Fprint(w, `{"error":{"type":"security_exception","reason":"current license is non-compliant for [field and document level security]"},"status":403}`)
			})
			_, err := p.CreateAPIKey(t.Context(), rc, APIKeyCreate{Name: "restricted", RoleDescriptors: map[string]RoleDescriptor{
				"reader": {Indices: []IndexPrivileges{{Names: []string{"public-*"}, Privileges: []string{"read"}}, tc.index}},
			}})
			var field *engine.ValidationError
			want := "roleDescriptors.reader.indices[1]." + tc.name
			if !errors.As(err, &field) || field.Field != want || field.Reason != domain.ReasonUnsupported || !strings.Contains(field.Message, want) {
				t.Fatal(err)
			}
		})
	}
}

func TestRepeatedAPIKeyInvalidation(t *testing.T) {
	invalidated, writes := false, 0
	p, rc := accessFixture(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == "GET" {
			_ = json.NewEncoder(w).Encode(map[string]any{"api_keys": []esapi.APIKeyInfo{{ID: "key", Username: AdminUser, Invalidated: invalidated, Metadata: map[string]any{"karakoram": true}}}})
			return
		}
		writes++
		invalidated = true
		fmt.Fprint(w, `{"invalidated_api_keys":["key"],"previously_invalidated_api_keys":[],"error_count":0}`)
	})
	for range 2 {
		if err := p.InvalidateAPIKey(t.Context(), rc, "key"); err != nil {
			t.Fatal(err)
		}
	}
	if writes != 1 {
		t.Fatalf("native invalidation called %d times", writes)
	}
}

func TestAccessLicenceClassification(t *testing.T) {
	native := &esapi.Error{Status: 403, Type: "security_exception", Reason: "current license is non-compliant for [field and document level security]"}
	var field *engine.ValidationError
	if err := accessError(native); !errors.As(err, &field) || field.Field != "query" || field.Reason != domain.ReasonUnsupported {
		t.Fatal(err)
	}
	for _, native := range []*esapi.Error{
		{Status: 403, Type: "security_exception", Reason: "action is unauthorized"},
		{Status: 403, Type: "security_exception", Reason: "unauthorized to read document in [license] index"},
		{Status: 403, Type: "other_exception", Reason: "current license is non-compliant for [field and document level security]"},
	} {
		if accessError(native) != native {
			t.Fatal("non-licence rejection changed", native)
		}
	}
}
