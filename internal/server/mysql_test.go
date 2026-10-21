// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/api"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql"
	"github.com/KarakoramDev/Karakoram/internal/engine/pxc"
)

type fakeMySQLAccess struct {
	fakeProvider
	writes                     int
	lastUser                   mysql.UserWrite
	lastPatch                  mysql.UserPatch
	lastDatabase               mysql.Database
	retain                     bool
	force                      bool
	userFilter, databaseFilter string
	err                        error
}

var _ mysql.Access = (*fakeMySQLAccess)(nil)

func (p *fakeMySQLAccess) ListUsers(context.Context, *engine.Context) ([]mysql.User, error) {
	return []mysql.User{{Name: "app", Host: "%", MaxUserConnections: 0, RoutableThroughProxy: true}}, p.err
}
func (p *fakeMySQLAccess) GetUser(_ context.Context, _ *engine.Context, name string) (*mysql.User, error) {
	return &mysql.User{Name: name, Host: "%", MaxUserConnections: p.lastUser.MaxUserConnections, Locked: p.lastUser.Locked, PasswordRetained: p.retain, RoutableThroughProxy: true}, p.err
}
func (p *fakeMySQLAccess) CreateUser(_ context.Context, _ *engine.Context, user mysql.UserWrite) (string, error) {
	p.writes++
	p.lastUser = user
	if user.Password != nil {
		return "", p.err
	}
	return "generated-password-returned-once", p.err
}
func (p *fakeMySQLAccess) PatchUser(_ context.Context, _ *engine.Context, _ string, patch mysql.UserPatch) error {
	p.writes++
	p.lastPatch = patch
	if patch.Locked != nil {
		p.lastUser.Locked = *patch.Locked
	}
	if patch.MaxUserConnections != nil {
		p.lastUser.MaxUserConnections = *patch.MaxUserConnections
	}
	return p.err
}
func (p *fakeMySQLAccess) DeleteUser(context.Context, *engine.Context, string) error {
	p.writes++
	return p.err
}
func (p *fakeMySQLAccess) RotateUserPassword(_ context.Context, _ *engine.Context, _ string, retain bool) (string, error) {
	p.writes++
	p.retain = retain
	return "rotated-password-returned-once", p.err
}
func (p *fakeMySQLAccess) DiscardOldPassword(context.Context, *engine.Context, string) error {
	p.writes++
	p.retain = false
	return p.err
}
func (p *fakeMySQLAccess) ListDatabases(context.Context, *engine.Context) ([]mysql.Database, error) {
	return []mysql.Database{{Name: "app", CharacterSet: "utf8mb4", Collation: "utf8mb4_0900_ai_ci"}}, p.err
}
func (p *fakeMySQLAccess) GetDatabase(context.Context, *engine.Context, string) (*mysql.Database, error) {
	return &mysql.Database{Name: "app", CharacterSet: "utf8mb4", Collation: "utf8mb4_0900_ai_ci"}, p.err
}
func (p *fakeMySQLAccess) CreateDatabase(_ context.Context, _ *engine.Context, database mysql.Database) error {
	p.writes++
	p.lastDatabase = database
	return p.err
}
func (p *fakeMySQLAccess) DeleteDatabase(_ context.Context, _ *engine.Context, _ string, force bool) error {
	p.writes++
	p.force = force
	return p.err
}
func (p *fakeMySQLAccess) ListGrants(_ context.Context, _ *engine.Context, user, database string) ([]mysql.Grant, error) {
	p.userFilter, p.databaseFilter = user, database
	return []mysql.Grant{{User: "app", Database: "app", Privileges: []string{"SELECT"}}}, p.err
}
func (p *fakeMySQLAccess) Grant(context.Context, *engine.Context, mysql.Grant) error {
	p.writes++
	return p.err
}
func (p *fakeMySQLAccess) Revoke(context.Context, *engine.Context, mysql.Grant) error {
	p.writes++
	return p.err
}

func mysqlTestServer(t *testing.T) (*Server, string, *fakeMySQLAccess) {
	t.Helper()
	s, token := namespaceTestServer(t)
	p := &fakeMySQLAccess{}
	var err error
	s.Registry, err = engine.NewRegistry(p)
	if err != nil {
		t.Fatal(err)
	}
	return s, token, p
}

func TestMySQLSecretIdempotency(t *testing.T) {
	s, token, p := mysqlTestServer(t)
	for _, tc := range []struct {
		path, body string
		status     int
	}{
		{"/api/v1/clusters/prod/mysql/users", `{"name":"app"}`, http.StatusCreated},
		{"/api/v1/clusters/prod/mysql/users/app/password", `{"retainCurrent":true}`, http.StatusOK},
	} {
		before := p.writes
		first := idempotentCall(s, http.MethodPost, tc.path, tc.body, token, "mysql-secret-once")
		second := idempotentCall(s, http.MethodPost, tc.path, tc.body, token, "mysql-secret-once")
		assertReplay(t, first, second, tc.status)
		if p.writes != before+1 || second.Header().Get("Cache-Control") != "no-store" {
			t.Fatal("secret replay wrote again or is cacheable")
		}
		var out map[string]any
		if err := json.Unmarshal(first.Body.Bytes(), &out); err != nil || out["generatedPassword"] == nil || out["password"] != nil {
			t.Fatal("invalid one-time secret response", err)
		}
	}
	if !p.retain {
		t.Fatal("retainCurrent not passed to provider")
	}
	read := call(s, "GET", "/api/v1/clusters/prod/mysql/users/app", "", token)
	if read.Code != http.StatusOK || strings.Contains(read.Body.String(), "generatedPassword") || strings.Contains(read.Body.String(), "returned-once") {
		t.Fatal("read exposed secret", read.Body.String())
	}
	discard := call(s, "POST", "/api/v1/clusters/prod/mysql/users/app/password/discard-old", "", token)
	if discard.Code != http.StatusNoContent || p.retain {
		t.Fatal("retained password not discarded", discard.Code)
	}
}

func TestMySQLDefaultsPatchAndFilters(t *testing.T) {
	s, token, p := mysqlTestServer(t)
	w := call(s, "POST", "/api/v1/clusters/prod/mysql/databases", `{"name":"app"}`, token)
	if w.Code != http.StatusCreated || p.lastDatabase.CharacterSet != "utf8mb4" || p.lastDatabase.Collation != "utf8mb4_0900_ai_ci" {
		t.Fatal("database defaults missing", w.Code, w.Body.String())
	}
	w = call(s, "PATCH", "/api/v1/clusters/prod/mysql/users/app", `{"locked":true}`, token)
	if w.Code != http.StatusOK || p.lastPatch.Locked == nil || !*p.lastPatch.Locked || p.lastPatch.MaxUserConnections != nil {
		t.Fatal("patch lost field presence", w.Code, w.Body.String())
	}
	for _, filter := range []string{"user=app", "database=app", "user=app&database=app"} {
		w = call(s, "GET", "/api/v1/clusters/prod/mysql/grants?"+filter, "", token)
		if w.Code != http.StatusOK {
			t.Fatal("valid filter rejected", filter, w.Code, w.Body.String())
		}
	}
	w = call(s, "GET", "/api/v1/clusters/prod/mysql/grants", "", token)
	if w.Code != http.StatusBadRequest {
		t.Fatal("missing filters accepted", w.Code)
	}
	w = call(s, "DELETE", "/api/v1/clusters/prod/mysql/databases/app?force=true", "", token)
	if w.Code != http.StatusNoContent || !p.force {
		t.Fatal("force not passed", w.Code)
	}
	p.err = engine.ErrUnavailable
	w = call(s, "GET", "/api/v1/clusters/prod/mysql/users", "", token)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatal("unavailability not mapped", w.Code)
	}
}

func TestMySQLWrongEngineRejected(t *testing.T) {
	s, token := namespaceTestServer(t)
	w := call(s, "GET", "/api/v1/clusters/prod/mysql/users", "", token)
	expected := s.failure(engine.ErrNotSupported).status
	if w.Code != expected {
		t.Fatal("wrong engine accepted", w.Code, w.Body.String())
	}
}

func TestMySQLRBACMetadata(t *testing.T) {
	for operation, action := range map[string]string{
		"listMySQLUsers": "read", "getMySQLUser": "read", "createMySQLUser": "create", "patchMySQLUser": "update", "deleteMySQLUser": "delete",
		"rotateMySQLUserPassword": "update", "discardMySQLUserOldPassword": "update", "listMySQLDatabases": "read", "getMySQLDatabase": "read", "createMySQLDatabase": "create", "deleteMySQLDatabase": "delete", "listMySQLGrants": "read", "grantMySQLPrivileges": "update", "revokeMySQLPrivileges": "update",
	} {
		rule, exists := api.OperationRBAC[operation]
		if !exists || rule.Resource != "database-users" || rule.Action != action || rule.ObjectParam != "cluster" || rule.Public {
			t.Fatalf("%s has incorrect RBAC metadata: %+v", operation, rule)
		}
	}
}

func TestMySQLRealProviderDispatch(t *testing.T) {
	s, _ := namespaceTestServer(t)
	var err error
	s.Registry, err = engine.NewRegistry(mysql.New(mysql.Options{}), pxc.New(mysql.Options{}))
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"mysql", "pxc"} {
		provider, err := s.Registry.Get(name)
		if err != nil {
			t.Fatal(err)
		}
		// An unformed real cluster must reach the provider's lifecycle fence, not
		// the server's unsupported-provider fallback.
		rc := &engine.Context{Cluster: &domain.Cluster{Name: "native", Spec: domain.ClusterSpec{Provider: name}}}
		result := s.mysql(t.Context(), "ListMySQLUsers", input{}, rc, provider)
		if result.status != http.StatusServiceUnavailable {
			t.Fatalf("%s real provider dispatch: status %d", name, result.status)
		}
	}
}
