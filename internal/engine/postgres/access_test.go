// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"encoding/json"
	"encoding/pem"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/xdg-go/scram"
)

func TestSCRAMVerifier(t *testing.T) {
	password := "strong-password-'\\雪"
	verifier, err := secrets.SCRAMVerifier(password)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(verifier, "SCRAM-SHA-256$4096:") || strings.Contains(verifier, password) {
		t.Fatal("invalid verifier format")
	}
	other, err := secrets.SCRAMVerifier(password)
	if err != nil || verifier == other {
		t.Fatal("SCRAM salt must be independently random", err)
	}
	client, err := scram.SHA256.NewClient("app", password, "")
	if err != nil || client == nil {
		t.Fatal(err)
	}
}

func TestAccessRoleValidation(t *testing.T) {
	p := New(Options{ReservedRole: func(name string) bool { return name == "anon" || strings.HasPrefix(name, "supabase_") }})
	for _, name := range []string{"postgres", "kk_monitor", "kb_0123456789abcdef0123456789abcdef", "pg_execute_server_program", "anon", "supabase_admin", "", strings.Repeat("x", 64), "nul\x00role"} {
		if err := p.validateRole(t.Context(), RoleWrite{Role: Role{Name: name}}); err == nil {
			t.Errorf("accepted protected/invalid role %q", name)
		}
	}
	for _, name := range predefinedMemberships {
		if err := p.validateRole(t.Context(), RoleWrite{Role: Role{Name: "app", MemberOf: []string{name}}}); err != nil {
			t.Fatal(name, err)
		}
	}
	for _, name := range []string{"postgres", "kk_monitor", "pg_execute_server_program", "anon", "app"} {
		if err := p.validateRole(t.Context(), RoleWrite{Role: Role{Name: "app", MemberOf: []string{name}}}); err == nil {
			t.Errorf("accepted protected/circular membership %q", name)
		}
	}
	for _, name := range predefinedMemberships {
		role := RoleWrite{Role: Role{Name: "app", MemberOf: []string{name}, MemberOfAdmin: []string{name}}}
		if err := p.validateRole(t.Context(), role); err != nil {
			t.Fatal("allowed admin membership rejected", err)
		}
		role.MemberOf = nil
		if err := p.validateRole(t.Context(), role); err == nil {
			t.Fatal("admin membership outside memberOf accepted")
		}
	}
	for _, password := range []string{"short", "password-with\nnewline", strings.Repeat("x", 1025)} {
		if err := p.ValidateDatabasePassword(password); err == nil {
			t.Error("accepted invalid password")
		}
	}
	if err := identifier(`odd"; DROP ROLE postgres; --`); err != nil {
		t.Fatal("valid quoted identifiers must be safely supported", err)
	}
}

func TestManagedRolesSCRAMAndProxyLifecycle(t *testing.T) {
	p, rc, _ := setup(t, "ha")
	s, err := load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	s.allocate(rc.Cluster)
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	roles := map[string]Role{"postgres": {Name: "postgres"}, "kk_monitor": {Name: "kk_monitor"}}
	primary := "demop1"
	var mutations []string
	var verifiers []string
	failUpdate := false
	agentServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		member := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if r.URL.Path == "/v1/postgres/status" {
			role := "replica"
			if member == primary {
				role = "primary"
			}
			json.NewEncoder(w).Encode(pgspec.StatusResponse{Role: role, Running: true})
			return
		}
		if r.URL.Path != "/v1/postgres/roles" {
			t.Errorf("unexpected action %s", r.URL.Path)
			w.WriteHeader(400)
			return
		}
		var request pgspec.AccessRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
		}
		switch request.Operation {
		case "capabilities":
			// Exact legacy contract: unknown operations are rejected pre-SQL.
			w.WriteHeader(http.StatusBadRequest)
		case "list":
			out := []Role{}
			for _, role := range roles {
				out = append(out, role)
			}
			json.NewEncoder(w).Encode(out)
		case "create", "update":
			if failUpdate {
				w.WriteHeader(409)
				return
			}
			mutations = append(mutations, member)
			if request.Role.Password != nil {
				verifiers = append(verifiers, *request.Role.Password)
			}
			roles[request.Role.Name] = request.Role.Role
			json.NewEncoder(w).Encode(map[string]bool{"ok": true})
		case "delete":
			delete(roles, request.Name)
			json.NewEncoder(w).Encode(map[string]bool{"ok": true})
		}
	}))
	t.Cleanup(agentServer.Close)
	rc.Agents = fakeAgents{url: agentServer.URL, ca: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: agentServer.Certificate().Raw})}
	ctx := t.Context()
	password, err := p.CreateRole(ctx, rc, RoleWrite{Role: Role{Name: "app", Login: true, ConnectionLimit: -1}})
	if err != nil || password == "" {
		t.Fatal("create generated role", err)
	}
	if len(verifiers) != 1 || !strings.HasPrefix(verifiers[0], "SCRAM-SHA-256$") || strings.Contains(verifiers[0], password) {
		t.Fatal("agent must receive only a SCRAM verifier")
	}
	if strings.Contains(string(rc.Cluster.Internal), password) {
		t.Fatal("password leaked into cluster state")
	}
	proxy, err := p.proxyUsers(ctx, rc)
	if err != nil || len(proxy) != 1 || proxy[0].Password != password || !proxy[0].RequireTLS {
		t.Fatal("proxy did not receive managed login", err)
	}
	before, err := p.accessRevision(ctx, rc)
	if err != nil || before == "" {
		t.Fatal("missing access revision", err)
	}
	primary = "demop2"
	rotated, err := p.RotateRolePassword(ctx, rc, "app")
	if err != nil || rotated == "" || rotated == password {
		t.Fatal("rotate", err)
	}
	if len(mutations) != 2 || mutations[0] != "demop1" || mutations[1] != "demop2" {
		t.Fatal("access did not re-resolve primary", mutations)
	}
	proxy, err = p.proxyUsers(ctx, rc)
	if err != nil || proxy[0].Password != rotated {
		t.Fatal("rotation did not update proxy password", err)
	}
	after, _ := p.accessRevision(ctx, rc)
	if before == after {
		t.Fatal("rotation did not invalidate config")
	}
	failUpdate = true
	if _, err := p.RotateRolePassword(ctx, rc, "app"); !errors.Is(err, engine.ErrConflict) {
		t.Fatal("agent conflict not mapped", err)
	}
	proxy, err = p.proxyUsers(ctx, rc)
	if err != nil || proxy[0].Password != rotated {
		t.Fatal("rejected mutation changed routed password", err)
	}
	failUpdate = false
	if err := p.DeleteRole(ctx, rc, "app"); err != nil {
		t.Fatal(err)
	}
	proxy, err = p.proxyUsers(ctx, rc)
	if err != nil || len(proxy) != 0 {
		t.Fatal("deleted role remains in proxy", err)
	}
	if _, err := secret(ctx, rc, rolePasswordKey("app")); !missing(err) {
		t.Fatal("deleted password remains sealed", err)
	}
	primary = ""
	if _, err := p.ListRoles(ctx, rc); !errors.Is(err, engine.ErrUnavailable) {
		t.Fatal("no primary must be unavailable", err)
	}
}

func TestProtectedSchemaGrantAdmission(t *testing.T) {
	p, rc, _ := setup(t, "single")
	for _, schema := range []string{"pg_catalog", "pg_toast", "information_schema", "_karakoram", "auth", "storage", "realtime", "_realtime", "extensions", "graphql", "graphql_public", "supabase_functions", "vault", "pgsodium", "pgsodium_masks", "net", "cron", "pgbouncer", "_supavisor", "_analytics"} {
		v := Grant{Role: "app", Database: "postgres", Schema: schema, Privileges: []string{"CREATE"}}
		err := p.Grant(t.Context(), rc, v)
		if err == nil || errors.Is(err, engine.ErrUnavailable) {
			t.Errorf("protected CREATE reached agent dispatch: %s: %v", schema, err)
		}
		if err := p.Revoke(t.Context(), rc, v); !errors.Is(err, engine.ErrUnavailable) {
			t.Errorf("historical grant cannot be revoked: %s: %v", schema, err)
		}
	}
}
