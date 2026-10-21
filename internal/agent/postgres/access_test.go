// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"encoding/base64"
	"errors"
	"net/http"
	"path/filepath"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func testVerifier() string {
	salt := base64.StdEncoding.EncodeToString(make([]byte, 16))
	key := base64.StdEncoding.EncodeToString(make([]byte, 32))
	return "SCRAM-SHA-256$4096:" + salt + "$" + key + ":" + key
}
func TestRoleSQLSafety(t *testing.T) {
	verifier := testVerifier()
	role := pgspec.RoleWrite{Role: pgspec.Role{Name: `app"; DROP ROLE postgres; --`, Login: true, ConnectionLimit: -1, MemberOf: []string{"pg_monitor", "readers"}}, Password: &verifier}
	sql, args, err := roleStatement("create", role)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(sql, `CREATE ROLE "app""; DROP ROLE postgres; --" WITH LOGIN NOSUPERUSER`) {
		t.Fatal("role identifier was not quoted")
	}
	if strings.Contains(sql, verifier) || !strings.Contains(sql, "PASSWORD $2") || len(args) != 3 || args[2] != verifier {
		t.Fatal("verifier was interpolated into statement text")
	}
	if args[0] != pgx.QueryExecModeSimpleProtocol {
		t.Fatal("utility statement parameters require pgx simple protocol interpolation")
	}
	for _, name := range []string{"postgres", "kk_monitor", "pg_monitor", "pg_execute_server_program"} {
		role.Name = name
		if _, _, err = roleStatement("update", role); err == nil {
			t.Errorf("protected role %q accepted", name)
		}
	}
	role.Name = "app"
	role.MemberOf = []string{"postgres"}
	if _, _, err = roleStatement("update", role); err == nil {
		t.Fatal("privileged membership accepted")
	}
	password := "plaintext-password"
	role.MemberOf = nil
	role.Password = &password
	if _, _, err = roleStatement("create", role); err == nil {
		t.Fatal("plaintext password accepted on agent protocol")
	}
}

func TestRoleAdminMembershipValidation(t *testing.T) {
	role := pgspec.RoleWrite{Role: pgspec.Role{Name: "app", MemberOf: []string{"pg_read_all_data"}, MemberOfAdmin: []string{"pg_read_all_data"}}}
	if err := validateRoleWrite(role); err != nil {
		t.Fatal(err)
	}
	for _, admin := range [][]string{{"pg_write_all_data"}, {"pg_read_all_data", "pg_read_all_data"}, {"postgres"}} {
		role.MemberOfAdmin = admin
		if err := validateRoleWrite(role); err == nil {
			t.Fatalf("accepted invalid admin membership %v", admin)
		}
	}
}

func TestRoleAdminMembershipSQL(t *testing.T) {
	role := pgspec.Role{Name: `app"name`, MemberOf: []string{`read"ers`}, MemberOfAdmin: []string{`read"ers`}}
	for _, tc := range []struct {
		name    string
		current []roleMembership
		admin   []string
		members []string
		want    string
	}{
		{"create", nil, role.MemberOfAdmin, role.MemberOf, `GRANT "read""ers" TO "app""name" WITH ADMIN OPTION`},
		{"upgrade", []roleMembership{{parent: `read"ers`, grantor: "postgres"}}, role.MemberOfAdmin, role.MemberOf, `GRANT "read""ers" TO "app""name" WITH ADMIN OPTION`},
		{"unchanged", []roleMembership{{parent: `read"ers`, grantor: "postgres", admin: true}}, role.MemberOfAdmin, role.MemberOf, ""},
		{"revoke-admin", []roleMembership{{parent: `read"ers`, grantor: `grant"or`, admin: true}}, nil, role.MemberOf, `REVOKE ADMIN OPTION FOR "read""ers" FROM "app""name" GRANTED BY "grant""or"`},
		{"revoke-membership", []roleMembership{{parent: `read"ers`, grantor: "postgres", admin: true}}, nil, nil, `REVOKE "read""ers" FROM "app""name" GRANTED BY "postgres"`},
		{"ordinary-create", nil, nil, role.MemberOf, `GRANT "read""ers" TO "app""name"`},
		{"ordinary-unchanged", []roleMembership{{parent: `read"ers`, grantor: "postgres"}}, nil, role.MemberOf, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			desired := role
			desired.MemberOf, desired.MemberOfAdmin = tc.members, tc.admin
			got := strings.Join(roleMembershipStatements(desired, tc.current), ";")
			if got != tc.want {
				t.Fatalf("SQL = %q, want %q", got, tc.want)
			}
		})
	}
}
func TestVerifierValidation(t *testing.T) {
	if !validVerifier(testVerifier()) {
		t.Fatal("valid verifier rejected")
	}
	for _, value := range []string{"", "plain", "SCRAM-SHA-256$1:AA==$AA==:AA==", "SCRAM-SHA-256$4096:bad$bad:bad", testVerifier() + "\n"} {
		if validVerifier(value) {
			t.Fatal("invalid verifier accepted")
		}
	}
}
func TestDatabaseSQLSafety(t *testing.T) {
	db := pgspec.Database{Name: `db"name`, Owner: `owner"name`, Encoding: "UTF8", Locale: "C.UTF-8", AllowConnections: true}
	sql, args, err := createDatabaseStatement(db)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(sql, `CREATE DATABASE "db""name"`) || !strings.Contains(sql, `OWNER "owner""name"`) {
		t.Fatal("database identifiers were not quoted")
	}
	if !strings.Contains(sql, "ENCODING $1") || !strings.Contains(sql, "LC_COLLATE $2 LC_CTYPE $2 TEMPLATE template0") || len(args) != 3 {
		t.Fatal("database locale/encoding not bound")
	}
	for _, name := range []string{"postgres", "template0", "template1"} {
		db.Name = name
		if _, _, err = createDatabaseStatement(db); err == nil {
			t.Errorf("protected database %q accepted", name)
		}
	}
}
func TestGrantSQLSafety(t *testing.T) {
	grant := pgspec.Grant{Role: `app"role`, Database: `db"name`, Privileges: []string{"TEMPORARY", "CONNECT"}}
	sql, err := grantStatement("grant", grant)
	if err != nil {
		t.Fatal(err)
	}
	if sql != `GRANT CONNECT,TEMP ON DATABASE "db""name" TO "app""role"` {
		t.Fatal(sql)
	}
	grant.Schema = `s"name`
	grant.Privileges = []string{"usage", "create"}
	sql, err = grantStatement("revoke", grant)
	if err != nil {
		t.Fatal(err)
	}
	if sql != `REVOKE CREATE,USAGE ON SCHEMA "s""name" FROM "app""role"` {
		t.Fatal(sql)
	}
	for _, privileges := range [][]string{nil, {"ALL"}, {"CONNECT"}, {"USAGE; DROP DATABASE postgres"}, {"USAGE", "USAGE"}} {
		grant.Privileges = privileges
		if _, err = grantStatement("grant", grant); err == nil {
			t.Fatal("invalid privilege list accepted")
		}
	}
	grant.Role = "kk_monitor"
	grant.Privileges = []string{"USAGE"}
	if _, err = grantStatement("grant", grant); err == nil {
		t.Fatal("system role grant accepted")
	}
}
func TestAccessSQLStateSanitization(t *testing.T) {
	for code, status := range map[string]int{"42704": 404, "3D000": 404, "42710": 409, "2BP01": 409, "55006": 409, "42501": 403, "22023": 400, "08006": 503, "25006": 503} {
		err := accessFailure(&pgconn.PgError{Code: code, Message: "secret-verifier-and-statement", Detail: "private"}).(*accessError)
		if err.status != status || strings.Contains(err.Error(), "secret") || strings.Contains(err.Error(), "private") {
			t.Fatalf("unsafe SQLSTATE mapping %s: %v", code, err)
		}
	}
	if accessFailure(pgx.ErrNoRows).(*accessError).status != http.StatusNotFound {
		t.Fatal("not found mapping")
	}
	if accessFailure(errors.New("password=secret")).(*accessError).status != http.StatusServiceUnavailable {
		t.Fatal("connection mapping")
	}
}
func TestAccessRequestValidation(t *testing.T) {
	for _, tc := range []struct {
		action  string
		request pgspec.AccessRequest
	}{{pgspec.ActionRoles, pgspec.AccessRequest{Operation: "shell", Name: "app"}}, {pgspec.ActionRoles, pgspec.AccessRequest{Operation: "delete", Name: "postgres"}}, {pgspec.ActionDatabases, pgspec.AccessRequest{Operation: "delete", Name: "postgres"}}, {pgspec.ActionGrants, pgspec.AccessRequest{Operation: "list"}}} {
		if validateAccessRequest(tc.action, tc.request) == nil {
			t.Fatal("invalid access action accepted")
		}
	}
	if err := validateAccessRequest(pgspec.ActionGrants, pgspec.AccessRequest{Operation: "list", Name: "postgres"}); err != nil {
		t.Fatal(err)
	}
}

func TestProtectedSchemaGrants(t *testing.T) {
	for _, schema := range []string{"pg_catalog", "pg_toast", "pg_temp_3", "information_schema", "_karakoram", "auth", "storage", "realtime", "_realtime", "extensions", "graphql", "graphql_public", "supabase_functions", "supabase_migrations", "vault", "pgsodium", "pgsodium_masks", "net", "cron", "pgbouncer", "_supavisor", "_analytics"} {
		grant := pgspec.Grant{Role: "app", Database: "postgres", Schema: schema, Privileges: []string{"create"}}
		if _, err := grantStatement("grant", grant); err == nil {
			t.Errorf("accepted CREATE on %s", schema)
		}
		if _, err := grantStatement("revoke", grant); err != nil {
			t.Errorf("cannot revoke historical CREATE on %s: %v", schema, err)
		}
		grant.Privileges = []string{"USAGE"}
		if _, err := grantStatement("grant", grant); err != nil {
			t.Errorf("read-only schema usage rejected: %v", err)
		}
		for _, privilege := range []string{"ALL", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "TRIGGER"} {
			grant.Privileges = []string{privilege}
			if _, err := grantStatement("grant", grant); err == nil {
				t.Errorf("accepted %s on %s", privilege, schema)
			}
		}
	}
}

func TestManagementSessionProtection(t *testing.T) {
	params := managementParams()
	for name, want := range map[string]string{"search_path": "pg_catalog", "log_statement": "none", "log_min_duration_statement": "-1", "log_min_duration_sample": "-1", "log_min_error_statement": "panic", "log_parameter_max_length": "0", "log_parameter_max_length_on_error": "0"} {
		if params[name] != want {
			t.Errorf("unsafe management setting %s", name)
		}
	}
	if strings.Contains(databaseSelect, "to_jsonb(") || !strings.Contains(databaseSelect, "pg_catalog.row_to_json(ROW(d))") || !strings.Contains(databaseSelect, "FROM pg_catalog.pg_database") {
		t.Fatal("database query permits catalog-row overload hijacking")
	}
}

func TestSystemRoleSQLUsesVerifiers(t *testing.T) {
	s := testSpec()
	statements, err := roleStatements(s)
	if err != nil {
		t.Fatal(err)
	}
	sql := strings.Join(statements, ";")
	for _, c := range []pgspec.Credentials{s.Superuser, s.Monitor, s.Replication, s.Rewind} {
		if strings.Contains(sql, c.Password) {
			t.Fatal("plaintext password in system role SQL")
		}
	}
	if strings.Count(sql, "PASSWORD 'SCRAM-SHA-256$4096:") != 4 {
		t.Fatal("missing system-role SCRAM verifier")
	}
	firstCredential := strings.Index(sql, "ALTER ROLE")
	for _, setting := range []string{"SET search_path = pg_catalog", "SET log_statement = 'none'", "SET log_min_duration_statement = -1", "SET log_min_error_statement = 'panic'"} {
		if index := strings.Index(sql, setting); index < 0 || index > firstCredential {
			t.Fatalf("credential SQL precedes protection %s", setting)
		}
	}
}

func TestSystemRoleVerifierCache(t *testing.T) {
	s := testSpec()
	runner := New()
	first, err := runner.systemRoleSQL(s)
	if err != nil {
		t.Fatal(err)
	}
	second, err := runner.systemRoleSQL(s)
	if err != nil || strings.Join(first, ";") != strings.Join(second, ";") {
		t.Fatal("unchanged credentials regenerated salted bootstrap SQL")
	}
	s.Monitor.Password = "new-monitor-secret"
	third, err := runner.systemRoleSQL(s)
	if err != nil || strings.Join(first, ";") == strings.Join(third, ";") {
		t.Fatal("credential rotation did not invalidate cached bootstrap SQL")
	}
}

func TestRecoveryCredentialFallbackHasIndependentReceipt(t *testing.T) {
	s := testSpec()
	p := dataPaths(t.TempDir())
	s.Bootstrap = &pgspec.Bootstrap{Method: "restore", Generation: 4}
	if !recoveryCredentialsPending(s, p) {
		t.Fatal("restore must rekey before a credential receipt exists")
	}
	if err := durableJSON(filepath.Join(p.root, "bootstrap-generation.json"), 4); err != nil {
		t.Fatal(err)
	}
	if !recoveryCredentialsPending(s, p) {
		t.Fatal("restore completion incorrectly authorizes skipping credential rekey")
	}
	if err := durableJSON(filepath.Join(p.root, "credentials-generation.json"), 4); err != nil {
		t.Fatal(err)
	}
	if recoveryCredentialsPending(s, p) {
		t.Fatal("completed credential generation still allows source fallback")
	}
	s.Bootstrap.Generation++
	if !recoveryCredentialsPending(s, p) {
		t.Fatal("new restore incorrectly reused an old credential receipt")
	}
	s.Bootstrap = nil
	if recoveryCredentialsPending(s, p) {
		t.Fatal("non-recovery management session allows source fallback")
	}
}
