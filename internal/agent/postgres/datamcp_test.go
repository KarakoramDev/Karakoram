package postgres

import (
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
)

func testDataRoles() *pgspec.DataAccessRoles {
	return &pgspec.DataAccessRoles{Databases: []string{"postgres"}, Schemas: []string{"app"}, Roles: []pgspec.DataAccessRole{
		{Credentials: pgspec.Credentials{Username: "kk_mcp_ro", Password: "reader-secret"}, Enabled: true, Settings: map[string]string{"default_transaction_read_only": "on", "statement_timeout": "30s", "idle_in_transaction_session_timeout": "60s"}},
		{Credentials: pgspec.Credentials{Username: "kk_mcp_rw", Password: "writer-secret"}, Enabled: true, Write: true, Settings: map[string]string{"statement_timeout": "30s"}},
	}}
}
func TestDataRoleSQL(t *testing.T) {
	s := testSpec()
	s.DataAccess = testDataRoles()
	statements, err := roleStatements(s)
	if err != nil {
		t.Fatal(err)
	}
	sql := strings.Join(statements, ";\n")
	for _, want := range []string{`ALTER ROLE "kk_mcp_ro" LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS CONNECTION LIMIT 10 PASSWORD 'SCRAM-SHA-256$`, `ALTER ROLE "kk_mcp_ro" SET "default_transaction_read_only" = 'on'`, `ALTER ROLE "kk_mcp_ro" SET "statement_timeout" = '30s'`, `ALTER ROLE "kk_mcp_ro" SET "idle_in_transaction_session_timeout" = '60s'`, `REVOKE pg_read_all_data FROM "kk_mcp_ro"`} {
		if !strings.Contains(sql, want) {
			t.Fatalf("missing %s", want)
		}
	}
	if strings.Contains(sql, "reader-secret") || strings.Contains(sql, "writer-secret") || strings.Contains(sql, `GRANT pg_monitor TO "kk_mcp_`) {
		t.Fatal("unsafe data role SQL")
	}
	if strings.Contains(sql, `GRANT pg_read_all_data TO "kk_mcp_ro"`) {
		t.Fatal("scoped role has cluster-wide read grant")
	}
	s.DataAccess.GlobalRead = true
	statements, err = roleStatements(s)
	if err != nil || !strings.Contains(strings.Join(statements, ";"), `GRANT pg_read_all_data TO "kk_mcp_ro"`) {
		t.Fatal("unscoped read membership missing", err)
	}
	s.DataAccess.Roles[0].Enabled = false
	statements, err = roleStatements(s)
	if err != nil || !strings.Contains(strings.Join(statements, ";"), `ALTER ROLE "kk_mcp_ro" NOLOGIN`) {
		t.Fatal("disabled role not fenced", err)
	}
	if dataDropStatement(`kk_mcp_ro`) != `DROP ROLE IF EXISTS "kk_mcp_ro"` {
		t.Fatal("role drop SQL")
	}
}
func TestDataSchemaSQL(t *testing.T) {
	roles := testDataRoles()
	ro := strings.Join(dataSchemaStatements(roles.Roles[0], `app"schema`, []string{`app"owner`}), ";\n")
	if !strings.Contains(ro, `GRANT SELECT ON ALL TABLES IN SCHEMA "app""schema" TO "kk_mcp_ro"`) || !strings.Contains(ro, `ALTER DEFAULT PRIVILEGES FOR ROLE "app""owner"`) {
		t.Fatal(ro)
	}
	for _, forbidden := range []string{"INSERT", "UPDATE", "DELETE", "CREATE", "ALL PRIVILEGES"} {
		if strings.Contains(ro, forbidden) {
			t.Fatal("reader got " + forbidden)
		}
	}
	rw := strings.Join(dataSchemaStatements(roles.Roles[1], "app", []string{"owner"}), ";")
	if !strings.Contains(rw, "SELECT, INSERT, UPDATE, DELETE") || strings.Contains(rw, "CREATE") {
		t.Fatal(rw)
	}
	for _, want := range []string{
		`GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA "app" TO "kk_mcp_rw"`,
		`ALTER DEFAULT PRIVILEGES FOR ROLE "owner" IN SCHEMA "app" GRANT USAGE, SELECT ON SEQUENCES TO "kk_mcp_rw"`,
	} {
		if !strings.Contains(rw, want) {
			t.Fatalf("missing sequence grant: %s", want)
		}
	}
	if strings.Contains(ro, "SEQUENCES") {
		t.Fatal("reader received sequence mutation privileges")
	}
}
func TestDataRoleValidationAndCache(t *testing.T) {
	s := testSpec()
	s.DataAccess = testDataRoles()
	runner := New()
	first, err := runner.systemRoleSQL(s)
	if err != nil {
		t.Fatal(err)
	}
	s.DataAccess.Revision = "new-application-role"
	unchanged, err := runner.systemRoleSQL(s)
	if err != nil || strings.Join(first, ";") != strings.Join(unchanged, ";") {
		t.Fatal("catalog-only revision regenerated password verifiers", err)
	}
	s.DataAccess.Roles[0].Settings["statement_timeout"] = "15s"
	next, err := runner.systemRoleSQL(s)
	if err != nil || strings.Join(first, ";") == strings.Join(next, ";") {
		t.Fatal("GUC change not cached correctly", err)
	}
	s.DataAccess.Roles[0].Settings["search_path"] = "public"
	if _, err = roleStatements(s); err == nil {
		t.Fatal("arbitrary role GUC accepted")
	}
	delete(s.DataAccess.Roles[0].Settings, "search_path")
	s.DataAccess.Roles[0].Username = "postgres"
	if _, err = roleStatements(s); err == nil {
		t.Fatal("non-data role accepted")
	}
}

func TestPostgresRespectsExplicitListenHost(t *testing.T) {
	s := testSpec()
	if got := parameters(s, dataPaths(t.TempDir()))["listen_addresses"]; got != "*" {
		t.Fatalf("legacy default listener changed: %v", got)
	}
	s.ListenHost = "127.0.0.1"
	if got := parameters(s, dataPaths(t.TempDir()))["listen_addresses"]; got != "127.0.0.1" {
		t.Fatalf("explicit loopback listener ignored: %v", got)
	}
}

func TestDataGrantRefreshCadence(t *testing.T) {
	spec := testDataRoles()
	now := time.Unix(10000, 0)
	cache := dataGrantCache{}
	if !cache.due(spec, now) {
		t.Fatal("initial grants not scheduled")
	}
	cache = dataGrantCache{digest: hash(spec), appliedAt: now}
	if cache.due(spec, now.Add(dataGrantRefreshInterval-time.Nanosecond)) {
		t.Fatal("unchanged grants refreshed before the interval")
	}
	if !cache.due(spec, now.Add(dataGrantRefreshInterval)) {
		t.Fatal("direct SQL catalog changes never receive a periodic refresh")
	}
	if !cache.due(spec, now.Add(-time.Second)) {
		t.Fatal("backwards clock change indefinitely deferred refresh")
	}
	spec.Revision = "application-role-created"
	if !cache.due(spec, now.Add(time.Second)) {
		t.Fatal("managed role catalog revision failed to invalidate ACL cache")
	}
}
