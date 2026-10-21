//go:build integration

package postgres

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"github.com/jackc/pgx/v5"
)

func TestNativeDataMcpRoles(t *testing.T) {
	bin := os.Getenv("KARAKORAM_PG_BIN_DIR")
	if bin == "" {
		t.Skip("set KARAKORAM_PG_BIN_DIR for native data MCP acceptance")
	}
	if os.Geteuid() == 0 {
		t.Fatal("native PostgreSQL requires an unprivileged runtime user")
	}
	s := testSpec()
	s.BinDir = bin
	s.ListenHost = "127.0.0.1"
	s.Postgres.Parameters = map[string]string{"shared_buffers": "16MB", "max_connections": "20"}
	p := startNativeDataPostgres(t, s)
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	admin, err := waitLocal(ctx, s, p)
	if err != nil {
		log, _ := os.ReadFile(filepath.Join(p.root, "native-test.log"))
		t.Fatalf("%v; postgres log: %s", err, log)
	}
	defer admin.Close(ctx)
	exec := func(c *pgx.Conn, sql string) {
		t.Helper()
		if _, err := c.Exec(ctx, sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	exec(admin, `CREATE ROLE app_owner; CREATE SCHEMA app AUTHORIZATION app_owner; CREATE SCHEMA hidden; CREATE TABLE app.probe(id int); CREATE TABLE hidden.probe(id int); INSERT INTO app.probe VALUES(1); ALTER TABLE app.probe OWNER TO app_owner`)
	s.DataAccess = testDataRoles()
	if err = bootstrapRoles(ctx, admin, s); err != nil {
		t.Fatal(err)
	}
	reader, err := userConnect(ctx, "127.0.0.1", "postgres", "kk_mcp_ro", "reader-secret")
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close(ctx)
	var count int
	if err = reader.QueryRow(ctx, "SELECT count(*) FROM app.probe").Scan(&count); err != nil || count != 1 {
		t.Fatal("reader SELECT failed", err)
	}
	if _, err = reader.Exec(ctx, "INSERT INTO app.probe VALUES(2)"); err == nil {
		t.Fatal("read-only default allowed INSERT")
	}
	exec(reader, "SET default_transaction_read_only=off")
	if _, err = reader.Exec(ctx, "INSERT INTO app.probe VALUES(2)"); err == nil {
		t.Fatal("reader bypassed SELECT-only grants")
	}
	if _, err = reader.Exec(ctx, "SELECT * FROM hidden.probe"); err == nil {
		t.Fatal("reader escaped schema scope")
	}
	if _, err = reader.Exec(ctx, "CREATE TABLE app.no_ddl(id int)"); err == nil {
		t.Fatal("reader received DDL")
	}
	exec(admin, "SET ROLE app_owner; CREATE TABLE app.future(id int); RESET ROLE")
	exec(reader, "SELECT * FROM app.future")
	writer, err := userConnect(ctx, "127.0.0.1", "postgres", "kk_mcp_rw", "writer-secret")
	if err != nil {
		t.Fatal(err)
	}
	exec(writer, "INSERT INTO app.probe VALUES(2); UPDATE app.probe SET id=3 WHERE id=2; DELETE FROM app.probe WHERE id=3")
	exec(admin, "SET ROLE app_owner; CREATE TABLE app.serial_future(id serial PRIMARY KEY); CREATE TABLE app.identity_future(id int GENERATED ALWAYS AS IDENTITY); RESET ROLE")
	exec(writer, "INSERT INTO app.serial_future DEFAULT VALUES; INSERT INTO app.identity_future DEFAULT VALUES")
	if _, err = writer.Exec(ctx, "CREATE TABLE app.no_ddl(id int)"); err == nil {
		t.Fatal("writer received DDL")
	}
	writer.Close(ctx)
	// RLS is not bypassed by either identity, unlike Supabase service_role.
	exec(admin, "ALTER TABLE app.probe ENABLE ROW LEVEL SECURITY")
	if err = reader.QueryRow(ctx, "SELECT count(*) FROM app.probe").Scan(&count); err != nil || count != 0 {
		t.Fatal("reader bypassed RLS", err)
	}
	runner := New()
	refreshAt := time.Unix(10000, 0)
	if err = runner.refreshDataAccess(ctx, admin, s, refreshAt); err != nil {
		t.Fatal(err)
	}
	var verifierBefore, verifierAfter string
	if err = admin.QueryRow(ctx, "SELECT rolpassword FROM pg_authid WHERE rolname='kk_mcp_ro'").Scan(&verifierBefore); err != nil {
		t.Fatal(err)
	}
	exec(admin, "CREATE ROLE later_owner; GRANT USAGE, CREATE ON SCHEMA app TO later_owner")
	s.DataAccess.Revision = "new-managed-owner"
	if err = runner.refreshDataAccess(ctx, admin, s, refreshAt.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	exec(admin, "SET ROLE later_owner; CREATE TABLE app.managed_future(id int); RESET ROLE")
	exec(reader, "SELECT * FROM app.managed_future")
	if err = admin.QueryRow(ctx, "SELECT rolpassword FROM pg_authid WHERE rolname='kk_mcp_ro'").Scan(&verifierAfter); err != nil || verifierAfter != verifierBefore {
		t.Fatal("catalog revision regenerated a password verifier", err)
	}

	// A scope with no schema list must also discover objects created directly
	// through SQL, without an API revision or a password/bootstrap operation.
	s.DataAccess.Schemas = nil
	if err = runner.refreshDataAccess(ctx, admin, s, refreshAt.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	exec(admin, "CREATE ROLE sql_owner; CREATE SCHEMA sql_app AUTHORIZATION sql_owner; SET ROLE sql_owner; CREATE TABLE sql_app.probe(id int); RESET ROLE")
	if _, err = reader.Exec(ctx, "SELECT * FROM sql_app.probe"); err == nil {
		t.Fatal("new SQL schema unexpectedly granted before refresh")
	}
	if err = admin.QueryRow(ctx, "SELECT rolpassword FROM pg_authid WHERE rolname='kk_mcp_ro'").Scan(&verifierBefore); err != nil {
		t.Fatal(err)
	}
	if err = runner.refreshDataAccess(ctx, admin, s, refreshAt.Add(2*time.Minute+dataGrantRefreshInterval)); err != nil {
		t.Fatal(err)
	}
	exec(reader, "SELECT * FROM sql_app.probe")
	exec(admin, "SET ROLE sql_owner; CREATE TABLE sql_app.future(id int); RESET ROLE")
	exec(reader, "SELECT * FROM sql_app.future")
	if err = admin.QueryRow(ctx, "SELECT rolpassword FROM pg_authid WHERE rolname='kk_mcp_ro'").Scan(&verifierAfter); err != nil || verifierAfter != verifierBefore {
		t.Fatal("periodic ACL refresh regenerated a password verifier", err)
	}
	s.DataAccess.Schemas = []string{"hidden"}
	if err = bootstrapRoles(ctx, admin, s); err != nil {
		t.Fatal(err)
	}
	if _, err = reader.Exec(ctx, "SELECT * FROM app.future"); err == nil {
		t.Fatal("scope narrowing retained old grants")
	}
	exec(admin, "SET ROLE app_owner; CREATE TABLE app.post_scope_serial(id serial); RESET ROLE")
	for _, sequence := range []string{"app.serial_future_id_seq", "app.post_scope_serial_id_seq"} {
		var granted bool
		if err = admin.QueryRow(ctx, "SELECT has_sequence_privilege('kk_mcp_rw', $1, 'USAGE')", sequence).Scan(&granted); err != nil || granted {
			t.Fatal("scope narrowing retained sequence grants/default privileges", err)
		}
	}
	exec(reader, "SELECT * FROM hidden.probe")
	reader.Close(ctx)
	for i := range s.DataAccess.Roles {
		s.DataAccess.Roles[i].Enabled = false
	}
	if err = bootstrapRoles(ctx, admin, s); err != nil {
		t.Fatal(err)
	}
	if err = admin.QueryRow(ctx, "SELECT count(*) FROM pg_roles WHERE rolname IN ('kk_mcp_ro','kk_mcp_rw')").Scan(&count); err != nil || count != 0 {
		t.Fatal("disabled roles retained", err)
	}
	if c, err := userConnect(ctx, "127.0.0.1", "postgres", "kk_mcp_ro", "reader-secret"); err == nil {
		c.Close(ctx)
		t.Fatal("disabled reader still logs in")
	}
}

// This SQL-path test starts a small native server directly, rather than changing
// the production member-address policy merely to request a loopback-only test.
func startNativeDataPostgres(t *testing.T, s pgspec.Spec) paths {
	t.Helper()
	p := dataPaths(t.TempDir())
	for _, dir := range []string{p.root, p.socket} {
		if err := os.MkdirAll(dir, 0700); err != nil {
			t.Fatal(err)
		}
	}
	passwordFile := filepath.Join(p.root, "initdb.password")
	if err := os.WriteFile(passwordFile, []byte(s.Superuser.Password+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	if output, err := command(ctx, filepath.Join(s.BinDir, "initdb"), "-D", p.pgdata, "--username="+s.Superuser.Username, "--pwfile="+passwordFile, "--auth-local=scram-sha-256", "--auth-host=scram-sha-256", "--encoding=UTF8", "--no-locale"); err != nil {
		t.Fatalf("native initdb: %v: %s", err, output)
	}
	if err := os.Remove(passwordFile); err != nil {
		t.Fatal(err)
	}
	for name, contents := range map[string]string{
		"postgresql.conf": renderPostgres(s, p),
		"pg_hba.conf":     strings.Join(hba(s), "\n") + "\n",
	} {
		if err := os.WriteFile(filepath.Join(p.root, name), []byte(contents), 0600); err != nil {
			t.Fatal(err)
		}
	}
	log, err := os.OpenFile(filepath.Join(p.root, "native-test.log"), os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = log.Close() })
	process := exec.Command(filepath.Join(s.BinDir, "postgres"), "-D", p.pgdata, "-c", "config_file="+filepath.Join(p.root, "postgresql.conf"))
	process.Stdout, process.Stderr = log, log
	process.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := process.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- process.Wait() }()
	t.Cleanup(func() {
		_ = syscall.Kill(-process.Process.Pid, syscall.SIGTERM)
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			_ = syscall.Kill(-process.Process.Pid, syscall.SIGKILL)
			<-done
			t.Error("native PostgreSQL shutdown timed out")
		}
	})
	return p
}
