//go:build integration

// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package initsql

// Run only against a disposable PostgreSQL 17 primary. This suite installs
// cluster-global roles and _supabase. KARAKORAM_PG_DSN enables it;
// KARAKORAM_GOTRUE_BIN and KARAKORAM_POSTGREST_BIN select the official binaries.
import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine/supabase/sbkeys"
	"github.com/jackc/pgx/v5"
)

func TestNativeBootstrap(t *testing.T) {
	dsn := os.Getenv("KARAKORAM_PG_DSN")
	if dsn == "" {
		t.Skip("set KARAKORAM_PG_DSN to a disposable PostgreSQL 17 primary")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal("cannot connect native PostgreSQL")
	}
	defer conn.Close(context.Background())
	var version int
	if err := conn.QueryRow(ctx, "SELECT current_setting('server_version_num')::integer").Scan(&version); err != nil || version/10000 != 17 {
		t.Fatal("integration requires PostgreSQL 17")
	}
	passwords := map[string]string{}
	for _, role := range passwordRoles {
		raw := make([]byte, 24)
		if _, err := rand.Read(raw); err != nil {
			t.Fatal(err)
		}
		passwords[role] = base64.RawURLEncoding.EncodeToString(raw) + "'\\quoted"
	}
	minimal := Plan([]string{"gateway", "auth", "rest"}, nil)
	if err := Apply(ctx, conn, minimal, passwords); err != nil {
		t.Fatal(err)
	}
	var exists bool
	if err := conn.QueryRow(ctx, "SELECT EXISTS(SELECT FROM pg_database WHERE datname='_supabase')").Scan(&exists); err != nil || exists {
		t.Fatal("minimal bootstrap created optional metadata database")
	}
	var schemaOwner string
	var schemaUsage bool
	if err := conn.QueryRow(ctx, `SELECT pg_get_userbyid(nspowner),
 has_schema_privilege('anon',oid,'USAGE') AND has_schema_privilege('authenticated',oid,'USAGE') AND has_schema_privilege('service_role',oid,'USAGE')
 FROM pg_namespace WHERE nspname='graphql_public'`).Scan(&schemaOwner, &schemaUsage); err != nil || schemaOwner != "supabase_admin" || !schemaUsage {
		t.Fatal("default PostgREST GraphQL schema missing before optional extension selection")
	}
	var firstCount, secondCount int
	conn.QueryRow(ctx, "SELECT count(*) FROM _karakoram.supabase_migrations").Scan(&firstCount)
	if err := Apply(ctx, conn, minimal, passwords); err != nil {
		t.Fatal(err)
	}
	conn.QueryRow(ctx, "SELECT count(*) FROM _karakoram.supabase_migrations").Scan(&secondCount)
	if firstCount != len(minimal) || secondCount != firstCount {
		t.Fatal("native migration receipts are not idempotent")
	}
	var postgresSuper, escalation, anonLogin, serviceBypass bool
	if err := conn.QueryRow(ctx, `SELECT (SELECT rolsuper FROM pg_roles WHERE rolname='postgres'),
 pg_has_role('authenticator','supabase_admin','MEMBER'),
 (SELECT rolcanlogin FROM pg_roles WHERE rolname='anon'),
 (SELECT rolbypassrls FROM pg_roles WHERE rolname='service_role')`).Scan(&postgresSuper, &escalation, &anonLogin, &serviceBypass); err != nil || !postgresSuper || escalation || anonLogin || !serviceBypass {
		t.Fatal("bootstrap role privilege boundary")
	}
	var owner string
	if err := conn.QueryRow(ctx, "SELECT pg_get_userbyid(proowner) FROM pg_proc WHERE oid='auth.uid()'::regprocedure").Scan(&owner); err != nil || owner != "supabase_auth_admin" {
		t.Fatal("Auth helper ownership")
	}
	for _, role := range []string{"authenticator", "supabase_auth_admin"} {
		config := conn.Config().Copy()
		config.Host = "127.0.0.1"
		config.User = role
		config.Password = passwords[role]
		login, err := pgx.ConnectConfig(ctx, config)
		if err != nil {
			t.Fatal("managed role password cannot authenticate")
		}
		login.Close(ctx)
	}
	full := Plan([]string{"auth", "rest", "storage", "realtime", "pooler", "analytics"}, nil)
	if err := Apply(ctx, conn, full, passwords); err != nil {
		t.Fatal(err)
	}
	if err := Apply(ctx, conn, full, passwords); err != nil {
		t.Fatal(err)
	}
	var publication bool
	if err := conn.QueryRow(ctx, "SELECT EXISTS(SELECT FROM pg_publication WHERE pubname='supabase_realtime' AND NOT puballtables)").Scan(&publication); err != nil || !publication {
		t.Fatal("Realtime publication")
	}
	for _, schema := range []string{"storage", "realtime", "_realtime"} {
		if err := conn.QueryRow(ctx, "SELECT EXISTS(SELECT FROM pg_namespace WHERE nspname=$1)", schema).Scan(&exists); err != nil || !exists {
			t.Fatal("missing selected service schema")
		}
	}
	config := conn.Config().Copy()
	config.Database = "_supabase"
	private, err := pgx.ConnectConfig(ctx, config)
	if err != nil {
		t.Fatal("private service database unavailable")
	}
	defer private.Close(ctx)
	for _, schema := range []string{"_analytics", "_supavisor"} {
		if err := private.QueryRow(ctx, "SELECT pg_get_userbyid(nspowner) FROM pg_namespace WHERE nspname=$1", schema).Scan(&owner); err != nil || owner != "supabase_admin" {
			t.Fatal("private service schema ownership")
		}
	}
	// A real auth-query cannot disclose credentials to API roles.
	if err := conn.QueryRow(ctx, "SELECT has_function_privilege('anon','pgbouncer.get_auth(text)','EXECUTE')").Scan(&exists); err != nil || exists {
		t.Fatal("pooler credential function exposed")
	}
	keys, err := sbkeys.Generate(rand.Reader, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	authBin := nativeBinary(t, "KARAKORAM_GOTRUE_BIN", "auth")
	restBin := nativeBinary(t, "KARAKORAM_POSTGREST_BIN", "postgrest")
	authVersion, err := exec.Command(authBin, "version").CombinedOutput()
	if err != nil || !bytes.Contains(authVersion, []byte("v2.197.0")) {
		t.Fatal("expected official GoTrue v2.197.0")
	}
	restVersion, err := exec.Command(restBin, "--version").CombinedOutput()
	if err != nil || !bytes.Contains(restVersion, []byte("14.18")) {
		t.Fatal("expected official PostgREST v14.18")
	}
	authPort := nativePort(t)
	restPort := nativePort(t)
	authURL := "http://127.0.0.1:" + strconv.Itoa(authPort)
	restURL := "http://127.0.0.1:" + strconv.Itoa(restPort)
	authDone := nativeProcess(t, authBin, []string{"GOTRUE_API_HOST=127.0.0.1", "GOTRUE_API_PORT=" + strconv.Itoa(authPort), "API_EXTERNAL_URL=" + authURL, "GOTRUE_DB_DRIVER=postgres", "GOTRUE_DB_DATABASE_URL=" + serviceURL(conn.Config(), "supabase_auth_admin", passwords["supabase_auth_admin"]), "GOTRUE_SITE_URL=" + authURL, "GOTRUE_JWT_SECRET=" + keys.JWTSecret, "GOTRUE_JWT_AUD=authenticated", "GOTRUE_JWT_DEFAULT_GROUP_NAME=authenticated", "GOTRUE_JWT_ADMIN_ROLES=service_role", "GOTRUE_JWT_EXP=3600", "GOTRUE_EXTERNAL_EMAIL_ENABLED=true", "GOTRUE_MAILER_AUTOCONFIRM=true", "GOTRUE_DISABLE_SIGNUP=false", "GOTRUE_SMTP_ADMIN_EMAIL=integration@example.test", "GOTRUE_LOG_LEVEL=error"})
	nativeReady(t, authURL+"/health", authDone)
	var migrations int
	if err := conn.QueryRow(ctx, "SELECT count(*) FROM auth.schema_migrations").Scan(&migrations); err != nil || migrations < 50 {
		t.Fatal("GoTrue migrations did not finish")
	}
	client := &http.Client{Timeout: 10 * time.Second}
	response, err := client.Post(authURL+"/signup", "application/json", strings.NewReader(`{"email":"bootstrap@example.test","password":"integration-only-password"}`))
	if err != nil {
		t.Fatal("GoTrue signup unavailable")
	}
	defer response.Body.Close()
	var session struct {
		AccessToken string `json:"access_token"`
		User        struct {
			ID string `json:"id"`
		} `json:"user"`
	}
	if err := json.NewDecoder(response.Body).Decode(&session); err != nil || response.StatusCode != 200 || session.AccessToken == "" || session.User.ID == "" {
		t.Fatal("GoTrue signup/session failed")
	}
	if _, err := conn.Exec(ctx, `CREATE TABLE public.bootstrap_probe(owner uuid NOT NULL, label text NOT NULL);
 ALTER TABLE public.bootstrap_probe ENABLE ROW LEVEL SECURITY;
 CREATE POLICY own_row ON public.bootstrap_probe FOR SELECT TO authenticated USING(owner=auth.uid());
 GRANT SELECT ON public.bootstrap_probe TO anon, authenticated, service_role;`); err != nil {
		t.Fatal("creating native RLS probe")
	}
	if _, err := conn.Exec(ctx, "INSERT INTO public.bootstrap_probe(owner,label) VALUES($1,'visible'),('00000000-0000-0000-0000-000000000000','hidden')", session.User.ID); err != nil {
		t.Fatal("inserting native RLS probe")
	}
	restDone := nativeProcess(t, restBin, []string{"PGRST_DB_URI=" + serviceURL(conn.Config(), "authenticator", passwords["authenticator"]), "PGRST_DB_SCHEMAS=public,graphql_public", "PGRST_DB_ANON_ROLE=anon", "PGRST_JWT_SECRET=" + keys.JWTSecret, "PGRST_SERVER_HOST=127.0.0.1", "PGRST_SERVER_PORT=" + strconv.Itoa(restPort), "PGRST_LOG_LEVEL=crit"})
	nativeReady(t, restURL+"/", restDone)
	for _, tc := range []struct {
		token string
		rows  int
	}{{"", 0}, {session.AccessToken, 1}, {keys.ServiceRoleKey, 2}} {
		req, _ := http.NewRequestWithContext(ctx, "GET", restURL+"/bootstrap_probe?select=label", nil)
		if tc.token != "" {
			req.Header.Set("Authorization", "Bearer "+tc.token)
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal("PostgREST native request failed")
		}
		var rows []map[string]string
		err = json.NewDecoder(resp.Body).Decode(&rows)
		resp.Body.Close()
		if err != nil || resp.StatusCode != 200 || len(rows) != tc.rows {
			t.Fatal("native Auth JWT / PostgREST role switch / RLS mismatch")
		}
	}
	// Replay after GoTrue owns its current objects must remain a receipt-only no-op.
	if err := Apply(ctx, conn, full, passwords); err != nil {
		t.Fatal(err)
	}
	t.Logf("PostgreSQL %d; %d GoTrue migrations; anon/user/service RLS matrix passed; optional schemas and repeated receipts passed", version, migrations)
}
func serviceURL(config *pgx.ConnConfig, role, password string) string {
	return (&url.URL{Scheme: "postgres", User: url.UserPassword(role, password), Host: net.JoinHostPort("127.0.0.1", strconv.Itoa(int(config.Port))), Path: "/" + config.Database, RawQuery: "sslmode=disable"}).String()
}
func nativeBinary(t *testing.T, key, name string) string {
	t.Helper()
	if value := os.Getenv(key); value != "" {
		return value
	}
	path, err := exec.LookPath(name)
	if err != nil {
		t.Fatalf("%s is required for the native service proof", key)
	}
	return path
}
func nativePort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	listener.Close()
	return port
}
func nativeProcess(t *testing.T, binary string, environment []string) <-chan error {
	t.Helper()
	logfile, err := os.OpenFile(filepath.Join(t.TempDir(), "service.log"), os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(binary)
	cmd.Dir = filepath.Dir(binary)
	cmd.Env = append([]string{"PATH=/usr/bin:/bin", "LANG=C.UTF-8"}, environment...)
	cmd.Stdout = logfile
	cmd.Stderr = logfile
	if err := cmd.Start(); err != nil {
		t.Fatal("cannot start native service")
	}
	done := make(chan error, 1)
	go func() {
		done <- cmd.Wait()
		close(done)
	}()
	t.Cleanup(func() {
		cmd.Process.Signal(os.Interrupt)
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			cmd.Process.Kill()
			<-done
		}
		logfile.Close()
	})
	return done
}
func nativeReady(t *testing.T, address string, done <-chan error) {
	t.Helper()
	deadline := time.NewTimer(30 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	client := &http.Client{Timeout: time.Second}
	for {
		select {
		case <-done:
			t.Fatal("native service exited before readiness (logs are private)")
		case <-deadline.C:
			t.Fatal("native service readiness timeout (logs are private)")
		case <-ticker.C:
			resp, err := client.Get(address)
			if err == nil {
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				if resp.StatusCode == 200 {
					return
				}
			}
		}
	}
}
