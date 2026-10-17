// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

//go:build integration

package postgres

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
)

func TestNativeManagementSecurity(t *testing.T) {
	bin := os.Getenv("KARAKORAM_PG_BIN_DIR")
	if bin == "" {
		t.Skip("set KARAKORAM_PG_BIN_DIR for native management security acceptance")
	}
	if os.Geteuid() == 0 {
		t.Fatal("native PostgreSQL requires an unprivileged runtime user")
	}
	logs := t.TempDir()
	s := testSpec()
	s.BinDir = bin
	s.Postgres.Parameters = map[string]string{"shared_buffers": "32MB", "max_connections": "30", "logging_collector": "on", "log_directory": logs, "log_filename": "security.log", "log_statement": "all", "log_min_duration_statement": "0", "log_min_error_statement": "error"}
	h := startNativeAgent(t, s)
	defer h.stop(t)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	c, err := waitLocal(ctx, s, dataPaths(h.dir))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(ctx)
	var path string
	if err := c.QueryRow(ctx, "SHOW search_path").Scan(&path); err != nil || path != "pg_catalog" {
		t.Fatalf("unsafe management search path: %q: %v", path, err)
	}
	// Simulate pre-upgrade overloads. Neither public search-path shadowing nor
	// an exact named-catalog-row overload may be invoked by management queries.
	if _, err := c.Exec(ctx, `CREATE FUNCTION public.to_jsonb(pg_catalog.pg_database) RETURNS jsonb LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'public overload executed'; END $$;
 CREATE FUNCTION pg_catalog.to_jsonb(pg_catalog.pg_database) RETURNS jsonb LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'catalog overload executed'; END $$;
 CREATE FUNCTION pg_catalog.row_to_json(pg_catalog.pg_database) RETURNS json LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'row overload executed'; END $$`); err != nil {
		t.Fatal(err)
	}
	defer c.Exec(ctx, "DROP FUNCTION pg_catalog.to_jsonb(pg_catalog.pg_database); DROP FUNCTION public.to_jsonb(pg_catalog.pg_database); DROP FUNCTION pg_catalog.row_to_json(pg_catalog.pg_database)")
	databases, err := readDatabases(ctx, c, "postgres")
	if err != nil || len(databases) != 1 || databases[0].Locale == "" {
		t.Fatal("database management invoked a hostile overload", err)
	}
	if status := h.status(t); status.Locale == "" || !status.Running {
		t.Fatal("status invoked a hostile overload")
	}
	password := "native-management-credential-secret"
	verifier := nativeVerifier(t, password)
	role := pgspec.RoleWrite{Role: pgspec.Role{Name: "security_app", Login: true, ConnectionLimit: -1}, Password: &verifier}
	nativeRequest(t, h, pgspec.ActionRoles, pgspec.AccessRequest{Operation: "create", Role: &role}, nil)
	// Agent restart bootstrap is covered by invoking the same bootstrap routine
	// again while the server's application logging options remain enabled.
	if err := bootstrapRoles(ctx, c, s); err != nil {
		t.Fatal(err)
	}
	// A logging collector may buffer; stopping PostgreSQL flushes its output.
	c.Close(ctx)
	h.stop(t)
	data, err := os.ReadFile(filepath.Join(logs, "security.log"))
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{s.Superuser.Password, s.Monitor.Password, s.Replication.Password, s.Rewind.Password, password, verifier} {
		if strings.Contains(string(data), secret) {
			t.Fatal("credential material leaked to PostgreSQL logs")
		}
	}
	if strings.Contains(string(data), "PASSWORD 'SCRAM-SHA-256$") {
		t.Fatal("system verifier leaked to PostgreSQL logs")
	}
}
