// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"github.com/jackc/pgx/v5"
	"os"
	"testing"
)

// TestNativeProfileSQL requires a dedicated disposable native PG17 instance.
func TestNativeProfileSQL(t *testing.T) {
	dsn := os.Getenv("PGPROFILE_TEST_DSN")
	if dsn == "" {
		t.Skip("PGPROFILE_TEST_DSN is not set")
	}
	c, err := pgx.Connect(t.Context(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(t.Context())
	s := pgspec.Spec{Role: pgspec.RoleData, Major: 17, Profiles: []string{"postgis", "pgvector"}}
	plan, err := profilePlan(s, pgspec.ProfileBootstrapRequest{Profiles: s.Profiles})
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err = applyProfilePlan(t.Context(), c, plan); err != nil {
			t.Fatal(err)
		}
	}
	assertNativeExtensionNamespaces(t, c, []string{"postgis", "postgis_topology", "postgis_raster", "vector"})
	var searchPath string
	if err = c.QueryRow(t.Context(), "SHOW search_path").Scan(&searchPath); err != nil || searchPath != "pg_catalog" {
		t.Fatalf("management search_path=%q: %v", searchPath, err)
	}
	var receipts, installed int
	if err = c.QueryRow(t.Context(), "SELECT count(*) FROM _karakoram.profile_migrations").Scan(&receipts); err != nil {
		t.Fatal(err)
	}
	if receipts != 4 {
		t.Fatalf("receipts=%d", receipts)
	}
	if err = c.QueryRow(t.Context(), "SELECT count(*) FROM pg_catalog.pg_extension WHERE extname IN ('postgis','postgis_topology','postgis_raster','vector')").Scan(&installed); err != nil || installed != 4 {
		t.Fatal(installed, err)
	}
	if _, err = c.Exec(t.Context(), "CREATE DATABASE profile_consumer"); err != nil {
		t.Fatal(err)
	}
	extensions := []string{"postgis", "postgis_topology", "postgis_raster", "vector", "pgcrypto"}
	for range 2 {
		if err = createDatabaseExtensions(t.Context(), c, pgspec.Database{Name: "profile_consumer", Extensions: extensions}, extensions); err != nil {
			t.Fatal(err)
		}
	}
	consumer, err := databaseConnect(t.Context(), c, "profile_consumer")
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close(t.Context())
	assertNativeExtensionNamespaces(t, consumer, extensions)
	results, err := updateInstalledExtensions(t.Context(), c, extensions)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 {
		t.Fatalf("expected postgres and consumer database results, got %+v", results)
	}
	t.Logf("Applied and replayed %d profile migrations; created per-database extensions idempotently; updated extensions in %d databases", receipts, len(results))
}

func assertNativeExtensionNamespaces(t *testing.T, c *pgx.Conn, extensions []string) {
	t.Helper()
	for _, name := range extensions {
		var actual, expected string
		err := c.QueryRow(t.Context(), `SELECT n.nspname, COALESCE(v.schema, 'public')
FROM pg_catalog.pg_extension AS x
JOIN pg_catalog.pg_namespace AS n ON n.oid = x.extnamespace
JOIN pg_catalog.pg_available_extensions AS e ON e.name = x.extname
JOIN pg_catalog.pg_available_extension_versions AS v
  ON v.name = e.name AND v.version = e.default_version
WHERE x.extname = $1`, name).Scan(&actual, &expected)
		if err != nil {
			t.Fatal(name, err)
		}
		if actual == "pg_catalog" || actual != expected {
			t.Fatalf("extension %s namespace=%s, want %s (never pg_catalog)", name, actual, expected)
		}
		t.Logf("extension %s namespace=%s", name, actual)
	}
}
