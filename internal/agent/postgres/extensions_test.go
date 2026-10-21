// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgprofile"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"strings"
	"testing"
)

func TestExtensionStatementsAndValidation(t *testing.T) {
	for _, name := range []string{"pgcrypto", "uuid-ossp", "vector"} {
		if err := validateExtensionNames([]string{name}, []string{name}); err != nil {
			t.Fatal(err)
		}
		if got := pgprofile.CreateExtensionSQL(name, ""); got != `CREATE EXTENSION IF NOT EXISTS "`+name+`" WITH SCHEMA public CASCADE` {
			t.Fatal(got)
		}
		if got := updateExtensionStatement(name); got != `ALTER EXTENSION "`+name+`" UPDATE` {
			t.Fatal(got)
		}
	}
	if got := pgprofile.CreateExtensionSQL("postgis_topology", "topology"); got != `CREATE EXTENSION IF NOT EXISTS "postgis_topology" CASCADE` {
		t.Fatal(got)
	}
	if got := pgprofile.CreateExtensionSQL(`a"b`, ""); got != `CREATE EXTENSION IF NOT EXISTS "a""b" WITH SCHEMA public CASCADE` {
		t.Fatal(got)
	}
	for _, name := range []string{"", "pgcrypto;DROP DATABASE app", `a"b`, "public.vector", "a b", strings.Repeat("a", 64)} {
		if err := validateExtensionNames([]string{name}, []string{name}); err == nil {
			t.Fatalf("accepted %q", name)
		}
	}
	if err := validateExtensionNames([]string{"vector"}, []string{"pgcrypto"}); err == nil {
		t.Fatal("accepted unlisted extension")
	}
	if err := validateExtensionNames(make([]string, 33), nil); err == nil {
		t.Fatal("accepted oversized list")
	}
}

func TestExtensionAccessValidation(t *testing.T) {
	req := pgspec.AccessRequest{Operation: "create", Database: &pgspec.Database{Name: "app", Owner: "kb_app", AllowConnections: true, Extensions: []string{"pgcrypto"}}, AvailableExtensions: []string{"pgcrypto"}}
	for _, op := range []string{"create", "extensions"} {
		req.Operation = op
		if err := validateAccessRequest(pgspec.ActionDatabases, req); err != nil {
			t.Fatal(err)
		}
	}
	req.AvailableExtensions = nil
	if err := validateAccessRequest(pgspec.ActionDatabases, req); err == nil {
		t.Fatal("agent trusted unlisted extension")
	}
	if err := validateAccessRequest(pgspec.ActionUpdateExtensions, pgspec.AccessRequest{Extensions: []string{"vector"}}); err == nil {
		t.Fatal("update trusted unlisted extension")
	}
}
