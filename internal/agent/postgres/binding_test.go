// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"testing"
)

func TestBindingRoleAdmission(t *testing.T) {
	name := "kb_0123456789abcdef0123456789abcdef"
	role := pgspec.RoleWrite{Role: pgspec.Role{Name: name, Login: true, ConnectionLimit: 100}}
	if err := validateRoleWrite(role); err == nil {
		t.Fatal("public request admitted reserved role")
	}
	role.Binding = true
	if err := validateRoleWrite(role); err != nil {
		t.Fatal(err)
	}
	role.Superuser = true
	if err := validateRoleWrite(role); err == nil {
		t.Fatal("privileged binding admitted")
	}
	role.Superuser = false
	role.ConnectionLimit = -1
	if err := validateRoleWrite(role); err == nil {
		t.Fatal("unbounded binding admitted")
	}
	role.ConnectionLimit = 100
	if err := validateAccessRequest(pgspec.ActionRoles, pgspec.AccessRequest{Operation: "create", Role: &role}); err == nil {
		t.Fatal("role flag bypassed request scope")
	}
	if err := validateAccessRequest(pgspec.ActionRoles, pgspec.AccessRequest{Operation: "create", Role: &role, Binding: true}); err != nil {
		t.Fatal(err)
	}
	if err := validateAccessRequest(pgspec.ActionRoles, pgspec.AccessRequest{Operation: "delete", Name: name}); err == nil {
		t.Fatal("public delete admitted reserved role")
	}
	db := pgspec.Database{Name: "identity", Owner: name}
	if err := validateAccessRequest(pgspec.ActionDatabases, pgspec.AccessRequest{Operation: "public-owner", Database: &db}); err == nil {
		t.Fatal("public schema ownership bypass")
	}
	if err := validateAccessRequest(pgspec.ActionDatabases, pgspec.AccessRequest{Operation: "public-owner", Database: &db, Binding: true}); err != nil {
		t.Fatal(err)
	}
	db.Owner = "postgres"
	if err := validateAccessRequest(pgspec.ActionDatabases, pgspec.AccessRequest{Operation: "public-owner", Database: &db, Binding: true}); err == nil {
		t.Fatal("system schema ownership admitted")
	}
}
