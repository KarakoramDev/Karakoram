// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package pgspec

import "strings"

// ProtectedSchema reserves PostgreSQL and engine-managed namespaces on both
// admission boundaries, including optional Supabase extension/service schemas.
func ProtectedSchema(name string) bool {
	if strings.HasPrefix(name, "pg_") || strings.HasPrefix(name, "_karakoram") ||
		strings.HasPrefix(name, "graphql") || strings.HasPrefix(name, "supabase_") {
		return true
	}
	switch name {
	case "information_schema", "auth", "storage", "realtime", "_realtime",
		"extensions", "vault", "pgsodium", "pgsodium_masks", "net", "cron",
		"pgbouncer", "_supavisor", "_analytics":
		return true
	}
	return false
}

// Role describes a managed PostgreSQL role without its password.
type Role struct {
	Name            string   `json:"name"`
	Login           bool     `json:"login"`
	Superuser       bool     `json:"superuser"`
	Createdb        bool     `json:"createdb"`
	Createrole      bool     `json:"createrole"`
	Replication     bool     `json:"replication"`
	BypassRLS       bool     `json:"bypassrls"`
	ConnectionLimit int      `json:"connectionLimit"`
	MemberOf        []string `json:"memberOf"`
	ValidUntil      *string  `json:"validUntil,omitempty"`
}

// RoleWrite carries a client password at admission, and only a SCRAM verifier
// on the agent protocol. Plaintext passwords are never included in SQL.
type RoleWrite struct {
	Role
	Password *string `json:"password,omitempty"`
}

type Database struct {
	Name             string `json:"name"`
	Owner            string `json:"owner"`
	Encoding         string `json:"encoding"`
	Locale           string `json:"locale"`
	AllowConnections bool   `json:"allowConnections"`
}

type Grant struct {
	Role       string   `json:"role"`
	Database   string   `json:"database"`
	Schema     string   `json:"schema,omitempty"`
	Privileges []string `json:"privileges"`
}

type AccessRequest struct {
	Operation string     `json:"operation"`
	Name      string     `json:"name,omitempty"`
	Role      *RoleWrite `json:"role,omitempty"`
	Database  *Database  `json:"database,omitempty"`
	Grant     *Grant     `json:"grant,omitempty"`
	Force     bool       `json:"force,omitempty"`
}
