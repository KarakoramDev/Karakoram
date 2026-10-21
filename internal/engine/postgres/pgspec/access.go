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
	Binding  bool    `json:"binding,omitempty"`
}

type Database struct {
	Name             string   `json:"name"`
	Owner            string   `json:"owner"`
	OwnerOID         uint32   `json:"ownerOid,omitempty"`
	Encoding         string   `json:"encoding"`
	Locale           string   `json:"locale"`
	AllowConnections bool     `json:"allowConnections"`
	Extensions       []string `json:"extensions,omitempty"`
}

type Grant struct {
	Role       string   `json:"role"`
	Database   string   `json:"database"`
	Schema     string   `json:"schema,omitempty"`
	Privileges []string `json:"privileges"`
}

type AccessRequest struct {
	// Journal uses a distinct operation so older agents reject it before SQL.
	RequestID           string     `json:"requestId,omitempty"`
	RoleOperation       string     `json:"roleOperation,omitempty"`
	Operation           string     `json:"operation"`
	Name                string     `json:"name,omitempty"`
	Role                *RoleWrite `json:"role,omitempty"`
	Database            *Database  `json:"database,omitempty"`
	Grant               *Grant     `json:"grant,omitempty"`
	Force               bool       `json:"force,omitempty"`
	AvailableExtensions []string   `json:"availableExtensions,omitempty"`
	Extensions          []string   `json:"extensions,omitempty"`
	Binding             bool       `json:"binding,omitempty"`
	// ExpectedRoleOID fences binding updates/deletes against native replacement.
	ExpectedRoleOID uint32 `json:"expectedRoleOid,omitempty"`
}

// AccessResult is returned with HTTP 200 for journaled role execution. Missing
// or unrecognized outcomes are ambiguous, never evidence of application.
type AccessResult struct {
	Outcome string `json:"outcome"` // applied, rejected, unknown
	Code    string `json:"code,omitempty"`
	Message string `json:"message,omitempty"`
	Status  int    `json:"status,omitempty"`
	RoleOID uint32 `json:"roleOid,omitempty"`
}

type AccessCapabilities struct {
	CredentialReceipts bool `json:"credentialReceipts"`
}

const ActionUpdateExtensions = "update-extensions"

type ExtensionUpdateResult struct {
	Database string   `json:"database"`
	Updated  []string `json:"updated"`
}
