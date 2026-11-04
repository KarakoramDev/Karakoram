// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package myspec

import (
	"regexp"
	"strings"
)

const (
	ActionAccessUsers        = "access-users"
	ActionAccessDatabases    = "access-databases"
	ActionAccessGrants       = "access-grants"
	ActionAccessCapabilities = "access-capabilities"
)

var userNamePattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,31}$`)
var databaseNamePattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_$]{0,63}$`)

// ProtectedName reserves native and orchestration-owned accounts and schemas.
func ProtectedName(name string) bool {
	name = strings.ToLower(name)
	if strings.HasPrefix(name, "kk_") || strings.HasPrefix(name, "mysql.") || strings.HasPrefix(name, "_karakoram") {
		return true
	}
	switch name {
	case "root", "mysql", "sys", "performance_schema", "information_schema":
		return true
	}
	return false
}

// UserAdmin is the cluster connection account; it is managed by the lifecycle,
// not by the access API.
const UserAdmin = "admin"

func ManagedUser(name string) bool {
	return userNamePattern.MatchString(name) && !ProtectedName(name) && !strings.EqualFold(name, UserAdmin)
}
func ManagedDatabase(name string) bool {
	return databaseNamePattern.MatchString(name) && !ProtectedName(name)
}

// User is a TLS-only application account at the fixed '%' host. Proxy
// routability is enriched by the control plane from its sealed credential index.
type User struct {
	Name                 string `json:"name"`
	Host                 string `json:"host"`
	MaxUserConnections   int    `json:"maxUserConnections"`
	Locked               bool   `json:"locked"`
	PasswordRetained     bool   `json:"passwordRetained"`
	RoutableThroughProxy bool   `json:"routableThroughProxy"`
}
type UserWrite struct {
	Name               string  `json:"name"`
	MaxUserConnections int     `json:"maxUserConnections"`
	Locked             bool    `json:"locked"`
	Password           *string `json:"password,omitempty"`
}
type UserPatch struct {
	MaxUserConnections *int  `json:"maxUserConnections,omitempty"`
	Locked             *bool `json:"locked,omitempty"`
}
type Database struct {
	Name         string `json:"name"`
	CharacterSet string `json:"characterSet"`
	Collation    string `json:"collation"`
}
type Grant struct {
	User       string   `json:"user"`
	Database   string   `json:"database"`
	Privileges []string `json:"privileges"`
}

// AccessRequest is a bounded local administration command, not arbitrary SQL.
// Credentials travel only in the authenticated encrypted member protocol.
// RequestID is mandatory for every user mutation, including deletion, and must
// survive retries. These mutations return AccessResult, never an empty success.
type AccessRequest struct {
	Operation      string     `json:"operation"`
	RequestID      string     `json:"requestId,omitempty"`
	Name           string     `json:"name,omitempty"`
	User           *UserWrite `json:"user,omitempty"`
	Patch          *UserPatch `json:"patch,omitempty"`
	Password       *string    `json:"password,omitempty"`
	RetainCurrent  bool       `json:"retainCurrent,omitempty"`
	Database       *Database  `json:"database,omitempty"`
	Grant          *Grant     `json:"grant,omitempty"`
	UserFilter     string     `json:"userFilter,omitempty"`
	DatabaseFilter string     `json:"databaseFilter,omitempty"`
	Force          bool       `json:"force,omitempty"`
}

// AccessResult deliberately contains no password or native authentication hash.
// Unknown must preserve the sealed intent; it is never permission to create a
// fresh operation identity or roll back published credentials by guessing.
type AccessResult struct {
	Outcome string `json:"outcome"` // applied, rejected, unknown
	Code    string `json:"code,omitempty"`
	Message string `json:"message,omitempty"`
	Status  int    `json:"status,omitempty"`
}
type AccessCapabilities struct {
	CredentialReceipts bool `json:"credentialReceipts"`
}

// DatabasePrivileges is the complete public allowlist. Underscores are the wire
// spelling for SQL privilege names containing spaces; global privileges and
// GRANT OPTION are intentionally absent.
var DatabasePrivileges = [...]string{"SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER", "INDEX", "REFERENCES", "CREATE_VIEW", "SHOW_VIEW", "TRIGGER", "EXECUTE", "CREATE_ROUTINE", "ALTER_ROUTINE", "EVENT", "LOCK_TABLES", "CREATE_TEMPORARY_TABLES", "ALL"}
