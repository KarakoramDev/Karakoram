// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package mysql

import (
	"encoding/json"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

func quoteIdentifier(name string) string { return "`" + strings.ReplaceAll(name, "`", "``") + "`" }
func account(name string) string         { return "'" + name + "'@'%'" }
func validPassword(password string) bool {
	return len(password) >= 12 && len(password) <= 1024 && utf8.ValidString(password) && strings.IndexFunc(password, unicode.IsControl) < 0
}

var sqlOptionName = regexp.MustCompile(`^[A-Za-z0-9_]{1,64}$`)
var requestIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)

// statement never contains the password. Password-bearing statements are
// assembled on the server with QUOTE(@kk_access_pw), after binding the password
// with SET @kk_access_pw = ?. CREATE/ALTER USER do not support parameter markers.
// The dedicated connection disables NO_BACKSLASH_ESCAPES so QUOTE's escaping
// has its documented meaning; utf8mb4 is required by the connection contract.
type accessStatement struct {
	prefix, suffix string
	password       *string
}

func userStatement(req myspec.AccessRequest, digest string) (accessStatement, error) {
	name := req.Name
	if req.Operation == "create" && req.User != nil {
		name = req.User.Name
	}
	if !myspec.ManagedUser(name) {
		return accessStatement{}, invalidAccess("invalid or protected user name")
	}
	stmt := accessStatement{}
	switch req.Operation {
	case "create":
		if req.User == nil || req.User.Password == nil || req.User.MaxUserConnections < 0 {
			return stmt, invalidAccess("invalid user definition")
		}
		stmt.prefix = "CREATE USER " + account(name) + " IDENTIFIED BY "
		stmt.password = req.User.Password
		stmt.suffix = " REQUIRE SSL WITH MAX_USER_CONNECTIONS " + strconv.Itoa(req.User.MaxUserConnections) + lockClause(req.User.Locked)
	case "patch":
		if req.Patch == nil {
			return stmt, invalidAccess("missing user patch")
		}
		stmt.prefix = "ALTER USER " + account(name) + " REQUIRE SSL"
		if req.Patch.MaxUserConnections != nil {
			if *req.Patch.MaxUserConnections < 0 {
				return stmt, invalidAccess("invalid user connection limit")
			}
			stmt.prefix += " WITH MAX_USER_CONNECTIONS " + strconv.Itoa(*req.Patch.MaxUserConnections)
		}
		if req.Patch.Locked != nil {
			stmt.prefix += lockClause(*req.Patch.Locked)
		}
	case "password":
		stmt.prefix = "ALTER USER " + account(name) + " IDENTIFIED BY "
		stmt.password = req.Password
		if stmt.password == nil {
			return stmt, invalidAccess("missing password")
		}
		if req.RetainCurrent {
			stmt.suffix = " RETAIN CURRENT PASSWORD"
		}
		stmt.suffix += " REQUIRE SSL"
	case "discard-old":
		stmt.prefix = "ALTER USER " + account(name) + " DISCARD OLD PASSWORD REQUIRE SSL"
	default:
		return stmt, invalidAccess("invalid user operation")
	}
	if stmt.password != nil && !validPassword(*stmt.password) {
		return accessStatement{}, invalidAccess("invalid password")
	}
	if !requestIDPattern.MatchString(req.RequestID) || len(digest) != 64 {
		return accessStatement{}, invalidAccess("credential operation identity required")
	}
	attr := markerAttribute(req.RequestID, digest, "applied")
	if stmt.password == nil {
		stmt.prefix += attr
	} else {
		stmt.suffix += attr
	}
	return stmt, nil
}

// All inputs are generated or validated ASCII tokens, never user-provided JSON.
func markerAttribute(operation, digest, phase string) string {
	metadata, _ := json.Marshal(map[string]any{"_karakoram_access": map[string]string{"operation": operation, "requestHash": digest, "phase": phase}})
	return " ATTRIBUTE '" + string(metadata) + "'"
}
func lockClause(locked bool) string {
	if locked {
		return " ACCOUNT LOCK"
	}
	return " ACCOUNT UNLOCK"
}
func databaseStatement(database myspec.Database) (string, error) {
	if !myspec.ManagedDatabase(database.Name) {
		return "", invalidAccess("invalid or protected database name")
	}
	if database.CharacterSet == "" {
		database.CharacterSet = "utf8mb4"
	}
	if database.Collation == "" {
		database.Collation = "utf8mb4_0900_ai_ci"
	}
	if !sqlOptionName.MatchString(database.CharacterSet) || !sqlOptionName.MatchString(database.Collation) {
		return "", invalidAccess("invalid character set or collation")
	}
	return "CREATE DATABASE " + quoteIdentifier(database.Name) + " CHARACTER SET " + quoteIdentifier(database.CharacterSet) + " COLLATE " + quoteIdentifier(database.Collation), nil
}
func grantStatement(operation string, grant myspec.Grant, partialRevokes bool) (string, error) {
	if !myspec.ManagedUser(grant.User) || !myspec.ManagedDatabase(grant.Database) || len(grant.Privileges) == 0 || len(grant.Privileges) > len(myspec.DatabasePrivileges) {
		return "", invalidAccess("invalid grant target or privileges")
	}
	seen := make(map[string]bool, len(grant.Privileges))
	privileges := make([]string, len(grant.Privileges))
	for i, privilege := range grant.Privileges {
		if !slices.Contains(myspec.DatabasePrivileges[:], privilege) || seen[privilege] {
			return "", invalidAccess("invalid or duplicate database privilege")
		}
		seen[privilege] = true
		privileges[i] = strings.ReplaceAll(privilege, "_", " ")
	}
	if seen["ALL"] && len(seen) != 1 {
		return "", invalidAccess("ALL cannot be combined with other privileges")
	}
	verb, to := "GRANT", " TO "
	if operation == "revoke" {
		verb, to = "REVOKE", " FROM "
	} else if operation != "grant" {
		return "", invalidAccess("invalid grant operation")
	}
	database := grant.Database
	// With partial_revokes=OFF MySQL interprets '_' in a database GRANT as
	// a wildcard even inside backticks. Escape it, or grants leak to other DBs.
	if !partialRevokes {
		database = strings.ReplaceAll(database, "_", `\_`)
	}
	return fmt.Sprintf("%s %s ON %s.*%s%s", verb, strings.Join(privileges, ", "), quoteIdentifier(database), to, account(grant.User)), nil
}
