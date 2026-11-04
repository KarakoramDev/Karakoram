// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package mysql

import (
	"context"
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
)

type User = myspec.User
type UserWrite = myspec.UserWrite
type UserPatch = myspec.UserPatch
type Database = myspec.Database
type Grant = myspec.Grant

// Access is the native MySQL account API, also shared by compatible providers.
//
// Implementations must resolve the live writable member on every call and send
// bounded AccessRequests to /v1/mysql/access-{users,databases,grants}. All users
// are at host '%' and REQUIRE SSL. Hidden native accounts/schemas cannot be
// read or changed. RoutableThroughProxy is true only for accounts whose current
// credentials are owned by the sealed managed-user index, never for SQL-created
// accounts discovered by observation.
//
// Before any user mutation, seal a pending intent containing the exact request,
// any generated password, stable random RequestID, prior credential/index
// snapshot, and intended proxy revision. Never put these secrets in cluster
// state, logs, operation status, or receipts. Replay this SAME intent until the member's
// AccessResult proves applied or rejected; an unknown outcome preserves it and
// blocks subsequent credential mutations. MySQL account DDL implicitly commits:
// the member atomically attaches an operation marker to the account DDL and
// reconciles its receipt table from that marker, not a fictitious SQL transaction.
// After application, seal current (and retained secondary, if requested)
// credentials and managed-user index before publishing the matching ProxySQL
// configuration revision. Resume interrupted publication before new mutations.
// Definitive rejection restores the prior sealed snapshot. Creation must not
// adopt a foreign native account. Retained passwords require both credentials
// to remain usable through every proxy until discard completes; reject the
// operation before SQL if the deployed proxy cannot represent that contract.
// Delete waits for its durable account tombstone before removing sealed
// credentials and the managed index and publishing a new proxy revision.
// Pending deletion can drop only the original native marker; an applied
// tombstone never removes a subsequently recreated account. Serialize access
// mutations with lifecycle changes, and never report success before required
// proxy configuration acknowledgements.
//
// CreateUser returns the generated password (empty when supplied by the caller).
// RotateUserPassword always returns the new generated password exactly once.
// DeleteDatabase refuses a nonempty database unless force is true.
// ListGrants requires at least one of the user and database filters.
type Access interface {
	ListUsers(context.Context, *engine.Context) ([]User, error)
	GetUser(context.Context, *engine.Context, string) (*User, error)
	CreateUser(context.Context, *engine.Context, UserWrite) (string, error)
	PatchUser(context.Context, *engine.Context, string, UserPatch) error
	DeleteUser(context.Context, *engine.Context, string) error
	RotateUserPassword(context.Context, *engine.Context, string, bool) (string, error)
	DiscardOldPassword(context.Context, *engine.Context, string) error
	ListDatabases(context.Context, *engine.Context) ([]Database, error)
	GetDatabase(context.Context, *engine.Context, string) (*Database, error)
	CreateDatabase(context.Context, *engine.Context, Database) error
	DeleteDatabase(context.Context, *engine.Context, string, bool) error
	ListGrants(context.Context, *engine.Context, string, string) ([]Grant, error)
	Grant(context.Context, *engine.Context, Grant) error
	Revoke(context.Context, *engine.Context, Grant) error
}

func ValidateUserName(name string) error {
	if !myspec.ManagedUser(name) {
		return engine.Invalid("user name must be an unreserved 1..32 character ASCII identifier")
	}
	return nil
}
func ValidateDatabaseName(name string) error {
	if !myspec.ManagedDatabase(name) {
		return engine.Invalid("database name must be an unreserved 1..64 character ASCII identifier")
	}
	return nil
}
func ValidatePassword(password string) error {
	if len(password) < 12 || len(password) > 1024 || !utf8.ValidString(password) || strings.IndexFunc(password, unicode.IsControl) >= 0 {
		return engine.Invalid("password must contain 12..1024 UTF-8 bytes without control characters")
	}
	return nil
}
func (*Core) ValidateDatabasePassword(password string) error { return ValidatePassword(password) }
func GeneratePassword() (string, error)                      { return secrets.Password() }
func ValidateUserWrite(user UserWrite) error {
	if err := ValidateUserName(user.Name); err != nil {
		return err
	}
	if user.MaxUserConnections < 0 {
		return engine.Invalid("maxUserConnections must be nonnegative")
	}
	if user.Password != nil {
		return ValidatePassword(*user.Password)
	}
	return nil
}
func ValidateUserPatch(patch UserPatch) error {
	if patch.MaxUserConnections != nil && *patch.MaxUserConnections < 0 {
		return engine.Invalid("maxUserConnections must be nonnegative")
	}
	return nil
}
func ValidateGrant(grant Grant) error {
	if err := ValidateUserName(grant.User); err != nil {
		return err
	}
	if err := ValidateDatabaseName(grant.Database); err != nil {
		return err
	}
	if len(grant.Privileges) == 0 || len(grant.Privileges) > len(myspec.DatabasePrivileges) {
		return engine.Invalid("privileges must contain allowed database privileges")
	}
	seen := make(map[string]bool, len(grant.Privileges))
	for _, privilege := range grant.Privileges {
		if !slices.Contains(myspec.DatabasePrivileges[:], privilege) || seen[privilege] {
			return engine.Invalid("invalid or duplicate database privilege")
		}
		seen[privilege] = true
	}
	if seen["ALL"] && len(seen) != 1 {
		return engine.Invalid("ALL cannot be combined with other privileges")
	}
	return nil
}

var _ engine.DatabasePasswordValidator = (*Core)(nil)
