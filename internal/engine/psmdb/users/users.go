// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

// Package users manages MongoDB application users, custom roles, and the
// Percona system-user split. Callers supply an authenticated userAdmin client;
// passwords are never returned from MongoDB or included in command errors.
package users

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// SystemUserDef describes a provider-managed account in the admin database.
type SystemUserDef struct {
	Name        string
	Roles       []domain.RoleRef
	Description string
}

// SystemUserDefs returns independently owned definitions for the five Percona
// system users. The monitor has read@local in addition to clusterMonitor@admin;
// the optional PMM explainRole is not needed by Karakoram's metrics exporter.
func SystemUserDefs() []SystemUserDef {
	admin := func(names ...string) []domain.RoleRef {
		roles := make([]domain.RoleRef, len(names))
		for i, name := range names {
			roles[i] = domain.RoleRef{Role: name, DB: "admin"}
		}
		return roles
	}
	return []SystemUserDef{
		{Name: "clusterAdmin", Roles: admin("clusterAdmin"), Description: "Cluster topology and administration"},
		{Name: "userAdmin", Roles: admin("userAdminAnyDatabase"), Description: "User and role administration"},
		{Name: "clusterMonitor", Roles: append(admin("clusterMonitor"), domain.RoleRef{Role: "read", DB: "local"}), Description: "Read-only cluster monitoring"},
		{Name: "backup", Roles: admin("backup", "restore", "clusterMonitor", "readWrite", "pbmAnyAction"), Description: "PBM backup and restore, including broad pbmAnyAction privileges"},
		{Name: "databaseAdmin", Roles: admin("readWriteAnyDatabase", "readAnyDatabase", "dbAdminAnyDatabase", "backup", "restore", "clusterMonitor"), Description: "Database administration"},
	}
}

// IsSystemUser reports whether name is reserved for a provider-managed user.
func IsSystemUser(name string) bool {
	switch name {
	case "clusterAdmin", "userAdmin", "clusterMonitor", "backup", "databaseAdmin", engine.DataAccessReadUser, engine.DataAccessReadWriteUser:
		return true
	}
	return false
}

// EnsureSystemUsers reconciles system-user roles and ensures the PBM role.
// Supplied passwords are compared against stored SCRAM credentials before any
// write. If MongoDB denies access to showCredentials, reconciliation falls back
// to writing supplied passwords on every call. Other read failures are returned.
// Missing passwords preserve credentials; creating a user requires a password.
func EnsureSystemUsers(ctx context.Context, c *mongo.Client, passwords map[string]string) error {
	if c == nil {
		return invalid("MongoDB client is nil")
	}
	for name, password := range passwords {
		if !IsSystemUser(name) {
			return invalid("unknown system user %q", name)
		}
		if err := validatePassword(password); err != nil {
			return err
		}
	}
	if err := EnsurePBMRole(ctx, c); err != nil {
		return err
	}
	defs := SystemUserDefs()
	// Rotate the authenticating user last so pooled connections remain usable
	// while all other users are being reconciled.
	sort.SliceStable(defs, func(i, j int) bool { return defs[i].Name != "userAdmin" && defs[j].Name == "userAdmin" })
	for _, def := range defs {
		current, credentialsVisible, err := getSystemUser(ctx, c, def.Name)
		if err != nil && !errors.Is(err, engine.ErrNotFound) {
			return err
		}
		password, supplied := passwords[def.Name]
		if errors.Is(err, engine.ErrNotFound) {
			if !supplied {
				return invalid("password required to create system user %q", def.Name)
			}
			cmd := bson.D{{Key: "createUser", Value: def.Name}, {Key: "pwd", Value: password}, {Key: "roles", Value: encodeRoles(def.Roles)}}
			if err := run(ctx, c, "admin", cmd); err != nil {
				return err
			}
			continue
		}
		passwordChanged := supplied && (!credentialsVisible || !current.passwordMatches(def.Name, password))
		if passwordChanged || !sameRoles(decodeRoles(current.Roles), def.Roles) {
			cmd := bson.D{{Key: "updateUser", Value: def.Name}, {Key: "roles", Value: encodeRoles(def.Roles)}}
			if passwordChanged {
				cmd = append(cmd, bson.E{Key: "pwd", Value: password})
				if credentialsVisible {
					cmd = append(cmd, bson.E{Key: "mechanisms", Value: current.requiredMechanisms()})
				}
			}
			if err := run(ctx, c, "admin", cmd); err != nil {
				return err
			}
		}
	}
	return nil
}

// SetPassword changes an existing application or system user's password.
func SetPassword(ctx context.Context, c *mongo.Client, db, user, password string) error {
	if err := validateIdentity(db, user); err != nil {
		return err
	}
	if err := validatePassword(password); err != nil {
		return err
	}
	return run(ctx, c, db, bson.D{{Key: "updateUser", Value: user}, {Key: "pwd", Value: password}})
}

// ListUsers returns application users across all databases, excluding reserved
// system-user names. Passwords and authentication hashes are not requested.
func ListUsers(ctx context.Context, c *mongo.Client) ([]domain.DatabaseUser, error) {
	users, err := queryUsers(ctx, c, bson.D{{Key: "forAllDBs", Value: true}})
	if err != nil {
		return nil, err
	}
	result := make([]domain.DatabaseUser, 0, len(users))
	for _, user := range users {
		if !IsSystemUser(user.Name) {
			result = append(result, user)
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Database != result[j].Database {
			return result[i].Database < result[j].Database
		}
		return result[i].Name < result[j].Name
	})
	return result, nil
}

// GetUser returns an application user or engine.ErrNotFound when absent.
func GetUser(ctx context.Context, c *mongo.Client, db, name string) (*domain.DatabaseUser, error) {
	if err := validateApplicationUser(db, name); err != nil {
		return nil, err
	}
	return getUser(ctx, c, db, name)
}

func getUser(ctx context.Context, c *mongo.Client, db, name string) (*domain.DatabaseUser, error) {
	users, err := queryUsers(ctx, c, bson.D{{Key: "user", Value: name}, {Key: "db", Value: db}})
	if err != nil {
		return nil, err
	}
	if len(users) == 0 {
		return nil, fmt.Errorf("user %q in %q: %w", name, db, engine.ErrNotFound)
	}
	return &users[0], nil
}

func queryUsers(ctx context.Context, c *mongo.Client, selector any) ([]domain.DatabaseUser, error) {
	if c == nil {
		return nil, invalid("MongoDB client is nil")
	}
	var response struct {
		Users []wireUser `bson:"users"`
	}
	err := c.Database("admin").RunCommand(ctx, bson.D{{Key: "usersInfo", Value: selector}, {Key: "showCredentials", Value: false}}).Decode(&response)
	if err != nil {
		return nil, commandError(err)
	}
	result := make([]domain.DatabaseUser, len(response.Users))
	for i, u := range response.Users {
		result[i] = u.domain()
	}
	return result, nil
}

// CreateUser creates an application user with the specified password and roles.
func CreateUser(ctx context.Context, c *mongo.Client, u domain.DatabaseUser, password string) error {
	if err := validateApplicationUser(u.Database, u.Name); err != nil {
		return err
	}
	if err := validatePassword(password); err != nil {
		return err
	}
	if err := validateRoles(u.Roles); err != nil {
		return err
	}
	if _, err := getUser(ctx, c, u.Database, u.Name); err == nil {
		return fmt.Errorf("user already exists: %w", engine.ErrConflict)
	} else if !errors.Is(err, engine.ErrNotFound) {
		return err
	}
	if err := requireRoles(ctx, c, u.Roles); err != nil {
		return err
	}
	return run(ctx, c, u.Database, bson.D{{Key: "createUser", Value: u.Name}, {Key: "pwd", Value: password}, {Key: "roles", Value: encodeRoles(u.Roles)}})
}

// UpdateUser replaces an application user's roles. Empty password preserves the
// current password; the separate password argument is authoritative, not u.Password.
func UpdateUser(ctx context.Context, c *mongo.Client, u domain.DatabaseUser, password string) error {
	if err := validateApplicationUser(u.Database, u.Name); err != nil {
		return err
	}
	if err := validateRoles(u.Roles); err != nil {
		return err
	}
	if password != "" {
		if err := validatePassword(password); err != nil {
			return err
		}
	}
	if _, err := getUser(ctx, c, u.Database, u.Name); err != nil {
		return err
	}
	if err := requireRoles(ctx, c, u.Roles); err != nil {
		return err
	}
	cmd := bson.D{{Key: "updateUser", Value: u.Name}, {Key: "roles", Value: encodeRoles(u.Roles)}}
	if password != "" {
		cmd = append(cmd, bson.E{Key: "pwd", Value: password})
	}
	return run(ctx, c, u.Database, cmd)
}

// DeleteUser deletes an application user, refusing reserved system names.
func DeleteUser(ctx context.Context, c *mongo.Client, db, name string) error {
	if err := validateApplicationUser(db, name); err != nil {
		return err
	}
	return run(ctx, c, db, bson.D{{Key: "dropUser", Value: name}})
}

func requireRoles(ctx context.Context, c *mongo.Client, roles []domain.RoleRef) error {
	if len(roles) == 0 {
		return nil
	}
	found, err := queryRoles(ctx, c, encodeRoles(roles))
	if err != nil {
		return err
	}
	for _, wanted := range roles {
		present := false
		for _, role := range found {
			if role.Name == wanted.Role && role.Database == wanted.DB {
				present = true
				break
			}
		}
		if !present {
			return invalid("referenced role %q in %q does not exist", wanted.Role, wanted.DB)
		}
	}
	return nil
}

func run(ctx context.Context, c *mongo.Client, db string, cmd bson.D) error {
	if c == nil {
		return invalid("MongoDB client is nil")
	}
	return commandError(c.Database(db).RunCommand(ctx, cmd).Err())
}

func commandError(err error) error {
	if err == nil {
		return nil
	}
	var ce mongo.CommandError
	if !errors.As(err, &ce) {
		return err
	}
	var sentinel error
	switch ce.Code {
	case 11, 31:
		sentinel = engine.ErrNotFound // UserNotFound, RoleNotFound
	case 11000, 51002, 51003:
		sentinel = engine.ErrConflict
	case 2, 9, 14, 23, 72, 73:
		sentinel = engine.ErrInvalid
	}
	if sentinel == nil {
		switch ce.Name {
		case "UserNotFound", "RoleNotFound":
			sentinel = engine.ErrNotFound
		case "UserAlreadyExists", "RoleAlreadyExists", "DuplicateKey":
			sentinel = engine.ErrConflict
		case "BadValue", "FailedToParse", "TypeMismatch", "InvalidRoleModification", "InvalidOptions", "InvalidNamespace":
			sentinel = engine.ErrInvalid
		}
	}
	if sentinel == nil {
		// Retain only classification metadata, never the raw response/message:
		// dataplane reads still need to recognize temporary topology failures.
		return mongo.CommandError{Code: ce.Code, Labels: ce.Labels,
			Message: fmt.Sprintf("MongoDB command failed (code %d)", ce.Code)}
	}
	// Do not interpolate the server message: it can echo command values.
	return fmt.Errorf("MongoDB command failed (code %d): %w", ce.Code, sentinel)
}
