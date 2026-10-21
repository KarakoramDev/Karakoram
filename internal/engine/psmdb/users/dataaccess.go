// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package users

import (
	"context"
	"errors"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// DataAccessRoles validates the application database scope and returns only
// data roles, never user, database or cluster administration privileges.
func DataAccessRoles(level engine.DataAccessLevel, databases []string) ([]domain.RoleRef, error) {
	if !level.Valid() {
		return nil, invalid("invalid data access level %q", level)
	}
	role := "read"
	if level == engine.DataAccessReadWrite {
		role = "readWrite"
	}
	if len(databases) == 0 {
		return []domain.RoleRef{{Role: role + "AnyDatabase", DB: "admin"}}, nil
	}
	roles := make([]domain.RoleRef, 0, len(databases))
	seen := map[string]bool{}
	for _, db := range databases {
		if err := validateDB(db); err != nil {
			return nil, err
		}
		if db == "admin" || db == "local" || db == "config" {
			return nil, invalid("data MCP database %q is reserved", db)
		}
		if !seen[db] {
			roles = append(roles, domain.RoleRef{Role: role, DB: db})
			seen[db] = true
		}
	}
	return roles, nil
}

// EnsureDataAccessUser creates or updates a reserved data identity, including
// its complete grant set so narrowing scope revokes previous database access.
func EnsureDataAccessUser(ctx context.Context, c *mongo.Client, level engine.DataAccessLevel, databases []string, password string) error {
	roles, err := DataAccessRoles(level, databases)
	if err != nil {
		return err
	}
	if err := validatePassword(password); err != nil {
		return err
	}
	name := engine.DataAccessUser(level)
	current, err := getUser(ctx, c, "admin", name)
	if err != nil && !errors.Is(err, engine.ErrNotFound) {
		return err
	}
	command := "updateUser"
	if current == nil {
		command = "createUser"
	}
	return run(ctx, c, "admin", bson.D{{Key: command, Value: name}, {Key: "pwd", Value: password}, {Key: "roles", Value: encodeRoles(roles)}})
}

// DropDataAccessUser removes a reserved data identity, tolerating prior removal.
func DropDataAccessUser(ctx context.Context, c *mongo.Client, level engine.DataAccessLevel) error {
	if !level.Valid() {
		return invalid("invalid data access level %q", level)
	}
	err := run(ctx, c, "admin", bson.D{{Key: "dropUser", Value: engine.DataAccessUser(level)}})
	if errors.Is(err, engine.ErrNotFound) {
		return nil
	}
	return err
}
