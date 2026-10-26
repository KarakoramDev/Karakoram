// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package users

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// IsSystemRole reports whether db.name is the provider-managed PBM role, which
// the public role API must neither list nor modify.
func IsSystemRole(db, name string) bool { return db == "admin" && name == "pbmAnyAction" }

// EnsurePBMRole creates or reconciles the Percona-required pbmAnyAction role.
// This intentionally grants anyAction on anyResource, including restore rights.
func EnsurePBMRole(ctx context.Context, c *mongo.Client) error {
	desired := domain.DatabaseRole{Name: "pbmAnyAction", Database: "admin", Roles: []domain.RoleRef{},
		Privileges: []domain.Privilege{{Resource: json.RawMessage(`{"anyResource":true}`), Actions: []string{"anyAction"}}}}
	current, err := GetRole(ctx, c, desired.Database, desired.Name)
	if errors.Is(err, engine.ErrNotFound) {
		return CreateRole(ctx, c, desired)
	}
	if err != nil {
		return err
	}
	if sameRoles(current.Roles, desired.Roles) && samePrivileges(current.Privileges, desired.Privileges) {
		return nil
	}
	return UpdateRole(ctx, c, desired)
}

// ListRoles returns custom roles across all databases, including databases with
// no collections. MongoDB has no all-databases rolesInfo selector, so identities
// are read from admin.system.roles (allowed by userAdminAnyDatabase), then
// resolved with rolesInfo and showPrivileges. Built-ins are never returned.
func ListRoles(ctx context.Context, c *mongo.Client) ([]domain.DatabaseRole, error) {
	if c == nil {
		return nil, invalid("MongoDB client is nil")
	}
	cursor, err := c.Database("admin").Collection("system.roles").Find(ctx, bson.D{},
		options.Find().SetProjection(bson.D{{Key: "_id", Value: 0}, {Key: "role", Value: 1}, {Key: "db", Value: 1}}))
	if err != nil {
		return nil, commandError(err)
	}
	defer cursor.Close(ctx)
	var identities []wireRoleRef
	if err := cursor.All(ctx, &identities); err != nil {
		return nil, commandError(err)
	}
	result := make([]domain.DatabaseRole, 0, len(identities))
	if len(identities) > 0 {
		roles, err := queryRoles(ctx, c, identities)
		if err != nil {
			return nil, err
		}
		for _, role := range roles {
			if role.IsBuiltin {
				continue
			}
			mapped, err := role.domain()
			if err != nil {
				return nil, err
			}
			result = append(result, mapped)
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

// GetRole returns a custom role or engine.ErrNotFound. Built-ins are excluded.
func GetRole(ctx context.Context, c *mongo.Client, db, name string) (*domain.DatabaseRole, error) {
	if err := validateIdentity(db, name); err != nil {
		return nil, err
	}
	roles, err := queryRoles(ctx, c, wireRoleRef{Role: name, DB: db})
	if err != nil {
		return nil, err
	}
	if len(roles) == 0 || roles[0].IsBuiltin {
		return nil, fmt.Errorf("custom role %q in %q: %w", name, db, engine.ErrNotFound)
	}
	role, err := roles[0].domain()
	if err != nil {
		return nil, err
	}
	return &role, nil
}

// CreateRole creates a custom role, validating privileges and inherited roles.
func CreateRole(ctx context.Context, c *mongo.Client, role domain.DatabaseRole) error {
	privileges, err := validateRole(role)
	if err != nil {
		return err
	}
	existing, err := queryRoles(ctx, c, wireRoleRef{Role: role.Name, DB: role.Database})
	if err != nil {
		return err
	}
	if len(existing) != 0 {
		return fmt.Errorf("role already exists: %w", engine.ErrConflict)
	}
	if err := requireRoles(ctx, c, role.Roles); err != nil {
		return err
	}
	return run(ctx, c, role.Database, bson.D{{Key: "createRole", Value: role.Name},
		{Key: "roles", Value: encodeRoles(role.Roles)}, {Key: "privileges", Value: privileges}})
}

// UpdateRole replaces a custom role's direct privileges and inherited roles.
func UpdateRole(ctx context.Context, c *mongo.Client, role domain.DatabaseRole) error {
	privileges, err := validateRole(role)
	if err != nil {
		return err
	}
	if _, err := GetRole(ctx, c, role.Database, role.Name); err != nil {
		return err
	}
	if err := requireRoles(ctx, c, role.Roles); err != nil {
		return err
	}
	return run(ctx, c, role.Database, bson.D{{Key: "updateRole", Value: role.Name},
		{Key: "roles", Value: encodeRoles(role.Roles)}, {Key: "privileges", Value: privileges}})
}

// DeleteRole removes a custom role and never modifies a built-in role.
func DeleteRole(ctx context.Context, c *mongo.Client, db, name string) error {
	if _, err := GetRole(ctx, c, db, name); err != nil {
		return err
	}
	assigned, err := queryUsers(ctx, c, bson.D{{Key: "forAllDBs", Value: true}})
	if err != nil {
		return err
	}
	roles, err := ListRoles(ctx, c)
	if err != nil {
		return err
	}
	if err := validateRoleDeletion(db, name, assigned, roles); err != nil {
		return err
	}
	return run(ctx, c, db, bson.D{{Key: "dropRole", Value: name}})
}

// MongoDB dropRole silently strips user and role assignments. Require callers
// to explicitly remove those references before allowing that destructive step.
func validateRoleDeletion(db, name string, assigned []domain.DatabaseUser, roles []domain.DatabaseRole) error {
	var blockers []string
	ref := domain.RoleRef{DB: db, Role: name}
	for _, user := range assigned {
		for _, role := range user.Roles {
			if role == ref {
				blockers = append(blockers, "user "+user.Name+"@"+user.Database)
				break
			}
		}
	}
	for _, inherited := range roles {
		for _, role := range inherited.Roles {
			if role == ref {
				blockers = append(blockers, "role "+inherited.Name+"@"+inherited.Database)
				break
			}
		}
	}
	if len(blockers) > 0 {
		sort.Strings(blockers)
		return domain.Conflict("databaseRole", name+"@"+db, domain.ReasonInvalidState, engine.ErrConflict,
			"database role %s@%s is still assigned to %s; remove these role references before deleting it", name, db, strings.Join(blockers, ", "))
	}
	return nil
}

func queryRoles(ctx context.Context, c *mongo.Client, selector any) ([]wireRole, error) {
	ctx, cancel := context.WithTimeout(ctx, 4*time.Second)
	defer cancel()
	if c == nil {
		return nil, invalid("MongoDB client is nil")
	}
	var response struct {
		Roles []wireRole `bson:"roles"`
	}
	err := c.Database("admin").RunCommand(ctx, bson.D{{Key: "rolesInfo", Value: selector},
		{Key: "showPrivileges", Value: true}, {Key: "showBuiltinRoles", Value: false}}).Decode(&response)
	if err != nil && (errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded)) {
		return nil, fmt.Errorf("PSMDB rolesInfo read exceeded 4s budget: inspect member responsiveness/connectivity: %w", context.DeadlineExceeded)
	}
	if err != nil {
		return nil, commandError(err)
	}
	return response.Roles, nil
}

func samePrivileges(a, b []domain.Privilege) bool {
	if len(a) != len(b) {
		return false
	}
	canonical := func(ps []domain.Privilege) []string {
		result := make([]string, len(ps))
		for i, p := range ps {
			var resource map[string]any
			if json.Unmarshal(p.Resource, &resource) != nil {
				return nil
			}
			normalized, _ := json.Marshal(resource)
			actions := append([]string(nil), p.Actions...)
			sort.Strings(actions)
			encodedActions, _ := json.Marshal(actions)
			result[i] = string(normalized) + string(encodedActions)
		}
		sort.Strings(result)
		return result
	}
	aa, bb := canonical(a), canonical(b)
	if aa == nil || bb == nil {
		return len(a) == 0 && len(b) == 0
	}
	for i := range aa {
		if aa[i] != bb[i] {
			return false
		}
	}
	return true
}
