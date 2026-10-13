// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package users

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"go.mongodb.org/mongo-driver/v2/bson"
)

type wireRoleRef struct {
	Role string `bson:"role"`
	DB   string `bson:"db"`
}

type wireUser struct {
	Name     string        `bson:"user"`
	Database string        `bson:"db"`
	Roles    []wireRoleRef `bson:"roles"`
}

type wirePrivilege struct {
	Resource bson.M   `bson:"resource"`
	Actions  []string `bson:"actions"`
}

type wireRole struct {
	Name       string          `bson:"role"`
	Database   string          `bson:"db"`
	Roles      []wireRoleRef   `bson:"roles"`
	Privileges []wirePrivilege `bson:"privileges"`
	IsBuiltin  bool            `bson:"isBuiltin"`
}

func (u wireUser) domain() domain.DatabaseUser {
	return domain.DatabaseUser{Name: u.Name, Database: u.Database, Roles: decodeRoles(u.Roles)}
}

func (r wireRole) domain() (domain.DatabaseRole, error) {
	result := domain.DatabaseRole{Name: r.Name, Database: r.Database, Roles: decodeRoles(r.Roles),
		Privileges: make([]domain.Privilege, len(r.Privileges))}
	for i, p := range r.Privileges {
		resource, err := json.Marshal(p.Resource)
		if err != nil {
			return domain.DatabaseRole{}, fmt.Errorf("map role privilege resource: %w", err)
		}
		result.Privileges[i] = domain.Privilege{Resource: resource, Actions: append([]string{}, p.Actions...)}
	}
	return result, nil
}

func encodeRoles(roles []domain.RoleRef) []wireRoleRef {
	result := make([]wireRoleRef, len(roles))
	for i, r := range roles {
		result[i] = wireRoleRef{Role: r.Role, DB: r.DB}
	}
	return result
}

func decodeRoles(roles []wireRoleRef) []domain.RoleRef {
	result := make([]domain.RoleRef, len(roles))
	for i, r := range roles {
		result[i] = domain.RoleRef{Role: r.Role, DB: r.DB}
	}
	return result
}

func sameRoles(a, b []domain.RoleRef) bool {
	if len(a) != len(b) {
		return false
	}
	aa, bb := append([]domain.RoleRef(nil), a...), append([]domain.RoleRef(nil), b...)
	less := func(roles []domain.RoleRef) func(int, int) bool {
		return func(i, j int) bool {
			if roles[i].DB != roles[j].DB {
				return roles[i].DB < roles[j].DB
			}
			return roles[i].Role < roles[j].Role
		}
	}
	sort.Slice(aa, less(aa))
	sort.Slice(bb, less(bb))
	for i := range aa {
		if aa[i] != bb[i] {
			return false
		}
	}
	return true
}

func invalid(format string, args ...any) error { return engine.Invalid(format, args...) }

func validateName(name string) error {
	if name == "" || !utf8.ValidString(name) || strings.TrimSpace(name) != name || strings.ContainsFunc(name, unicode.IsControl) {
		return invalid("name must be non-empty UTF-8 without control characters or surrounding whitespace")
	}
	return nil
}

func validateDB(db string) error {
	if err := validateName(db); err != nil {
		return err
	}
	// MongoDB database names must contain fewer than 64 bytes. Use the union
	// of Unix/Windows prohibited characters to keep identities portable.
	if len(db) >= 64 || strings.ContainsAny(db, `/\. "$*<>:|?`) {
		return invalid("invalid MongoDB database name %q", db)
	}
	if db == "$external" {
		return invalid("external authentication databases are not password-managed")
	}
	return nil
}

func validateIdentity(db, name string) error {
	if err := validateDB(db); err != nil {
		return err
	}
	return validateName(name)
}

func validateApplicationUser(db, name string) error {
	if err := validateIdentity(db, name); err != nil {
		return err
	}
	if db == "local" {
		return invalid("users cannot be created in the local database")
	}
	if IsSystemUser(name) {
		return invalid("user name %q is reserved for a system user", name)
	}
	return nil
}

func validatePassword(password string) error {
	if password == "" || !utf8.ValidString(password) || strings.ContainsRune(password, '\x00') {
		return invalid("password must be non-empty UTF-8 without NUL")
	}
	return nil
}

func validateRoles(roles []domain.RoleRef) error {
	seen := make(map[domain.RoleRef]bool, len(roles))
	for _, role := range roles {
		if err := validateIdentity(role.DB, role.Role); err != nil {
			return err
		}
		if seen[role] {
			return invalid("duplicate role reference %q in %q", role.Role, role.DB)
		}
		seen[role] = true
	}
	return nil
}

func validateRole(role domain.DatabaseRole) ([]wirePrivilege, error) {
	if err := validateIdentity(role.Database, role.Name); err != nil {
		return nil, err
	}
	if role.Database == "local" {
		return nil, invalid("custom roles cannot be created in local")
	}
	if err := validateRoles(role.Roles); err != nil {
		return nil, err
	}
	for _, inherited := range role.Roles {
		if inherited.DB == role.Database && inherited.Role == role.Name {
			return nil, invalid("a role cannot inherit itself")
		}
		if role.Database != "admin" && inherited.DB != role.Database {
			return nil, invalid("non-admin roles can inherit only roles in their own database")
		}
	}
	privileges := make([]wirePrivilege, len(role.Privileges))
	for i, p := range role.Privileges {
		resource, err := validateResource(p.Resource)
		if err != nil {
			return nil, err
		}
		if role.Database != "admin" && resource["db"] != role.Database {
			return nil, invalid("non-admin roles can grant privileges only on their own database")
		}
		if len(p.Actions) == 0 {
			return nil, invalid("privilege actions cannot be empty")
		}
		seen := make(map[string]bool, len(p.Actions))
		for _, action := range p.Actions {
			if err := validateName(action); err != nil {
				return nil, invalid("invalid privilege action")
			}
			if strings.ContainsFunc(action, unicode.IsSpace) {
				return nil, invalid("privilege action cannot contain whitespace")
			}
			if seen[action] {
				return nil, invalid("duplicate privilege action %q", action)
			}
			seen[action] = true
		}
		privileges[i] = wirePrivilege{Resource: resource, Actions: append([]string{}, p.Actions...)}
	}
	return privileges, nil
}

func validateResource(raw json.RawMessage) (bson.M, error) {
	var resource map[string]any
	if json.Unmarshal(raw, &resource) != nil || len(resource) == 0 {
		return nil, invalid("privilege resource must be a resource document")
	}
	if len(resource) == 1 {
		for _, key := range []string{"cluster", "anyResource"} {
			if value, exists := resource[key]; exists {
				if value != true {
					return nil, invalid("%s resource must be true", key)
				}
				return bson.M{key: true}, nil
			}
		}
	}
	if len(resource) != 2 {
		return nil, invalid("privilege resource must specify db and collection, cluster, or anyResource")
	}
	db, dbOK := resource["db"].(string)
	collection, collectionOK := resource["collection"].(string)
	if !dbOK || !collectionOK {
		return nil, invalid("privilege db and collection must be strings")
	}
	if db != "" {
		if err := validateDB(db); err != nil {
			return nil, err
		}
	}
	if !utf8.ValidString(collection) || strings.ContainsRune(collection, '\x00') {
		return nil, invalid("invalid privilege collection")
	}
	return bson.M{"db": db, "collection": collection}, nil
}
