// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package weaviate

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/weaviate/wvapi"
)

type User struct {
	Name       string     `json:"name"`
	UserType   string     `json:"userType"`
	Active     bool       `json:"active"`
	Roles      []string   `json:"roles"`
	CreatedAt  *time.Time `json:"createdAt,omitempty"`
	LastUsedAt *time.Time `json:"lastUsedAt,omitempty"`
}
type UserCreate struct {
	Name  string   `json:"name"`
	Roles []string `json:"roles"`
}
type UserCreated struct {
	User   User   `json:"user"`
	APIKey string `json:"apiKey"`
}
type UserUpdate struct {
	Roles  []string `json:"roles"`
	Active *bool    `json:"active,omitempty"`
}
type Role = wvapi.Role
type Permission = wvapi.Permission

// Access manages dynamic database identities. Keys are returned only at creation
// and rotation, and are never included in ordinary user views or native errors.
type Access interface {
	ListUsers(context.Context, *engine.Context) ([]User, error)
	GetUser(context.Context, *engine.Context, string) (*User, error)
	CreateUser(context.Context, *engine.Context, UserCreate) (*UserCreated, error)
	UpdateUser(context.Context, *engine.Context, string, UserUpdate) error
	DeleteUser(context.Context, *engine.Context, string) error
	RotateUserKey(context.Context, *engine.Context, string) (string, error)
	ListRoles(context.Context, *engine.Context) ([]Role, error)
	GetRole(context.Context, *engine.Context, string) (*Role, error)
	CreateRole(context.Context, *engine.Context, Role) error
	ReplaceRole(context.Context, *engine.Context, Role) error
	DeleteRole(context.Context, *engine.Context, string) error
}

var _ Access = (*Provider)(nil)

// These constraints and built-ins mirror Weaviate 1.40.1 handlers_authz.go,
// handlers_db_users.go, and usecases/auth/authorization/types.go.
var accessUserPattern = regexp.MustCompile(`^[A-Za-z][-_0-9A-Za-z@.]*$`)
var accessRolePattern = regexp.MustCompile(`^[A-Za-z][-_0-9A-Za-z]*$`)

func accessUserName(name string) error {
	if len(name) > 128 || !accessUserPattern.MatchString(name) {
		return engine.Invalid("user name must start with an ASCII letter, contain only letters, digits, -, _, @ or ., and be at most 128 bytes")
	}
	return nil
}
func accessRoleName(name string) error {
	if len(name) > 64 || !accessRolePattern.MatchString(name) {
		return engine.Invalid("role name must start with an ASCII letter, contain only letters, digits, - or _, and be at most 64 bytes")
	}
	return nil
}
func accessProtectedUser(name string) error {
	if name == AdminUser || name == ClusterUser {
		return fmt.Errorf("weaviate identity is managed by Karakoram and is read-only: %w", engine.ErrConflict)
	}
	return nil
}
func accessProtectedRole(name string) error {
	if slices.Contains([]string{"root", "admin", "viewer", "read-only", "operator_metadata_reader"}, name) {
		return fmt.Errorf("weaviate built-in roles are read-only: %w", engine.ErrConflict)
	}
	return nil
}
func accessError(err error) error {
	if err == nil {
		return nil
	}
	var native *wvapi.Error
	if errors.As(err, &native) {
		switch native.Status {
		case 404:
			return fmt.Errorf("weaviate access object not found: %w", engine.ErrNotFound)
		case 409:
			return fmt.Errorf("weaviate access object already exists or conflicts: %w", engine.ErrConflict)
		case 400, 422:
			return engine.Invalid("Weaviate rejected the access request")
		}
	}
	// Never surface native response text: it can contain credentials, including
	// a just-created key when a subsequent role assignment fails.
	return fmt.Errorf("weaviate native access is unavailable: %w", engine.ErrUnavailable)
}
func (p *Provider) accessClient(ctx context.Context, rc *engine.Context) (*wvapi.Client, error) {
	if rc == nil || rc.Cluster == nil {
		return nil, accessError(engine.ErrUnavailable)
	}
	s, err := load(rc.Cluster)
	if err != nil || s.Suspended || s.Phase == "cleanup" || s.Phase == "restoring" {
		return nil, accessError(engine.ErrUnavailable)
	}
	names := []string{}
	for _, m := range ordered(s) {
		if !m.Removing && (m.Stage == "ready" || m.Stage == "deployed") {
			names = append(names, m.Name)
		}
	}
	if len(names) == 0 {
		return nil, accessError(engine.ErrUnavailable)
	}
	c, err := p.native(ctx, rc, s, names...)
	if err != nil {
		return nil, accessError(err)
	}
	return c, nil
}
func accessTime(v *string) *time.Time {
	if v == nil {
		return nil
	}
	t, err := time.Parse(time.RFC3339Nano, *v)
	if err != nil {
		return nil
	}
	return &t
}
func accessUser(v wvapi.DBUser) User {
	roles := v.Roles
	if roles == nil {
		roles = []string{}
	}
	return User{Name: v.UserID, UserType: v.DBUserType, Active: v.Active, Roles: roles, CreatedAt: accessTime(v.CreatedAt), LastUsedAt: accessTime(v.LastUsedAt)}
}
func mutableAccessUser(ctx context.Context, c *wvapi.Client, name string) (wvapi.DBUser, error) {
	if err := accessUserName(name); err != nil {
		return wvapi.DBUser{}, err
	}
	if err := accessProtectedUser(name); err != nil {
		return wvapi.DBUser{}, err
	}
	v, err := c.DBUser(ctx, name, false)
	if err != nil {
		return v, accessError(err)
	}
	if v.DBUserType == "db_env_user" {
		return v, fmt.Errorf("weaviate environment users are read-only: %w", engine.ErrConflict)
	}
	return v, nil
}
func accessRolesValid(roles []string) error {
	for _, name := range roles {
		if err := accessRoleName(name); err != nil {
			return err
		}
	}
	return nil
}
func (p *Provider) ListUsers(ctx context.Context, rc *engine.Context) ([]User, error) {
	c, err := p.accessClient(ctx, rc)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	values, err := c.DBUsers(ctx, true)
	if err != nil {
		return nil, accessError(err)
	}
	out := []User{}
	for _, v := range values {
		if v.DBUserType != "db_env_user" && v.UserID != AdminUser && v.UserID != ClusterUser {
			out = append(out, accessUser(v))
		}
	}
	return out, nil
}
func (p *Provider) GetUser(ctx context.Context, rc *engine.Context, name string) (*User, error) {
	if err := accessUserName(name); err != nil {
		return nil, err
	}
	c, err := p.accessClient(ctx, rc)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	v, err := c.DBUser(ctx, name, true)
	if err != nil {
		return nil, accessError(err)
	}
	out := accessUser(v)
	return &out, nil
}
func (p *Provider) CreateUser(ctx context.Context, rc *engine.Context, v UserCreate) (*UserCreated, error) {
	if err := accessUserName(v.Name); err != nil {
		return nil, err
	}
	if err := accessProtectedUser(v.Name); err != nil {
		return nil, err
	}
	if err := accessRolesValid(v.Roles); err != nil {
		return nil, err
	}
	c, err := p.accessClient(ctx, rc)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	key, err := c.CreateDBUser(ctx, v.Name)
	if err != nil {
		return nil, accessError(err)
	}
	rollback := func(cause error) (*UserCreated, error) {
		// Cleanup must survive cancellation of the incoming request.
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), engine.ObservationTimeout)
		defer cancel()
		if err := c.DeleteDBUser(cleanup, v.Name); err != nil {
			return nil, fmt.Errorf("weaviate user provisioning failed and rollback could not delete the new identity: %w", engine.ErrUnavailable)
		}
		return nil, accessError(cause)
	}
	if len(v.Roles) > 0 {
		if err = c.AssignDBUserRoles(ctx, v.Name, v.Roles); err != nil {
			return rollback(err)
		}
	}
	native, err := c.DBUser(ctx, v.Name, true)
	if err != nil {
		return rollback(err)
	}
	return &UserCreated{User: accessUser(native), APIKey: key.APIKey}, nil
}
func (p *Provider) UpdateUser(ctx context.Context, rc *engine.Context, name string, v UserUpdate) error {
	if err := accessProtectedUser(name); err != nil {
		return err
	}
	if err := accessUserName(name); err != nil {
		return err
	}
	if err := accessRolesValid(v.Roles); err != nil {
		return err
	}
	c, err := p.accessClient(ctx, rc)
	if err != nil {
		return err
	}
	defer c.Close()
	current, err := mutableAccessUser(ctx, c, name)
	if err != nil {
		return err
	}
	roles, err := c.DBUserRoles(ctx, name)
	if err != nil {
		return accessError(err)
	}
	old := make([]string, 0, len(roles))
	for _, role := range roles {
		old = append(old, role.Name)
	}
	add, remove := accessStringDiff(old, v.Roles)
	if len(remove) > 0 {
		if err = c.RevokeDBUserRoles(ctx, name, remove); err != nil {
			return accessError(err)
		}
	}
	if len(add) > 0 {
		if err = c.AssignDBUserRoles(ctx, name, add); err != nil {
			return accessError(err)
		}
	}
	if v.Active != nil && *v.Active != current.Active {
		if *v.Active {
			err = c.ActivateDBUser(ctx, name)
		} else {
			err = c.DeactivateDBUser(ctx, name, false)
		}
		return accessError(err)
	}
	return nil
}
func accessStringDiff(old, want []string) (add, remove []string) {
	for _, v := range want {
		if !slices.Contains(old, v) && !slices.Contains(add, v) {
			add = append(add, v)
		}
	}
	for _, v := range old {
		if !slices.Contains(want, v) && !slices.Contains(remove, v) {
			remove = append(remove, v)
		}
	}
	return
}
func (p *Provider) DeleteUser(ctx context.Context, rc *engine.Context, name string) error {
	if err := accessProtectedUser(name); err != nil {
		return err
	}
	if err := accessUserName(name); err != nil {
		return err
	}
	c, err := p.accessClient(ctx, rc)
	if err != nil {
		return err
	}
	defer c.Close()
	if _, err = mutableAccessUser(ctx, c, name); err != nil {
		return err
	}
	return accessError(c.DeleteDBUser(ctx, name))
}
func (p *Provider) RotateUserKey(ctx context.Context, rc *engine.Context, name string) (string, error) {
	if err := accessProtectedUser(name); err != nil {
		return "", err
	}
	if err := accessUserName(name); err != nil {
		return "", err
	}
	c, err := p.accessClient(ctx, rc)
	if err != nil {
		return "", err
	}
	defer c.Close()
	if _, err = mutableAccessUser(ctx, c, name); err != nil {
		return "", err
	}
	key, err := c.RotateDBUserKey(ctx, name)
	if err != nil {
		return "", accessError(err)
	}
	return key.APIKey, nil
}
func (p *Provider) ListRoles(ctx context.Context, rc *engine.Context) ([]Role, error) {
	c, err := p.accessClient(ctx, rc)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	v, err := c.Roles(ctx)
	if err != nil {
		return nil, accessError(err)
	}
	if v == nil {
		v = []Role{}
	}
	return v, nil
}
func (p *Provider) GetRole(ctx context.Context, rc *engine.Context, name string) (*Role, error) {
	if err := accessRoleName(name); err != nil {
		return nil, err
	}
	c, err := p.accessClient(ctx, rc)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	v, err := c.Role(ctx, name)
	if err != nil {
		return nil, accessError(err)
	}
	return &v, nil
}
func accessRoleValid(v Role) error {
	if err := accessProtectedRole(v.Name); err != nil {
		return err
	}
	if err := accessRoleName(v.Name); err != nil {
		return err
	}
	for _, perm := range v.Permissions {
		if perm.Action == "" {
			return engine.Invalid("permission action is required")
		}
		if _, err := json.Marshal(perm); err != nil {
			return engine.Invalid("permissions must contain valid JSON")
		}
	}
	return nil
}
func (p *Provider) CreateRole(ctx context.Context, rc *engine.Context, v Role) error {
	if err := accessRoleValid(v); err != nil {
		return err
	}
	c, err := p.accessClient(ctx, rc)
	if err != nil {
		return err
	}
	defer c.Close()
	return accessError(c.CreateRole(ctx, v))
}
func accessPermissionKey(v Permission) string {
	raw, _ := json.Marshal(v)
	// Unknown native scope fields are raw JSON; normalize their object key
	// ordering as well, so equivalent permissions do not churn on replacement.
	var canonical any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	_ = decoder.Decode(&canonical)
	raw, _ = json.Marshal(canonical)
	return string(raw)
}
func accessPermissionDiff(old, want []Permission) (add, remove []Permission) {
	before := map[string]bool{}
	after := map[string]bool{}
	for _, v := range old {
		before[accessPermissionKey(v)] = true
	}
	for _, v := range want {
		after[accessPermissionKey(v)] = true
	}
	for _, v := range want {
		key := accessPermissionKey(v)
		if !before[key] {
			add = append(add, v)
			before[key] = true
		}
	}
	for _, v := range old {
		key := accessPermissionKey(v)
		if !after[key] {
			remove = append(remove, v)
			after[key] = true
		}
	}
	return
}
func (p *Provider) ReplaceRole(ctx context.Context, rc *engine.Context, v Role) error {
	if err := accessRoleValid(v); err != nil {
		return err
	}
	c, err := p.accessClient(ctx, rc)
	if err != nil {
		return err
	}
	defer c.Close()
	old, err := c.Role(ctx, v.Name)
	if err != nil {
		return accessError(err)
	}
	add, remove := accessPermissionDiff(old.Permissions, v.Permissions)
	if len(remove) > 0 {
		if err = c.RemovePermissions(ctx, v.Name, remove); err != nil {
			return accessError(err)
		}
	}
	if len(add) > 0 {
		return accessError(c.AddPermissions(ctx, v.Name, add))
	}
	return nil
}
func (p *Provider) DeleteRole(ctx context.Context, rc *engine.Context, name string) error {
	if err := accessProtectedRole(name); err != nil {
		return err
	}
	if err := accessRoleName(name); err != nil {
		return err
	}
	c, err := p.accessClient(ctx, rc)
	if err != nil {
		return err
	}
	defer c.Close()
	return accessError(c.DeleteRole(ctx, name))
}
