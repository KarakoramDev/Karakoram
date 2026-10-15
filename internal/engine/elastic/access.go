// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/elastic/esapi"
)

// User is a user-managed native realm identity; reserved identities are hidden.
type User struct {
	Username string   `json:"username"`
	Roles    []string `json:"roles"`
	FullName string   `json:"fullName,omitempty"`
	Email    string   `json:"email,omitempty"`
	Enabled  bool     `json:"enabled"`
}

// UserWrite replaces a native user's properties and optionally its password.
type UserWrite struct {
	User
	Password string `json:"password,omitempty"`
}

// AccessRole is a named security role. Built-ins are readable but immutable.
type AccessRole struct {
	Name     string `json:"name"`
	Reserved bool   `json:"reserved"`
	Role
}

// RoleDescriptor defines API-key permissions without a role name.
type RoleDescriptor = Role

// APIKeyCreate requests a key owned by the control-plane identity.
type APIKeyCreate struct {
	Name            string                    `json:"name"`
	Expiration      string                    `json:"expiration,omitempty"`
	RoleDescriptors map[string]RoleDescriptor `json:"roleDescriptors"`
}

// APIKeyCreated contains a secret returned only when a key is created.
type APIKeyCreated struct {
	ID         string `json:"id"`
	Name       string `json:"name"`
	APIKey     string `json:"apiKey"`
	Encoded    string `json:"encoded"`
	Expiration int64  `json:"expiration,omitempty"`
}

// APIKey is the non-secret inventory of a Karakoram-created key.
type APIKey struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Creation    int64  `json:"creation"`
	Expiration  int64  `json:"expiration,omitempty"`
	Invalidated bool   `json:"invalidated"`
	Username    string `json:"username"`
}

// Access is Elasticsearch-specific native user, role and API-key management.
type Access interface {
	ListUsers(context.Context, *engine.Context) ([]User, error)
	GetUser(context.Context, *engine.Context, string) (*User, error)
	CreateUser(context.Context, *engine.Context, UserWrite) error
	UpdateUser(context.Context, *engine.Context, UserWrite) error
	DeleteUser(context.Context, *engine.Context, string) error
	ListRoles(context.Context, *engine.Context) ([]AccessRole, error)
	GetRole(context.Context, *engine.Context, string) (*AccessRole, error)
	PutRole(context.Context, *engine.Context, AccessRole, bool) error
	DeleteRole(context.Context, *engine.Context, string) error
	ListAPIKeys(context.Context, *engine.Context) ([]APIKey, error)
	CreateAPIKey(context.Context, *engine.Context, APIKeyCreate) (*APIKeyCreated, error)
	InvalidateAPIKey(context.Context, *engine.Context, string) error
}

var _ Access = (*Provider)(nil)

func (p *Provider) accessClient(ctx context.Context, rc *engine.Context) (*esapi.Client, error) {
	s, err := load(rc.Cluster)
	if err != nil {
		return nil, err
	}
	return p.es(ctx, rc, s)
}

func accessError(err error) error {
	if err == nil {
		return nil
	}
	if esapi.IsNotFound(err) {
		return engine.ErrNotFound
	}
	if esapi.IsConflict(err) {
		return engine.ErrConflict
	}
	var e *esapi.Error
	if errors.As(err, &e) {
		if securityLicenseRejected(e) {
			return engine.InvalidField("query", domain.ReasonUnsupported, "query and fieldSecurity require an Elasticsearch licence tier that includes document- and field-level security; the current licence does not include it")
		}
		if e.Status == 400 || e.Status == 422 {
			return engine.ErrInvalid
		}
	}
	return err
}

func securityLicenseRejected(native *esapi.Error) bool {
	if native.Status != 403 || native.Type != "security_exception" {
		return false
	}
	reason := strings.ToLower(native.Reason)
	return (strings.Contains(reason, "license") || strings.Contains(reason, "licence")) &&
		(strings.Contains(reason, "document level security") || strings.Contains(reason, "document-level security") ||
			strings.Contains(reason, "field level security") || strings.Contains(reason, "field-level security"))
}

func protectedAccess(resource, name string) error {
	return domain.Conflict(resource, name, domain.ReasonProtected, engine.ErrInvalid, "%s %q is protected and managed by Elasticsearch or Karakoram; it cannot be changed through native access management", resource, name)
}

func roleWriteError(err error, role Role) error {
	var native *esapi.Error
	if !errors.As(err, &native) {
		return accessError(err)
	}
	if securityLicenseRejected(native) {
		for i, index := range role.Indices {
			field := ""
			if index.Query != nil {
				field = fmt.Sprintf("indices[%d].query", i)
			} else if index.FieldSecurity != nil {
				field = fmt.Sprintf("indices[%d].fieldSecurity", i)
			}
			if field != "" {
				return engine.InvalidField(field, domain.ReasonUnsupported, "%s requires an Elasticsearch licence tier that includes document- and field-level security; the current licence does not include it", field)
			}
		}
	}
	if native.Status == 400 || native.Status == 422 {
		reason := strings.ToLower(native.Reason)
		switch {
		case strings.Contains(reason, "unknown cluster privilege"):
			return engine.InvalidField("cluster", domain.ReasonInvalid, "cluster contains an invalid Elasticsearch privilege; select a supported cluster privilege")
		case strings.Contains(reason, "application privilege"):
			return engine.InvalidField("applications", domain.ReasonInvalid, "applications contains an invalid Elasticsearch application privilege")
		case strings.Contains(reason, "unknown index privilege"):
			for i, index := range role.Indices {
				for _, privilege := range index.Privileges {
					if strings.Contains(reason, "["+strings.ToLower(privilege)+"]") {
						field := fmt.Sprintf("indices[%d].privileges", i)
						return engine.InvalidField(field, domain.ReasonInvalid, "%s contains invalid Elasticsearch privilege %q", field, privilege)
					}
				}
			}
			return engine.InvalidField("indices", domain.ReasonInvalid, "indices contains an invalid Elasticsearch privilege; select supported index privileges")
		}
	}
	return accessError(err)
}
func invalidAccess(err error) error { return fmt.Errorf("%w: %s", engine.ErrInvalid, err) }
func visibleUser(name string, u esapi.User) bool {
	return !ProtectedUserName(name) && u.Metadata["_reserved"] != true
}
func userFromES(name string, u esapi.User) User {
	roles := u.Roles
	if roles == nil {
		roles = []string{}
	}
	return User{Username: name, Roles: roles, FullName: u.FullName, Email: u.Email, Enabled: u.Enabled == nil || *u.Enabled}
}

// ListUsers lists native users, hiding reserved and managed identities.
func (p *Provider) ListUsers(ctx context.Context, rc *engine.Context) ([]User, error) {
	c, err := p.accessClient(ctx, rc)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	users, err := c.GetUsers(ctx)
	if err != nil {
		return nil, accessError(err)
	}
	out := make([]User, 0, len(users))
	for name, u := range users {
		if visibleUser(name, u) {
			out = append(out, userFromES(name, u))
		}
	}
	slices.SortFunc(out, func(a, b User) int { return strings.Compare(a.Username, b.Username) })
	return out, nil
}

// GetUser reads a visible native realm identity.
func (p *Provider) GetUser(ctx context.Context, rc *engine.Context, name string) (*User, error) {
	if ProtectedUserName(name) {
		return nil, protectedAccess("elasticsearchUser", name)
	}
	c, err := p.accessClient(ctx, rc)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	users, err := c.GetUsers(ctx, name)
	if err != nil {
		return nil, accessError(err)
	}
	u, ok := users[name]
	if !ok {
		return nil, engine.ErrNotFound
	}
	if !visibleUser(name, u) {
		return nil, protectedAccess("elasticsearchUser", name)
	}
	out := userFromES(name, u)
	return &out, nil
}
func validateUser(u UserWrite, create bool) error {
	if ProtectedUserName(u.Username) {
		return protectedAccess("elasticsearchUser", u.Username)
	}
	if err := ValidateUserName(u.Username); err != nil {
		return engine.InvalidField("username", domain.ReasonInvalid, "username: %s", err)
	}
	if (create || u.Password != "") && utf8.RuneCountInString(u.Password) < 8 {
		return engine.InvalidField("password", domain.ReasonOutOfRange, "password must contain at least 8 characters")
	}
	for i, r := range u.Roles {
		if !securityName.MatchString(r) {
			return engine.InvalidField(fmt.Sprintf("roles[%d]", i), domain.ReasonInvalid, "roles[%d] contains invalid role name %q", i, r)
		}
	}
	return nil
}
func (p *Provider) putUser(ctx context.Context, rc *engine.Context, u UserWrite, create bool) error {
	if err := validateUser(u, create); err != nil {
		return err
	}
	c, err := p.accessClient(ctx, rc)
	if err != nil {
		return err
	}
	defer c.Close()
	users, err := c.GetUsers(ctx, u.Username)
	if err != nil && !esapi.IsNotFound(err) {
		return accessError(err)
	}
	existing, exists := users[u.Username]
	if exists && !visibleUser(u.Username, existing) {
		return protectedAccess("elasticsearchUser", u.Username)
	}
	if create && exists {
		return domain.Conflict("elasticsearchUser", u.Username, domain.ReasonAlreadyExists, engine.ErrConflict, "Elasticsearch user %q already exists; edit the existing user instead", u.Username)
	}
	if !create && !exists {
		return engine.ErrNotFound
	}
	if len(u.Roles) > 0 {
		roles, err := c.GetRoles(ctx)
		if err != nil {
			return accessError(err)
		}
		for i, name := range u.Roles {
			if _, exists := roles[name]; !exists {
				return engine.InvalidField(fmt.Sprintf("roles[%d]", i), domain.ReasonNotFound, "roles[%d]: Elasticsearch role %q does not exist; create or select an existing role first", i, name)
			}
		}
	}
	return accessError(c.PutUser(ctx, u.Username, esapi.User{Password: u.Password, Roles: u.Roles, FullName: u.FullName, Email: u.Email, Enabled: &u.Enabled}))
}

// CreateUser creates a native realm user, rejecting an existing identity.
func (p *Provider) CreateUser(ctx context.Context, rc *engine.Context, u UserWrite) error {
	return p.putUser(ctx, rc, u, true)
}

// UpdateUser replaces an existing native realm user.
func (p *Provider) UpdateUser(ctx context.Context, rc *engine.Context, u UserWrite) error {
	return p.putUser(ctx, rc, u, false)
}

// DeleteUser removes an existing native realm user.
func (p *Provider) DeleteUser(ctx context.Context, rc *engine.Context, name string) error {
	if ProtectedUserName(name) {
		return protectedAccess("elasticsearchUser", name)
	}
	if err := ValidateUserName(name); err != nil {
		return engine.InvalidField("username", domain.ReasonInvalid, "username: %s", err)
	}
	c, err := p.accessClient(ctx, rc)
	if err != nil {
		return err
	}
	defer c.Close()
	users, err := c.GetUsers(ctx, name)
	if err != nil {
		return accessError(err)
	}
	u, exists := users[name]
	if !exists {
		return engine.ErrNotFound
	}
	if !visibleUser(name, u) {
		return protectedAccess("elasticsearchUser", name)
	}
	return accessError(c.DeleteUser(ctx, name))
}
func roleFromES(name string, r esapi.Role) AccessRole {
	out := AccessRole{Name: name, Reserved: ProtectedRoleName(name) || r.Metadata["_reserved"] == true, Role: Role{Cluster: r.Cluster, Indices: make([]IndexPrivileges, len(r.Indices)), Applications: make([]ApplicationPrivileges, len(r.Applications))}}
	if out.Cluster == nil {
		out.Cluster = []string{}
	}
	for i, v := range r.Indices {
		out.Indices[i] = IndexPrivileges{Names: v.Names, Privileges: v.Privileges, AllowRestrictedIndices: v.AllowRestrictedIndices, Query: v.Query}
		if v.FieldSecurity != nil {
			out.Indices[i].FieldSecurity = &FieldSecurity{Grant: v.FieldSecurity.Grant, Except: v.FieldSecurity.Except}
		}
	}
	for i, v := range r.Applications {
		out.Applications[i] = ApplicationPrivileges{Application: v.Application, Privileges: v.Privileges, Resources: v.Resources}
	}
	return out
}
func roleToES(r Role) esapi.Role {
	out := esapi.Role{Cluster: r.Cluster, Indices: make([]esapi.IndexPrivileges, len(r.Indices)), Applications: make([]esapi.ApplicationPrivileges, len(r.Applications))}
	for i, v := range r.Indices {
		out.Indices[i] = esapi.IndexPrivileges{Names: v.Names, Privileges: v.Privileges, AllowRestrictedIndices: v.AllowRestrictedIndices, Query: v.Query}
		if v.FieldSecurity != nil {
			out.Indices[i].FieldSecurity = &esapi.FieldSecurity{Grant: v.FieldSecurity.Grant, Except: v.FieldSecurity.Except}
		}
	}
	for i, v := range r.Applications {
		out.Applications[i] = esapi.ApplicationPrivileges{Application: v.Application, Privileges: v.Privileges, Resources: v.Resources}
	}
	return out
}

// ListRoles lists user roles and readable built-ins, hiding managed roles.
func (p *Provider) ListRoles(ctx context.Context, rc *engine.Context) ([]AccessRole, error) {
	c, err := p.accessClient(ctx, rc)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	roles, err := c.GetRoles(ctx)
	if err != nil {
		return nil, accessError(err)
	}
	out := make([]AccessRole, 0, len(roles))
	for name, r := range roles {
		if !strings.HasPrefix(name, "karakoram") {
			out = append(out, roleFromES(name, r))
		}
	}
	slices.SortFunc(out, func(a, b AccessRole) int { return strings.Compare(a.Name, b.Name) })
	return out, nil
}

// GetRole reads a user role or built-in role.
func (p *Provider) GetRole(ctx context.Context, rc *engine.Context, name string) (*AccessRole, error) {
	if strings.HasPrefix(name, "karakoram") {
		return nil, engine.ErrNotFound
	}
	c, err := p.accessClient(ctx, rc)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	roles, err := c.GetRoles(ctx, name)
	if err != nil {
		return nil, accessError(err)
	}
	r, ok := roles[name]
	if !ok {
		return nil, engine.ErrNotFound
	}
	out := roleFromES(name, r)
	return &out, nil
}

// PutRole creates (create=true) or replaces an existing role (create=false).
func (p *Provider) PutRole(ctx context.Context, rc *engine.Context, r AccessRole, create bool) error {
	if ProtectedRoleName(r.Name) {
		return protectedAccess("elasticsearchRole", r.Name)
	}
	if err := ValidateRoleName(r.Name); err != nil {
		return engine.InvalidField("name", domain.ReasonInvalid, "name: %s", err)
	}
	c, err := p.accessClient(ctx, rc)
	if err != nil {
		return err
	}
	defer c.Close()
	roles, err := c.GetRoles(ctx, r.Name)
	if err != nil && !esapi.IsNotFound(err) {
		return accessError(err)
	}
	existing, exists := roles[r.Name]
	if existing.Metadata["_reserved"] == true {
		return protectedAccess("elasticsearchRole", r.Name)
	}
	if create && exists {
		return domain.Conflict("elasticsearchRole", r.Name, domain.ReasonAlreadyExists, engine.ErrConflict, "Elasticsearch role %q already exists; edit the existing role instead", r.Name)
	}
	if !create && !exists {
		return engine.ErrNotFound
	}
	return roleWriteError(c.PutRole(ctx, r.Name, roleToES(r.Role)), r.Role)
}

// DeleteRole removes a user-managed native role.
func (p *Provider) DeleteRole(ctx context.Context, rc *engine.Context, name string) error {
	if ProtectedRoleName(name) {
		return protectedAccess("elasticsearchRole", name)
	}
	if err := ValidateRoleName(name); err != nil {
		return engine.InvalidField("name", domain.ReasonInvalid, "name: %s", err)
	}
	c, err := p.accessClient(ctx, rc)
	if err != nil {
		return err
	}
	defer c.Close()
	roles, err := c.GetRoles(ctx, name)
	if err != nil {
		return accessError(err)
	}
	r, ok := roles[name]
	if !ok {
		return engine.ErrNotFound
	}
	if r.Metadata["_reserved"] == true {
		return protectedAccess("elasticsearchRole", name)
	}
	return accessError(c.DeleteRole(ctx, name))
}
func managedKey(k esapi.APIKeyInfo) bool {
	return k.Username == AdminUser && k.Metadata["karakoram"] == true
}

// ListAPIKeys lists only keys created through Karakoram, without their secrets.
func (p *Provider) ListAPIKeys(ctx context.Context, rc *engine.Context) ([]APIKey, error) {
	c, err := p.accessClient(ctx, rc)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	keys, err := c.GetAPIKeys(ctx, true, "")
	if err != nil {
		return nil, accessError(err)
	}
	out := make([]APIKey, 0, len(keys))
	for _, k := range keys {
		if managedKey(k) {
			out = append(out, APIKey{ID: k.ID, Name: k.Name, Creation: k.Creation, Expiration: k.Expiration, Invalidated: k.Invalidated, Username: k.Username})
		}
	}
	slices.SortFunc(out, func(a, b APIKey) int { return strings.Compare(a.ID, b.ID) })
	return out, nil
}

// CreateAPIKey creates a managed key and returns its one-time secret.
func (p *Provider) CreateAPIKey(ctx context.Context, rc *engine.Context, req APIKeyCreate) (*APIKeyCreated, error) {
	if strings.TrimSpace(req.Name) == "" {
		return nil, engine.ErrInvalid
	}
	if len(req.RoleDescriptors) == 0 {
		return nil, invalidAccess(errors.New("API key requires at least one role descriptor"))
	}
	c, err := p.accessClient(ctx, rc)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	roles := make(map[string]esapi.Role, len(req.RoleDescriptors))
	for name, r := range req.RoleDescriptors {
		roles[name] = roleToES(r)
	}
	k, err := c.CreateAPIKey(ctx, esapi.APIKeyRequest{Name: req.Name, Expiration: req.Expiration, RoleDescriptors: roles, Metadata: map[string]any{"karakoram": true}})
	if err != nil {
		return nil, accessError(err)
	}
	return &APIKeyCreated{ID: k.ID, Name: k.Name, APIKey: k.APIKey, Encoded: k.Encoded, Expiration: k.Expiration}, nil
}

// InvalidateAPIKey revokes only a key created through Karakoram.
func (p *Provider) InvalidateAPIKey(ctx context.Context, rc *engine.Context, id string) error {
	c, err := p.accessClient(ctx, rc)
	if err != nil {
		return err
	}
	defer c.Close()
	keys, err := c.GetAPIKeys(ctx, true, "")
	if err != nil {
		return accessError(err)
	}
	for _, key := range keys {
		if key.ID == id && managedKey(key) {
			if key.Invalidated {
				return nil
			}
			return accessError(c.InvalidateAPIKeys(ctx, []string{id}))
		}
	}
	return engine.ErrNotFound
}
