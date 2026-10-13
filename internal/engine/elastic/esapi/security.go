// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package esapi

import (
	"context"
	"errors"
	"net/url"
)

// User is a native user definition. Password is write-only and omitted when empty.
type User struct {
	Password string         `json:"password,omitempty"`
	Roles    []string       `json:"roles"`
	FullName string         `json:"full_name,omitempty"`
	Email    string         `json:"email,omitempty"`
	Enabled  *bool          `json:"enabled,omitempty"`
	Metadata map[string]any `json:"metadata,omitempty"`
}

// IndexPrivileges grants privileges to the selected index patterns.
type IndexPrivileges struct {
	Names                  []string `json:"names"`
	Privileges             []string `json:"privileges"`
	AllowRestrictedIndices bool     `json:"allow_restricted_indices"`
}

// ApplicationPrivileges grants application-defined privileges.
type ApplicationPrivileges struct {
	Application string   `json:"application"`
	Privileges  []string `json:"privileges"`
	Resources   []string `json:"resources"`
}

// Role is a native role or API-key role descriptor.
type Role struct {
	Cluster      []string                `json:"cluster,omitempty"`
	Indices      []IndexPrivileges       `json:"indices,omitempty"`
	Metadata     map[string]any          `json:"metadata,omitempty"`
	Applications []ApplicationPrivileges `json:"applications,omitempty"`
}

// ChangePassword changes a native or built-in user's password.
func (c *Client) ChangePassword(ctx context.Context, user, password string) error {
	return c.doSecret(ctx, "POST", "/_security/user/"+segment(user)+"/_password", nil, map[string]string{"password": password}, nil, []string{password})
}

// PutUser creates or updates a native user.
func (c *Client) PutUser(ctx context.Context, name string, user User) error {
	if user.Roles == nil {
		user.Roles = []string{}
	}
	return c.doSecret(ctx, "PUT", "/_security/user/"+segment(name), nil, user, nil, []string{user.Password})
}

// GetUsers returns named native and built-in users, or all users when names is empty.
func (c *Client) GetUsers(ctx context.Context, names ...string) (map[string]User, error) {
	var v map[string]User
	p := "/_security/user"
	if len(names) > 0 {
		p += "/" + listPath(names)
	}
	err := c.do(ctx, "GET", p, nil, nil, &v)
	for name, u := range v {
		u.Password = ""
		v[name] = u
	}
	return v, err
}

// DeleteUser deletes a native user.
func (c *Client) DeleteUser(ctx context.Context, name string) error {
	return c.do(ctx, "DELETE", "/_security/user/"+segment(name), nil, nil, nil)
}

// PutRole creates or updates a native role.
func (c *Client) PutRole(ctx context.Context, name string, role Role) error {
	return c.do(ctx, "PUT", "/_security/role/"+segment(name), nil, role, nil)
}

// GetRoles returns named roles, or all roles when names is empty.
func (c *Client) GetRoles(ctx context.Context, names ...string) (map[string]Role, error) {
	var v map[string]Role
	p := "/_security/role"
	if len(names) > 0 {
		p += "/" + listPath(names)
	}
	err := c.do(ctx, "GET", p, nil, nil, &v)
	return v, err
}

// DeleteRole deletes a native role.
func (c *Client) DeleteRole(ctx context.Context, name string) error {
	return c.do(ctx, "DELETE", "/_security/role/"+segment(name), nil, nil, nil)
}
func tokenPath(namespace, service, name string) string {
	return "/_security/service/" + segment(namespace) + "/" + segment(service) + "/credential/token/" + segment(name)
}

// CreateServiceToken creates a cluster-wide service account bearer token.
func (c *Client) CreateServiceToken(ctx context.Context, namespace, service, name string) (string, error) {
	var v struct {
		Token struct {
			Value string `json:"value"`
		} `json:"token"`
	}
	err := c.do(ctx, "POST", tokenPath(namespace, service, name), nil, nil, &v)
	if err != nil {
		return "", err
	}
	if v.Token.Value == "" {
		return "", errors.New("service token response from Elasticsearch is empty")
	}
	return v.Token.Value, nil
}

// DeleteServiceToken deletes an API-created service account token.
func (c *Client) DeleteServiceToken(ctx context.Context, namespace, service, name string) error {
	return c.do(ctx, "DELETE", tokenPath(namespace, service, name), nil, nil, nil)
}

// Authentication identifies the current credential's principal and mechanism.
type Authentication struct {
	Username           string `json:"username"`
	AuthenticationType string `json:"authentication_type"`
}

// Authenticate validates the configured basic or bearer credential.
func (c *Client) Authenticate(ctx context.Context) (Authentication, error) {
	var v Authentication
	err := c.do(ctx, "GET", "/_security/_authenticate", nil, nil, &v)
	return v, err
}

// APIKeyRequest specifies a new API key and optional scoped permissions.
type APIKeyRequest struct {
	Name            string          `json:"name"`
	Expiration      string          `json:"expiration,omitempty"`
	RoleDescriptors map[string]Role `json:"role_descriptors,omitempty"`
	Metadata        map[string]any  `json:"metadata,omitempty"`
}

// APIKey contains the key's one-time secret and expiration in Unix milliseconds.
type APIKey struct {
	ID         string `json:"id"`
	Name       string `json:"name"`
	APIKey     string `json:"api_key"`
	Encoded    string `json:"encoded"`
	Expiration int64  `json:"expiration"`
}

// APIKeyInfo is non-secret API-key inventory, with timestamps in Unix milliseconds.
type APIKeyInfo struct {
	ID          string         `json:"id"`
	Name        string         `json:"name"`
	Creation    int64          `json:"creation"`
	Expiration  int64          `json:"expiration"`
	Invalidated bool           `json:"invalidated"`
	Username    string         `json:"username"`
	Realm       string         `json:"realm"`
	Metadata    map[string]any `json:"metadata"`
}

// CreateAPIKey creates a key, returning its secret only in this response.
func (c *Client) CreateAPIKey(ctx context.Context, req APIKeyRequest) (APIKey, error) {
	var v APIKey
	err := c.do(ctx, "POST", "/_security/api_key", nil, req, &v)
	return v, err
}

// GetAPIKeys lists keys, optionally restricting to the owner and a name pattern.
func (c *Client) GetAPIKeys(ctx context.Context, owner bool, name string) ([]APIKeyInfo, error) {
	q := boolQuery("owner", owner)
	if name != "" {
		q.Set("name", name)
	}
	var v struct {
		Keys []APIKeyInfo `json:"api_keys"`
	}
	err := c.do(ctx, "GET", "/_security/api_key", q, nil, &v)
	return v.Keys, err
}

// InvalidateAPIKeys revokes the supplied key IDs and reports per-key failures.
func (c *Client) InvalidateAPIKeys(ctx context.Context, ids []string) error {
	var v struct {
		ErrorCount int `json:"error_count"`
	}
	if err := c.do(ctx, "DELETE", "/_security/api_key", url.Values{}, map[string]any{"ids": ids}, &v); err != nil {
		return err
	}
	if v.ErrorCount > 0 {
		return errors.New("API key invalidation failed on Elasticsearch")
	}
	return nil
}
