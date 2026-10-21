// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package wvapi

import (
	"context"
	"encoding/json"
	"net/url"
	"strconv"
)

// Ready succeeds only when the selected member's readiness endpoint succeeds.
func (c *Client) Ready(ctx context.Context) error {
	return c.do(ctx, "GET", "/v1/.well-known/ready", nil, nil, nil)
}
func (c *Client) Meta(ctx context.Context) (Meta, error) {
	var v Meta
	err := c.do(ctx, "GET", "/v1/meta", nil, nil, &v)
	return v, err
}

// Nodes requests verbose shard and object statistics.
func (c *Client) Nodes(ctx context.Context) ([]Node, error) {
	var v struct {
		Nodes []Node `json:"nodes"`
	}
	err := c.do(ctx, "GET", "/v1/nodes", url.Values{"output": {"verbose"}}, nil, &v)
	return v.Nodes, err
}
func (c *Client) ClusterStatistics(ctx context.Context) (ClusterStatistics, error) {
	var v ClusterStatistics
	err := c.do(ctx, "GET", "/v1/cluster/statistics", nil, nil, &v)
	return v, err
}

// RemoveNode uses the private cluster API, not the public REST port. Construct
// this client with NewInternal and the cluster basic-auth credentials.
func (c *Client) RemoveNode(ctx context.Context, node string) error {
	return c.do(ctx, "POST", "/v1/cluster/remove", nil, struct {
		Node string `json:"node"`
	}{node}, nil)
}

// Collection is the small schema subset needed for data-presence helpers and
// destructive restore. Schema configuration outside this subset is not managed.
type Collection struct {
	Class             string          `json:"class"`
	Properties        []Property      `json:"properties,omitempty"`
	Vectorizer        string          `json:"vectorizer,omitempty"`
	VectorIndexType   string          `json:"vectorIndexType,omitempty"`
	VectorIndexConfig json.RawMessage `json:"vectorIndexConfig,omitempty"`
	ReplicationConfig json.RawMessage `json:"replicationConfig,omitempty"`
}
type Property struct {
	Name     string   `json:"name"`
	DataType []string `json:"dataType"`
}

func (c *Client) Collections(ctx context.Context) ([]Collection, error) {
	var v struct {
		Classes []Collection `json:"classes"`
	}
	err := c.do(ctx, "GET", "/v1/schema", nil, nil, &v)
	return v.Classes, err
}
func (c *Client) CreateCollection(ctx context.Context, req Collection) (Collection, error) {
	var v Collection
	err := c.do(ctx, "POST", "/v1/schema", nil, req, &v)
	return v, err
}
func (c *Client) DeleteCollection(ctx context.Context, name string) error {
	return c.do(ctx, "DELETE", "/v1/schema/"+segment(name), nil, nil, nil)
}
func (c *Client) Shards(ctx context.Context, collection string) ([]ShardStatus, error) {
	var v []ShardStatus
	err := c.do(ctx, "GET", "/v1/schema/"+segment(collection)+"/shards", nil, nil, &v)
	return v, err
}
func (c *Client) ShardingState(ctx context.Context, collection string) (ShardingStateResponse, error) {
	var v ShardingStateResponse
	err := c.do(ctx, "GET", "/v1/replication/sharding-state", url.Values{"collection": {collection}}, nil, &v)
	return v, err
}
func (c *Client) Replicate(ctx context.Context, req ReplicateRequest) (ReplicateResponse, error) {
	var v ReplicateResponse
	err := c.do(ctx, "POST", "/v1/replication/replicate", nil, req, &v)
	return v, err
}
func (c *Client) Replication(ctx context.Context, id string) (Replication, error) {
	var v Replication
	err := c.do(ctx, "GET", "/v1/replication/replicate/"+segment(id), nil, nil, &v)
	return v, err
}
func (c *Client) Replications(ctx context.Context) ([]Replication, error) {
	var v []Replication
	err := c.do(ctx, "GET", "/v1/replication/replicate/list", nil, nil, &v)
	return v, err
}
func (c *Client) CancelReplication(ctx context.Context, id string) error {
	return c.do(ctx, "POST", "/v1/replication/replicate/"+segment(id)+"/cancel", nil, nil, nil)
}

func userPath(id string) string { return "/v1/users/db/" + segment(id) }
func (c *Client) DBUsers(ctx context.Context, includeLastUsedTime bool) ([]DBUser, error) {
	var v []DBUser
	err := c.do(ctx, "GET", "/v1/users/db", url.Values{"includeLastUsedTime": {strconv.FormatBool(includeLastUsedTime)}}, nil, &v)
	return v, err
}
func (c *Client) DBUser(ctx context.Context, id string, includeLastUsedTime bool) (DBUser, error) {
	var v DBUser
	err := c.do(ctx, "GET", userPath(id), url.Values{"includeLastUsedTime": {strconv.FormatBool(includeLastUsedTime)}}, nil, &v)
	return v, err
}

// CreateDBUser returns a secret exactly once. Callers must not log or persist it
// outside their credential store.
func (c *Client) CreateDBUser(ctx context.Context, id string) (APIKey, error) {
	var v APIKey
	err := c.do(ctx, "POST", userPath(id), nil, nil, &v)
	return v, err
}
func (c *Client) DeleteDBUser(ctx context.Context, id string) error {
	return c.do(ctx, "DELETE", userPath(id), nil, nil, nil)
}

// RotateDBUserKey invalidates the previous key. The result is a secret.
func (c *Client) RotateDBUserKey(ctx context.Context, id string) (APIKey, error) {
	var v APIKey
	err := c.do(ctx, "POST", userPath(id)+"/rotate-key", nil, nil, &v)
	return v, err
}
func (c *Client) ActivateDBUser(ctx context.Context, id string) error {
	return c.do(ctx, "POST", userPath(id)+"/activate", nil, nil, nil)
}
func (c *Client) DeactivateDBUser(ctx context.Context, id string, revokeKey bool) error {
	return c.do(ctx, "POST", userPath(id)+"/deactivate", nil, struct {
		RevokeKey bool `json:"revoke_key"`
	}{revokeKey}, nil)
}
func (c *Client) AssignDBUserRoles(ctx context.Context, id string, roles []string) error {
	return c.userRoles(ctx, id, "assign", roles)
}
func (c *Client) RevokeDBUserRoles(ctx context.Context, id string, roles []string) error {
	return c.userRoles(ctx, id, "revoke", roles)
}
func (c *Client) userRoles(ctx context.Context, id, action string, roles []string) error {
	return c.do(ctx, "POST", "/v1/authz/users/"+segment(id)+"/"+action, nil, struct {
		Roles    []string `json:"roles"`
		UserType string   `json:"userType"`
	}{roles, "db"}, nil)
}
func (c *Client) DBUserRoles(ctx context.Context, id string) ([]Role, error) {
	var v []Role
	err := c.do(ctx, "GET", "/v1/authz/users/"+segment(id)+"/roles/db", url.Values{"includeFullRoles": {"true"}}, nil, &v)
	return v, err
}
func (c *Client) Roles(ctx context.Context) ([]Role, error) {
	var v []Role
	err := c.do(ctx, "GET", "/v1/authz/roles", nil, nil, &v)
	return v, err
}
func (c *Client) Role(ctx context.Context, id string) (Role, error) {
	var v Role
	err := c.do(ctx, "GET", "/v1/authz/roles/"+segment(id), nil, nil, &v)
	return v, err
}
func (c *Client) CreateRole(ctx context.Context, role Role) error {
	return c.do(ctx, "POST", "/v1/authz/roles", nil, role, nil)
}
func (c *Client) DeleteRole(ctx context.Context, id string) error {
	return c.do(ctx, "DELETE", "/v1/authz/roles/"+segment(id), nil, nil, nil)
}
func (c *Client) AddPermissions(ctx context.Context, id string, permissions []Permission) error {
	return c.permissions(ctx, id, "add-permissions", permissions)
}
func (c *Client) RemovePermissions(ctx context.Context, id string, permissions []Permission) error {
	return c.permissions(ctx, id, "remove-permissions", permissions)
}
func (c *Client) permissions(ctx context.Context, id, action string, permissions []Permission) error {
	return c.do(ctx, "POST", "/v1/authz/roles/"+segment(id)+"/"+action, nil, struct {
		Permissions []Permission `json:"permissions"`
	}{permissions}, nil)
}

// BackupLocation overrides the backup module's configured bucket and path.
type BackupLocation struct{ Bucket, Path string }

func (l BackupLocation) query() url.Values {
	q := url.Values{}
	if l.Bucket != "" {
		q.Set("bucket", l.Bucket)
	}
	if l.Path != "" {
		q.Set("path", l.Path)
	}
	return q
}

// BackupListEntry includes the collection selection and completion metadata.
type BackupListEntry struct {
	BackupStatus
	Classes []string `json:"classes"`
}

func (c *Client) CreateBackup(ctx context.Context, req BackupRequest) (Backup, error) {
	var v Backup
	err := c.do(ctx, "POST", "/v1/backups/s3", nil, req, &v)
	return v, err
}
func (c *Client) BackupStatus(ctx context.Context, id string, location BackupLocation) (BackupStatus, error) {
	var v BackupStatus
	err := c.do(ctx, "GET", "/v1/backups/s3/"+segment(id), location.query(), nil, &v)
	return v, err
}
func (c *Client) Backups(ctx context.Context) ([]BackupListEntry, error) {
	var v []BackupListEntry
	err := c.do(ctx, "GET", "/v1/backups/s3", nil, nil, &v)
	return v, err
}

// CancelBackup cancels an in-flight backup; it does not delete stored artifacts.
func (c *Client) CancelBackup(ctx context.Context, id string, location BackupLocation) error {
	return c.do(ctx, "DELETE", "/v1/backups/s3/"+segment(id), location.query(), nil, nil)
}
func (c *Client) Restore(ctx context.Context, id string, req RestoreRequest) (Restore, error) {
	var v Restore
	err := c.do(ctx, "POST", "/v1/backups/s3/"+segment(id)+"/restore", nil, req, &v)
	return v, err
}
func (c *Client) RestoreStatus(ctx context.Context, id string, location BackupLocation) (RestoreStatus, error) {
	var v RestoreStatus
	err := c.do(ctx, "GET", "/v1/backups/s3/"+segment(id)+"/restore", location.query(), nil, &v)
	return v, err
}

// Object is the vector-bearing subset used by data-presence helpers.
type Object struct {
	Class      string         `json:"class"`
	ID         string         `json:"id,omitempty"`
	Properties map[string]any `json:"properties,omitempty"`
	Vector     []float32      `json:"vector,omitempty"`
}

// ObjectResult includes the native per-object batch outcome. HTTP success alone
// does not imply that every object was inserted.
type ObjectResult struct {
	Object
	Result struct {
		Status string        `json:"status"`
		Errors *ObjectErrors `json:"errors,omitempty"`
	} `json:"result"`
}
type ObjectErrors struct {
	Error []struct {
		Message string `json:"message"`
	} `json:"error"`
}

func (c *Client) BatchObjects(ctx context.Context, objects []Object) ([]ObjectResult, error) {
	var v []ObjectResult
	err := c.do(ctx, "POST", "/v1/batch/objects", nil, struct {
		Objects []Object `json:"objects"`
	}{objects}, &v)
	return v, err
}

// Objects returns a bounded page plus the native totalResults count. No vector
// search abstraction is needed for checking that restored data is present.
func (c *Client) Objects(ctx context.Context, collection string, limit int) (ObjectsPage, error) {
	var v ObjectsPage
	err := c.do(ctx, "GET", "/v1/objects", url.Values{"class": {collection}, "limit": {strconv.Itoa(limit)}}, nil, &v)
	return v, err
}

type ObjectsPage struct {
	Objects      []Object `json:"objects"`
	TotalResults int64    `json:"totalResults"`
}
