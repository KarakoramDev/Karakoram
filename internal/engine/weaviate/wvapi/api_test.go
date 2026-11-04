// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package wvapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"testing"
)

// These fixtures use the property spelling and enum values from the pinned
// v1.40.1 OpenAPI definitions. Those definitions publish no response examples
// for these operations; values below are representative, not captured secrets.
const nodeFixture = `{"nodes":[{"name":"member-0","status":"HEALTHY","version":"1.40.1","stats":{"shardCount":1,"objectCount":42},"shards":[{"name":"shard-a","class":"Article","objectCount":42,"vectorIndexingStatus":"READY","loaded":true}]}]}`
const userFixture = `{"userId":"alice","dbUserType":"db_user","active":true,"roles":["reader"],"createdAt":"2026-10-01T12:00:00Z","lastUsedAt":"2026-10-02T12:00:00Z","apiKeyFirstLetters":"abc"}`
const roleFixture = `{"name":"reader","permissions":[{"action":"read_data","data":{"collection":"Article","tenant":"*"}}]}`
const replicationFixture = `{"id":"3aa1e131-b30f-4c49-bb76-93a591902061","collection":"Article","shard":"shard-a","sourceNode":"member-0","targetNode":"member-3","type":"MOVE","status":{"state":"READY","errors":[{"message":"transient connection failure","whenErroredUnixMs":1234}]},"whenStartedUnixMs":1230}`
const backupFixture = `{"id":"backup-1","classes":["Article"],"backend":"s3","bucket":"archive","path":"prefix/backup-1","status":"STARTED"}`
const backupStatusFixture = `{"id":"backup-1","backend":"s3","path":"prefix/backup-1","status":"SUCCESS","startedAt":"2026-10-01T12:00:00Z","completedAt":"2026-10-01T12:01:00Z","size":0.5}`

func TestTypedResponses(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name, path, query, fixture, want string
		call                             func(*Client) (string, error)
	}{
		{"meta", "/v1/meta", "", `{"version":"1.40.1","hostname":"https://member-0:8080","modules":{"backup-s3":{"version":"1.40.1"}}}`, "1.40.1", func(c *Client) (string, error) {
			v, e := c.Meta(ctx)
			if !strings.Contains(string(v.Modules), "backup-s3") {
				t.Error("modules missing")
			}
			return v.Version, e
		}},
		{"nodes", "/v1/nodes", "output=verbose", nodeFixture, "Article:42", func(c *Client) (string, error) {
			v, e := c.Nodes(ctx)
			if e != nil {
				return "", e
			}
			if len(v) != 1 || len(v[0].Shards) != 1 {
				t.Fatal("missing nodes/shards")
			}
			return fmt.Sprintf("%s:%d", v[0].Shards[0].Class, v[0].Stats.ObjectCount), e
		}},
		{"statistics", "/v1/cluster/statistics", "", `{"synchronized":true,"statistics":[{"name":"member-0","isVoter":true,"ready":true,"dbLoaded":true,"leaderId":"member-0","raft":{"state":"Leader","numPeers":"2","commitIndex":"123"}}]}`, "true:true:Leader", func(c *Client) (string, error) {
			v, e := c.ClusterStatistics(ctx)
			if e != nil {
				return "", e
			}
			if len(v.Statistics) != 1 {
				t.Fatal("missing raft statistics")
			}
			return fmt.Sprintf("%t:%t:%s", v.Synchronized, v.Statistics[0].IsVoter, v.Statistics[0].Raft.State), e
		}},
		{"collections", "/v1/schema", "", `{"classes":[{"class":"Article","vectorizer":"none","properties":[{"name":"title","dataType":["text"]}]}]}`, "Article", func(c *Client) (string, error) {
			v, e := c.Collections(ctx)
			if e != nil {
				return "", e
			}
			if len(v) != 1 {
				t.Fatal("missing collections")
			}
			return v[0].Class, e
		}},
		{"shards", "/v1/schema/Article/shards", "", `[{"name":"shard-a","status":"READY","per_node_status":{"member-0":"READY"},"vectorQueueSize":0}]`, "READY", func(c *Client) (string, error) {
			v, e := c.Shards(ctx, "Article")
			if e != nil {
				return "", e
			}
			if len(v) != 1 {
				t.Fatal("missing shards")
			}
			return v[0].PerNodeStatus["member-0"], e
		}},
		{"placement", "/v1/replication/sharding-state", "collection=Article", `{"shardingState":{"collection":"Article","shards":[{"shard":"shard-a","replicas":["member-0","member-1"]}]}}`, "member-0,member-1", func(c *Client) (string, error) {
			v, e := c.ShardingState(ctx, "Article")
			if e != nil {
				return "", e
			}
			if len(v.ShardingState.Shards) != 1 {
				t.Fatal("missing placement")
			}
			return strings.Join(v.ShardingState.Shards[0].Replicas, ","), e
		}},
		{"replication", "/v1/replication/replicate/move-1", "", replicationFixture, "READY:transient connection failure", func(c *Client) (string, error) {
			v, e := c.Replication(ctx, "move-1")
			if e != nil {
				return "", e
			}
			if len(v.Status.Errors) != 1 {
				t.Fatal("missing movement errors")
			}
			return string(v.Status.State) + ":" + v.Status.Errors[0].Message, e
		}},
		{"replications", "/v1/replication/replicate/list", "", "[" + replicationFixture + "]", "MOVE", func(c *Client) (string, error) {
			v, e := c.Replications(ctx)
			if e != nil {
				return "", e
			}
			if len(v) != 1 {
				t.Fatal("missing movements")
			}
			return string(v[0].Type), e
		}},
		{"users", "/v1/users/db", "includeLastUsedTime=true", "[" + userFixture + "]", "alice", func(c *Client) (string, error) {
			v, e := c.DBUsers(ctx, true)
			if e != nil {
				return "", e
			}
			if len(v) != 1 || v[0].LastUsedAt == nil {
				t.Fatal("missing users/last use")
			}
			return v[0].UserID, e
		}},
		{"user", "/v1/users/db/alice", "includeLastUsedTime=false", userFixture, "db_user", func(c *Client) (string, error) { v, e := c.DBUser(ctx, "alice", false); return v.DBUserType, e }},
		{"roles", "/v1/authz/roles", "", "[" + roleFixture + "]", "Article", func(c *Client) (string, error) {
			v, e := c.Roles(ctx)
			if e != nil {
				return "", e
			}
			if len(v) != 1 || len(v[0].Permissions) != 1 || v[0].Permissions[0].Data == nil {
				t.Fatal("missing roles/scopes")
			}
			return v[0].Permissions[0].Data.Collection, e
		}},
		{"role", "/v1/authz/roles/reader", "", roleFixture, "reader", func(c *Client) (string, error) { v, e := c.Role(ctx, "reader"); return v.Name, e }},
		{"user roles", "/v1/authz/users/alice/roles/db", "includeFullRoles=true", "[" + roleFixture + "]", "reader", func(c *Client) (string, error) {
			v, e := c.DBUserRoles(ctx, "alice")
			if e != nil {
				return "", e
			}
			if len(v) != 1 {
				t.Fatal("missing user roles")
			}
			return v[0].Name, e
		}},
		{"backup status", "/v1/backups/s3/backup-1", "bucket=archive&path=prefix", backupStatusFixture, "SUCCESS:0.5", func(c *Client) (string, error) {
			v, e := c.BackupStatus(ctx, "backup-1", BackupLocation{"archive", "prefix"})
			return fmt.Sprintf("%s:%.1f", v.Status, v.Size), e
		}},
		{"backups", "/v1/backups/s3", "", `[{"id":"backup-1","classes":["Article"],"status":"SUCCESS","size":0.5,"completedAt":"2026-10-01T12:01:00Z"}]`, "Article:SUCCESS", func(c *Client) (string, error) {
			v, e := c.Backups(ctx)
			if e != nil {
				return "", e
			}
			if len(v) != 1 || len(v[0].Classes) != 1 {
				t.Fatal("missing backups")
			}
			return v[0].Classes[0] + ":" + string(v[0].Status), e
		}},
		{"restore status", "/v1/backups/s3/backup-1/restore", "bucket=archive&path=prefix", `{"id":"backup-1","backend":"s3","path":"prefix/backup-1","status":"SUCCESS"}`, "SUCCESS", func(c *Client) (string, error) {
			v, e := c.RestoreStatus(ctx, "backup-1", BackupLocation{"archive", "prefix"})
			return string(v.Status), e
		}},
		{"objects", "/v1/objects", "class=Article&limit=1", `{"objects":[{"class":"Article","id":"c54d2c80-f91a-4e99-8c29-ad6f46b7afff","properties":{"title":"hello"},"vector":[0.1,0.2]}],"totalResults":42}`, "hello:42", func(c *Client) (string, error) {
			v, e := c.Objects(ctx, "Article", 1)
			if e != nil {
				return "", e
			}
			if len(v.Objects) != 1 {
				t.Fatal("missing objects")
			}
			return fmt.Sprintf("%s:%d", v.Objects[0].Properties["title"], v.TotalResults), e
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, _ := plainClient(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method != "GET" || r.URL.EscapedPath() != tt.path || r.URL.RawQuery != tt.query {
					t.Errorf("request %s %s", r.Method, r.URL.String())
				}
				fmt.Fprint(w, tt.fixture)
			})
			got, err := tt.call(c)
			if err != nil || got != tt.want {
				t.Fatalf("got %q, %v; want %q", got, err, tt.want)
			}
		})
	}
}

func TestMutations(t *testing.T) {
	ctx := context.Background()
	loc := BackupLocation{"archive", "prefix"}
	tests := []struct {
		name, method, path, body, response string
		call                               func(*Client) error
	}{
		{"create user", "POST", "/v1/users/db/alice", "", `{"apikey":"new-user-secret"}`, func(c *Client) error {
			v, e := c.CreateDBUser(ctx, "alice")
			if e == nil && v.APIKey != "new-user-secret" {
				t.Error("missing key")
			}
			return e
		}},
		{"rotate user", "POST", "/v1/users/db/alice/rotate-key", "", `{"apikey":"rotated-user-secret"}`, func(c *Client) error {
			v, e := c.RotateDBUserKey(ctx, "alice")
			if e == nil && v.APIKey != "rotated-user-secret" {
				t.Error("missing rotated key")
			}
			return e
		}},
		{"delete user", "DELETE", "/v1/users/db/alice", "", "", func(c *Client) error { return c.DeleteDBUser(ctx, "alice") }},
		{"activate", "POST", "/v1/users/db/alice/activate", "", "", func(c *Client) error { return c.ActivateDBUser(ctx, "alice") }},
		{"deactivate", "POST", "/v1/users/db/alice/deactivate", `{"revoke_key":true}`, "", func(c *Client) error { return c.DeactivateDBUser(ctx, "alice", true) }},
		{"assign", "POST", "/v1/authz/users/alice/assign", `{"roles":["reader"],"userType":"db"}`, "", func(c *Client) error { return c.AssignDBUserRoles(ctx, "alice", []string{"reader"}) }},
		{"revoke", "POST", "/v1/authz/users/alice/revoke", `{"roles":["reader"],"userType":"db"}`, "", func(c *Client) error { return c.RevokeDBUserRoles(ctx, "alice", []string{"reader"}) }},
		{"create role", "POST", "/v1/authz/roles", `{"name":"reader","permissions":[{"action":"read_cluster"}]}`, "", func(c *Client) error {
			return c.CreateRole(ctx, Role{Name: "reader", Permissions: []Permission{{Action: "read_cluster"}}})
		}},
		{"delete role", "DELETE", "/v1/authz/roles/reader", "", "", func(c *Client) error { return c.DeleteRole(ctx, "reader") }},
		{"add permission", "POST", "/v1/authz/roles/reader/add-permissions", `{"permissions":[{"action":"read_cluster"}]}`, "", func(c *Client) error { return c.AddPermissions(ctx, "reader", []Permission{{Action: "read_cluster"}}) }},
		{"remove permission", "POST", "/v1/authz/roles/reader/remove-permissions", `{"permissions":[{"action":"read_cluster"}]}`, "", func(c *Client) error {
			return c.RemovePermissions(ctx, "reader", []Permission{{Action: "read_cluster"}})
		}},
		{"delete collection", "DELETE", "/v1/schema/Article", "", "", func(c *Client) error { return c.DeleteCollection(ctx, "Article") }},
		{"create collection", "POST", "/v1/schema", `{"class":"Article","vectorizer":"none"}`, `{"class":"Article","vectorizer":"none"}`, func(c *Client) error {
			v, e := c.CreateCollection(ctx, Collection{Class: "Article", Vectorizer: "none"})
			if e == nil && v.Class != "Article" {
				t.Error("missing collection")
			}
			return e
		}},
		{"move", "POST", "/v1/replication/replicate", `{"collection":"Article","shard":"shard-a","sourceNode":"member-0","targetNode":"member-3","type":"MOVE"}`, `{"id":"move-1"}`, func(c *Client) error {
			v, e := c.Replicate(ctx, ReplicateRequest{Collection: "Article", Shard: "shard-a", SourceNode: "member-0", TargetNode: "member-3", Type: ReplicaMove})
			if e == nil && v.ID != "move-1" {
				t.Error("missing replication id")
			}
			return e
		}},
		{"cancel movement", "POST", "/v1/replication/replicate/move-1/cancel", "", "", func(c *Client) error { return c.CancelReplication(ctx, "move-1") }},
		{"backup", "POST", "/v1/backups/s3", `{"id":"backup-1","include":["Article"],"config":{"Endpoint":"s3.example.com","Bucket":"archive","Path":"prefix","CPUPercentage":50,"CompressionLevel":"BestSpeed"}}`, backupFixture, func(c *Client) error {
			v, e := c.CreateBackup(ctx, BackupRequest{ID: "backup-1", Include: []string{"Article"}, Config: BackupConfig{Endpoint: "s3.example.com", Bucket: "archive", Path: "prefix", CPUPercentage: 50, CompressionLevel: "BestSpeed"}})
			if e == nil && (v.Status != BackupStarted || v.Bucket != "archive") {
				t.Error("missing backup")
			}
			return e
		}},
		{"cancel backup", "DELETE", "/v1/backups/s3/backup-1?bucket=archive&path=prefix", "", "", func(c *Client) error { return c.CancelBackup(ctx, "backup-1", loc) }},
		{"restore", "POST", "/v1/backups/s3/backup-1/restore", `{"config":{"Endpoint":"s3.example.com","Bucket":"archive","Path":"prefix","CPUPercentage":50,"rolesOptions":"all","usersOptions":"noRestore"},"include":["Article"],"node_mapping":{"old-0":"member-0"}}`, backupFixture, func(c *Client) error {
			v, e := c.Restore(ctx, "backup-1", RestoreRequest{Include: []string{"Article"}, NodeMapping: map[string]string{"old-0": "member-0"}, Config: RestoreConfig{Endpoint: "s3.example.com", Bucket: "archive", Path: "prefix", CPUPercentage: 50, RolesOptions: RestoreAll, UsersOptions: RestoreNone}})
			if e == nil && v.Status != BackupStarted {
				t.Error("missing restore")
			}
			return e
		}},
		{"batch", "POST", "/v1/batch/objects", `{"objects":[{"class":"Article","id":"object-1","properties":{"title":"hello"},"vector":[0.1,0.2]}]}`, `[{"class":"Article","id":"object-1","result":{"status":"SUCCESS"}},{"class":"Article","id":"object-2","result":{"status":"FAILED","errors":{"error":[{"message":"invalid vector"}]}}}]`, func(c *Client) error {
			v, e := c.BatchObjects(ctx, []Object{{Class: "Article", ID: "object-1", Properties: map[string]any{"title": "hello"}, Vector: []float32{0.1, 0.2}}})
			if e == nil && (len(v) != 2 || v[0].Result.Status != "SUCCESS" || v[1].Result.Errors == nil) {
				t.Error("missing per-object results")
			}
			return e
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, _ := plainClient(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method != tt.method || r.URL.String() != tt.path {
					t.Errorf("request %s %s", r.Method, r.URL.String())
				}
				if tt.body != "" {
					var got, want any
					if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
						t.Error(err)
					}
					if err := json.Unmarshal([]byte(tt.body), &want); err != nil {
						t.Fatal(err)
					}
					if !reflect.DeepEqual(got, want) {
						t.Errorf("body got %#v want %#v", got, want)
					}
				}
				if tt.response == "" {
					w.WriteHeader(204)
				} else {
					fmt.Fprint(w, tt.response)
				}
			})
			if err := tt.call(c); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestPermissionUnknownFieldsRoundTrip(t *testing.T) {
	input := `{"action":"read_data","data":{"collection":"Article","tenant":"tenant-1","futureScope":{"selected":true}},"cluster":{"region":"eu"},"futurePermission":[1,2]}`
	var p Permission
	if err := json.Unmarshal([]byte(input), &p); err != nil {
		t.Fatal(err)
	}
	if p.Data == nil || p.Data.Collection != "Article" {
		t.Fatal("typed scope missing")
	}
	output, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	var got, want any
	json.Unmarshal(output, &got)
	json.Unmarshal([]byte(input), &want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("permission round-trip: %s", output)
	}
	// Unknown data must never be able to override a typed action.
	p.Extra["action"] = json.RawMessage(`"delete_data"`)
	output, err = json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(output), "delete_data") {
		t.Fatal("unknown field overrode typed action")
	}
}

func TestBackupEmptyIdentitySelection(t *testing.T) {
	empty := []string{}
	v := BackupRequest{IncludeUsers: &empty, IncludeRoles: &empty}
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `"includeUsers":[]`) || !strings.Contains(string(data), `"includeRoles":[]`) {
		t.Fatalf("empty selector became all identities: %s", data)
	}
}
