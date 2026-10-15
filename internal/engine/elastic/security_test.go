// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"reflect"
	"strings"
	"testing"

	"golang.org/x/crypto/bcrypt"
	"gopkg.in/yaml.v3"
)

func TestSecurityNames(t *testing.T) {
	for _, tt := range []struct {
		name  string
		valid bool
	}{
		{"reader", true}, {"Reader_1@tenant.example-test", true}, {strings.Repeat("a", 64), true}, {strings.Repeat("a", 65), false}, {"", false}, {" leading", false}, {"trailing ", false}, {"new\nline", false}, {"colon:name", false}, {"é", false}, {"karakoram_custom", false}, {"karakoram", false}, {"elastic", false}, {"kibana", false}, {"kibana_system", false}, {"logstash_system", false}, {"beats_system", false}, {"apm_system", false}, {"remote_monitoring_user", false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if (ValidateUserName(tt.name) == nil) != tt.valid || (ValidateRoleName(tt.name) == nil) != tt.valid {
				t.Fatal("security name validity")
			}
		})
	}
	for _, name := range protectedUsers {
		if !ProtectedUserName(name) {
			t.Fatal("unprotected built-in user")
		}
	}
	for _, name := range protectedRoles {
		if !ProtectedRoleName(name) || ValidateRoleName(name) == nil {
			t.Fatal("unprotected built-in role")
		}
	}
	for _, name := range []string{AdminUser, MonitorUser, LogstashUser, APMUser, "karakoram-anything"} {
		if !ProtectedUserName(name) {
			t.Fatal("unprotected provider user")
		}
	}
	for _, name := range []string{MonitorRole, LogstashWriterRole, APMWriterRole} {
		if !ProtectedRoleName(name) {
			t.Fatal("unprotected provider role")
		}
	}
}

func TestFileRealm(t *testing.T) {
	users := map[string]string{AdminUser: "test-admin-password", MonitorUser: "test-monitor-password"}
	roles := map[string][]string{"superuser": {AdminUser}, MonitorRole: {MonitorUser, MonitorUser}}
	first, mapping, err := RenderFileRealm(users, roles, "")
	if err != nil {
		t.Fatal(err)
	}
	if mapping != "karakoram_monitor:karakoram-monitor\nsuperuser:karakoram\n" {
		t.Fatalf("role mapping: %q", mapping)
	}
	for _, line := range strings.Split(strings.TrimSpace(first), "\n") {
		name, hash, ok := strings.Cut(line, ":")
		if !ok {
			t.Fatal("invalid users line")
		}
		if !strings.HasPrefix(hash, "$2a$10$") {
			t.Fatal("hash format")
		}
		if err := bcrypt.CompareHashAndPassword([]byte(hash), []byte(users[name])); err != nil {
			t.Fatal("password mismatch")
		}
	}
	for _, tt := range []struct {
		name, previous string
		changed        bool
	}{
		{"reuse", first, false}, {"malformed", "karakoram:not-a-hash\n", true}, {"wrong password", "karakoram:" + strings.Split(strings.Split(first, "\n")[1], ":")[1] + "\n", true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, gotRoles, err := RenderFileRealm(users, roles, tt.previous)
			if err != nil {
				t.Fatal(err)
			}
			if (got != first) != tt.changed || gotRoles != mapping {
				t.Fatal("render stability")
			}
		})
	}
	changedUsers := map[string]string{AdminUser: "rotated-password", MonitorUser: users[MonitorUser]}
	rotated, _, err := RenderFileRealm(changedUsers, roles, first)
	if err != nil {
		t.Fatal(err)
	}
	oldLines, newLines := strings.Split(first, "\n"), strings.Split(rotated, "\n")
	if oldLines[0] == newLines[0] || oldLines[1] != newLines[1] {
		t.Fatal("rotation must change only the changed password hash")
	}
	for _, tt := range []struct {
		name  string
		users map[string]string
		roles map[string][]string
	}{
		{"invalid user", map[string]string{"a:b": "secret"}, nil}, {"long password", map[string]string{"user": strings.Repeat("x", 73)}, nil}, {"invalid role", users, map[string][]string{"a:b": {AdminUser}}}, {"unknown user", users, map[string][]string{"reader": {"missing"}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			u, r, err := RenderFileRealm(tt.users, tt.roles, "")
			if err == nil || u != "" || r != "" || strings.Contains(err.Error(), "secret") {
				t.Fatal("unsafe file realm error")
			}
		})
	}
	empty, emptyRoles, err := RenderFileRealm(nil, nil, first)
	if err != nil || empty != "" || emptyRoles != "" {
		t.Fatal("empty realm")
	}
}

func TestRoleDefinitions(t *testing.T) {
	for _, tt := range []struct {
		name      string
		got, want Role
	}{
		{"monitor", MonitorRoleDefinition(), Role{Cluster: []string{"monitor"}, Indices: []IndexPrivileges{{Names: []string{"*"}, Privileges: []string{"monitor", "view_index_metadata"}, AllowRestrictedIndices: true}}}},
		{"logstash", LogstashRoleDefinition(), Role{Cluster: []string{"monitor", "manage_index_templates", "manage_ilm", "read_ilm"}, Indices: []IndexPrivileges{{Names: []string{"logstash-*", "ecs-logstash-*", "logs-*", "metrics-*"}, Privileges: []string{"write", "create", "create_index", "create_doc", "auto_configure", "manage", "manage_ilm", "view_index_metadata"}}}}},
		{"apm", APMRoleDefinition(), Role{Cluster: []string{"monitor"}, Indices: []IndexPrivileges{{Names: []string{"traces-apm*", "logs-apm*", "metrics-apm*"}, Privileges: []string{"auto_configure", "create_doc"}}, {Names: []string{".apm-agent-configuration", ".apm-source-map"}, Privileges: []string{"read"}, AllowRestrictedIndices: true}}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if !reflect.DeepEqual(tt.got, tt.want) {
				t.Fatalf("role: %+v", tt.got)
			}
		})
	}
	doc, err := RenderFileRoles()
	if err != nil {
		t.Fatal(err)
	}
	var parsed map[string]Role
	if err = yaml.Unmarshal([]byte(doc), &parsed); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(parsed, map[string]Role{MonitorRole: MonitorRoleDefinition()}) {
		t.Fatalf("roles YAML: %s", doc)
	}
	role := MonitorRoleDefinition()
	role.Cluster[0] = "all"
	if MonitorRoleDefinition().Cluster[0] != "monitor" {
		t.Fatal("mutable shared role slices")
	}
}
