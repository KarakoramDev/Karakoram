// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"

	"golang.org/x/crypto/bcrypt"
	"gopkg.in/yaml.v3"
)

// Provider-owned security identities.
const (
	AdminUser          = "karakoram"
	MonitorUser        = "karakoram-monitor"
	MonitorRole        = "karakoram_monitor"
	LogstashUser       = "karakoram_logstash"
	LogstashWriterRole = "karakoram_logstash_writer"
	APMUser            = "karakoram_apm"
	APMWriterRole      = "karakoram_apm_writer"
)

// Role is the provider's JSON/YAML security privilege definition.
type Role struct {
	Cluster      []string                `json:"cluster" yaml:"cluster"`
	Indices      []IndexPrivileges       `json:"indices" yaml:"indices"`
	Applications []ApplicationPrivileges `json:"applications,omitempty" yaml:"applications,omitempty"`
}

// IndexPrivileges grants privileges over a set of index patterns.
type IndexPrivileges struct {
	Names                  []string `json:"names" yaml:"names"`
	Privileges             []string `json:"privileges" yaml:"privileges"`
	AllowRestrictedIndices bool     `json:"allow_restricted_indices" yaml:"allow_restricted_indices"`
}

// ApplicationPrivileges grants application-defined privileges.
type ApplicationPrivileges struct {
	Application string   `json:"application" yaml:"application"`
	Privileges  []string `json:"privileges" yaml:"privileges"`
	Resources   []string `json:"resources" yaml:"resources"`
}

// MonitorRoleDefinition returns fresh slices for the least-privilege monitor.
func MonitorRoleDefinition() Role {
	return Role{Cluster: []string{"monitor"}, Indices: []IndexPrivileges{{Names: []string{"*"}, Privileges: []string{"monitor", "view_index_metadata"}, AllowRestrictedIndices: true}}}
}

// LogstashRoleDefinition returns the managed Logstash writer privileges.
func LogstashRoleDefinition() Role {
	return Role{Cluster: []string{"monitor", "manage_index_templates", "manage_ilm", "read_ilm"}, Indices: []IndexPrivileges{{Names: []string{"logstash-*", "ecs-logstash-*", "logs-*", "metrics-*"}, Privileges: []string{"write", "create", "create_index", "create_doc", "auto_configure", "manage", "manage_ilm", "view_index_metadata"}}}}
}

// APMRoleDefinition returns the managed event writer and configuration reader.
func APMRoleDefinition() Role {
	return Role{Cluster: []string{"monitor"}, Indices: []IndexPrivileges{
		{Names: []string{"traces-apm*", "logs-apm*", "metrics-apm*"}, Privileges: []string{"auto_configure", "create_doc"}},
		{Names: []string{".apm-agent-configuration", ".apm-source-map"}, Privileges: []string{"read"}, AllowRestrictedIndices: true},
	}}
}

var securityName = regexp.MustCompile(`^[a-zA-Z0-9_@.\-]{1,64}$`)
var protectedUsers = []string{"elastic", "kibana", "kibana_system", "logstash_system", "beats_system", "apm_system", "remote_monitoring_user"}

// Built-in roles are immutable in Elasticsearch. This includes older names
// still protected on the supported 8.19 bridge release.
var protectedRoles = []string{"elastic", "kibana", "kibana_system", "logstash_system", "beats_system", "apm_system", "remote_monitoring_user", "beats_admin", "editor", "enrich_user", "inference_admin", "inference_user", "ingest_admin", "kibana_dashboard_only_user", "kibana_admin", "kibana_user", "logstash_admin", "machine_learning_admin", "machine_learning_user", "monitoring_user", "remote_monitoring_agent", "remote_monitoring_collector", "reporting_user", "rollup_admin", "rollup_user", "snapshot_user", "superuser", "transform_admin", "transform_user", "transport_client", "viewer", "watcher_admin", "watcher_user"}

// ProtectedUserName identifies built-in and provider-owned identities.
func ProtectedUserName(name string) bool {
	return strings.HasPrefix(name, "karakoram") || slices.Contains(protectedUsers, name)
}

// ProtectedRoleName identifies built-in and provider-owned roles.
func ProtectedRoleName(name string) bool {
	return strings.HasPrefix(name, "karakoram") || slices.Contains(protectedRoles, name)
}

// ValidateUserName validates names for user-managed native users, excluding
// reserved identities. Karakoram deliberately accepts a narrower ES-safe set.
func ValidateUserName(name string) error {
	if !securityName.MatchString(name) {
		return errors.New("user name must be 1..64 ASCII letters, digits, underscores, @, dots or hyphens")
	}
	if ProtectedUserName(name) {
		return errors.New("user name is reserved")
	}
	return nil
}

// ValidateRoleName validates names for user-managed roles, excluding built-ins.
func ValidateRoleName(name string) error {
	if !securityName.MatchString(name) {
		return errors.New("role name must be 1..64 ASCII letters, digits, underscores, @, dots or hyphens")
	}
	if ProtectedRoleName(name) {
		return errors.New("role name is reserved")
	}
	return nil
}

// RenderFileRealm renders users (name -> password) and roles (role -> usernames).
// previousUsersFile is the persisted prior rendering: matching bcrypt hashes are
// reused to prevent salted-hash churn. New hashes use $2a$ at cost 10, the format
// demonstrated by Elastic's file-realm documentation:
// https://www.elastic.co/docs/deploy-manage/users-roles/cluster-or-deployment-auth/file-based
// Internal/reserved identities are allowed here, but all names remain file-safe.
func RenderFileRealm(users map[string]string, roles map[string][]string, previousUsersFile string) (usersFile, usersRolesFile string, err error) {
	previous := make(map[string]string)
	for _, line := range strings.Split(previousUsersFile, "\n") {
		if name, hash, ok := strings.Cut(line, ":"); ok {
			previous[name] = hash
		}
	}
	var userLines, roleLines strings.Builder
	for _, name := range sortedKeys(users) {
		if !securityName.MatchString(name) {
			return "", "", errors.New("invalid file-realm user name")
		}
		password := []byte(users[name])
		// bcrypt truncates inputs above 72 bytes in some implementations. Never
		// reuse a hash for an ambiguous longer password.
		if len(password) > 72 {
			return "", "", errors.New("file-realm passwords must not exceed 72 bytes")
		}
		hash := previous[name]
		if !(strings.HasPrefix(hash, "$2a$") || strings.HasPrefix(hash, "$2y$") || strings.HasPrefix(hash, "$2b$")) || bcrypt.CompareHashAndPassword([]byte(hash), password) != nil {
			generated, e := bcrypt.GenerateFromPassword(password, 10)
			if e != nil {
				return "", "", errors.New("hash file-realm password failed")
			}
			hash = string(generated)
		}
		fmt.Fprintf(&userLines, "%s:%s\n", name, hash)
	}
	for _, role := range sortedKeys(roles) {
		if !securityName.MatchString(role) {
			return "", "", errors.New("invalid file-realm role name")
		}
		names := slices.Clone(roles[role])
		slices.Sort(names)
		names = slices.Compact(names)
		for _, name := range names {
			if _, exists := users[name]; !exists {
				return "", "", errors.New("file-realm role references an unknown user")
			}
		}
		if len(names) != 0 {
			fmt.Fprintf(&roleLines, "%s:%s\n", role, strings.Join(names, ","))
		}
	}
	return userLines.String(), roleLines.String(), nil
}

// RenderFileRoles renders roles.yml for the provider-owned monitoring role.
func RenderFileRoles() (string, error) {
	b, err := yaml.Marshal(map[string]Role{MonitorRole: MonitorRoleDefinition()})
	return string(b), err
}
