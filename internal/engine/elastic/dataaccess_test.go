// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"encoding/json"
	"errors"
	"net/http"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/elastic/esapi"
)

func TestDataMcpScope(t *testing.T) {
	p := &Provider{}
	for _, pattern := range []string{"*", "logs-*", "events-2026.10", "orders-?", "logs_2026", strings.Repeat("x", 255)} {
		c := &domain.Cluster{Spec: domain.ClusterSpec{DataMcp: &domain.DataMcpSpec{Scope: domain.DataMcpScope{IndexPatterns: []string{pattern}}}}}
		backend, err := p.DataMcp(c)
		if err != nil || backend.Kind != engine.DataMcpNative || backend.Upstream != "native-elasticsearch" {
			t.Fatalf("pattern %q: %+v %v", pattern, backend, err)
		}
	}
	for _, pattern := range []string{"", ".*", ".hidden", "_all", "_logs", "-logs", "+logs", "Logs", "résumé-*", "a+b", "a[b]", "a/b", `a\b`, "a:b", "a,b", "a#b", "a b", "a\tb", "a\x00b", "<logs>", `a"b`, "a|b", strings.Repeat("x", 256), string([]byte{0xff})} {
		c := &domain.Cluster{Spec: domain.ClusterSpec{DataMcp: &domain.DataMcpSpec{Scope: domain.DataMcpScope{IndexPatterns: []string{pattern}}}}}
		if _, err := p.DataMcp(c); err == nil {
			t.Errorf("accepted unsafe pattern %q", pattern)
		}
	}
	for _, scope := range []domain.DataMcpScope{{Databases: []string{"db"}}, {Schemas: []string{"public"}}, {Vhosts: []string{"/"}}, {IndexPatterns: []string{".hidden"}}} {
		c := &domain.Cluster{Spec: domain.ClusterSpec{DataMcp: &domain.DataMcpSpec{Scope: scope}}}
		if _, err := p.DataMcp(c); err == nil {
			t.Errorf("accepted scope %+v", scope)
		}
		if err := p.Validate(t.Context(), nil, c); err == nil {
			t.Errorf("Validate accepted scope %+v", scope)
		}
	}
	for _, c := range []*domain.Cluster{nil, {}} {
		if _, err := p.DataMcp(c); err != nil {
			t.Fatal(err)
		}
	}
}

func TestDataAccessRolesAndProtection(t *testing.T) {
	for _, level := range []engine.DataAccessLevel{engine.DataAccessRead, engine.DataAccessReadWrite} {
		patterns := []string{"logs-*", "events-*"}
		role := DataAccessRoleDefinition(patterns, level)
		want := []string{"read", "view_index_metadata", "monitor"}
		if level == engine.DataAccessReadWrite {
			want = append(want, "write")
		}
		if !reflect.DeepEqual(role.Cluster, []string{"monitor"}) || len(role.Indices) != 1 || !reflect.DeepEqual(role.Indices[0].Privileges, want) || role.Indices[0].AllowRestrictedIndices || !reflect.DeepEqual(role.Indices[0].Names, patterns) {
			t.Fatalf("unexpected role %+v", role)
		}
		role.Indices[0].Names[0] = "changed"
		if patterns[0] != "logs-*" {
			t.Fatal("role aliases input patterns")
		}
		if !reflect.DeepEqual(DataAccessRoleDefinition(nil, level).Indices[0].Names, []string{"*"}) {
			t.Fatal("default scope")
		}
		user := engine.DataAccessUser(level)
		if !ProtectedUserName(user) || ValidateUserName(user) == nil || visibleUser(user, esapi.User{}) {
			t.Fatalf("unprotected identity %s", user)
		}
	}
	for _, role := range []string{DataAccessReadRole, DataAccessReadWriteRole} {
		if !ProtectedRoleName(role) || ValidateRoleName(role) == nil {
			t.Fatalf("unprotected role %s", role)
		}
	}
}

func TestDataAccessSecurityLifecycle(t *testing.T) {
	users := map[string]esapi.User{}
	roles := map[string]esapi.Role{}
	failDelete := false
	p, rc, s, _ := opsFixture(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/_security/_authenticate" {
			opsJSON(w, map[string]string{"username": "elastic/kibana"})
			return
		}
		if strings.HasPrefix(r.URL.Path, "/_security/user/karakoram-mcp-") {
			name := strings.TrimPrefix(r.URL.Path, "/_security/user/")
			if r.Method == "DELETE" {
				if failDelete {
					http.Error(w, "unavailable", http.StatusServiceUnavailable)
					return
				}
				if _, exists := users[name]; !exists {
					w.WriteHeader(http.StatusNotFound)
				}
				delete(users, name)
			} else if r.Method == "PUT" {
				var user esapi.User
				if err := json.NewDecoder(r.Body).Decode(&user); err != nil {
					t.Error(err)
				}
				users[name] = user
			}
		}
		if strings.HasPrefix(r.URL.Path, "/_security/role/karakoram_mcp_") {
			name := strings.TrimPrefix(r.URL.Path, "/_security/role/")
			if r.Method == "DELETE" {
				if _, exists := roles[name]; !exists {
					w.WriteHeader(http.StatusNotFound)
				}
				delete(roles, name)
			} else {
				var role esapi.Role
				if err := json.NewDecoder(r.Body).Decode(&role); err != nil {
					t.Error(err)
				}
				roles[name] = role
			}
		}
		opsJSON(w, map[string]any{})
	})
	sec := rc.Secrets.(*opsSecrets)
	rc.Cluster.Spec.DataMcp = &domain.DataMcpSpec{Enabled: true, Access: domain.DataMcpAccessReadWrite, Scope: domain.DataMcpScope{IndexPatterns: []string{"logs-*"}}}
	reconcile := func() {
		t.Helper()
		if err := p.ensureSecurity(t.Context(), rc, s, false); err != nil {
			t.Fatal(err)
		}
	}
	reconcile()
	for _, entry := range []struct{ user, role, key string }{{engine.DataAccessReadUser, DataAccessReadRole, secretDataAccessRead}, {engine.DataAccessReadWriteUser, DataAccessReadWriteRole, secretDataAccessReadWrite}} {
		user := users[entry.user]
		if len(sec.values[entry.key]) == 0 || user.Password != string(sec.values[entry.key]) || !reflect.DeepEqual(user.Roles, []string{entry.role}) || user.Enabled == nil || !*user.Enabled {
			t.Fatalf("user definition %+v", user)
		}
		level := engine.DataAccessRead
		if entry.user == engine.DataAccessReadWriteUser {
			level = engine.DataAccessReadWrite
		}
		want, err := json.Marshal(roleToES(DataAccessRoleDefinition([]string{"logs-*"}, level)))
		if err != nil {
			t.Fatal(err)
		}
		got, err := json.Marshal(roles[entry.role])
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != string(want) {
			t.Fatalf("role definition %s, want %s", got, want)
		}
	}
	original := string(sec.values[secretDataAccessRead])
	delete(users, engine.DataAccessReadUser)
	rc.Cluster.Spec.DataMcp.Scope.IndexPatterns = []string{"events-*"}
	reconcile()
	if users[engine.DataAccessReadUser].Password != original || roles[DataAccessReadRole].Indices[0].Names[0] != "events-*" {
		t.Fatal("drift or scope not repaired")
	}
	sec.values[secretDataAccessRead] = []byte("rotated-password")
	reconcile()
	if users[engine.DataAccessReadUser].Password != "rotated-password" {
		t.Fatal("rotation not installed")
	}
	rc.Cluster.Spec.DataMcp.Access = domain.DataMcpAccessRead
	reconcile()
	if len(users) != 1 || len(roles) != 1 || sec.values[secretDataAccessReadWrite] != nil {
		t.Fatal("write identity not removed")
	}
	rc.Cluster.Spec.DataMcp.Enabled = false
	failDelete = true
	if err := p.ensureSecurity(t.Context(), rc, s, false); err == nil || sec.values[secretDataAccessRead] == nil {
		t.Fatal("failed native deletion must retain secret for retry")
	}
	failDelete = false
	reconcile()
	reconcile()
	if len(users) != 0 || len(roles) != 0 || sec.values[secretDataAccessRead] != nil {
		t.Fatal("disabled identity not removed")
	}
}

func TestDataAccessCredentialsAndSystemUsers(t *testing.T) {
	p, rc, s, _ := opsFixture(t, func(w http.ResponseWriter, r *http.Request) { opsJSON(w, map[string]any{}) })
	sec := rc.Secrets.(*opsSecrets)
	sec.values[secretDataAccessRead] = []byte("read-password")
	sec.values[secretDataAccessReadWrite] = []byte("write-password")
	s.Members = append(s.Members, &member{Name: "coordinator", Kind: kindNode, Stage: stageDeployed}, &member{Name: "master", Kind: kindMaster, Stage: stageJoined}, &member{Name: "kibana", Kind: kindKibana, Stage: stageJoined}, &member{Name: "planned", Kind: kindNode, Stage: stagePlanned}, &member{Name: "removing", Kind: kindNode, Stage: stageJoined, Removing: true})
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"disabled", domain.DataMcpAccessRead, domain.DataMcpAccessReadWrite} {
		rc.Cluster.Spec.DataMcp = &domain.DataMcpSpec{Enabled: mode != "disabled", Access: mode}
		listed, err := p.SystemUsers(t.Context(), rc)
		if err != nil {
			t.Fatal(err)
		}
		for _, level := range []engine.DataAccessLevel{engine.DataAccessRead, engine.DataAccessReadWrite} {
			wanted := mode != "disabled" && (level == engine.DataAccessRead || mode == domain.DataMcpAccessReadWrite)
			name := engine.DataAccessUser(level)
			if slices.ContainsFunc(listed, func(user domain.SystemUser) bool { return user.Name == name }) != wanted {
				t.Fatalf("listing %s/%s", mode, level)
			}
			key, kind := rotationTarget(name)
			if key != "users/"+name || kind != "" {
				t.Fatal("rotation target")
			}
			credentials, err := p.DataAccessCredentials(t.Context(), rc, level)
			if !wanted {
				if err == nil || credentials.Password != "" {
					t.Fatal("disabled credential exposed")
				}
				continue
			}
			if err != nil || credentials.Username != name || credentials.Password != string(sec.values[key]) || !credentials.TLS || !reflect.DeepEqual(credentials.CAPEM, sec.values[secretCA]) || !reflect.DeepEqual(credentials.Hosts, []string{"https://example.com:9200", "https://coordinator:9200"}) {
				t.Fatalf("credentials %+v %v", credentials, err)
			}
		}
	}
	if _, err := p.DataAccessCredentials(t.Context(), rc, "invalid"); err == nil {
		t.Fatal("invalid level accepted")
	}
	delete(sec.values, secretDataAccessRead)
	if credentials, err := p.DataAccessCredentials(t.Context(), rc, engine.DataAccessRead); !errors.Is(err, engine.ErrNotFound) || credentials.Password != "" {
		t.Fatal("missing secret did not fail closed")
	}
	s.Members = nil
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	if _, err := p.DataAccessCredentials(t.Context(), rc, engine.DataAccessRead); !errors.Is(err, errWaiting) {
		t.Fatal("empty endpoints did not wait")
	}
}
