// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package rabbitmq

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/enginetest"
	"github.com/KarakoramDev/Karakoram/internal/engine/rabbitmq/mgmt"
)

func TestDataMcpScope(t *testing.T) {
	p := New(Options{})
	c := &domain.Cluster{}
	backend, err := p.DataMcp(c)
	if err != nil || backend.Kind != engine.DataMcpNative || backend.Upstream != "native-rabbitmq" {
		t.Fatal(backend, err)
	}
	for _, scope := range []domain.DataMcpScope{{Vhosts: []string{"/", "app/events", "µ"}}, {}} {
		c.Spec.DataMcp = &domain.DataMcpSpec{Scope: scope}
		if _, err := p.DataMcp(c); err != nil {
			t.Fatal(err)
		}
	}
	for _, scope := range []domain.DataMcpScope{{Databases: []string{"app"}}, {Schemas: []string{"public"}}, {IndexPatterns: []string{"*"}}, {Vhosts: []string{""}}, {Vhosts: []string{"bad\nname"}}} {
		c.Spec.DataMcp.Scope = scope
		if _, err := p.DataMcp(c); !errors.Is(err, engine.ErrInvalid) {
			t.Fatal(scope, err)
		}
	}
	for _, user := range []string{engine.DataAccessReadUser, engine.DataAccessReadWriteUser} {
		if !protectedUser(user) {
			t.Fatal("unprotected data identity", user)
		}
		if err := p.DeleteUser(t.Context(), nil, user); !errors.Is(err, engine.ErrConflict) {
			t.Fatal(err)
		}
	}
}

func TestDataAccessReconcile(t *testing.T) {
	p, rc, _, _ := setup(t)
	ready(t, p, rc)
	rc.Cluster.Spec.DataMcp = &domain.DataMcpSpec{Enabled: true, Access: "readWrite"}
	users := map[string]mgmt.UserWrite{}
	permissions := map[string]mgmt.Permission{}
	vhosts := []mgmt.Vhost{{Name: "/"}, {Name: "app/events"}}
	fail := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if fail {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/api/"), "/")
		switch {
		case r.Method == "GET" && r.URL.Path == "/api/permissions":
			out := []mgmt.Permission{}
			for _, v := range permissions {
				out = append(out, v)
			}
			_ = json.NewEncoder(w).Encode(out)
		case r.Method == "GET" && r.URL.Path == "/api/vhosts":
			_ = json.NewEncoder(w).Encode(vhosts)
		case len(parts) == 2 && parts[0] == "users":
			user := parts[1]
			if r.Method == "DELETE" {
				delete(users, user)
				for key, v := range permissions {
					if v.User == user {
						delete(permissions, key)
					}
				}
			} else if r.Method == "PUT" {
				var v mgmt.UserWrite
				if err := json.NewDecoder(r.Body).Decode(&v); err != nil {
					t.Error(err)
				}
				users[user] = v
			} else {
				t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			}
		case parts[0] == "permissions":
			if r.Method == "DELETE" {
				// The vhost can contain slashes, so use the final segment as username.
				user := parts[len(parts)-1]
				vhost := strings.Join(parts[1:len(parts)-1], "/")
				delete(permissions, user+":"+vhost)
			} else if r.Method == "PUT" {
				var v mgmt.Permission
				if err := json.NewDecoder(r.Body).Decode(&v); err != nil {
					t.Error(err)
				}
				permissions[v.User+":"+v.Vhost] = v
			} else {
				t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			}
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
		}
	}))
	defer srv.Close()
	client, err := mgmt.New(srv.URL, "karakoram", "secret", srv.Client())
	if err != nil {
		t.Fatal(err)
	}
	reconcile := func() {
		t.Helper()
		if err := reconcileDataAccessUsers(t.Context(), rc, client, dataAccessUsers(rc.Cluster)); err != nil {
			t.Fatal(err)
		}
	}
	reconcile()
	if len(users) != 2 || len(permissions) != 4 {
		t.Fatal(users, permissions)
	}
	for user, v := range users {
		if len(v.Tags) != 1 || v.Tags[0] != "monitoring" || v.Password == nil || *v.Password == "" {
			t.Fatal(user, v)
		}
		generatedPasswordLimit.Assert(t, "native data password", *v.Password)
		persisted, err := secret(t.Context(), rc, "users/"+user)
		if err != nil {
			t.Fatal(err)
		}
		generatedPasswordLimit.Assert(t, "persisted data password", string(persisted))
		level := engine.DataAccessRead
		if user == engine.DataAccessReadWriteUser {
			level = engine.DataAccessReadWrite
		}
		creds, err := p.DataAccessCredentials(t.Context(), rc, level)
		if err != nil {
			t.Fatal(err)
		}
		generatedPasswordLimit.Assert(t, "returned data password", creds.Password)
		if *v.Password != string(persisted) || creds.Password != string(persisted) {
			t.Fatal("native, persisted and returned data passwords differ")
		}
	}
	for _, v := range permissions {
		write := "^$"
		if v.User == engine.DataAccessReadWriteUser {
			write = ".*"
		}
		if v.Configure != "^$" || v.Write != write || v.Read != ".*" {
			t.Fatal(v)
		}
	}
	// New vhosts are discovered without a spec change; passwords and grants drift
	// back to the sealed desired state without modifying customer identities.
	vhosts = append(vhosts, mgmt.Vhost{Name: "later"})
	ro := engine.DataAccessReadUser
	users[ro] = mgmt.UserWrite{Tags: []string{"administrator"}}
	permissions[ro+":/"] = mgmt.Permission{User: ro, Vhost: "/", Configure: ".*"}
	reconcile()
	if len(permissions) != 6 || users[ro].Tags[0] != "monitoring" || permissions[ro+":/"].Configure != "^$" {
		t.Fatal(users, permissions)
	}
	if err := putSecret(t.Context(), rc, "users/"+ro, []byte("rotated-password")); err != nil {
		t.Fatal(err)
	}
	reconcile()
	if *users[ro].Password != "rotated-password" {
		t.Fatal("rotation not published")
	}
	rc.Cluster.Spec.DataMcp.Scope.Vhosts = []string{"app/events"}
	rc.Cluster.Spec.DataMcp.Access = "read"
	reconcile()
	if len(users) != 1 || len(permissions) != 1 || permissions[ro+":app/events"].Read != ".*" {
		t.Fatal(users, permissions)
	}
	if _, err := secret(t.Context(), rc, "users/"+engine.DataAccessReadWriteUser); !missing(err) {
		t.Fatal("disabled write secret survived", err)
	}
	fail = true
	if err := reconcileDataAccessUsers(t.Context(), rc, client, dataAccessUsers(rc.Cluster)); err == nil {
		t.Fatal("management failure was ignored")
	}
	fail = false
	rc.Cluster.Spec.DataMcp.Enabled = false
	reconcile()
	if len(users)+len(permissions) != 0 {
		t.Fatal(users, permissions)
	}
	if _, err := secret(t.Context(), rc, "users/"+ro); !missing(err) {
		t.Fatal("disabled read secret survived", err)
	}
}

func TestDataAccessRotation(t *testing.T) {
	for _, user := range []string{"karakoram", "admin", engine.DataAccessReadUser, engine.DataAccessReadWriteUser} {
		t.Run(user, func(t *testing.T) {
			p, rc, _, agents := setup(t)
			ready(t, p, rc)
			rc.Cluster.Spec.DataMcp = &domain.DataMcpSpec{Enabled: true, Access: "readWrite"}
			s := state(t, rc)
			if err := p.ensureSecrets(t.Context(), rc, s); err != nil {
				t.Fatal(err)
			}
			before, err := secret(t.Context(), rc, "users/"+user)
			if err != nil {
				t.Fatal(err)
			}
			recorded := enginetest.RecordCredentials(rc)
			var nativePassword string
			agents.configObserver = func(cfg agentapi.MemberConfig) {
				nativePassword = cfg.SystemUsers[user].Password
				generatedPasswordLimit.Assert(t, "agent-loaded rotation password", nativePassword)
			}
			op := &domain.Operation{ID: "rotate-data-user", Kind: domain.OperationRotateSystemUser, Payload: json.RawMessage(`{"user":"` + user + `"}`)}
			done := false
			for range 30 {
				result, err := p.RunOperation(t.Context(), rc, op)
				if err != nil {
					t.Fatal(err)
				}
				op.Checkpoint = result.Checkpoint
				if result.Done {
					done = true
					break
				}
			}
			after, err := secret(t.Context(), rc, "users/"+user)
			if !done || err != nil || string(before) == string(after) {
				t.Fatal("rotation incomplete", done, err)
			}
			recorded.AssertValues(t, map[string]enginetest.CredentialLimit{
				"rotations/" + op.ID: generatedPasswordLimit,
				"users/" + user:      generatedPasswordLimit,
			}, 0)
			if string(recorded.Values["rotations/"+op.ID]) != string(after) {
				t.Fatal("staged and persisted rotation passwords differ")
			}
			generatedPasswordLimit.Assert(t, "persisted rotated password", string(after))
			if nativePassword != string(after) {
				t.Fatal("agent-loaded password differs from persisted rotation")
			}
			if user == "admin" {
				out, err := p.Connection(t.Context(), rc)
				if err != nil || len(out.Credentials) == 0 {
					t.Fatal("missing returned admin credentials", err)
				}
				for _, credential := range out.Credentials {
					generatedPasswordLimit.Assert(t, "returned rotated admin password", credential.Password)
					if credential.Password != string(after) {
						t.Fatal("returned admin password differs from persisted rotation")
					}
				}
			}
			if user == engine.DataAccessReadUser || user == engine.DataAccessReadWriteUser {
				level := engine.DataAccessRead
				if user == engine.DataAccessReadWriteUser {
					level = engine.DataAccessReadWrite
				}
				creds, err := p.DataAccessCredentials(t.Context(), rc, level)
				if err != nil {
					t.Fatal(err)
				}
				generatedPasswordLimit.Assert(t, "returned rotated data password", creds.Password)
				if creds.Password != string(after) {
					t.Fatal("returned password differs from persisted rotation")
				}
			}
			listed, err := p.SystemUsers(t.Context(), rc)
			if err != nil || len(listed) != 4 {
				t.Fatal(listed, err)
			}
		})
	}
}

func TestDataAccessCredentials(t *testing.T) {
	for _, mode := range []string{"disabled", "requireTLS"} {
		t.Run(mode, func(t *testing.T) {
			p, rc, _, a := setup(t)
			rc.Cluster.Spec.TLS.Mode = mode
			ready(t, p, rc)
			rc.Cluster.Spec.DataMcp = &domain.DataMcpSpec{Enabled: true}
			if err := putSecret(t.Context(), rc, "users/"+engine.DataAccessReadUser, []byte("data-password")); err != nil {
				t.Fatal(err)
			}
			out, err := p.DataAccessCredentials(t.Context(), rc, engine.DataAccessRead)
			if err != nil {
				t.Fatal(err)
			}
			prefix, port := "http://", ":15672"
			if mode != "disabled" {
				prefix, port = "https://", ":15671"
			}
			if out.Username != engine.DataAccessReadUser || out.Password != "data-password" || len(out.Hosts) != 3 || !strings.HasPrefix(out.Hosts[0], prefix) || !strings.HasSuffix(out.Hosts[0], port) || !strings.Contains(out.URI, "karakoram-mcp-ro:data-password@") {
				t.Fatal(out)
			}
			if out.TLS != (mode != "disabled") || out.TLS && len(out.CAPEM) == 0 {
				t.Fatal("missing TLS material")
			}
			if _, err := p.DataAccessCredentials(t.Context(), rc, engine.DataAccessReadWrite); !errors.Is(err, engine.ErrConflict) {
				t.Fatal(err)
			}
			if _, err := p.DataAccessCredentials(t.Context(), rc, engine.DataAccessLevel("invalid")); !errors.Is(err, engine.ErrInvalid) {
				t.Fatal(err)
			}
			for _, m := range state(t, rc).runningMembers() {
				a.failures[m.Name+":/v1/status"] = http.StatusServiceUnavailable
			}
			if _, err := p.DataAccessCredentials(t.Context(), rc, engine.DataAccessRead); !errors.Is(err, engine.ErrConflict) {
				t.Fatal("unhealthy member exposed", err)
			}
		})
	}
}
