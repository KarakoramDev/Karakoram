// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
)

func TestRuntimeRoleCredentialContracts(t *testing.T) {
	for _, profile := range []struct{ name, topology, options string }{
		{"postgres", "single", "{}"},
		{"citus", "distributed", `{"profiles":["citus"]}`},
		{"timescale", "single", `{"profiles":["timescaledb"]}`},
	} {
		for _, binding := range []bool{false, true} {
			name := "app"
			if binding {
				name = "kb_0123456789abcdef0123456789abcdef"
			}
			t.Run(profile.name+"/"+name, func(t *testing.T) {
				p, rc, _ := setup(t, profile.topology)
				rc.Cluster.Spec.EngineOptions = json.RawMessage(profile.options)
				s, err := load(rc.Cluster)
				if err != nil {
					t.Fatal(err)
				}
				s.Topology = profile.topology
				s.allocate(rc.Cluster)
				if err := save(rc.Cluster, s); err != nil {
					t.Fatal(err)
				}
				leaders := map[string]bool{}
				for _, group := range s.groups() {
					leaders[group.data()[0].Name] = true
				}
				var native *Role
				var verifier string
				var recoveredVerifiers []string
				var pendingID string
				loseResponse := true
				mutations, recoveries := 0, 0
				installBackupAgent(t, rc, func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path == "/v1/postgres/status" {
						member := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
						role := "replica"
						if leaders[member] {
							role = "primary"
						}
						json.NewEncoder(w).Encode(pgspec.StatusResponse{Running: true, Role: role, Leader: member})
						return
					}
					if r.URL.Path != "/v1/postgres/roles" {
						t.Errorf("unexpected credential route %s", r.URL.Path)
						w.WriteHeader(404)
						return
					}
					var req pgspec.AccessRequest
					if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
						t.Error(err)
						w.WriteHeader(400)
						return
					}
					switch req.Operation {
					case "capabilities":
						json.NewEncoder(w).Encode(pgspec.AccessCapabilities{CredentialReceipts: true})
					case "list":
						roles := []Role{}
						if native != nil {
							roles = append(roles, *native)
						}
						json.NewEncoder(w).Encode(roles)
					case "journal":
						if req.Role == nil || req.Role.Password == nil || req.RequestID == "" {
							t.Error("credential mutation lacks password or journal identity")
							w.WriteHeader(400)
							return
						}
						if req.RequestID == pendingID {
							if verifier != *req.Role.Password {
								t.Error("credential replay changed native verifier")
							}
						} else {
							mutations++
							pendingID, verifier = req.RequestID, *req.Role.Password
							role := req.Role.Role
							native = &role
							if loseResponse {
								loseResponse = false
								w.WriteHeader(503)
								return
							}
						}
						json.NewEncoder(w).Encode(pgspec.AccessResult{Outcome: "applied", RoleOID: 17001})
					case "password", "recover-password", "recover-password-local":
						recoveries++
						if req.Role == nil || req.Role.Password == nil {
							t.Error("recovery omitted native verifier")
						} else {
							verifier = *req.Role.Password
							recoveredVerifiers = append(recoveredVerifiers, verifier)
						}
						json.NewEncoder(w).Encode(pgspec.ActionResponse{OK: true})
					default:
						t.Errorf("unexpected credential operation %s", req.Operation)
						w.WriteHeader(400)
					}
				})
				ctx := t.Context()
				if binding {
					ctx = BindingContext(ctx)
				}
				assertPublished := func(password string) {
					t.Helper()
					postgresPasswordLimit.Assert(t, "returned password", password)
					stored, err := secret(ctx, rc, rolePasswordKey(name))
					if err != nil || string(stored) != password {
						t.Fatal("returned credential differs from persisted password", err)
					}
					postgresPasswordLimit.Assert(t, "persisted password", string(stored))
					assertRecoveryPassword(t, verifier, password)
					users, err := p.proxyUsers(ctx, rc)
					if err != nil || len(users) != 1 || users[0].Username != name || users[0].Password != password {
						t.Fatal("native proxy configuration differs from generated credential", err)
					}
					postgresPasswordLimit.Assert(t, "proxy password", users[0].Password)
				}
				var created string
				if binding {
					if err := p.EnsureBindingRole(ctx, rc, name, 100); err == nil {
						t.Fatal("lost create acknowledgement was not surfaced")
					}
					if err := p.EnsureBindingRole(ctx, rc, name, 100); err != nil {
						t.Fatal(err)
					}
					stored, err := secret(ctx, rc, rolePasswordKey(name))
					if err != nil {
						t.Fatal(err)
					}
					created = string(stored) // EnsureBindingRole intentionally returns no plaintext.
				} else {
					write := RoleWrite{Role: Role{Name: name, Login: true, ConnectionLimit: 100}}
					if _, err := p.CreateRole(ctx, rc, write); err == nil {
						t.Fatal("lost create acknowledgement was not surfaced")
					}
					created, err = p.CreateRole(ctx, rc, write)
					if err != nil {
						t.Fatal(err)
					}
				}
				assertPublished(created)
				loseResponse = true
				if _, err := p.RotateRolePassword(ctx, rc, name); err == nil {
					t.Fatal("lost rotation acknowledgement was not surfaced")
				}
				rotated, err := p.RotateRolePassword(ctx, rc, name)
				if err != nil || rotated == created {
					t.Fatal("rotation failed to recover a fresh credential", err)
				}
				assertPublished(rotated)
				if mutations != 2 {
					t.Fatalf("expected create and rotation, got %d unique mutations", mutations)
				}
				if err := saveBackupState(rc.Cluster, backupState{ManagedRolesHeld: true}); err != nil {
					t.Fatal(err)
				}
				if err := p.reconcileRecoveredRoles(ctx, rc); err != nil {
					t.Fatal(err)
				}
				if recoveries != len(s.groups()) {
					t.Fatalf("recovered %d groups, want %d", recoveries, len(s.groups()))
				}
				for _, recovered := range recoveredVerifiers {
					assertRecoveryPassword(t, recovered, rotated)
				}
				assertPublished(rotated)
			})
		}
	}
}

func TestRuntimeDataMcpCredentialContracts(t *testing.T) {
	for _, options := range []string{"{}", `{"profiles":["timescaledb"]}`} {
		t.Run(options, func(t *testing.T) {
			p, rc, _ := setup(t, "single")
			rc.Cluster.Spec.EngineOptions = json.RawMessage(options)
			rc.Cluster.Spec.DataMcp = &domain.DataMcpSpec{Enabled: true, Access: domain.DataMcpAccessReadWrite}
			s := converge(t, p, rc)
			assertCredentials := func(level engine.DataAccessLevel) string {
				t.Helper()
				credentials, err := p.DataAccessCredentials(t.Context(), rc, level)
				if err != nil {
					t.Fatal(err)
				}
				postgresPasswordLimit.Assert(t, "returned MCP password", credentials.Password)
				stored, err := secret(t.Context(), rc, "users/"+engine.DataAccessUser(level))
				if err != nil || string(stored) != credentials.Password {
					t.Fatal("MCP returned password differs from persisted credential", err)
				}
				postgresPasswordLimit.Assert(t, "persisted MCP password", string(stored))
				uri, err := url.Parse(credentials.URI)
				if err != nil || uri.User == nil {
					t.Fatal("missing MCP URI credentials", err)
				}
				password, ok := uri.User.Password()
				if !ok || password != credentials.Password {
					t.Fatal("MCP URI password differs from returned credential")
				}
				cfg, err := p.MemberConfig(t.Context(), rc, s.data()[0].Name)
				if err != nil {
					t.Fatal(err)
				}
				var spec pgspec.Spec
				if err := json.Unmarshal(cfg.EngineSpec, &spec); err != nil {
					t.Fatal(err)
				}
				found := false
				if spec.DataAccess != nil {
					for _, role := range spec.DataAccess.Roles {
						if role.Username == credentials.Username && role.Enabled {
							found = true
							postgresPasswordLimit.Assert(t, "native MCP password", role.Password)
							if role.Password != credentials.Password {
								t.Error("native MCP payload differs from returned credential")
							}
						}
					}
				}
				if !found {
					t.Fatal("native MCP credential payload missing")
				}
				return credentials.Password
			}
			for _, level := range []engine.DataAccessLevel{engine.DataAccessRead, engine.DataAccessReadWrite} {
				old := assertCredentials(level)
				payload, err := json.Marshal(map[string]string{"user": engine.DataAccessUser(level)})
				if err != nil {
					t.Fatal(err)
				}
				op := &domain.Operation{ID: "contract-" + string(level), Kind: domain.OperationRotateSystemUser, Payload: payload}
				result, err := p.RunOperation(t.Context(), rc, op)
				if err != nil || result.Step != "password-staged" {
					t.Fatal("MCP rotation did not stage credential", err)
				}
				staged, err := secret(t.Context(), rc, "rotations/"+op.ID+"/"+engine.DataAccessUser(level))
				if err != nil {
					t.Fatal(err)
				}
				postgresPasswordLimit.Assert(t, "staged MCP password", string(staged))
				for range 50 {
					op.Checkpoint = result.Checkpoint
					result, err = p.RunOperation(t.Context(), rc, op)
					if err != nil {
						t.Fatal(err)
					}
					if result.Done {
						break
					}
				}
				if !result.Done {
					t.Fatal("MCP rotation did not complete")
				}
				rotated := assertCredentials(level)
				if rotated == old || rotated != string(staged) {
					t.Fatal("MCP rotation did not publish its generated staged credential")
				}
			}
		})
	}
}
