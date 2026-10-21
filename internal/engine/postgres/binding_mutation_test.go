// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"encoding/json"
	"errors"
	"net/http"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
)

func TestBindingMutationsCarryCreationIdentity(t *testing.T) {
	for _, operation := range []string{"rotate", "disable", "delete"} {
		for _, replaced := range []bool{false, true} {
			t.Run(operation+map[bool]string{false: "/owned", true: "/replaced"}[replaced], func(t *testing.T) {
				p, rc, _ := setup(t, "single")
				converge(t, p, rc)
				name := "kb_0123456789abcdef0123456789abcdef"
				native := &Role{Name: name, Login: true, ConnectionLimit: 100}
				receipt := pgspec.AccessRequest{RequestID: "binding-creation", Operation: "create", Binding: true, Role: &RoleWrite{Role: *native, Binding: true}}
				raw, err := json.Marshal(receipt)
				if err != nil {
					t.Fatal(err)
				}
				if err = putSecret(t.Context(), rc, "bindings/roles/"+name, raw); err != nil {
					t.Fatal(err)
				}
				if err = putSecret(t.Context(), rc, rolePasswordKey(name), []byte("initial-password")); err != nil {
					t.Fatal(err)
				}
				mutations := 0
				installBackupAgent(t, rc, func(w http.ResponseWriter, r *http.Request) {
					switch r.URL.Path {
					case "/v1/status":
						json.NewEncoder(w).Encode(agentapi.Status{})
					case "/v1/postgres/status":
						json.NewEncoder(w).Encode(pgspec.StatusResponse{Running: true, Role: "standalone"})
					case "/v1/postgres/roles":
						var req pgspec.AccessRequest
						if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
							t.Error(err)
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
							if req.RoleOperation == "create" {
								json.NewEncoder(w).Encode(pgspec.AccessResult{Outcome: "applied", RoleOID: 17001})
								return
							}
							mutations++
							if req.ExpectedRoleOID != 17001 || !req.Binding || req.Role == nil || !req.Role.Binding {
								t.Errorf("unfenced mutation: %+v", req)
							}
							if replaced {
								json.NewEncoder(w).Encode(pgspec.AccessResult{Outcome: "rejected", Code: "role_identity_changed", Status: 409})
								return
							}
							if req.RoleOperation == "delete" {
								native = nil
							} else {
								v := req.Role.Role
								native = &v
							}
							json.NewEncoder(w).Encode(pgspec.AccessResult{Outcome: "applied", RoleOID: 17001})
						default:
							t.Errorf("unexpected role operation %s", req.Operation)
							w.WriteHeader(400)
						}
					default:
						w.WriteHeader(404)
					}
				})
				ctx := BindingContext(t.Context())
				if operation == "delete" {
					err = p.DeleteRole(ctx, rc, name)
				} else {
					role := *native
					write := RoleWrite{Role: role}
					if operation == "rotate" {
						password := "new-password-value"
						write.Password = &password
					} else {
						write.Login = false
					}
					err = p.UpdateRole(ctx, rc, write)
				}
				if replaced && !errors.Is(err, engine.ErrConflict) {
					t.Fatalf("replacement not rejected: %v", err)
				}
				if !replaced && err != nil {
					t.Fatal(err)
				}
				if mutations != 1 {
					t.Fatalf("mutations=%d", mutations)
				}
				if replaced && (native == nil || !native.Login) {
					t.Fatal("replacement mutated")
				}
			})
		}
	}
}
