// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
)

func TestBindingRoleReceiptRecoveryAndForeignRole(t *testing.T) {
	for _, foreign := range []bool{false, true} {
		t.Run(map[bool]string{false: "recovery", true: "foreign"}[foreign], func(t *testing.T) {
			p, rc, _ := setup(t, "single")
			converge(t, p, rc)
			name := "kb_0123456789abcdef0123456789abcdef"
			var native *Role
			if foreign {
				native = &Role{Name: name, Login: true, ConnectionLimit: 100}
			}
			journals := 0
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
						journals++
						if !req.Binding || req.Role == nil || !req.Role.Binding {
							t.Error("binding scope lost from journal")
						}
						if !strings.HasPrefix(req.RequestID, name+"/") {
							t.Error("receipt request identity is not incarnation-scoped")
						}
						role := req.Role.Role
						native = &role
						json.NewEncoder(w).Encode(pgspec.AccessResult{Outcome: "applied", RoleOID: 17001})
					default:
						t.Errorf("unexpected operation %s", req.Operation)
						w.WriteHeader(400)
					}
				default:
					w.WriteHeader(404)
				}
			})
			original := rc.Secrets
			rc.Secrets = &failingAccessSecrets{SecretStore: original, key: "bindings/roles/" + name}
			err := p.EnsureBindingRole(t.Context(), rc, name, 100)
			if foreign {
				if !errors.Is(err, engine.ErrConflict) || journals != 0 {
					t.Fatalf("foreign role adopted: journals=%d err=%v", journals, err)
				}
				return
			}
			if err == nil {
				t.Fatal("receipt publication fault did not fail")
			}
			rc.Secrets = original
			p = New(Options{})
			if err := p.EnsureBindingRole(t.Context(), rc, name, 100); err != nil {
				t.Fatal(err)
			}
			if journals < 2 {
				t.Fatal("native receipt not replayed")
			}
			if oid, err := p.VerifyBindingRole(t.Context(), rc, name); err != nil || oid != 17001 {
				t.Fatalf("verified role OID: %d %v", oid, err)
			}
			users, err := p.proxyUsers(t.Context(), rc)
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, user := range users {
				if user.Username == name && user.Password != "" {
					found = true
				}
			}
			if !found {
				t.Fatal("binding role not routed through proxy")
			}
		})
	}
}
