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

func installCredentialAgent(t *testing.T, rc *engine.Context, roles func(http.ResponseWriter, pgspec.AccessRequest)) {
	t.Helper()
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
			if req.Operation == "capabilities" {
				json.NewEncoder(w).Encode(pgspec.AccessCapabilities{CredentialReceipts: true})
				return
			}
			roles(w, req)
		default:
			t.Errorf("unexpected credential route %s", r.URL.Path)
			w.WriteHeader(404)
		}
	})
}

func TestCredentialDefinitiveRejectionReleasesReconciliation(t *testing.T) {
	for _, operation := range []string{"create", "update"} {
		t.Run(operation, func(t *testing.T) {
			p, rc, backend := setup(t, "single")
			rc.Cluster.Spec.Version = "17.10.1"
			converge(t, p, rc)
			ordinaryAgents := rc.Agents
			if err := putSecret(t.Context(), rc, "access/roles", []byte(`["app"]`)); err != nil {
				t.Fatal(err)
			}
			if err := putSecret(t.Context(), rc, rolePasswordKey("app"), []byte("original-app-password")); err != nil {
				t.Fatal(err)
			}
			if err := putSecret(t.Context(), rc, "access/revision", []byte("original-revision")); err != nil {
				t.Fatal(err)
			}
			parentCreated := false
			installCredentialAgent(t, rc, func(w http.ResponseWriter, req pgspec.AccessRequest) {
				if req.Operation == "list" {
					roles := []Role{{Name: "app", Login: true}}
					json.NewEncoder(w).Encode(roles)
					return
				}
				if req.Operation != "journal" || req.RequestID == "" {
					t.Error("credential mutation bypassed journal identity")
				}
				if req.Role.Name == "missing_parent" {
					parentCreated = true
				}
				result := pgspec.AccessResult{Outcome: "applied", RoleOID: 17001}
				if len(req.Role.MemberOf) != 0 && !parentCreated {
					result = pgspec.AccessResult{Outcome: "rejected", Code: "role_or_membership_missing", Message: "PostgreSQL rejected the role change: the role or a requested membership target does not exist", Status: 404}
				}
				json.NewEncoder(w).Encode(result)
			})
			password := "rejected-new-password"
			role := RoleWrite{Role: Role{Name: "app", Login: true, MemberOf: []string{"missing_parent"}}, Password: &password}
			var err error
			if operation == "create" {
				role.Name = "child"
				_, err = p.CreateRole(t.Context(), rc, role)
			} else {
				err = p.UpdateRole(t.Context(), rc, role)
			}
			if !errors.Is(err, engine.ErrNotFound) || !strings.Contains(err.Error(), "role_or_membership_missing") {
				t.Fatalf("rejection not surfaced precisely: %v", err)
			}
			if pending, err := accessIntentPending(t.Context(), rc); pending || err != nil {
				t.Fatal("definitive rollback retained recovery fence", err)
			}
			if stored, err := secret(t.Context(), rc, rolePasswordKey("app")); err != nil || string(stored) != "original-app-password" {
				t.Fatal("rejection published a password", err)
			}
			if revision, err := p.accessRevision(t.Context(), rc); err != nil || revision != "original-revision" {
				t.Fatal("rejection published a revision", err)
			}
			if _, err := p.CreateRole(t.Context(), rc, RoleWrite{Role: Role{Name: "missing_parent"}}); err != nil || !parentCreated {
				t.Fatal("valid subsequent request fenced", err)
			}
			// Restore the ordinary member fixture and make an unrelated engine
			// change. Full Sync must pass the retired journal gate and converge.
			rc.Agents = ordinaryAgents
			backend.actions = nil
			rc.Cluster.Spec.Version = "17.11.1"
			rc.Cluster.Generation++
			converge(t, p, rc)
			deployed := false
			for _, action := range backend.actions {
				deployed = deployed || strings.HasPrefix(action, "deploy:")
			}
			if !deployed {
				t.Fatal("ordinary minor-version reconciliation did not execute")
			}
		})
	}
}

func TestCredentialCreateRefusesForeignRoleOnRecovery(t *testing.T) {
	for _, boundary := range []string{"first-write", "lost-response", "partial-publication"} {
		t.Run(boundary, func(t *testing.T) {
			p, rc, _ := setup(t, "single")
			converge(t, p, rc)
			originalSecrets := rc.Secrets
			if boundary == "partial-publication" {
				rc.Secrets = &failingAccessSecrets{SecretStore: originalSecrets, key: "access/revision"}
			}
			writes, foreignTouched := 0, false
			var firstID, firstVerifier string
			installCredentialAgent(t, rc, func(w http.ResponseWriter, req pgspec.AccessRequest) {
				if req.Operation == "list" {
					json.NewEncoder(w).Encode([]Role{})
					return
				}
				writes++
				if req.Operation != "journal" || req.RoleOperation != "create" {
					foreignTouched = true
				}
				if writes == 1 {
					firstID, firstVerifier = req.RequestID, *req.Role.Password
					if boundary == "lost-response" {
						w.WriteHeader(503)
						return
					}
					if boundary == "partial-publication" {
						json.NewEncoder(w).Encode(pgspec.AccessResult{Outcome: "applied", RoleOID: 17001})
						return
					}
				} else if req.RequestID != firstID || *req.Role.Password != firstVerifier {
					t.Error("recovery changed sealed intent")
				}
				code := "foreign_role_exists"
				if boundary != "first-write" {
					code = "role_identity_changed"
				}
				json.NewEncoder(w).Encode(pgspec.AccessResult{Outcome: "rejected", Code: code, Message: "Credential recovery refused: same-name native role belongs to a different identity", Status: 409})
			})
			if _, err := p.CreateRole(t.Context(), rc, RoleWrite{Role: Role{Name: "app", Login: true}}); err == nil {
				t.Fatal("fault did not reject the create")
			}
			if boundary != "first-write" {
				if pending, err := accessIntentPending(t.Context(), rc); !pending || err != nil {
					t.Fatal("ambiguous applied write lost intent", err)
				}
				rc.Secrets = originalSecrets
				p = New(Options{})
				if _, err := p.reconcileAccessIntent(t.Context(), rc); !errors.Is(err, engine.ErrConflict) || !strings.Contains(err.Error(), "role_identity_changed") {
					t.Fatalf("replacement conflict not surfaced: %v", err)
				}
			}
			if foreignTouched {
				t.Fatal("CREATE replay adopted foreign role as UPDATE")
			}
			if pending, err := accessIntentPending(t.Context(), rc); pending || err != nil {
				t.Fatal("foreign role refusal retained fence", err)
			}
			if _, err := secret(t.Context(), rc, rolePasswordKey("app")); !missing(err) {
				t.Fatal("foreign role password published", err)
			}
			if names, err := p.managedRoleNames(t.Context(), rc); err != nil || len(names) != 0 {
				t.Fatal("foreign role adopted into managed index", err)
			}
			if revision, err := p.accessRevision(t.Context(), rc); err != nil || revision != "" {
				t.Fatal("foreign role revision published", err)
			}
		})
	}
}

func TestCredentialLegacyAgentKeepsDirectMutationContract(t *testing.T) {
	p, rc, _ := setup(t, "single")
	converge(t, p, rc)
	mutations := 0
	installBackupAgent(t, rc, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/postgres/status" {
			json.NewEncoder(w).Encode(pgspec.StatusResponse{Running: true, Role: "standalone"})
			return
		}
		var req pgspec.AccessRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Error(err)
		}
		switch req.Operation {
		case "capabilities":
			w.WriteHeader(400)
		case "list":
			json.NewEncoder(w).Encode([]Role{})
		case "create":
			mutations++
			json.NewEncoder(w).Encode(pgspec.ActionResponse{OK: true})
		default:
			t.Errorf("new protocol sent to legacy agent: %s", req.Operation)
			w.WriteHeader(400)
		}
	})
	password, err := p.CreateRole(t.Context(), rc, RoleWrite{Role: Role{Name: "app", Login: true}})
	if err != nil || password == "" || mutations != 1 {
		t.Fatal("legacy credential management regressed", err)
	}
	if pending, err := accessIntentPending(t.Context(), rc); pending || err != nil {
		t.Fatal("legacy member acquired unsupported journal", err)
	}
	if users, err := p.proxyUsers(t.Context(), rc); err != nil || len(users) != 1 || users[0].Password != password {
		t.Fatal("legacy publication failed", err)
	}
}

func TestCredentialPreReceiptCreateNeverAdoptsSameNameRole(t *testing.T) {
	for _, legacyAgent := range []bool{false, true} {
		t.Run(map[bool]string{false: "new-agent", true: "old-agent"}[legacyAgent], func(t *testing.T) {
			p, rc, _ := setup(t, "single")
			converge(t, p, rc)
			selected := "sealed-pre-receipt-password"
			intent := accessIntent{Request: pgspec.AccessRequest{Operation: "create", Role: &RoleWrite{Role: Role{Name: "foreign"}}}, Password: &selected, Names: []string{"foreign"}, Revision: "pre-receipt-revision"}
			raw, err := json.Marshal(intent)
			if err != nil {
				t.Fatal(err)
			}
			if err = putSecret(t.Context(), rc, "access/pending", raw); err != nil {
				t.Fatal(err)
			}
			installBackupAgent(t, rc, func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/v1/postgres/status" {
					json.NewEncoder(w).Encode(pgspec.StatusResponse{Running: true, Role: "standalone"})
					return
				}
				var req pgspec.AccessRequest
				if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
					t.Error(err)
				}
				switch req.Operation {
				case "capabilities":
					if legacyAgent {
						w.WriteHeader(400)
					} else {
						json.NewEncoder(w).Encode(pgspec.AccessCapabilities{CredentialReceipts: true})
					}
				case "create":
					if !legacyAgent {
						t.Error("new member bypassed receipt")
					}
					w.WriteHeader(409)
				case "journal":
					if legacyAgent || req.RoleOperation != "create" || req.RequestID != intent.Revision {
						t.Error("invalid pre-receipt migration")
					}
					json.NewEncoder(w).Encode(pgspec.AccessResult{Outcome: "rejected", Code: "foreign_role_exists", Message: "Native role exists without an attributable receipt", Status: 409})
				default:
					t.Errorf("pre-receipt CREATE adopted native role: %s", req.Operation)
					w.WriteHeader(400)
				}
			})
			if _, err := p.reconcileAccessIntent(t.Context(), rc); !errors.Is(err, engine.ErrConflict) {
				t.Fatal("same-name legacy intent did not conflict", err)
			}
			if pending, err := accessIntentPending(t.Context(), rc); pending || err != nil {
				t.Fatal("legacy rejection retained fence", err)
			}
			if _, err := secret(t.Context(), rc, rolePasswordKey("foreign")); !missing(err) {
				t.Fatal("legacy recovery published foreign password", err)
			}
		})
	}
}
