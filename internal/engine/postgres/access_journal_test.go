// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
)

type failingAccessSecrets struct {
	engine.SecretStore
	key    string
	failed bool
}

func (s *failingAccessSecrets) SetClusterSecret(ctx context.Context, ref domain.ClusterRef, key string, data []byte) error {
	if key == s.key && !s.failed {
		s.failed = true
		return errors.New("sealed write unavailable")
	}
	return s.SecretStore.SetClusterSecret(ctx, ref, key, data)
}

func TestPasswordRotationReplaysSameIntentAfterCrash(t *testing.T) {
	for _, boundary := range []string{rolePasswordKey("app"), "access/roles", "access/revision", "sql-response", "sql-unknown"} {
		t.Run(boundary, func(t *testing.T) {
			p, rc, _ := setup(t, "single")
			converge(t, p, rc)
			// Avoid unrelated stanza work: the recovery must run before normal Sync.
			rc.Cluster.Spec.Backup.Storage = ""
			if err := putSecret(t.Context(), rc, "access/roles", []byte(`["app"]`)); err != nil {
				t.Fatal(err)
			}
			if err := putSecret(t.Context(), rc, rolePasswordKey("app"), []byte("previous-password")); err != nil {
				t.Fatal(err)
			}
			var verifiers []string
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
					if req.Operation == "list" {
						json.NewEncoder(w).Encode([]Role{{Name: "app", Login: true}})
						return
					}
					if req.Role == nil || req.Role.Password == nil {
						t.Error("missing verifier")
						w.WriteHeader(400)
						return
					}
					verifiers = append(verifiers, *req.Role.Password)
					if boundary == "sql-response" && len(verifiers) == 1 {
						w.WriteHeader(503)
						return
					}
					if boundary == "sql-unknown" && len(verifiers) == 1 {
						json.NewEncoder(w).Encode(pgspec.AccessResult{Outcome: "unknown"})
						return
					}
					json.NewEncoder(w).Encode(pgspec.AccessResult{Outcome: "applied", RoleOID: 17001})
				default:
					t.Errorf("unexpected route %s", r.URL.Path)
					w.WriteHeader(404)
				}
			})
			original := rc.Secrets
			rc.Secrets = &failingAccessSecrets{SecretStore: original, key: boundary}
			if _, err := p.RotateRolePassword(t.Context(), rc, "app"); err == nil {
				t.Fatal("fault boundary did not fail")
			}
			pending, err := secret(t.Context(), rc, "access/pending")
			if err != nil || len(pending) == 0 {
				t.Fatal("committed SQL has no durable recovery intent", err)
			}
			if strings.Contains(string(rc.Cluster.Internal), "previous-password") {
				t.Fatal("credential leaked into public state")
			}
			rc.Secrets = original
			p = New(Options{})
			if _, err := p.Sync(t.Context(), rc); err != nil {
				t.Fatal("new control plane could not replay credential intent", err)
			}
			if len(verifiers) != 2 || verifiers[0] != verifiers[1] {
				t.Fatal("recovery selected a different password/verifier")
			}
			current, err := secret(t.Context(), rc, rolePasswordKey("app"))
			if err != nil || len(current) == 0 || string(current) == "previous-password" {
				t.Fatal("selected password not recovered", err)
			}
			if revision, err := p.accessRevision(t.Context(), rc); err != nil || revision == "" {
				t.Fatal("revision not recovered", err)
			}
			if _, err := secret(t.Context(), rc, "access/pending"); !missing(err) {
				t.Fatal("completed credential intent was not cleared", err)
			}
		})
	}
}
