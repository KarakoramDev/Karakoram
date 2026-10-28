// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/enginetest"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

// Ordinary SQL password bounds are the product validator, not GR's START limit.
// https://dev.mysql.com/doc/refman/8.4/en/user-names.html
var generatedPasswordContract = enginetest.CredentialLimit{Min: 12, Max: 1024, Alphabet: enginetest.Alphanumeric}

// START GROUP_REPLICATION maximum 32, ER_GROUP_REPLICATION_PASSWORD_LENGTH (3972).
var generatedRecoveryPasswordContract = enginetest.CredentialLimit{Min: 12, Max: 32, Alphabet: enginetest.Alphanumeric}

func TestProviderGeneratedCredentialContracts(t *testing.T) {
	for _, tc := range []struct{ provider, topology string }{{"mysql", "single"}, {"mysql", "group"}} {
		t.Run(tc.provider+"/"+tc.topology, func(t *testing.T) {
			p, rc, _ := setup(t, tc.topology)
			rc.Cluster.Spec.Provider = tc.provider
			recorded := enginetest.RecordCredentials(rc)
			s, err := load(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			s.allocate(rc.Cluster)
			if err := p.ensureSecrets(t.Context(), rc, s); err != nil {
				t.Fatal(err)
			}
			// Ordinary SQL passwords: product 12..1024, not the distinct GR limit.
			// https://dev.mysql.com/doc/refman/8.4/en/user-names.html (usernames <=32).
			password := generatedPasswordContract
			limits := map[string]enginetest.CredentialLimit{
				"users/root": password, "users/admin": password, "users/kk_agent": password,
				"users/kk_repl": password, "users/kk_monitor": password, "users/kk_backup": password, "users/kk_proxy_admin": password,
			}
			// START GROUP_REPLICATION password <=32 (ER_GROUP_REPLICATION_PASSWORD_LENGTH, 3972).
			// https://dev.mysql.com/doc/mysql-errors/8.4/en/server-error-reference.html
			limits["users/kk_repl"] = generatedRecoveryPasswordContract
			recorded.Assert(t, limits, 32)
		})
	}
}

func TestGeneratedApplicationCredentialContracts(t *testing.T) {
	for _, provider := range []string{"mysql", "pxc"} {
		t.Run(provider, func(t *testing.T) {
			var p *Core
			var rc *engine.Context
			var backend *fakeBackend
			if provider == "pxc" {
				p, rc, backend = setupPXC(t)
			} else {
				p, rc, backend = setup(t, "single")
			}
			proxy := rc.Cluster.Spec.Components.Extra["proxy"]
			proxy.Replicas = 0
			rc.Cluster.Spec.Components.Extra["proxy"] = proxy
			converge(t, p, rc, backend)
			var requests []myspec.AccessRequest
			installAccessAgent(t, rc, func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/v1/mysql/status":
					_ = json.NewEncoder(w).Encode(myspec.StatusResponse{MemberState: "ONLINE", MemberRole: "PRIMARY"})
				case "/v1/mysql/galera-status":
					_ = json.NewEncoder(w).Encode(myspec.GaleraStatusResponse{ClusterStatus: "Primary", LocalStateComment: "Synced", Ready: true, Connected: true, ClusterSize: 3})
				case "/v1/mysql/" + myspec.ActionAccessUsers:
					var request myspec.AccessRequest
					if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
						t.Error(err)
					}
					requests = append(requests, request)
					_ = json.NewEncoder(w).Encode(myspec.AccessResult{Outcome: "applied"})
				default:
					t.Errorf("unexpected access route %s", r.URL.Path)
					http.NotFound(w, r)
				}
			})
			var passwords []string
			for _, operation := range []string{"create", "rotate", "rotate-retaining-current"} {
				var password string
				var err error
				if operation == "create" {
					password, err = p.CreateUser(t.Context(), rc, UserWrite{Name: "application"})
				} else {
					password, err = p.RotateUserPassword(t.Context(), rc, "application", operation == "rotate-retaining-current")
				}
				if err != nil {
					t.Fatal(operation, err)
				}
				generatedPasswordContract.Assert(t, "returned "+operation, password)
				passwords = append(passwords, password)
			}
			if len(requests) != len(passwords) {
				t.Fatalf("native writes %d, want %d", len(requests), len(passwords))
			}
			for i, request := range requests {
				password := request.Password
				if request.User != nil {
					password = request.User.Password
				}
				if password == nil {
					t.Fatal("native request omitted generated credential")
				}
				generatedPasswordContract.Assert(t, "native "+request.Operation, *password)
				if *password != passwords[i] {
					t.Fatal("native and returned credentials differ")
				}
			}
			raw, err := secret(t.Context(), rc, accessCredentialsKey)
			if err != nil {
				t.Fatal(err)
			}
			var credentials map[string]accessCredential
			if err := json.Unmarshal(raw, &credentials); err != nil {
				t.Fatal(err)
			}
			if credentials["application"].Current != passwords[2] || credentials["application"].Secondary != passwords[1] {
				t.Fatal("persisted dual credentials differ from returned/native values")
			}
			generatedPasswordContract.Assert(t, "persisted current", credentials["application"].Current)
			generatedPasswordContract.Assert(t, "persisted secondary", credentials["application"].Secondary)
		})
	}
}
