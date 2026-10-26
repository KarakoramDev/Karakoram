// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/api"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

func claimedPGMutationFixture(t *testing.T) (*Server, string, *domain.Cluster, *domain.Operation) {
	t.Helper()
	s, _, token := testServer(t)
	p := postgres.New(postgres.Options{Store: s.Store})
	if err := s.Registry.Register(p); err != nil {
		t.Fatal(err)
	}
	c := &domain.Cluster{Name: "pgblocked", Spec: domain.ClusterSpec{Provider: "postgres", DataSource: &domain.DataSource{Cluster: "source", Backup: "base"}}}
	p.ApplyDefaults(&c.Spec)
	if err := s.Store.CreateBackup(t.Context(), &domain.Backup{Cluster: "source", Name: "base", Provider: "postgres", Type: "full", SourceVersion: c.Spec.Version, Phase: domain.PhaseSucceeded}); err != nil {
		t.Fatal(err)
	}
	op := &domain.Operation{ID: "owning-create", Kind: domain.OperationCreate}
	if err := s.Store.CreateCluster(store.WithOperation(t.Context(), op), c); err != nil {
		t.Fatal(err)
	}
	if err := s.Store.ClaimInitialDataSource(t.Context(), c.Ref()); err != nil {
		t.Fatal(err)
	}
	c.Status.Phase = domain.PhaseRestoring
	if err := s.Store.UpdateClusterState(t.Context(), c); err != nil {
		t.Fatal(err)
	}
	return s, token, c, op
}

func assertInitialMutationConflict(t *testing.T, status int, raw []byte, owner string) {
	t.Helper()
	var body api.Error
	if err := json.Unmarshal(raw, &body); err != nil {
		t.Fatal(err)
	}
	if status != 409 || body.Code != "conflict" || !strings.Contains(body.Message, owner) || body.Details == nil || (*body.Details)["resource"] != "operation" || (*body.Details)["name"] != owner || (*body.Details)["reason"] != domain.ReasonInvalidState {
		t.Fatalf("status=%d body=%s", status, raw)
	}
}

func TestClaimedInitialRestorePreemptsCredentialAndActionPrechecks(t *testing.T) {
	s, token, c, owner := claimedPGMutationFixture(t)
	for _, tc := range []struct{ name, method, path, body string }{
		{"rotate managed postgres", "POST", "/system-users/postgres/rotate", ""},
		{"rotate TLS", "POST", "/rotate-tls", ""},
		{"restart", "POST", "/restart", ""},
		{"missing member replace", "POST", "/members/notplaced/replace", ""},
		{"unsupported FCV", "POST", "/raise-fcv", `{"version":"8.0"}`},
		{"resume not suspended", "POST", "/resume", ""},
		{"retry no exhausted deployment", "POST", "/retry-deployments", ""},
		{"create database user", "POST", "/database-users", `{"username":"app","database":"app","roles":[]}`},
		{"update database user", "PUT", "/database-users/app/app", `{"roles":[]}`},
		{"delete database user", "DELETE", "/database-users/app/app", ""},
		{"create database role", "POST", "/database-roles", `{"name":"app","database":"app","privileges":[],"roles":[]}`},
		{"update database role", "PUT", "/database-roles/app/app", `{"privileges":[],"roles":[]}`},
		{"delete database role", "DELETE", "/database-roles/app/app", ""},
		{"create postgres role", "POST", "/postgres/roles", `{"name":"app"}`},
		{"patch postgres role", "PATCH", "/postgres/roles/app", `{"login":true}`},
		{"rotate postgres role", "POST", "/postgres/roles/app/password", ""},
		{"delete postgres role", "DELETE", "/postgres/roles/app", ""},
		{"create elastic user", "POST", "/elasticsearch/users", `{"username":"app","roles":[],"enabled":true,"password":"long-test-password"}`},
		{"update elastic user", "PUT", "/elasticsearch/users/app", `{"username":"app","roles":[],"enabled":true}`},
		{"delete elastic user", "DELETE", "/elasticsearch/users/app", ""},
		{"create elastic role", "POST", "/elasticsearch/roles", `{"name":"app","cluster":[],"indices":[],"applications":[]}`},
		{"create rabbit user", "POST", "/rabbitmq/users", `{"name":"app","tags":[],"password":"long-test-password"}`},
		{"set supabase SMTP credential", "PUT", "/supabase/smtp-password", `{"password":"long-test-password"}`},
		{"rotate supabase JWT", "POST", "/supabase/rotate-jwt-secret", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			response := call(s, tc.method, "/api/v1/clusters/"+c.Name+tc.path, tc.body, token)
			assertInitialMutationConflict(t, response.Code, response.Body.Bytes(), owner.ID)
		})
	}
	// The CLI does not send an idempotency key; raw HTTP clients must receive the
	// same owner error through that wrapper too, with no accepted operation.
	for range 2 {
		response := idempotentCall(s, "POST", "/api/v1/clusters/"+c.Name+"/system-users/postgres/rotate", "", token, "d574-rotate")
		assertInitialMutationConflict(t, response.Code, response.Body.Bytes(), owner.ID)
	}
	ops, err := s.Store.ListOperations(t.Context(), c.Ref())
	if err != nil || len(ops) != 1 || ops[0].ID != owner.ID {
		t.Fatalf("fenced actions mutated operation inventory: %+v %v", ops, err)
	}
}

func TestUnsupportedPostgresActionNamesEngineAndSupportedRoleAPI(t *testing.T) {
	s, _, token := testServer(t)
	p := postgres.New(postgres.Options{Store: s.Store})
	if err := s.Registry.Register(p); err != nil {
		t.Fatal(err)
	}
	c := &domain.Cluster{Name: "pgplain", Spec: domain.ClusterSpec{Provider: "postgres"}}
	p.ApplyDefaults(&c.Spec)
	if err := s.Store.CreateCluster(t.Context(), c); err != nil {
		t.Fatal(err)
	}
	response := call(s, "POST", "/api/v1/clusters/"+c.Name+"/system-users/postgres/rotate", "", token)
	var body api.Error
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if response.Code != 409 || body.Details == nil || (*body.Details)["resource"] != "operation" || (*body.Details)["name"] != domain.OperationRotateSystemUser || (*body.Details)["reason"] != domain.ReasonUnsupported || !strings.Contains(body.Message, "postgres") || !strings.Contains(body.Message, "non-reserved") || !strings.Contains(body.Message, "/postgres/roles/{role}/password") || !strings.Contains(body.Message, "engine-owned") {
		t.Fatalf("unsupported action stayed opaque/misleading: %d %s", response.Code, response.Body.String())
	}
	response = call(s, "POST", "/api/v1/clusters/"+c.Name+"/raise-fcv", `{"version":"8.0"}`, token)
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if response.Code != 409 || body.Details == nil || (*body.Details)["name"] != domain.OperationRaiseFCV || (*body.Details)["reason"] != domain.ReasonUnsupported || !strings.Contains(body.Message, "postgres") {
		t.Fatalf("unsupported FCV stayed opaque: %d %s", response.Code, response.Body.String())
	}
}

func TestInitialMutationClassifierPreservesReadAndLifecycleExceptions(t *testing.T) {
	for _, name := range []string{"GetPostgresRole", "ListDatabaseUsers", "ListElasticRoles", "GetRabbitMQUser", "GetSupabaseCredentials", "CreateBackup", "DeleteBackup", "DeleteCluster", "PatchCluster", "ReplaceCluster", "CreateRestore"} {
		if initialRestoreMutation(name) {
			t.Errorf("non-fenced/independent path %s was fenced", name)
		}
	}
	for _, name := range []string{"CreateDatabaseUser", "ReplaceDatabaseRole", "DeleteDatabaseRole", "CreatePostgresRole", "UpdatePostgresRole", "PatchPostgresRole", "DeletePostgresRole", "RotatePostgresRolePassword", "CreatePostgresDatabase", "DeletePostgresDatabase", "GrantPostgresPrivileges", "RevokePostgresPrivileges", "CreateElasticAPIKey", "InvalidateElasticAPIKey", "CreateElasticRole", "UpdateElasticRole", "DeleteElasticRole", "ReplaceRabbitMQUser", "CreateRabbitMQPermission", "ReplaceRabbitMQTopicPermission", "DeleteRabbitMQPolicy", "ImportRabbitMQDefinitions", "PutSupabaseFunction", "DeleteSupabaseFunction", "SetSupabaseSMTPPassword", "RotateSupabaseJWTSecret", "ReplaceClusterBalancer"} {
		if !initialRestoreMutation(name) {
			t.Errorf("native mutating path %s lost initial restore fence", name)
		}
	}
}
