// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/api"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/openbao"
	"github.com/KarakoramDev/Karakoram/internal/store"
	"github.com/KarakoramDev/Karakoram/pkg/client"
)

type fakeOpenBaoAccess struct {
	fakeProvider
	connections []openbao.DatabaseConnection
	err         error
	mutations   int
}

func (*fakeOpenBaoAccess) Name() string { return "openbao" }
func (f *fakeOpenBaoAccess) Describe() domain.ProviderInfo {
	info := f.fakeProvider.Describe()
	info.Name = "openbao"
	return info
}
func (f *fakeOpenBaoAccess) RevealUnsealKeys(context.Context, *engine.Context) (openbao.UnsealKeys, error) {
	return openbao.UnsealKeys{SealType: "shamir", Threshold: 1, Shares: []string{"private-share"}, InitialRootToken: "private-root", InitializedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}, f.err
}
func (f *fakeOpenBaoAccess) ListDatabaseConnections(context.Context, *engine.Context) ([]openbao.DatabaseConnection, error) {
	return f.connections, f.err
}
func (f *fakeOpenBaoAccess) CreateDatabaseConnection(_ context.Context, _ *engine.Context, target openbao.Target, req openbao.DatabaseConnectionWrite) (openbao.DatabaseConnection, error) {
	f.mutations++
	connection := openbao.DatabaseConnection{Name: req.Name, TargetCluster: target.Context.Cluster.Name, TargetProvider: target.Provider.Name(), MountPath: "database", Phase: "ready", Roles: []openbao.DatabaseRole{}, CreatedAt: time.Now()}
	f.connections = append(f.connections, connection)
	return connection, f.err
}
func (f *fakeOpenBaoAccess) DeleteDatabaseConnection(context.Context, *engine.Context, openbao.Target, string) error {
	f.mutations++
	f.connections = nil
	return f.err
}
func (f *fakeOpenBaoAccess) ConnectionTargets(_ context.Context, rc *engine.Context) ([]domain.ClusterRef, error) {
	refs := make([]domain.ClusterRef, len(f.connections))
	for i, c := range f.connections {
		refs[i] = domain.ClusterRef{Namespace: rc.Namespace, Name: c.TargetCluster}
	}
	return refs, f.err
}

type fakeOpenBaoTarget struct{ fakeProvider }

func (*fakeOpenBaoTarget) Name() string { return "postgres" }
func (f *fakeOpenBaoTarget) Describe() domain.ProviderInfo {
	info := f.fakeProvider.Describe()
	info.Name = "postgres"
	return info
}
func openbaoTestServer(t *testing.T) (*Server, *fakeOpenBaoAccess, string) {
	t.Helper()
	s, _, token := testServer(t)
	f := &fakeOpenBaoAccess{}
	registry, err := engine.NewRegistry(f, &fakeProvider{}, &fakeOpenBaoTarget{})
	if err != nil {
		t.Fatal(err)
	}
	s.Registry = registry
	cluster, err := s.Store.GetCluster(t.Context(), domain.ClusterRef{Name: "prod"})
	if err != nil {
		t.Fatal(err)
	}
	cluster.Spec.Provider = "openbao"
	if err := s.Store.UpdateClusterSpec(t.Context(), cluster.Ref(), cluster.Spec); err != nil {
		t.Fatal(err)
	}
	target := domain.Cluster{Name: "database", Spec: domain.ClusterSpec{Provider: "postgres", Version: "17.11", Topology: domain.Topology{Type: "single"}}}
	(&fakeProvider{}).ApplyDefaults(&target.Spec)
	if err := s.Store.CreateCluster(t.Context(), &target); err != nil {
		t.Fatal(err)
	}
	return s, f, token
}
func TestOpenBaoReveal(t *testing.T) {
	s, f, token := openbaoTestServer(t)
	path := "/api/v1/clusters/prod/openbao/unseal-keys/reveal"
	if w := call(s, "POST", path, `{"confirm":"wrong"}`, token); w.Code != 400 {
		t.Fatal(w.Code, w.Body.String())
	}
	w := call(s, "POST", path, `{"confirm":"prod"}`, token)
	if w.Code != 200 || w.Header().Get("Cache-Control") != "no-store" || !strings.Contains(w.Body.String(), "private-share") {
		t.Fatal(w.Code, w.Body.String(), w.Header())
	}
	events, err := s.Store.ListEvents(t.Context(), store.EventFilter{Cluster: "prod", Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(events)
	if !strings.Contains(string(data), "OpenBaoUnsealKeysRevealed") || strings.Contains(string(data), "private-") {
		t.Fatal(string(data))
	}
	f.err = engine.ErrUnavailable
	if w := call(s, "POST", path, `{"confirm":"prod"}`, token); w.Code != 503 {
		t.Fatal(w.Code, w.Body.String())
	}
	for _, role := range []string{"operator", "viewer"} {
		if _, err := s.Auth.CreateAccount(t.Context(), domain.Account{Name: role, Roles: []string{"role:" + role}}, "long-test-password"); err != nil {
			t.Fatal(err)
		}
		roleToken, _, err := s.Auth.CreateToken(t.Context(), role, "test", nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		if w := call(s, "POST", path, `{"confirm":"prod"}`, roleToken); w.Code != 403 {
			t.Fatal(role, w.Code, w.Body.String())
		}
	}
}

func TestOpenBaoRevealRestrictedTokens(t *testing.T) {
	s, _, _ := openbaoTestServer(t)
	for _, tc := range []struct {
		name         string
		restrictions domain.TokenRestrictions
		status       int
	}{
		{"no-secret-reveal", domain.TokenRestrictions{}, 403},
		{"read-only", domain.TokenRestrictions{ReadOnly: true, AllowCredentialReveal: true}, 403},
		{"wrong-permission", domain.TokenRestrictions{Permissions: []string{"cluster-credentials:read"}, AllowCredentialReveal: true}, 403},
		{"wrong-cluster", domain.TokenRestrictions{Permissions: []string{"openbao-unseal-keys:reveal"}, Clusters: []string{"default/database"}, AllowCredentialReveal: true}, 403},
		{"explicit-reveal", domain.TokenRestrictions{Permissions: []string{"openbao-unseal-keys:reveal"}, Clusters: []string{"default/prod"}, AllowCredentialReveal: true}, 200},
	} {
		t.Run(tc.name, func(t *testing.T) {
			token := restrictedAdmin(t, s, &tc.restrictions)
			w := call(s, "POST", "/api/v1/clusters/prod/openbao/unseal-keys/reveal", `{"confirm":"prod"}`, token)
			if w.Code != tc.status {
				t.Fatalf("status = %d, want %d: %s", w.Code, tc.status, w.Body.String())
			}
		})
	}
}
func TestOpenBaoConnectionAdmissionAndDeletion(t *testing.T) {
	s, f, token := openbaoTestServer(t)
	path := "/api/v1/clusters/prod/openbao/database-connections"
	for _, target := range []string{"prod", "dev", "other/database"} {
		body := `{"name":"app","targetCluster":"` + target + `","roles":[{"name":"reader","access":"readonly"}]}`
		if w := call(s, "POST", path, body, token); w.Code != 400 {
			t.Fatal(target, w.Code, w.Body.String())
		}
	}
	body := `{"name":"app","targetCluster":"database","roles":[{"name":"reader","access":"readonly"}]}`
	if w := call(s, "POST", path, body, token); w.Code != 201 {
		t.Fatal(w.Code, w.Body.String())
	}
	if w := call(s, "GET", path+"/app", "", token); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	if w := call(s, "DELETE", "/api/v1/clusters/database", "", token); w.Code != 409 || !strings.Contains(w.Body.String(), "prod connection app") {
		t.Fatal(w.Code, w.Body.String())
	}
	if w := call(s, "DELETE", path+"/app", "", token); w.Code != 204 {
		t.Fatal(w.Code, w.Body.String())
	}
	if f.mutations != 2 {
		t.Fatal(f.mutations)
	}
}
func TestOpenBaoTargetPermission(t *testing.T) {
	s, f, admin := openbaoTestServer(t)
	if _, err := s.Auth.CreateAccount(t.Context(), domain.Account{Name: "operator", Roles: []string{"role:operator"}}, "long-test-password"); err != nil {
		t.Fatal(err)
	}
	token, _, err := s.Auth.CreateToken(t.Context(), "operator", "test", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	policy := s.RBAC.Policy() + "p, role:operator, database-users, create, default/database, deny\n"
	if err := s.RBAC.ReplacePolicy(t.Context(), policy); err != nil {
		t.Fatal(err)
	}
	body := `{"name":"app","targetCluster":"database","roles":[{"name":"reader","access":"readonly"}]}`
	if w := call(s, "POST", "/api/v1/clusters/prod/openbao/database-connections", body, token); w.Code != 403 || f.mutations != 0 {
		t.Fatal(w.Code, w.Body.String())
	}
	if w := call(s, "GET", "/api/v1/clusters/dev/openbao/database-connections", "", admin); w.Code != 409 {
		t.Fatal(w.Code, w.Body.String())
	}
}

func TestOpenBaoSecretRevealNeverUsesIdempotencyStorage(t *testing.T) {
	s, f, token := openbaoTestServer(t)
	path := "/api/v1/clusters/prod/openbao/unseal-keys/reveal"
	first := idempotentCall(s, "POST", path, `{"confirm":"prod"}`, token, "reveal-request")
	if first.Code != 200 {
		t.Fatal(first.Code, first.Body.String())
	}
	f.err = engine.ErrUnavailable
	second := idempotentCall(s, "POST", path, `{"confirm":"prod"}`, token, "reveal-request")
	if second.Code != 503 || strings.Contains(second.Body.String(), "private-") {
		t.Fatal("reveal was cached", second.Code, second.Body.String())
	}
}

func TestOpenBaoConnectionIdempotency(t *testing.T) {
	s, f, token := openbaoTestServer(t)
	path := "/api/v1/clusters/prod/openbao/database-connections"
	body := `{"name":"app","targetCluster":"database","roles":[{"name":"reader","access":"readonly"}]}`
	first := idempotentCall(s, "POST", path, body, token, "connection-request")
	f.err = engine.ErrConflict
	second := idempotentCall(s, "POST", path, body, token, "connection-request")
	assertReplay(t, first, second, 201)
	if f.mutations != 1 {
		t.Fatal("connection was created more than once", f.mutations)
	}
}

func TestOpenBaoConnectionIdempotencyIncludesNamespace(t *testing.T) {
	s, f, token := openbaoTestServer(t)
	s.namespaces = handlerNamespaces{}
	s.Namespaces = s.namespaces
	if err := s.Store.CreateNamespace(t.Context(), &domain.Namespace{Name: "dev", Phase: "Ready"}); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"prod", "database"} {
		cluster, err := s.Store.GetCluster(t.Context(), domain.ClusterRef{Name: name})
		if err != nil {
			t.Fatal(err)
		}
		cluster.Namespace = "dev"
		if err := s.Store.CreateCluster(t.Context(), cluster); err != nil {
			t.Fatal(err)
		}
	}
	path := "/api/v1/clusters/prod/openbao/database-connections"
	body := `{"name":"app","targetCluster":"database","roles":[{"name":"reader","access":"readonly"}]}`
	for _, namespace := range []string{"default", "dev"} {
		first := idempotentCall(s, "POST", path+"?namespace="+namespace, body, token, "same-key")
		second := idempotentCall(s, "POST", path+"?namespace="+namespace, body, token, "same-key")
		assertReplay(t, first, second, 201)
	}
	if f.mutations != 2 {
		t.Fatalf("namespaces shared an idempotency identity: created %d connections", f.mutations)
	}
}

func TestOpenBaoSchemaPreservesRestorePhaseIdentifiers(t *testing.T) {
	serverPhases := []api.RestorePhase{api.RestorePhasePending, api.RestorePhaseRunning, api.RestorePhaseSucceeded, api.RestorePhaseFailed}
	clientPhases := []client.RestorePhase{client.RestorePhasePending, client.RestorePhaseRunning, client.RestorePhaseSucceeded, client.RestorePhaseFailed}
	for i, expected := range []string{"Pending", "Running", "Succeeded", "Failed"} {
		if string(serverPhases[i]) != expected || string(clientPhases[i]) != expected {
			t.Fatalf("restore phase compatibility changed: %v / %v", serverPhases, clientPhases)
		}
	}
}

func TestOpenBaoDeletionDoesNotBlockItsOwnCleanup(t *testing.T) {
	s, f, token := openbaoTestServer(t)
	f.connections = []openbao.DatabaseConnection{{Name: "app", TargetCluster: "database"}}
	if w := call(s, "DELETE", "/api/v1/clusters/prod", "", token); w.Code != 202 {
		t.Fatal(w.Code, w.Body.String())
	}
}

func TestOpenBaoTargetMustExistInSameNamespace(t *testing.T) {
	s, f, token := openbaoTestServer(t)
	if err := s.Store.CreateNamespace(t.Context(), &domain.Namespace{Name: "other", Phase: "Ready"}); err != nil {
		t.Fatal(err)
	}
	remote := domain.Cluster{Namespace: "other", Name: "remote", Spec: domain.ClusterSpec{Provider: "postgres"}}
	(&fakeProvider{}).ApplyDefaults(&remote.Spec)
	if err := s.Store.CreateCluster(t.Context(), &remote); err != nil {
		t.Fatal(err)
	}
	body := `{"name":"app","targetCluster":"remote","roles":[{"name":"reader","access":"readonly"}]}`
	if w := call(s, "POST", "/api/v1/clusters/prod/openbao/database-connections", body, token); w.Code != 404 || f.mutations != 0 {
		t.Fatal(w.Code, w.Body.String())
	}
}

func TestOpenBaoRevealPermissionCanBeChecked(t *testing.T) {
	s, _, admin := openbaoTestServer(t)
	for _, role := range []string{"admin", "operator", "viewer"} {
		token := admin
		if role != "admin" {
			if _, err := s.Auth.CreateAccount(t.Context(), domain.Account{Name: role, Roles: []string{"role:" + role}}, "long-test-password"); err != nil {
				t.Fatal(err)
			}
			var err error
			token, _, err = s.Auth.CreateToken(t.Context(), role, "test", nil, nil)
			if err != nil {
				t.Fatal(err)
			}
		}
		w := call(s, "POST", "/api/v1/rbac/can", `{"resource":"openbao-unseal-keys","action":"reveal","object":"default/prod"}`, token)
		var result struct {
			Allowed bool `json:"allowed"`
		}
		if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
			t.Fatal(err)
		}
		if w.Code != 200 || result.Allowed != (role == "admin") {
			t.Fatalf("%s: %d %s", role, w.Code, w.Body.String())
		}
	}
}
