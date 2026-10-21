// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres"
	"github.com/KarakoramDev/Karakoram/internal/pgbinding"
)

func TestRestoreRequiresBoundConsumerAcknowledgement(t *testing.T) {
	s, _, token := testServer(t)
	if err := s.Registry.Register(&restoreTestProvider{fakeProvider: &fakeProvider{}, name: "postgres"}); err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()
	c, err := s.Store.GetCluster(ctx, domain.ClusterRef{Name: "prod"})
	if err != nil {
		t.Fatal(err)
	}
	c.Spec.Provider = "postgres"
	if err := s.Store.UpdateClusterSpec(ctx, c.Ref(), c.Spec); err != nil {
		t.Fatal(err)
	}
	backup := domain.Backup{Name: "saved", Cluster: c.Name, Provider: "postgres", Type: "full", Phase: domain.PhaseSucceeded}
	if err := s.Store.CreateBackup(ctx, &backup); err != nil {
		t.Fatal(err)
	}
	binding := domain.PostgresBinding{ID: "identity-binding", SourceCluster: c.Name, ConsumerKind: "keycloak", ConsumerName: "identity", Databases: []string{"identity"}, RoleName: "kb_identity", State: "ready"}
	if _, err := s.Store.EnsurePostgresBinding(ctx, &binding); err != nil {
		t.Fatal(err)
	}
	w := call(s, "POST", "/api/v1/clusters/prod/restores", `{"name":"restore","source":{"backup":"saved"}}`, token)
	if w.Code != 409 || !strings.Contains(w.Body.String(), "bound_consumers") || !strings.Contains(w.Body.String(), "identity") {
		t.Fatalf("missing consumer conflict: %d %s", w.Code, w.Body.String())
	}
	operations, err := s.Store.ListOperations(ctx, c.Ref())
	if err != nil || len(operations) != 0 {
		t.Fatalf("rejected restore enqueued work: %+v %v", operations, err)
	}
	w = call(s, "POST", "/api/v1/clusters/prod/restores", `{"name":"restore","acknowledgeBoundConsumers":true,"source":{"backup":"saved"}}`, token)
	if w.Code != 202 {
		t.Fatalf("acknowledged restore rejected: %d %s", w.Code, w.Body.String())
	}
}

func TestBindingMetadataContainsNoCredentialFields(t *testing.T) {
	out := postgresBindingAPI(domain.PostgresBinding{ID: "binding", ConsumerKind: "keycloak", ConsumerNamespace: "default", ConsumerName: "identity", RoleName: "kb_identity", MinMajor: 16, MaxMajor: 18})
	data, err := json.Marshal(out)
	if err != nil {
		t.Fatal(err)
	}
	var body map[string]any
	if err := json.Unmarshal(data, &body); err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"password", "credentialRef", "uri", "sourceUid"} {
		if _, exists := body[forbidden]; exists {
			t.Fatalf("binding exposes %s", forbidden)
		}
	}
	if body["consumer"].(map[string]any)["name"] != "identity" || body["databases"] == nil || body["extensions"] == nil {
		t.Fatalf("invalid binding metadata: %s", data)
	}
}

func TestBindingListAndWrongSourceRotation(t *testing.T) {
	s, token, _ := postgresTestServer(t)
	ctx := t.Context()
	c, err := s.Store.GetCluster(ctx, domain.ClusterRef{Name: "prod"})
	if err != nil {
		t.Fatal(err)
	}
	c.Spec.Provider = "postgres"
	if err := s.Store.UpdateClusterSpec(ctx, c.Ref(), c.Spec); err != nil {
		t.Fatal(err)
	}
	if err := s.Registry.Register(&bindingAccessProvider{fakePostgresAccess: &fakePostgresAccess{}}); err != nil {
		t.Fatal(err)
	}
	binding := domain.PostgresBinding{ID: "identity-binding", SourceCluster: "prod", ConsumerKind: "keycloak", ConsumerName: "identity", Databases: []string{"identity"}, RoleName: "kb_identity", State: "ready"}
	if _, err := s.Store.EnsurePostgresBinding(ctx, &binding); err != nil {
		t.Fatal(err)
	}
	w := call(s, "GET", "/api/v1/clusters/prod/postgres/bindings", "", token)
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"name":"identity"`) || !strings.Contains(w.Body.String(), `"kind":"keycloak"`) {
		t.Fatalf("binding list: %d %s", w.Code, w.Body.String())
	}
	other := domain.Cluster{Name: "other", Spec: domain.ClusterSpec{Provider: "fake", Version: "8.0"}}
	if err := s.Store.CreateCluster(ctx, &other); err != nil {
		t.Fatal(err)
	}
	w = call(s, "POST", "/api/v1/clusters/other/postgres/bindings/identity-binding/rotate", "", token)
	if w.Code != 404 {
		t.Fatalf("rotation crossed source boundary: %d %s", w.Code, w.Body.String())
	}
	access := &bindingAccessProvider{fakePostgresAccess: &fakePostgresAccess{roles: []postgres.Role{{Name: binding.RoleName, Login: true}}}}
	s.postgresBindings = pgbinding.New(pgbinding.Options{
		Store: s.Store,
		Source: func(context.Context, domain.ClusterRef) (*engine.Context, pgbinding.Access, error) {
			return &engine.Context{Cluster: c}, access, nil
		},
	})
	first := idempotentCall(s, "POST", "/api/v1/clusters/prod/postgres/bindings/identity-binding/rotate", "", token, "binding-rotation")
	second := idempotentCall(s, "POST", "/api/v1/clusters/prod/postgres/bindings/identity-binding/rotate", "", token, "binding-rotation")
	assertReplay(t, first, second, 200)
	if access.writes != 1 || strings.Contains(first.Body.String(), "password") {
		t.Fatalf("rotation replay wrote again or leaked credentials: %d %s", access.writes, first.Body.String())
	}
	stored, err := s.Store.GetPostgresBinding(ctx, binding.ID)
	if err != nil || stored.CredentialGeneration != 1 {
		t.Fatalf("rotation generation: %+v %v", stored, err)
	}
}

type bindingAccessProvider struct{ *fakePostgresAccess }

func (*bindingAccessProvider) Name() string { return "postgres" }

func (*bindingAccessProvider) EnsureBindingRole(context.Context, *engine.Context, string, int) error {
	return nil
}
func (p *bindingAccessProvider) VerifyBindingRole(ctx context.Context, rc *engine.Context, name string) (uint32, error) {
	_, err := p.GetRole(ctx, rc, name)
	return 1, err
}
func (*bindingAccessProvider) OwnBindingDatabase(context.Context, *engine.Context, postgres.Database) error {
	return nil
}
func (*bindingAccessProvider) BindingEndpoint(context.Context, *engine.Context) (string, []byte, error) {
	return "postgres.example", []byte("test-ca"), nil
}
