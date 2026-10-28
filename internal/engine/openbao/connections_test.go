// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package openbao

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres"
)

type connectionPostgres struct {
	engine.Provider
	postgres.Access
	roles            map[string]postgres.RoleWrite
	creates, deletes int
	fail             bool
	onDelete         func()
}

func (p *connectionPostgres) GetRole(_ context.Context, _ *engine.Context, name string) (*postgres.Role, error) {
	r, ok := p.roles[name]
	if !ok {
		return nil, engine.ErrNotFound
	}
	return &r.Role, nil
}

func (p *connectionPostgres) CreateRole(_ context.Context, _ *engine.Context, r postgres.RoleWrite) (string, error) {
	p.creates++
	if _, ok := p.roles[r.Name]; ok {
		return "", engine.ErrConflict
	}
	p.roles[r.Name] = r
	if p.fail {
		p.fail = false
		return "", engine.ErrUnavailable
	}
	return "", nil
}
func (p *connectionPostgres) DeleteRole(_ context.Context, _ *engine.Context, name string) error {
	if p.onDelete != nil {
		p.onDelete()
	}
	p.deletes++
	delete(p.roles, name)
	return nil
}
func connectionTargetFixture(rc *engine.Context) (Target, *connectionPostgres) {
	c := &domain.Cluster{Name: "target", Namespace: rc.Cluster.Namespace, Spec: domain.ClusterSpec{Provider: "postgres"}, Status: domain.ClusterStatus{Phase: domain.PhaseReady}}
	c.Spec.TLS.Mode = "requireTLS"
	targetRC := *rc
	targetRC.Cluster = c
	p := &connectionPostgres{roles: map[string]postgres.RoleWrite{}}
	return Target{Provider: p, Context: &targetRC}, p
}
func connectionWrite() DatabaseConnectionWrite {
	return DatabaseConnectionWrite{Name: "orders", TargetCluster: "target", Roles: []DatabaseRoleWrite{{Name: "reader", Access: AccessReadOnly}}}
}

func TestConnectionValidation(t *testing.T) {
	_, rc, _, _ := setup(t)
	target, _ := connectionTargetFixture(rc)
	tests := []struct {
		name   string
		change func(*DatabaseConnectionWrite)
	}{
		{"name", func(r *DatabaseConnectionWrite) { r.Name = "../bad" }},
		{"roles-empty", func(r *DatabaseConnectionWrite) { r.Roles = nil }},
		{"roles-many", func(r *DatabaseConnectionWrite) { r.Roles = make([]DatabaseRoleWrite, 17) }},
		{"access", func(r *DatabaseConnectionWrite) { r.Roles[0].Access = "admin" }},
		{"ttl-negative", func(r *DatabaseConnectionWrite) { r.Roles[0].DefaultTTLSeconds = -1 }},
		{"ttl-order", func(r *DatabaseConnectionWrite) { r.Roles[0].DefaultTTLSeconds = 90000 }},
		{"ttl-limit", func(r *DatabaseConnectionWrite) { r.Roles[0].MaxTTLSeconds = 31536001 }},
		{"postgres-database", func(r *DatabaseConnectionWrite) { r.Roles[0].Database = "other" }},
		{"duplicate", func(r *DatabaseConnectionWrite) { r.Roles = append(r.Roles, r.Roles[0]) }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := connectionWrite()
			tt.change(&r)
			if _, e := validateConnection(rc, target, r); e == nil {
				t.Fatal("accepted invalid request")
			}
		})
	}
	r, e := validateConnection(rc, target, connectionWrite())
	if e != nil || r.Roles[0].DefaultTTLSeconds != 3600 || r.Roles[0].MaxTTLSeconds != 86400 {
		t.Fatalf("defaults: %+v %v", r, e)
	}
}

func TestConnectionPendingCrashResumeConflictAndSecrets(t *testing.T) {
	p, rc, _, _ := setup(t)
	target, pg := connectionTargetFixture(rc)
	pg.fail = true
	_, e := p.CreateDatabaseConnection(t.Context(), rc, target, connectionWrite())
	if !errors.Is(e, engine.ErrUnavailable) {
		t.Fatal(e)
	}
	records, e := p.ListDatabaseConnections(t.Context(), rc)
	if e != nil || len(records) != 1 || records[0].Phase != ConnectionPending {
		t.Fatalf("pending: %+v %v", records, e)
	}
	cred, e := readConnectionCredential(t.Context(), rc, "orders")
	if e != nil {
		t.Fatal(e)
	}
	connectionPasswordContract.Assert(t, "persisted connection password", cred.Password)
	connectionUsernameContract.Assert(t, "generated PostgreSQL management username", cred.Username)
	// A reconstructed provider resumes an ambiguous target create using the sealed
	// password and deterministic identity; it never allocates another target user.
	p = New(p.options)
	_, _ = p.CreateDatabaseConnection(t.Context(), rc, target, connectionWrite())
	if len(pg.roles) != 1 || pg.creates != 2 {
		t.Fatalf("identity duplicated: %d/%d", len(pg.roles), pg.creates)
	}
	for _, r := range pg.roles {
		connectionUsernameContract.Assert(t, "native target username", r.Name)
		if r.Password == nil {
			t.Fatal("native target request omitted generated password")
		}
		connectionPasswordContract.Assert(t, "native target password", *r.Password)
		if *r.Password != cred.Password || r.Name != cred.Username {
			t.Fatal("native target identity differs from persisted credential")
		}
		if !r.Login || !r.Createrole || len(r.MemberOfAdmin) != 2 || strings.HasPrefix(r.Name, "kk_") {
			t.Fatalf("incorrect grant set: %+v", r)
		}
	}
	changed := connectionWrite()
	changed.Roles[0].Access = AccessReadWrite
	if _, e = p.CreateDatabaseConnection(t.Context(), rc, target, changed); !errors.Is(e, engine.ErrConflict) {
		t.Fatal(e)
	}
	raw, _ := secret(t.Context(), rc, SecretConnections)
	if strings.Contains(string(raw), cred.Password) || strings.Contains(string(rc.Cluster.Internal), cred.Password) {
		t.Fatal("password escaped sealed credential")
	}
	targets, e := p.ConnectionTargets(t.Context(), rc)
	if e != nil || len(targets) != 1 || targets[0].Name != "target" {
		t.Fatalf("targets: %+v %v", targets, e)
	}
}

func TestConnectionTrustAndCleanupResolver(t *testing.T) {
	p, rc, _, _ := setup(t)
	target, pg := connectionTargetFixture(rc)
	record := DatabaseConnection{Name: "orders", TargetCluster: "target", TargetProvider: "postgres", Phase: ConnectionPending}
	if e := saveConnections(t.Context(), rc, []DatabaseConnection{record}); e != nil {
		t.Fatal(e)
	}
	if e := rc.Secrets.SetClusterSecret(t.Context(), target.Context.Cluster.Ref(), "tls/ca.crt", []byte("target CA")); e != nil {
		t.Fatal(e)
	}
	cas, e := p.trustCAs(t.Context(), rc)
	if e != nil || len(cas) != 1 || cas[0] != "target CA" {
		t.Fatalf("trust: %+v %v", cas, e)
	}
	cred := connectionCredential{Username: "identity", Password: "sealed-password", Step: "identity"}
	_ = saveConnectionCredential(t.Context(), rc, record.Name, cred)
	pg.roles[cred.Username] = postgres.RoleWrite{}
	p.options.Targets = func(_ context.Context, ref domain.ClusterRef) (Target, error) {
		if ref != target.Context.Cluster.Ref() {
			t.Fatalf("wrong target: %+v", ref)
		}
		return target, nil
	}
	p.cleanupConnections(t.Context(), rc)
	if pg.deletes != 1 || len(pg.roles) != 0 {
		t.Fatal("cleanup did not release target identity")
	}
}

func TestConnectionRolePayloads(t *testing.T) {
	for _, provider := range []string{"postgres", "psmdb", "rabbitmq"} {
		for _, access := range []string{AccessReadOnly, AccessReadWrite} {
			r := DatabaseRole{DatabaseRoleWrite: DatabaseRoleWrite{Name: "reader", Access: access, DefaultTTLSeconds: 3600, MaxTTLSeconds: 86400}}
			if provider == "rabbitmq" {
				r.Database = "/"
			}
			payload := rolePayload(DatabaseConnection{Name: "orders", TargetProvider: provider}, r)
			b, e := json.Marshal(payload)
			if e != nil || !json.Valid(b) {
				t.Fatal(e)
			}
			if provider == "postgres" && !strings.Contains(string(b), "VALID UNTIL") {
				t.Fatal("missing expiration")
			}
			if provider == "psmdb" && !strings.Contains(string(b), "AnyDatabase") {
				t.Fatal("missing MongoDB grants")
			}
		}
	}
}

func TestConnectionPluginArtifacts(t *testing.T) {
	for _, p := range memberPlugins(Options{}) {
		if p.Version == "" || !strings.Contains(p.Command, "_linux_amd64_v1") {
			t.Fatalf("invalid declarative plugin: %+v", p)
		}
	}
	if _, e := pluginPrepare(Options{}, "nobody"); e != nil {
		t.Fatal(e)
	}
}
