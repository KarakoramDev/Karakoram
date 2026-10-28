// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package supabase

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/enginetest"
	"github.com/KarakoramDev/Karakoram/internal/engine/supabase/sbspec"
)

var generatedRolePasswordLimit = enginetest.CredentialLimit{Min: 12, Max: 1024, Alphabet: enginetest.Alphanumeric}

func TestProviderGeneratedCredentialContracts(t *testing.T) {
	p, rc, _ := setupProvider(t)
	recorded := enginetest.RecordCredentials(rc)
	if err := ensureCredentials(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	if _, err := p.syncComponents(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	if _, ok := recorded.Values[credentialsKey]; !ok {
		t.Fatal("provider did not persist credentials")
	}
	c, err := loadCredentials(t.Context(), rc)
	if err != nil {
		t.Fatal(err)
	}
	// PostgreSQL roles <=63 bytes (default NAMEDATALEN), product password 12..1024.
	// https://www.postgresql.org/docs/current/sql-syntax-lexical.html
	// https://www.postgresql.org/docs/current/sql-createrole.html
	roles := map[string]bool{"supabase_admin": true, "authenticator": true, "supabase_auth_admin": true, "supabase_storage_admin": true, "supabase_functions_admin": true, "pgbouncer": true, "supabase_replication_admin": true, "supabase_read_only_user": true}
	for name, password := range c.Roles {
		if !roles[name] {
			t.Errorf("role %q has no declared credential contract", name)
		}
		if len(name) > 63 {
			t.Errorf("role %q exceeds PostgreSQL identifier limit", name)
		}
		generatedRolePasswordLimit.Assert(t, name, password)
	}
	for name := range roles {
		if _, ok := c.Roles[name]; !ok {
			t.Errorf("role %q was not generated", name)
		}
	}
	enginetest.AgentCredential.Assert(t, "VectorToken", c.VectorToken)
	enginetest.AgentCredential.Assert(t, "RealtimeAdminSecret", c.RealtimeAdminSecret)
	enginetest.SupabaseCredentialContracts(t, c.Keys)
	// Reject newly added fields in the persisted envelope, as well as new Keys fields.
	envelope := reflect.TypeOf(c)
	for i := range envelope.NumField() {
		switch envelope.Field(i).Name {
		case "Keys", "Roles", "VectorToken", "RealtimeAdminSecret", "RotationID":
		default:
			t.Errorf("credential field %q has no declared contract", envelope.Field(i).Name)
		}
	}
	delete(recorded.Values, credentialsKey) // Already checked the entire nested credential document above.
	recorded.Assert(t, map[string]enginetest.CredentialLimit{}, 0)
}

func assertRuntimeCredentialContracts(t *testing.T, p *Provider, rc *engine.Context) sealedCredentials {
	t.Helper()
	c, err := loadCredentials(t.Context(), rc)
	if err != nil {
		t.Fatal(err)
	}
	enginetest.SupabaseCredentialContracts(t, c.Keys)
	enginetest.AgentCredential.Assert(t, "RealtimeAdminSecret", c.RealtimeAdminSecret)
	enginetest.AgentCredential.Assert(t, "VectorToken", c.VectorToken)
	for _, role := range roleNames {
		generatedRolePasswordLimit.Assert(t, role, c.Roles[role])
	}
	returned, err := p.Credentials(t.Context(), rc)
	if err != nil {
		t.Fatal(err)
	}
	want := Credentials{APIURL: returned.APIURL, StudioURL: returned.StudioURL,
		AnonKey: c.Keys.AnonKey, ServiceRoleKey: c.Keys.ServiceRoleKey,
		PublishableKey: c.Keys.PublishableKey, SecretKey: c.Keys.SecretKey,
		DashboardUsername: "supabase", DashboardPassword: c.Keys.DashboardPassword, JWTSecret: c.Keys.JWTSecret}
	if returned != want {
		t.Fatal("returned credentials differ from persisted credentials")
	}
	s, err := loadState(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	if len(s.Members) == 0 {
		t.Fatal("no native component configs")
	}
	for _, member := range s.Members {
		config, err := p.MemberConfig(t.Context(), rc, member.Name)
		if err != nil {
			t.Fatal(err)
		}
		var spec sbspec.Spec
		if err := json.Unmarshal(config.EngineSpec, &spec); err != nil {
			t.Fatal(err)
		}
		// Each component receives only its consumed secrets. Reconstruct the
		// persisted credential envelope, then apply the production least-privilege
		// projection rather than requiring every secret on every component.
		want := spec
		want.DBRoles = make(map[string]string, len(c.Roles)+1)
		for role, password := range c.Roles {
			want.DBRoles[role] = password
		}
		// The core PostgreSQL password has its own engine contract.
		want.DBRoles["postgres"] = spec.DBRoles["postgres"]
		want.JWTSecret, want.JWKS, want.JWTKeys = c.Keys.JWTSecret, string(c.Keys.JWKS), string(c.Keys.JWTKeys)
		want.AnonKey, want.ServiceRoleKey = c.Keys.AnonKey, c.Keys.ServiceRoleKey
		want.PublishableKey, want.SecretKey = c.Keys.PublishableKey, c.Keys.SecretKey
		want.AnonAsymmetricJWT, want.ServiceAsymmetricJWT = c.Keys.AnonJWT, c.Keys.ServiceRoleJWT
		want.SecretKeyBase, want.RealtimeDBEncKey = c.Keys.SecretKeyBase, c.Keys.RealtimeDBEncKey
		want.VaultEncKey = c.Keys.VaultEncKey
		want.LogflarePublicToken, want.LogflarePrivateToken = c.Keys.LogflarePublicToken, c.Keys.LogflarePrivateToken
		want.Studio.DashboardUsername, want.Studio.DashboardPassword = "supabase", c.Keys.DashboardPassword
		want.Studio.MetaCryptoKey = c.Keys.PGMetaCryptoKey
		want.RealtimeAdminSecret, want.VectorSinkToken = c.RealtimeAdminSecret, c.VectorToken
		want = want.ProjectSecrets()
		if !reflect.DeepEqual(spec, want) {
			t.Fatalf("%s native component credentials differ from persisted projection", spec.Component)
		}
		for role, password := range want.DBRoles {
			if role != "postgres" {
				generatedRolePasswordLimit.Assert(t, spec.Component+"/"+role, password)
			}
		}
	}
	return c
}

func TestRuntimeCredentialContracts(t *testing.T) {
	p, rc, _ := setupProvider(t)
	convergeProvider(t, p, rc)
	before := assertRuntimeCredentialContracts(t, p, rc)
	op := &domain.Operation{ID: "credential-contract-rotation", Kind: "rotate-jwt-secret"}
	done := false
	for range 50 {
		result, err := p.RunOperation(t.Context(), rc, op)
		if err != nil {
			t.Fatal(err)
		}
		if result.Done {
			done = true
			break
		}
	}
	if !done {
		t.Fatal("JWT rotation did not finish")
	}
	after := assertRuntimeCredentialContracts(t, p, rc)
	if after.Keys.JWTSecret == before.Keys.JWTSecret {
		t.Fatal("JWT rotation did not replace signing credentials")
	}
	after.RealtimeAdminSecret = ""
	if err := storeCredentials(t.Context(), rc, after); err != nil {
		t.Fatal(err)
	}
	if err := ensureCredentials(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	convergeProvider(t, p, rc)
	repaired := assertRuntimeCredentialContracts(t, p, rc)
	after.RealtimeAdminSecret = repaired.RealtimeAdminSecret
	if !reflect.DeepEqual(after, repaired) {
		t.Fatal("legacy realtime credential repair changed unrelated credentials")
	}
}
