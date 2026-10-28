// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package supabase

import (
	"reflect"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine/enginetest"
)

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
		enginetest.CredentialLimit{Min: 12, Max: 1024, Alphabet: enginetest.Alphanumeric}.Assert(t, name, password)
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
