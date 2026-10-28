// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package keycloak

import (
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine/enginetest"
)

func TestProviderGeneratedCredentialContracts(t *testing.T) {
	_, rc, _, _ := setup(t)
	recorded := enginetest.RecordCredentials(rc)
	s, err := load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := secret(t.Context(), rc, "keycloak/admin"); err != nil {
		t.Fatal(err)
	}
	if _, err := token(t.Context(), rc, s); err != nil {
		t.Fatal(err)
	}
	// Bootstrap admin uses Karakoram's bearer format; realm-specific password
	// policies have no universal native maximum. PostgreSQL bindings own DB passwords.
	// https://www.keycloak.org/docs/latest/server_admin/#password-policies
	recorded.Assert(t, map[string]enginetest.CredentialLimit{"keycloak/admin": enginetest.AgentCredential}, 0)
}
