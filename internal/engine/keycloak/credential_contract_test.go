// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package keycloak

import (
	"encoding/json"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine/enginetest"
	"github.com/KarakoramDev/Karakoram/internal/engine/keycloak/kcspec"
)

func TestProviderGeneratedCredentialContracts(t *testing.T) {
	p, rc, _, _ := setup(t)
	recorded := enginetest.RecordCredentials(rc)
	converge(t, p, rc) // Exercise real create-time publish/token callsites.
	s, err := load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	config, err := p.MemberConfig(t.Context(), rc, s.Member)
	if err != nil {
		t.Fatal(err)
	}
	var spec kcspec.Spec
	if err := json.Unmarshal(config.EngineSpec, &spec); err != nil {
		t.Fatal(err)
	}
	if spec.AdminPassword != string(recorded.Values["keycloak/admin"]) {
		t.Fatal("published admin credential differs from its persisted source")
	}
	// This sealed projection duplicates checked credentials and bound PostgreSQL
	// credentials; it is not an independently generated credential.
	delete(recorded.Values, "keycloak/config/"+s.Member)
	delete(recorded.Values, "keycloak/agent-ca") // Transport CA bundle, not an engine password.
	// Bootstrap admin uses Karakoram's bearer format; realm-specific password
	// policies have no universal native maximum. PostgreSQL bindings own DB passwords.
	// https://www.keycloak.org/docs/latest/server_admin/#password-policies
	recorded.Assert(t, map[string]enginetest.CredentialLimit{"keycloak/admin": enginetest.AgentCredential}, 0)
}
