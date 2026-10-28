// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package neo4j

import (
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine/enginetest"
)

func TestProviderGeneratedCredentialContracts(t *testing.T) {
	_, rc, _, _ := setup(t)
	recorded := enginetest.RecordCredentials(rc)
	s := &state{Member: &member{Name: "demom0"}}
	if err := ensureSecrets(t.Context(), rc, s); err != nil {
		t.Fatal(err)
	}
	// Default dbms.security.auth_minimum_password_length = 8; no native maximum.
	// https://neo4j.com/docs/operations-manual/current/configuration/configuration-settings/
	recorded.Assert(t, map[string]enginetest.CredentialLimit{
		"users/neo4j": {Min: 8, Alphabet: enginetest.Alphanumeric},
	}, 0)
}
