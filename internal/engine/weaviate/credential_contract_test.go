// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package weaviate

import (
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine/enginetest"
)

// https://docs.weaviate.io/deploy/configuration/authentication
var generatedCredentialLimit = enginetest.CredentialLimit{Min: 1, Alphabet: enginetest.Alphanumeric}

func TestProviderGeneratedCredentialContracts(t *testing.T) {
	f := coreSetup(t, 1)
	recorded := enginetest.RecordCredentials(f.rc)
	s := &state{Members: []*member{{Name: "demom0"}}}
	if err := f.p.ensureSecrets(t.Context(), f.rc, s); err != nil {
		t.Fatal(err)
	}
	// Weaviate API keys/basic-auth secrets are opaque; no native length maximum.
	// https://docs.weaviate.io/deploy/configuration/authentication
	key := generatedCredentialLimit
	recorded.Assert(t, map[string]enginetest.CredentialLimit{
		"users/karakoram-admin": key, "users/karakoram-cluster": key,
	}, 0)
}
