// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package openbao

import (
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine/enginetest"
)

func TestProviderGeneratedCredentialContracts(t *testing.T) {
	p, rc, _, _ := setup(t)
	recorded := enginetest.RecordCredentials(rc)
	s := &internalState{Members: []*member{{Name: "demom0"}}}
	if err := p.ensureSecrets(t.Context(), rc, s); err != nil {
		t.Fatal(err)
	}
	// Only Karakoram agent bearers are locally generated. Root tokens and unseal/
	// recovery shares come from native /sys/init, so no local bounds are imposed.
	// https://openbao.org/api-docs/system/init/
	recorded.Assert(t, map[string]enginetest.CredentialLimit{}, 0)
}
