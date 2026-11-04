// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package temporal

import (
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine/enginetest"
)

func TestProviderGeneratedCredentialContracts(t *testing.T) {
	for _, topology := range []string{"combined", "split"} {
		t.Run(topology, func(t *testing.T) {
			p, rc, bindings, _ := fixture(t, topology)
			recorded := enginetest.RecordCredentials(rc)
			s, err := loadState(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			ca, err := ensureCA(t.Context(), rc)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := p.syncComponents(t.Context(), rc, s, bindings.conn, ca); err != nil {
				t.Fatal(err)
			}
			// Temporal generates only Karakoram agent bearers. Persistence passwords
			// are supplied by PostgreSQL bindings, covered by the PostgreSQL matrix.
			recorded.Assert(t, map[string]enginetest.CredentialLimit{}, 0)
		})
	}
}
