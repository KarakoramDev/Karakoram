// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package openbao

import (
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine/enginetest"
)

// OpenBao database connections generate a PostgreSQL management identity, not
// a native OpenBao token. Output is 32 random bytes encoded as 64 lowercase hex
// characters; PostgreSQL imposes no fixed native password maximum.
// https://www.postgresql.org/docs/current/sql-createrole.html
var connectionPasswordContract = enginetest.CredentialLimit{Min: 64, Max: 64, Alphabet: "0123456789abcdef"}

// PostgreSQL NAMEDATALEN: identifiers <=63 bytes.
// https://www.postgresql.org/docs/current/sql-syntax-lexical.html
var connectionUsernameContract = enginetest.CredentialLimit{Min: 1, Max: 63, Alphabet: "abcdefghijklmnopqrstuvwxyz0123456789_", Prefix: "karakoram_openbao_"}

func TestProviderGeneratedCredentialContracts(t *testing.T) {
	p, rc, _, _ := setup(t)
	recorded := enginetest.RecordCredentials(rc)
	s := &internalState{Members: []*member{{Name: "demom0"}}}
	if err := p.ensureSecrets(t.Context(), rc, s); err != nil {
		t.Fatal(err)
	}
	// Bootstrap seeding locally generates only Karakoram agent bearers. Root tokens and unseal/
	// recovery shares come from native /sys/init, so no local bounds are imposed.
	// https://openbao.org/api-docs/system/init/
	recorded.Assert(t, map[string]enginetest.CredentialLimit{}, 0)
}
