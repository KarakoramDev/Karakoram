// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine/enginetest"
)

func TestProviderGeneratedCredentialContracts(t *testing.T) {
	for _, tc := range []struct{ provider, topology string }{{"mysql", "single"}, {"mysql", "group"}} {
		t.Run(tc.provider+"/"+tc.topology, func(t *testing.T) {
			p, rc, _ := setup(t, tc.topology)
			rc.Cluster.Spec.Provider = tc.provider
			recorded := enginetest.RecordCredentials(rc)
			s, err := load(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			s.allocate(rc.Cluster)
			if err := p.ensureSecrets(t.Context(), rc, s); err != nil {
				t.Fatal(err)
			}
			// Ordinary SQL passwords: product 12..1024, not the distinct GR limit.
			// https://dev.mysql.com/doc/refman/8.4/en/user-names.html (usernames <=32).
			password := enginetest.CredentialLimit{Min: 12, Max: 1024, Alphabet: enginetest.Alphanumeric}
			limits := map[string]enginetest.CredentialLimit{
				"users/root": password, "users/admin": password, "users/kk_agent": password,
				"users/kk_repl": password, "users/kk_monitor": password, "users/kk_backup": password, "users/kk_proxy_admin": password,
			}
			// START GROUP_REPLICATION password <=32 (ER_GROUP_REPLICATION_PASSWORD_LENGTH, 3972).
			// https://dev.mysql.com/doc/mysql-errors/8.4/en/server-error-reference.html
			limits["users/kk_repl"] = enginetest.CredentialLimit{Min: 12, Max: 32, Alphabet: enginetest.Alphanumeric}
			recorded.Assert(t, limits, 32)
		})
	}
}
