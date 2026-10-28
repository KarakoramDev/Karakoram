// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"encoding/json"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/enginetest"
)

// PostgreSQL has no fixed native password maximum; these are the product's
// validator bounds and the generated password alphabet.
// https://www.postgresql.org/docs/current/sql-createrole.html
var postgresPasswordLimit = enginetest.CredentialLimit{Min: 12, Max: 1024, Alphabet: enginetest.Alphanumeric}

func TestProviderGeneratedCredentialContracts(t *testing.T) {
	for _, tc := range []struct{ name, topology, options string }{{"postgres", "single", "{}"}, {"citus", "distributed", `{"profiles":["citus"]}`}, {"timescale", "single", `{"profiles":["timescaledb"]}`}} {
		t.Run(tc.name, func(t *testing.T) {
			p, rc, _ := setup(t, tc.topology)
			rc.Cluster.Spec.EngineOptions = json.RawMessage(tc.options)
			if tc.topology != "distributed" {
				rc.Cluster.Spec.DataMcp = &domain.DataMcpSpec{Enabled: true, Access: domain.DataMcpAccessReadWrite}
			}
			recorded := enginetest.RecordCredentials(rc)
			s, err := load(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			s.Topology = tc.topology
			s.allocate(rc.Cluster)
			if err := p.ensureSecrets(t.Context(), rc, s); err != nil {
				t.Fatal(err)
			}
			// PostgreSQL has no fixed password maximum here; 12..1024 is the product validator.
			// https://www.postgresql.org/docs/current/sql-createrole.html
			// Identifiers are <=63 bytes: https://www.postgresql.org/docs/current/sql-syntax-lexical.html
			pw := postgresPasswordLimit
			limits := map[string]enginetest.CredentialLimit{
				"users/postgres": pw, "users/kk_replicator": pw, "users/kk_rewind": pw,
				"users/kk_monitor": pw, "users/kk_admin": pw, "users/kk_proxy_admin": pw,
			}
			if tc.topology != "distributed" {
				limits["users/karakoram-mcp-ro"] = pw
				limits["users/karakoram-mcp-rw"] = pw
				for _, level := range []engine.DataAccessLevel{engine.DataAccessRead, engine.DataAccessReadWrite} {
					if len(dataRoleName(level)) > 63 {
						t.Error("data-access role exceeds PostgreSQL identifier limit")
					}
				}
			}
			recorded.Assert(t, limits, 63)
		})
	}
}
