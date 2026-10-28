// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package pxc

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/enginetest"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql"
	"github.com/KarakoramDev/Karakoram/internal/infra/fake"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

func TestProviderGeneratedCredentialContracts(t *testing.T) {
	for _, topology := range []string{"single", "cluster"} {
		t.Run(topology, func(t *testing.T) {
			sealer, err := secrets.New(strings.Repeat("x", 32))
			if err != nil {
				t.Fatal(err)
			}
			st, err := store.Open(filepath.Join(t.TempDir(), "state.db"), sealer)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = st.Close() })
			p := New(mysql.Options{Store: st})
			c := &domain.Cluster{Name: "demo"}
			c.Spec.Topology.Type = topology
			p.ApplyDefaults(&c.Spec)
			if err := st.CreateCluster(t.Context(), c); err != nil {
				t.Fatal(err)
			}
			rc := &engine.Context{Cluster: c, Secrets: st, Store: st, Backend: fake.New(fake.Options{})}
			recorded := enginetest.RecordCredentials(rc)
			// The first real Sync seeds credentials before importing any engine runtime.
			if _, err := p.Sync(t.Context(), rc); err != nil {
				t.Fatal(err)
			}
			// Ordinary MySQL account passwords: product 12..1024; PXC does not use
			// START GROUP_REPLICATION's 32-character recovery-password restriction.
			// https://dev.mysql.com/doc/refman/8.4/en/user-names.html (usernames <=32).
			pw := enginetest.CredentialLimit{Min: 12, Max: 1024, Alphabet: enginetest.Alphanumeric}
			recorded.Assert(t, map[string]enginetest.CredentialLimit{
				"users/root": pw, "users/admin": pw, "users/kk_agent": pw, "users/kk_repl": pw,
				"users/kk_monitor": pw, "users/kk_backup": pw, "users/kk_proxy_admin": pw,
			}, 32)
		})
	}
}
