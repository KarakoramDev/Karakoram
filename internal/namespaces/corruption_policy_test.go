// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package namespaces

import (
	"database/sql"
	"path/filepath"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

func TestMalformedClusterDoesNotRevokeHealthyRelayPolicy(t *testing.T) {
	r, _, _ := setupRegistry(t)
	path := filepath.Join(t.TempDir(), "state.db")
	key, _ := secrets.New(strings.Repeat("k", 32))
	st, err := store.Open(path, key)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	r.o.Store = st
	n := createTestNamespace(t, r)
	for _, name := range []string{"broken", "healthy"} {
		if err := st.CreateCluster(t.Context(), &domain.Cluster{Namespace: n.Name, Name: name}); err != nil {
			t.Fatal(err)
		}
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec("UPDATE clusters SET spec=? WHERE name='broken'", []byte("{")); err != nil {
		t.Fatal(err)
	}
	var targets []string
	r.o.RelayTargets = func(c *domain.Cluster) []string { targets = append(targets, c.Name); return []string{c.Name + ":9443"} }
	r.step(t.Context(), n.Name)
	if len(targets) != 1 || targets[0] != "healthy" {
		t.Fatalf("valid relay policy lost with damaged row: %v", targets)
	}
}
