// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package reconciler

import (
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestDiscoveryIsolatesMalformedCluster(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	m, st := openAttributionManager(t, &operationProvider{}, path)
	for _, name := range []string{"broken", "healthy"} {
		if err := st.CreateCluster(t.Context(), &domain.Cluster{Name: name, Spec: domain.ClusterSpec{Provider: "fake"}}); err != nil {
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
	if err := m.discover(t.Context()); err != nil {
		t.Fatal("logical row damage stopped global discovery", err)
	}
	if len(m.workers) != 1 || m.workers["healthy"] == nil {
		t.Fatal("healthy worker missing or damaged worker started")
	}
}
