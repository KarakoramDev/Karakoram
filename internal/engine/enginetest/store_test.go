// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package enginetest

import (
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

func TestOpenStoreIsolationAndRestart(t *testing.T) {
	sealer, err := secrets.New(strings.Repeat("x", 32))
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "first.db")
	ref := domain.ClusterRef{Namespace: "default", Name: "demo"}
	t.Run("write", func(t *testing.T) {
		st := OpenStore(t, path, sealer)
		if err := st.CreateCluster(t.Context(), &domain.Cluster{Namespace: ref.Namespace, Name: ref.Name}); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("restart", func(t *testing.T) {
		st := OpenStore(t, path, sealer)
		if _, err := st.GetCluster(t.Context(), ref); err != nil {
			t.Fatalf("existing fixture was not preserved: %v", err)
		}
	})
	t.Run("isolated", func(t *testing.T) {
		st := OpenStore(t, filepath.Join(t.TempDir(), "second.db"), sealer)
		if _, err := st.GetCluster(t.Context(), ref); !errors.Is(err, store.ErrNotFound) {
			t.Fatalf("empty schema contains another fixture's record: %v", err)
		}
	})
}
