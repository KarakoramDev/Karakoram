// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package enginetest

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

// Cache only the closed, empty schema, never a live connection or test records.
// Each fixture still exercises the real file-backed store with isolated WALs,
// locks, encryption and transactions, without replaying every migration.
var emptyStoreImage = sync.OnceValues(func() ([]byte, error) {
	dir, err := os.MkdirTemp("", "karakoram-engine-schema-*")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(dir)
	sealer, err := secrets.New(strings.Repeat("x", 32))
	if err != nil {
		return nil, err
	}
	path := filepath.Join(dir, "state.db")
	st, err := store.Open(path, sealer)
	if err != nil {
		return nil, err
	}
	if err := st.Close(); err != nil {
		return nil, err
	}
	return os.ReadFile(path)
})

// OpenStore creates an independent database from the current migrated empty
// schema. Migration/recovery tests should use store.Open directly instead.
// An existing path is never overwritten, so restart fixtures retain their data.
func OpenStore(t *testing.T, path string, sealer *secrets.Sealer) *store.Store {
	t.Helper()
	image, err := emptyStoreImage()
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err == nil {
		_, writeErr := file.Write(image)
		if err := errors.Join(writeErr, file.Close()); err != nil {
			t.Fatal(err)
		}
	} else if !errors.Is(err, os.ErrExist) {
		t.Fatal(err)
	}
	st, err := store.Open(path, sealer)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}
