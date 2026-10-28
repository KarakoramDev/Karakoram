// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDirectoryHierarchyPersistenceAndInvalidLeaf(t *testing.T) {
	root := t.TempDir()
	nested := filepath.Join(root, "new hierarchy", "karakoram", "mysql")
	if err := privateDir(nested); err != nil {
		t.Fatal(err)
	}
	if err := privateDir(nested); err != nil {
		t.Fatal("existing durable directory", err)
	}
	// Already-existing ancestors can have just been created by volume safety;
	// still persist the entire chain rather than stopping at the first one.
	if err := syncDirectoryHierarchy(nested, root); err != nil {
		t.Fatal(err)
	}
	if err := syncDirectoryHierarchy(root, root); err != nil {
		t.Fatal(err)
	}
	if err := syncDirectoryHierarchy(filepath.Dir(root), root); err == nil {
		t.Fatal("path outside volume accepted")
	}
	if err := syncDirectoryHierarchy(filepath.Join(root, "missing"), root); err == nil {
		t.Fatal("missing directory persistence silently tolerated")
	}
	info, err := os.Stat(nested)
	if err != nil || !info.IsDir() || info.Mode().Perm() != 0700 {
		t.Fatalf("unexpected private directory: %v %v", info, err)
	}
	file := filepath.Join(root, "file")
	if err := os.WriteFile(file, []byte("not a directory"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := privateDir(filepath.Join(file, "child")); err == nil {
		t.Fatal("invalid ancestor accepted")
	}
	link := filepath.Join(root, "link")
	if err := os.Symlink(nested, link); err != nil {
		t.Fatal(err)
	}
	if err := privateDir(link); err == nil {
		t.Fatal("symlink leaf accepted")
	}
}
