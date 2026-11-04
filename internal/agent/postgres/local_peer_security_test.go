// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
)

const securityTestMapping = "karakoram-local runtime postgres\n"

func TestLocalPeerMapRejectsUnsafeExistingFile(t *testing.T) {
	for _, kind := range []string{"symlink", "group-writable", "other-writable", "wrong-owner", "fifo", "directory"} {
		t.Run(kind, func(t *testing.T) {
			dir := t.TempDir()
			name := filepath.Join(dir, "pg_ident.conf")
			target := filepath.Join(t.TempDir(), "target")
			if err := os.WriteFile(target, []byte(securityTestMapping), 0600); err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "symlink":
				if err := os.Symlink(target, name); err != nil {
					t.Fatal(err)
				}
			case "fifo":
				if err := unix.Mkfifo(name, 0600); err != nil {
					t.Fatal(err)
				}
			case "directory":
				if err := os.Mkdir(name, 0700); err != nil {
					t.Fatal(err)
				}
			default:
				// Exact desired content must not bypass the metadata checks.
				if err := os.WriteFile(name, []byte(securityTestMapping), 0600); err != nil {
					t.Fatal(err)
				}
				if kind == "wrong-owner" {
					if os.Geteuid() != 0 {
						t.Skip("changing ownership requires root")
					}
					if err := os.Chown(name, 1, -1); err != nil {
						t.Fatal(err)
					}
				} else {
					mode := os.FileMode(0620)
					if kind == "other-writable" {
						mode = 0602
					}
					if err := os.Chmod(name, mode); err != nil {
						t.Fatal(err)
					}
				}
			}
			if err := stageLocalPeerMap(name, securityTestMapping); err == nil {
				t.Fatal("unsafe identity map accepted")
			}
			data, err := os.ReadFile(target)
			if err != nil || string(data) != securityTestMapping {
				t.Fatal("symlink target changed", err)
			}
		})
	}
}

func TestLocalPeerMapRejectsUnsafeParent(t *testing.T) {
	for _, kind := range []string{"symlink", "group-writable", "wrong-owner"} {
		t.Run(kind, func(t *testing.T) {
			parent := t.TempDir()
			if kind == "symlink" {
				link := filepath.Join(t.TempDir(), "parent")
				if err := os.Symlink(parent, link); err != nil {
					t.Fatal(err)
				}
				parent = link
			} else if kind == "wrong-owner" {
				if os.Geteuid() != 0 {
					t.Skip("changing ownership requires root")
				}
				if err := os.Chown(parent, 1, -1); err != nil {
					t.Fatal(err)
				}
			} else if err := os.Chmod(parent, 0770); err != nil {
				t.Fatal(err)
			}
			name := filepath.Join(parent, "pg_ident.conf")
			if err := stageLocalPeerMap(name, securityTestMapping); err == nil {
				t.Fatal("unsafe identity directory accepted")
			}
			if _, err := os.Lstat(name); !os.IsNotExist(err) {
				t.Fatal("map published in unsafe directory", err)
			}
		})
	}
}

func TestLocalPeerMapDurableReplacementAndReplay(t *testing.T) {
	dir := t.TempDir()
	name := filepath.Join(dir, "pg_ident.conf")
	const native = "native runtime application\n"
	if err := os.WriteFile(name, []byte(native), 0600); err != nil {
		t.Fatal(err)
	}
	old, err := os.Open(name)
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	before, err := old.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if err := stageLocalPeerMap(name, securityTestMapping); err != nil {
		t.Fatal(err)
	}
	after, err := os.Stat(name)
	if err != nil {
		t.Fatal(err)
	}
	if os.SameFile(before, after) || after.Mode().Perm() != 0600 {
		t.Fatal("map was not replaced atomically with mode 0600")
	}
	data, err := os.ReadFile(name)
	if err != nil || string(data) != native+securityTestMapping {
		t.Fatal("map content not preserved", err)
	}
	if err := stageLocalPeerMap(name, securityTestMapping); err != nil {
		t.Fatal(err)
	}
	replayed, err := os.Stat(name)
	if err != nil || !os.SameFile(after, replayed) {
		t.Fatal("unchanged safe map replaced", err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 1 || strings.HasPrefix(entries[0].Name(), ".receipt-") {
		t.Fatal("temporary publication file remains", err)
	}
}

func TestDurableFileUsesPinnedDirectory(t *testing.T) {
	root := t.TempDir()
	parent := filepath.Join(root, "parent")
	if err := os.Mkdir(parent, 0700); err != nil {
		t.Fatal(err)
	}
	d, err := openDurableDirectory(parent)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	moved := filepath.Join(root, "moved")
	if err := os.Rename(parent, moved); err != nil {
		t.Fatal(err)
	}
	target := t.TempDir()
	if err := os.Symlink(target, parent); err != nil {
		t.Fatal(err)
	}
	if err := durableFileAt(d, "pg_ident.conf", []byte(securityTestMapping)); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(moved, "pg_ident.conf"))
	if err != nil || string(data) != securityTestMapping {
		t.Fatal("publication did not use pinned directory", err)
	}
	if _, err := os.Stat(filepath.Join(target, "pg_ident.conf")); !os.IsNotExist(err) {
		t.Fatal("publication followed replaced directory", err)
	}
}
