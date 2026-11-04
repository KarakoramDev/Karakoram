// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package main

import (
	"compress/gzip"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/cpbackup"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

func TestStateAdminRoundTripAndRefusals(t *testing.T) {
	t.Setenv("KARAKORAM_MASTER_KEY", strings.Repeat("k", 32))
	source := t.TempDir()
	t.Setenv("KARAKORAM_DATA_DIR", source)
	sealer, err := secrets.FromEnv()
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(filepath.Join(source, "karakoram.db"), sealer)
	if err != nil {
		t.Fatal(err)
	}
	if err = st.SetSetting(t.Context(), "round-trip", []byte(`{"preserved":true}`)); err != nil {
		t.Fatal(err)
	}
	archive := filepath.Join(t.TempDir(), "state.db.gz")
	// Export while another store connection is open, as on a running server.
	if err = run(t.Context(), []string{"admin", "backup-state", "--out", archive}, io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	st.Close()
	for _, p := range []string{archive, archive + ".json"} {
		info, err := os.Stat(p)
		if err != nil || info.Mode().Perm() != 0600 {
			t.Fatalf("private export %s: %v %v", p, info, err)
		}
	}
	manifestBytes, err := os.ReadFile(archive + ".json")
	if err != nil {
		t.Fatal(err)
	}
	originalBytes, err := os.ReadFile(archive)
	if err != nil {
		t.Fatal(err)
	}
	var original cpbackup.Manifest
	if err = json.Unmarshal(manifestBytes, &original); err != nil {
		t.Fatal(err)
	}
	rewrite := func(m cpbackup.Manifest, data []byte) {
		t.Helper()
		if err := os.WriteFile(archive, data, 0600); err != nil {
			t.Fatal(err)
		}
		b, _ := json.Marshal(m)
		if err := os.WriteFile(archive+".json", b, 0600); err != nil {
			t.Fatal(err)
		}
	}
	restore := func(dir string, replace bool) error {
		t.Helper()
		t.Setenv("KARAKORAM_DATA_DIR", dir)
		args := []string{"admin", "restore-state", "--file", archive}
		if replace {
			args = append(args, "--replace")
		}
		return run(t.Context(), args, io.Discard, io.Discard)
	}
	target := t.TempDir()
	if err = restore(target, false); err != nil {
		t.Fatal(err)
	}
	restored, err := store.Open(filepath.Join(target, "karakoram.db"), sealer)
	if err != nil {
		t.Fatal(err)
	}
	value, err := restored.GetSetting(t.Context(), "round-trip")
	if err != nil || string(value) != `{"preserved":true}` {
		t.Fatalf("round trip value %s: %v", value, err)
	}
	if err = restore(target, true); err == nil || !strings.Contains(err.Error(), "in use") {
		t.Fatalf("open store: %v", err)
	}
	restored.Close()
	if err = restore(target, false); err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("existing: %v", err)
	}
	// SQLite checkpoints/removes idle sidecars before preserving standalone state.
	for _, ext := range []string{"-wal", "-shm"} {
		if err = os.WriteFile(filepath.Join(target, "karakoram.db")+ext, nil, 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err = restore(target, true); err != nil {
		t.Fatal(err)
	}
	matches, _ := filepath.Glob(filepath.Join(target, "karakoram.db.replaced-*"))
	if len(matches) != 1 {
		t.Fatalf("replacement archives: %v", matches)
	}
	preserved, err := store.Open(matches[0], sealer)
	if err != nil {
		t.Fatal(err)
	}
	value, err = preserved.GetSetting(t.Context(), "round-trip")
	if err != nil || string(value) != `{"preserved":true}` {
		t.Fatalf("replacement backup lost state: %s: %v", value, err)
	}
	preserved.Close()
	locked, err := sql.Open("sqlite", "file:"+filepath.Join(target, "karakoram.db")+"?_pragma=busy_timeout(0)")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = locked.ExecContext(t.Context(), "BEGIN EXCLUSIVE"); err != nil {
		t.Fatal(err)
	}
	if err = restore(target, true); err == nil || !strings.Contains(err.Error(), "in use") {
		t.Fatalf("SQLite lock: %v", err)
	}
	if _, err = locked.ExecContext(t.Context(), "ROLLBACK"); err != nil {
		t.Fatal(err)
	}
	locked.Close()
	for _, test := range []struct {
		name, want string
		mutate     func(*cpbackup.Manifest) []byte
	}{
		{"fingerprint", "supply the original master key", func(m *cpbackup.Manifest) []byte { m.KeyFingerprint = "kkfp1:other"; return originalBytes }},
		{"newer schema", "newer", func(m *cpbackup.Manifest) []byte { m.SchemaVersion = "999.sql"; return originalBytes }},
		{"sha mismatch", "sha256", func(m *cpbackup.Manifest) []byte { m.SHA256 = strings.Repeat("0", 64); return originalBytes }},
		{"corrupt gzip", "gzip", func(m *cpbackup.Manifest) []byte {
			data := []byte("not gzip")
			sum := sha256.Sum256(data)
			m.SHA256 = hex.EncodeToString(sum[:])
			m.SizeBytes = int64(len(data))
			return data
		}},
		{"invalid sqlite", "integrity", func(m *cpbackup.Manifest) []byte {
			p := filepath.Join(t.TempDir(), "bad.gz")
			f, _ := os.Create(p)
			z := gzip.NewWriter(f)
			z.Write([]byte("not SQLite"))
			z.Close()
			f.Close()
			data, _ := os.ReadFile(p)
			sum := sha256.Sum256(data)
			m.SHA256 = hex.EncodeToString(sum[:])
			m.SizeBytes = int64(len(data))
			return data
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			m := original
			data := test.mutate(&m)
			rewrite(m, data)
			err := restore(t.TempDir(), false)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("want %q: %v", test.want, err)
			}
		})
	}
	rewrite(original, originalBytes)
}

func TestReplacementFailureAndInterruptionKeepAuthoritativeState(t *testing.T) {
	for _, failPublication := range []bool{false, true} {
		t.Run(map[bool]string{false: "interrupted-before-publication", true: "publication-error"}[failPublication], func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "karakoram.db")
			sealer, err := secrets.New(strings.Repeat("k", 32))
			if err != nil {
				t.Fatal(err)
			}
			st, err := store.Open(path, sealer)
			if err != nil {
				t.Fatal(err)
			}
			if err = st.SetSetting(t.Context(), "authoritative", []byte(`"original"`)); err != nil {
				t.Fatal(err)
			}
			if err = st.Close(); err != nil {
				t.Fatal(err)
			}
			backup := path + ".replaced-test"
			if err = preserveState(path, backup); err != nil {
				t.Fatal(err)
			}
			if failPublication {
				// A missing stage makes the exact publication operation fail.
				if err = os.Rename(filepath.Join(dir, "missing-stage"), path); err == nil {
					t.Fatal("publication unexpectedly succeeded")
				}
			}
			// Model restart at the durable pre-publication boundary: opening the
			// authoritative path must recover old state, not initialize a new DB.
			reopened, err := store.Open(path, sealer)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			value, err := reopened.GetSetting(t.Context(), "authoritative")
			if err != nil || string(value) != `"original"` {
				t.Fatalf("fresh initialization or lost state: %s: %v", value, err)
			}
			if _, err = os.Stat(backup); err != nil {
				t.Fatalf("backup not retained: %v", err)
			}
		})
	}
}

func TestPreserveStateRefusesSidecarsAndBackupCollision(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "karakoram.db")
	backup := path + ".replaced-test"
	if err := os.WriteFile(path, []byte("original"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, ext := range []string{"-wal", "-shm", "-journal"} {
		if err := os.WriteFile(path+ext, []byte("sidecar"), 0600); err != nil {
			t.Fatal(err)
		}
		if err := preserveState(path, backup); err == nil {
			t.Fatalf("accepted uncheckpointed %s", ext)
		}
		if err := os.Remove(path + ext); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(backup, []byte("previous backup"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := preserveState(path, backup); err == nil {
		t.Fatal("overwrote existing backup")
	}
	for name, want := range map[string]string{path: "original", backup: "previous backup"} {
		got, err := os.ReadFile(name)
		if err != nil || string(got) != want {
			t.Fatalf("%s: %q: %v", name, got, err)
		}
	}
}

func TestBackupStateRejectsWrongEnvironmentMasterKey(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("KARAKORAM_DATA_DIR", dir)
	t.Setenv("KARAKORAM_MASTER_KEY", strings.Repeat("k", 32))
	sealer, err := secrets.FromEnv()
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(filepath.Join(dir, "karakoram.db"), sealer)
	if err != nil {
		t.Fatal(err)
	}
	if err = st.PutRelayPKI(t.Context(), &store.RelayPKI{CAKey: []byte("original-private-key")}); err != nil {
		t.Fatal(err)
	}
	if err = st.Close(); err != nil {
		t.Fatal(err)
	}
	t.Setenv("KARAKORAM_MASTER_KEY", strings.Repeat("w", 32))
	out := filepath.Join(t.TempDir(), "state.db.gz")
	err = run(t.Context(), []string{"admin", "backup-state", "--out", out}, io.Discard, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "authenticate backup master key") {
		t.Fatalf("wrong environment key accepted: %v", err)
	}
	for _, name := range []string{out, out + ".json"} {
		if _, err = os.Stat(name); !os.IsNotExist(err) {
			t.Fatalf("wrong key published %s: %v", name, err)
		}
	}
}
