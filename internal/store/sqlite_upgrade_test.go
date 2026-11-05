// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package store

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestSQLiteDriverConnectionContract(t *testing.T) {
	s, _ := testStore(t)
	if got := s.writer.Stats().MaxOpenConnections; got != 1 {
		t.Fatalf("writer limit=%d", got)
	}
	if _, err := s.writer.Exec("CREATE TABLE driver_contract(value INTEGER); INSERT INTO driver_contract VALUES(0)"); err != nil {
		t.Fatal(err)
	}
	tx, err := s.writer.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec("UPDATE driver_contract SET value=1"); err != nil {
		t.Fatal(err)
	}
	// A second writer cannot enter the pool while the first owns a transaction.
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if _, err := s.writer.ExecContext(ctx, "UPDATE driver_contract SET value=2"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("second writer: %v", err)
	}
	// WAL readers still see committed state while the writer is uncommitted.
	var got int
	if err := s.reader.QueryRowContext(t.Context(), "SELECT value FROM driver_contract").Scan(&got); err != nil || got != 0 {
		t.Fatalf("concurrent reader=%d: %v", got, err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := s.reader.QueryRowContext(t.Context(), "SELECT value FROM driver_contract").Scan(&got); err != nil || got != 1 {
		t.Fatalf("committed reader=%d: %v", got, err)
	}
	// Pragmas are connection-local; verify every pooled connection, not just
	// whichever connection database/sql happens to return first.
	for _, pool := range []*sql.DB{s.writer, s.reader} {
		for range pool.Stats().MaxOpenConnections {
			conn, err := pool.Conn(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			var mode, version string
			var busy, foreignKeys int
			if err := conn.QueryRowContext(t.Context(), "PRAGMA journal_mode").Scan(&mode); err != nil {
				t.Fatal(err)
			}
			if err := conn.QueryRowContext(t.Context(), "PRAGMA busy_timeout").Scan(&busy); err != nil {
				t.Fatal(err)
			}
			if err := conn.QueryRowContext(t.Context(), "PRAGMA foreign_keys").Scan(&foreignKeys); err != nil {
				t.Fatal(err)
			}
			if err := conn.QueryRowContext(t.Context(), "SELECT sqlite_version()").Scan(&version); err != nil {
				t.Fatal(err)
			}
			if mode != "wal" || busy != 5000 || foreignKeys != 1 || version != "3.54.1" {
				t.Fatalf("mode=%s busy=%d foreign_keys=%d SQLite=%s", mode, busy, foreignKeys, version)
			}
		}
	}
}

func TestSQLiteCloseCheckpointsAndCleansWAL(t *testing.T) {
	s, path := testStore(t)
	if _, err := s.writer.Exec("CREATE TABLE close_checkpoint(value INTEGER); INSERT INTO close_checkpoint VALUES(42)"); err != nil {
		t.Fatal(err)
	}
	var value int
	if err := s.reader.QueryRow("SELECT value FROM close_checkpoint").Scan(&value); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path + "-wal")
	if err != nil || info.Size() == 0 {
		t.Fatalf("expected populated WAL: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	for _, suffix := range []string{"-wal", "-shm"} {
		if _, err := os.Stat(path + suffix); !os.IsNotExist(err) {
			t.Fatalf("sidecar %s remains: %v", suffix, err)
		}
	}
	// Copy only the main file: this must not depend on the removed WAL.
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	standalone := filepath.Join(t.TempDir(), "standalone.db")
	if err := os.WriteFile(standalone, raw, 0600); err != nil {
		t.Fatal(err)
	}
	restored, err := Open(standalone, s.sealer)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	if err := restored.reader.QueryRow("SELECT value FROM close_checkpoint").Scan(&value); err != nil || value != 42 {
		t.Fatalf("checkpointed value=%d: %v", value, err)
	}
}

func TestSQLiteBusyWaitHonorsCancellation(t *testing.T) {
	s, path := testStore(t)
	blocker, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Close()
	tx, err := blocker.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec("UPDATE schema_migrations SET name=name"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	started := time.Now()
	if _, err := s.writer.ExecContext(ctx, "UPDATE schema_migrations SET name=name"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("busy cancellation: %v", err)
	}
	if elapsed := time.Since(started); elapsed >= 2*time.Second {
		t.Fatalf("cancellation waited for busy_timeout: %s", elapsed)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.writer.ExecContext(t.Context(), "UPDATE schema_migrations SET name=name"); err != nil {
		t.Fatalf("writer poisoned after cancellation: %v", err)
	}
}

func TestSQLiteLegacyDriverMigrationAndSnapshot(t *testing.T) {
	s, _ := testStore(t)
	raw, err := os.ReadFile("testdata/legacy-v1.40.1.db")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "legacy.db")
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	legacy, err := Open(path, s.sealer)
	if err != nil {
		t.Fatal(err)
	}
	defer legacy.Close()
	var schema string
	if err := legacy.reader.QueryRow("SELECT MAX(name) FROM schema_migrations").Scan(&schema); err != nil || schema != LatestSchemaVersion() {
		t.Fatalf("migrated schema=%s: %v", schema, err)
	}
	snapshot := filepath.Join(t.TempDir(), "snapshot.db")
	if err := legacy.Snapshot(t.Context(), snapshot); err != nil {
		t.Fatal(err)
	}
	if err := CheckSnapshot(t.Context(), snapshot); err != nil {
		t.Fatal(err)
	}
	restored, err := Open(snapshot, s.sealer)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	got, err := restored.GetSetting(t.Context(), "legacy-driver")
	if err != nil || string(got) != `{"sqlite":"3.50.4"}` {
		t.Fatalf("legacy setting=%s: %v", got, err)
	}
}
