// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

//go:build integration

package postgres

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
)

func TestNativeRecoveredSystemRekey(t *testing.T) {
	bin := os.Getenv("KARAKORAM_PG_BIN_DIR")
	if bin == "" {
		t.Skip("set KARAKORAM_PG_BIN_DIR to exercise installed PostgreSQL")
	}
	if os.Geteuid() == 0 {
		t.Fatal("run native PostgreSQL tests as an unprivileged runtime user")
	}
	source := testSpec()
	source.BinDir = bin
	source.Postgres.Parameters = map[string]string{"shared_buffers": "32MB", "max_connections": "30"}
	h := startNativeAgent(t, source)
	defer h.stop(t)
	// Prevent ordinary source config pulls during the short simulated catalog
	// recovery. Exercise the explicit rekey action's helper without Apply.
	h.mu.Lock()
	defer h.mu.Unlock()
	// Reproduce strict synchronous recovery before any replica is restarted.
	// Local maintenance must fsync its own WAL without waiting for that replica.
	setup, err := localConnect(t.Context(), source, dataPaths(h.dir))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = setup.Exec(t.Context(), "ALTER SYSTEM SET synchronous_standby_names = '*'"); err != nil {
		t.Fatal(err)
	}
	if _, err = setup.Exec(t.Context(), "SELECT pg_catalog.pg_reload_conf()"); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		var standbys string
		if err = setup.QueryRow(t.Context(), "SHOW synchronous_standby_names").Scan(&standbys); err != nil {
			t.Fatal(err)
		}
		if standbys == "*" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("strict synchronous policy was not applied")
		}
		time.Sleep(10 * time.Millisecond)
	}
	setup.Close(t.Context())
	target := source
	target.Superuser.Password = "target-owned-superuser"
	target.Replication.Password = "target-owned-replication"
	target.Monitor.Password = "target-owned-monitor"
	target.Rewind.Password = "target-owned-rewind"
	recovery := source.Superuser
	target.Bootstrap = &pgspec.Bootstrap{Method: "restore", Generation: 2, RecoverySuperuser: &recovery}
	p := dataPaths(h.dir)
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	if err := rekeyRecoveredSystemRoles(ctx, target, p); err != nil {
		t.Fatal(err)
	}
	if recoveryCredentialsPending(target, p) {
		t.Fatal("system rekey completion was not durable")
	}
	target.Bootstrap = nil
	c, err := localConnect(ctx, target, p)
	if err != nil {
		t.Fatal("target-owned superuser cannot authenticate after recovery")
	}
	defer c.Close(ctx)
	var count int
	var syncCommit string
	if err := c.QueryRow(ctx, "SHOW synchronous_commit").Scan(&syncCommit); err != nil || syncCommit != "on" {
		t.Fatal("offline rekey changed the ordinary synchronous commit policy", syncCommit, err)
	}
	if err = c.QueryRow(ctx, "SELECT count(*) FROM pg_catalog.pg_authid WHERE rolname IN ('postgres','kk_monitor','kk_replicator','kk_rewind') AND rolpassword LIKE 'SCRAM-SHA-256$%'").Scan(&count); err != nil || count != 4 {
		t.Fatal("system-role verifiers not reapplied", count, err)
	}
	if old, err := localConnect(ctx, source, p); err == nil {
		old.Close(ctx)
		t.Fatal("source superuser password still authenticates target")
	}
	// Sampling must generate a committed WAL record even without application
	// writes; this supplies work for the default archive_timeout on idle clusters.
	var before, after string
	if err = c.QueryRow(ctx, "SELECT pg_catalog.pg_current_wal_lsn()::text").Scan(&before); err != nil {
		t.Fatal(err)
	}
	var watermark archiveWatermark
	_ = watermark.recoverableUntil(ctx, c, "system")
	if err = c.QueryRow(ctx, "SELECT pg_catalog.pg_current_wal_lsn()::text").Scan(&after); err != nil {
		t.Fatal(err)
	}
	start, validStart := parseLSN(before)
	end, validEnd := parseLSN(after)
	if !validStart || !validEnd || end <= start || len(watermark.samples) != 1 {
		t.Fatal("idle heartbeat did not produce a sample backed by committed WAL")
	}
}
