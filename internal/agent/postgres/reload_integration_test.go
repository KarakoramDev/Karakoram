// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

//go:build integration

package postgres

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"testing"
	"time"
)

func TestNativeSingleReloadAndFastRestart(t *testing.T) {
	if os.Getenv("KARAKORAM_PG_BIN_DIR") == "" {
		t.Skip("set KARAKORAM_PG_BIN_DIR for native PostgreSQL reload acceptance")
	}
	s := testSpec()
	s.BinDir = os.Getenv("KARAKORAM_PG_BIN_DIR")
	s.CAPEM, s.ServerCert = testTLS(t)
	s.Postgres.RequireTLS = true
	s.Postgres.Parameters = map[string]string{"shared_buffers": "32MB", "max_connections": "30"}
	h := startNativeAgent(t, s)
	defer h.stop(t)
	h.client.Timeout = 30 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	held, err := localConnect(ctx, s, dataPaths(h.dir))
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close(context.Background())
	var originalStart string
	if err = held.QueryRow(ctx, "SELECT pg_postmaster_start_time()::text").Scan(&originalStart); err != nil {
		t.Fatal(err)
	}
	reload := func() {
		t.Helper()
		raw, _ := json.Marshal(s)
		h.mu.Lock()
		h.config.EngineSpec = raw
		h.config.ConfigVersion++
		h.mu.Unlock()
		response, err := h.request(http.MethodPost, "/v1/reload", map[string]any{})
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusOK {
			t.Fatalf("reload HTTP%d", response.StatusCode)
		}
	}
	unchanged := func() {
		t.Helper()
		var current string
		if err := held.QueryRow(ctx, "SELECT pg_postmaster_start_time()::text").Scan(&current); err != nil {
			t.Fatal("reload disconnected an existing idle SQL session")
		}
		if current != originalStart {
			t.Fatal("reload restarted PostgreSQL")
		}
	}
	s.Postgres.RequireTLS = false
	reload()
	unchanged()
	reload()
	unchanged()
	s.Postgres.Parameters["work_mem"] = "8MB"
	reload()
	unchanged()
	// PostgreSQL, rather than a raw string comparison, normalizes GUC units.
	s.Postgres.Parameters["shared_buffers"] = "32768kB"
	reload()
	unchanged()
	s.Postgres.Parameters["max_connections"] = "40"
	reload()
	if err = held.Ping(ctx); err == nil {
		t.Fatal("postmaster setting did not fast-restart the old process")
	}
	current, err := waitLocal(ctx, s, dataPaths(h.dir))
	if err != nil {
		t.Fatal(err)
	}
	defer current.Close(context.Background())
	var maxConnections int
	if err = current.QueryRow(ctx, "SELECT current_setting('max_connections')::int").Scan(&maxConnections); err != nil || maxConnections != 40 {
		t.Fatal("restart-required setting was not activated")
	}
	response, err := h.request(http.MethodPost, "/v1/processes/postgres/restart", nil)
	if err != nil {
		t.Fatal("generic restart hung behind an idle SQL session:", err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("generic restart HTTP%d", response.StatusCode)
	}
	if err = current.Ping(ctx); err == nil {
		t.Fatal("generic restart did not replace the existing session")
	}
	t.Log("native PostgreSQL: TLS/HBA reload and equivalent memory units preserve idle sessions; actual postmaster change and generic restart use bounded fast shutdown")
}
