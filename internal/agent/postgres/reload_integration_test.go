// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

//go:build integration

package postgres

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"github.com/KarakoramDev/Karakoram/internal/pki"
	"github.com/jackc/pgx/v5"
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

func TestNativeTLSOnlyPendingRestart(t *testing.T) {
	bin := os.Getenv("KARAKORAM_PG_BIN_DIR")
	if bin == "" {
		t.Skip("set KARAKORAM_PG_BIN_DIR for native certificate-only reload acceptance")
	}
	s := testSpec()
	s.BinDir = bin
	s.Postgres.Parameters = map[string]string{"shared_buffers": "32MB", "max_connections": "30"}
	ca, err := pki.NewCA("native certificate test", 0)
	if err != nil {
		t.Fatal(err)
	}
	issue := func(at time.Time) *pgspec.Certificate {
		t.Helper()
		cert, key, err := ca.Issue(pki.CertRequest{DNSNames: []string{"localhost"}, Server: true, Client: true, Now: at})
		if err != nil {
			t.Fatal(err)
		}
		return &pgspec.Certificate{CertPEM: string(cert), KeyPEM: string(key)}
	}
	s.CAPEM, s.ServerCert = string(ca.CertPEM), issue(time.Now().Add(8*time.Hour))
	h := startNativeAgent(t, s)
	defer h.stop(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	held, err := localConnect(ctx, s, dataPaths(h.dir))
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close(context.Background())
	var originalStart string
	if err := held.QueryRow(ctx, "SELECT pg_postmaster_start_time()::text").Scan(&originalStart); err != nil {
		t.Fatal(err)
	}
	if _, err := held.Exec(ctx, "ALTER SYSTEM SET max_connections='40'"); err != nil {
		t.Fatal(err)
	}
	if _, err := held.Exec(ctx, "SELECT pg_reload_conf()"); err != nil {
		t.Fatal(err)
	}
	pending := false
	for range 100 {
		if err := held.QueryRow(ctx, "SELECT pending_restart FROM pg_settings WHERE name='max_connections'").Scan(&pending); err != nil {
			t.Fatal(err)
		}
		if pending {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !pending {
		t.Fatal("native PostgreSQL did not expose the pending postmaster change")
	}
	if restart, err := reloadSingle(ctx, s, dataPaths(h.dir)); err != nil || !restart {
		t.Fatalf("ordinary agent restart decision did not see pending postmaster change: restart=%v err=%v", restart, err)
	}
	before, err := os.ReadFile(filepath.Join(h.dir, "postgres", "postgresql.conf"))
	if err != nil {
		t.Fatal(err)
	}
	response, err := h.request(http.MethodGet, "/v1/status", nil)
	if err != nil {
		t.Fatal(err)
	}
	var generic agentapi.Status
	err = json.NewDecoder(response.Body).Decode(&generic)
	response.Body.Close()
	if err != nil || !generic.CertificateReloadOnly {
		t.Fatal("agent did not advertise reload-only certificate capability", err)
	}
	tlsConfig, err := pgx.ParseConfig("host=localhost port=5432 user=postgres dbname=postgres sslmode=verify-full")
	if err != nil {
		t.Fatal(err)
	}
	tlsConfig.Password = s.Superuser.Password
	tlsConfig.TLSConfig.RootCAs = x509.NewCertPool()
	tlsConfig.TLSConfig.RootCAs.AddCert(ca.Cert)
	if rejected, err := pgx.ConnectConfig(ctx, tlsConfig); err == nil {
		rejected.Close(ctx)
		t.Fatal("future native certificate was accepted")
	}
	s.ServerCert = issue(time.Now())
	// A certificate-only pull must not render even an unrelated desired GUC.
	s.Postgres.Parameters["max_connections"] = "99"
	raw, _ := json.Marshal(s)
	h.mu.Lock()
	h.config.EngineSpec = raw
	h.config.ConfigVersion++
	h.config.CertificateReloadOnly = true
	version := h.config.ConfigVersion
	h.mu.Unlock()
	response, err = h.request(http.MethodPost, "/v1/reload", nil)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("certificate-only reload HTTP%d", response.StatusCode)
	}
	var currentStart string
	if err := held.QueryRow(ctx, "SELECT pg_postmaster_start_time()::text").Scan(&currentStart); err != nil || currentStart != originalStart {
		t.Fatal("certificate-only reload restarted the writer or disconnected its SQL session", err)
	}
	if err := held.QueryRow(ctx, "SELECT pending_restart FROM pg_settings WHERE name='max_connections'").Scan(&pending); err != nil || !pending {
		t.Fatal("certificate-only reload consumed the pending postmaster change", err)
	}
	after, err := os.ReadFile(filepath.Join(h.dir, "postgres", "postgresql.conf"))
	if err != nil || string(before) != string(after) {
		t.Fatal("certificate-only reload changed native configuration", err)
	}
	verified := false
	for range 100 {
		connection, err := pgx.ConnectConfig(ctx, tlsConfig)
		if err == nil {
			connection.Close(ctx)
			verified = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !verified {
		t.Fatal("PostgreSQL did not load the currently valid replacement certificate")
	}
	response, err = h.request(http.MethodGet, "/v1/status", nil)
	if err != nil {
		t.Fatal(err)
	}
	err = json.NewDecoder(response.Body).Decode(&generic)
	response.Body.Close()
	if err != nil || generic.ConfigVersion != version {
		t.Fatal("certificate revision was not independently acknowledged", err)
	}
	t.Log("native PostgreSQL: invalid leaf replaced and verified; pending postmaster setting and writer SQL session preserved; ordinary restart decision bypassed")
}
