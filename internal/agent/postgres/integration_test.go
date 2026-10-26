// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

//go:build integration

package postgres

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agent"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
)

type nativeAgent struct {
	url, dir string
	client   *http.Client
	cancel   context.CancelFunc
	done     chan error
	mu       sync.Mutex
	config   agentapi.MemberConfig
}

func startNativeAgent(t *testing.T, s pgspec.Spec) *nativeAgent {
	t.Helper()
	ca, cert := testTLS(t)
	pair, err := tls.X509KeyPair([]byte(cert.CertPEM), []byte(cert.KeyPEM))
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(s)
	// Native PostgreSQL sockets must fit Linux's 107-byte pathname limit,
	// independent of the Go test name and the caller's temporary root.
	dir, err := os.MkdirTemp("/tmp", "kkpg-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	h := &nativeAgent{dir: dir, done: make(chan error, 1), config: agentapi.MemberConfig{Cluster: "integration", Member: s.Member, Engine: pgspec.Engine, EngineSpec: raw, AgentTLS: agentapi.Certificate{CertPEM: cert.CertPEM, KeyPEM: cert.KeyPEM}, ConfigVersion: 1}}
	cp := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/internal/agent/v1/config" || r.Header.Get("Authorization") != "Bearer integration-token" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		h.mu.Lock()
		defer h.mu.Unlock()
		_ = json.NewEncoder(w).Encode(h.config)
	}))
	cp.TLS = &tls.Config{Certificates: []tls.Certificate{pair}, MinVersion: tls.VersionTLS12}
	cp.StartTLS()
	t.Cleanup(cp.Close)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	listener.Close()
	h.url = "https://" + address
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM([]byte(ca))
	h.client = &http.Client{Transport: &http.Transport{Proxy: nil, TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}}, Timeout: 10 * time.Second}
	ctx, cancel := context.WithCancel(context.Background())
	h.cancel = cancel
	go func() {
		h.done <- agent.Run(ctx, agent.Options{ControlPlaneURL: cp.URL, InternalCAPEM: ca, Token: "integration-token", Cluster: "integration", Member: s.Member, DataDir: h.dir, Listen: address, Engine: pgspec.Engine, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}, New())
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-h.done:
			if err != nil {
				t.Errorf("native agent shutdown: %v", err)
			}
		case <-time.After(65 * time.Second):
			t.Error("native agent shutdown timed out")
		}
		h.client.CloseIdleConnections()
	})
	deadline := time.Now().Add(45 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := h.request("GET", "/v1/status", nil)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == 200 {
				return h
			}
		}
		select {
		case err := <-h.done:
			t.Fatalf("native agent exited before serving: %v", err)
		default:
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("native agent did not serve its API before deadline")
	return nil
}
func (h *nativeAgent) request(method, path string, value any) (*http.Response, error) {
	var body io.Reader
	if value != nil {
		b, _ := json.Marshal(value)
		body = bytes.NewReader(b)
	}
	req, _ := http.NewRequest(method, h.url+path, body)
	req.Header.Set("Authorization", "Bearer integration-token")
	req.Header.Set("Content-Type", "application/json")
	return h.client.Do(req)
}
func (h *nativeAgent) stop(t *testing.T) {
	t.Helper()
	h.cancel()
	select {
	case err := <-h.done:
		if err != nil {
			t.Fatal(err)
		}
		h.done <- nil
	case <-time.After(65 * time.Second):
		t.Fatal("agent stop timed out")
	}
}
func (h *nativeAgent) status(t *testing.T) pgspec.StatusResponse {
	t.Helper()
	r, err := h.request("POST", "/v1/postgres/status", struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Body.Close()
	if r.StatusCode != 200 {
		t.Fatalf("status HTTP %d", r.StatusCode)
	}
	var status pgspec.StatusResponse
	if err = json.NewDecoder(r.Body).Decode(&status); err != nil {
		t.Fatal(err)
	}
	return status
}
func TestNativePostgres(t *testing.T) {
	bin := os.Getenv("KARAKORAM_PG_BIN_DIR")
	if bin == "" {
		t.Skip("set KARAKORAM_PG_BIN_DIR to exercise installed PostgreSQL and ProxySQL")
	}
	if os.Geteuid() == 0 {
		t.Fatal("run native PostgreSQL tests as an unprivileged runtime user")
	}
	s := testSpec()
	s.BinDir = bin
	s.Postgres.Parameters = map[string]string{"shared_buffers": "32MB", "max_connections": "30"}
	t.Run("single", func(t *testing.T) {
		h := startNativeAgent(t, s)
		defer h.stop(t)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		c, err := localConnect(ctx, s, dataPaths(h.dir))
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close(ctx)
		var checksums string
		if err = c.QueryRow(ctx, "SHOW data_checksums").Scan(&checksums); err != nil || checksums != "on" {
			t.Fatalf("data checksums=%s err=%v", checksums, err)
		}
		var count int
		if err = c.QueryRow(ctx, "SELECT count(*) FROM pg_roles WHERE rolname IN ('kk_monitor','kk_replicator','kk_rewind')").Scan(&count); err != nil || count != 3 {
			t.Fatalf("system role count=%d err=%v", count, err)
		}
		var monitor, replication, rewind bool
		if err = c.QueryRow(ctx, "SELECT pg_has_role('kk_monitor','pg_monitor','MEMBER'),(SELECT rolreplication FROM pg_roles WHERE rolname='kk_replicator'),has_function_privilege('kk_rewind','pg_catalog.pg_read_binary_file(text)','EXECUTE')").Scan(&monitor, &replication, &rewind); err != nil || !monitor || !replication || !rewind {
			t.Fatalf("system role privileges incorrect: %v", err)
		}
		if _, err = c.Exec(ctx, "CREATE TABLE public.integration_probe(id integer)"); err != nil {
			t.Fatal(err)
		}
		if _, err = c.Exec(ctx, "INSERT INTO public.integration_probe SELECT generate_series(1,10)"); err != nil {
			t.Fatal(err)
		}
		metrics := databaseMetrics(ctx, c)
		seen := map[string]bool{}
		for _, m := range metrics {
			seen[m.Name] = true
		}
		for _, name := range []string{"karakoram_postgres_connections", "karakoram_postgres_max_connections", "karakoram_postgres_database_size_bytes", "karakoram_postgres_transactions_committed_total", "karakoram_postgres_archiver_failed_total"} {
			if !seen[name] {
				t.Errorf("missing metric %s", name)
			}
		}
		status := h.status(t)
		if !status.Running || status.Role != "standalone" || status.SystemID == "" || status.Timeline < 1 {
			t.Fatalf("unexpected single status: %+v", status)
		}
		if err = New().Readiness(ctx, agent.Options{DataDir: h.dir}, nil); err != nil {
			t.Fatal(err)
		}
		info, err := os.Stat(filepath.Join(h.dir, "postgres"))
		if err != nil || info.Mode().Perm() != 0700 {
			t.Fatalf("PGDATA mode incorrect: %v", err)
		}
		t.Log("single: checksums, SCRAM bootstrap, system-role grants, SQL writes, status, readiness and database/archiver metrics passed")
	})
	t.Run("supabase-bootstrap", func(t *testing.T) {
		sb := s
		sb.Flavour = pgspec.FlavourSupabase
		h := startNativeAgent(t, sb)
		defer h.stop(t)
		request := pgspec.SQLBootstrapRequest{Components: []string{"auth", "rest"}, Passwords: map[string]string{"authenticator": "authenticator-secret", "supabase_admin": "admin-secret", "supabase_auth_admin": "auth-secret"}}
		for range 2 {
			response, err := h.request("POST", "/v1/postgres/sql-bootstrap", request)
			if err != nil {
				t.Fatal(err)
			}
			response.Body.Close()
			if response.StatusCode != http.StatusOK {
				t.Fatalf("SQL bootstrap HTTP %d", response.StatusCode)
			}
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		c, err := localConnect(ctx, sb, dataPaths(h.dir))
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close(ctx)
		var migrations int
		if err = c.QueryRow(ctx, "SELECT count(*) FROM _karakoram.supabase_migrations").Scan(&migrations); err != nil || migrations < 4 {
			t.Fatalf("SQL migration ledger count=%d err=%v", migrations, err)
		}
		var postgresSuper, authenticatorSuper bool
		if err = c.QueryRow(ctx, "SELECT (SELECT rolsuper FROM pg_roles WHERE rolname='postgres'),(SELECT rolsuper FROM pg_roles WHERE rolname='authenticator')").Scan(&postgresSuper, &authenticatorSuper); err != nil || !postgresSuper || authenticatorSuper {
			t.Fatalf("SQL bootstrap privilege boundary: %v", err)
		}
		t.Log("Supabase: authenticated primary-only sql-bootstrap, curated auth/rest prerequisites, repeat-safe ledger and postgres/authenticator privilege boundary passed")
	})
	t.Run("proxy", func(t *testing.T) {
		p := s
		p.Role = pgspec.RoleProxy
		p.CAPEM, p.ServerCert = testTLS(t)
		p.Proxy = &pgspec.Proxy{Servers: []string{"127.0.0.1"}, Admin: pgspec.Credentials{Username: "kk_admin", Password: "proxy-admin-secret"}, Monitor: s.Monitor, Users: []pgspec.ProxyUser{{Username: "app", Password: "app-secret", RequireTLS: true}}, ReadWriteSplit: true, ReaderPort: true, BackendTLS: true, ServerCert: p.ServerCert}
		p.Proxy.Threads = 1
		p.Proxy.MaxConnections = 32
		h := startNativeAgent(t, p)
		defer h.stop(t)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		c, err := proxyConnect(ctx, p.Proxy)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close(ctx)
		admins, err := queryTable(ctx, c, "SELECT variable_name,variable_value FROM runtime_global_variables WHERE variable_name IN ('admin-mysql_ifaces','admin-pgsql_ifaces')")
		if err != nil || len(admins) != 2 {
			t.Fatalf("cannot inspect runtime admin listeners: %v", err)
		}
		for _, row := range admins {
			if !strings.HasPrefix(row["variable_value"], "127.0.0.1:") {
				t.Fatal("ProxySQL admin listener escaped loopback")
			}
		}
		frontend, err := (&net.Dialer{}).DialContext(ctx, "tcp", "127.0.0.1:5432")
		if err != nil {
			t.Fatal(err)
		}
		_ = frontend.SetDeadline(time.Now().Add(5 * time.Second))
		var sslRequest [8]byte
		binary.BigEndian.PutUint32(sslRequest[:4], 8)
		binary.BigEndian.PutUint32(sslRequest[4:], 80877103)
		if _, err = frontend.Write(sslRequest[:]); err != nil {
			t.Fatal(err)
		}
		var answer [1]byte
		if _, err = io.ReadFull(frontend, answer[:]); err != nil || answer[0] != 'S' {
			t.Fatalf("ProxySQL refused PostgreSQL TLS negotiation: %v", err)
		}
		roots := x509.NewCertPool()
		roots.AppendCertsFromPEM([]byte(p.CAPEM))
		tlsConn := tls.Client(frontend, &tls.Config{RootCAs: roots, ServerName: "localhost", MinVersion: tls.VersionTLS12})
		if err = tlsConn.HandshakeContext(ctx); err != nil {
			t.Fatal(err)
		}
		tlsConn.Close()
		count, err := reconcileProxy(ctx, c, p.Proxy, p.ListenHost, filepath.Join(dataPaths(h.dir).root, "ca.pem"))
		if err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Fatalf("unchanged proxy config emitted %d statements", count)
		}
		p.Proxy.Users[0].Password = "changed-secret"
		count, err = reconcileProxy(ctx, c, p.Proxy, p.ListenHost, filepath.Join(dataPaths(h.dir).root, "ca.pem"))
		if err != nil || count == 0 {
			t.Fatalf("changed proxy user not applied: statements=%d err=%v", count, err)
		}
		count, err = reconcileProxy(ctx, c, p.Proxy, p.ListenHost, filepath.Join(dataPaths(h.dir).root, "ca.pem"))
		if err != nil || count != 0 {
			t.Fatalf("second unchanged proxy reconciliation churned: %d %v", count, err)
		}
		rows, err := queryTable(ctx, c, "SELECT DISTINCT username FROM runtime_pgsql_users WHERE username='app'")
		if err != nil || len(rows) != 1 {
			t.Fatalf("runtime user not loaded: distinct users=%d err=%v", len(rows), err)
		}
		backends, err := queryTable(ctx, c, "SELECT max_connections,use_ssl FROM pgsql_servers")
		if err != nil || len(backends) == 0 || backends[0]["max_connections"] != "32" || backends[0]["use_ssl"] != "1" {
			t.Fatal("proxy backend resource/TLS settings did not reconcile")
		}
		status := h.status(t)
		if status.Running || status.Role != "proxy" || len(status.ProxyBackends) == 0 {
			t.Fatalf("unexpected configured proxy status without a PostgreSQL writer: %+v", status)
		}
		for _, backend := range status.ProxyBackends {
			if backend.Hostgroup == 10 && backend.Status == "ONLINE" {
				t.Fatalf("proxy without a PostgreSQL backend reported an ONLINE writer: %+v", backend)
			}
		}
		if len(proxyMetrics(ctx, p.Proxy)) == 0 {
			t.Fatal("proxy pool metrics missing")
		}
		c.Close(ctx)
		for _, reader := range []bool{false, true} {
			p.Proxy.ReaderPort = reader
			raw, _ := json.Marshal(p)
			h.mu.Lock()
			h.config.EngineSpec = raw
			h.config.ConfigVersion++
			h.mu.Unlock()
			response, err := h.request("POST", "/v1/reload", map[string]any{})
			if err != nil {
				t.Fatal(err)
			}
			response.Body.Close()
			if response.StatusCode != http.StatusOK {
				t.Fatalf("proxy configuration reload HTTP %d", response.StatusCode)
			}
			connection, dialErr := net.DialTimeout("tcp", "127.0.0.1:5433", time.Second)
			if connection != nil {
				connection.Close()
			}
			if (dialErr == nil) != reader {
				t.Fatalf("reader listener enabled=%t, connection result=%v", reader, dialErr)
			}
		}
		t.Log("ProxySQL: native PostgreSQL admin protocol, one-statement reconciliation, runtime LOAD/DISK SAVE, users, reader rules, status and pool metrics passed; unchanged config emits no statements")
	})
	t.Run("patroni-etcd", func(t *testing.T) {
		ha := s
		ha.Topology = pgspec.TopologyHA
		ha.Postgres = &pgspec.Postgres{Synchronous: pgspec.SyncOff, ClientCIDRs: []string{"127.0.0.0/8"}, Parameters: map[string]string{"shared_buffers": "32MB"}}
		ha.CAPEM, ha.ServerCert = testTLS(t)
		ha.Peers = []pgspec.Peer{{Name: ha.Member, Host: "localhost", Etcd: "voter"}}
		ha.Etcd = &pgspec.Etcd{InitialCluster: ha.Member + "=https://localhost:2380", State: "new", Token: "integration-dcs"}
		h := startNativeAgent(t, ha)
		defer h.stop(t)
		deadline := time.Now().Add(60 * time.Second)
		var status pgspec.StatusResponse
		for time.Now().Before(deadline) {
			status = h.status(t)
			if status.Running && status.Role == "primary" && status.Leader == ha.Member {
				break
			}
			time.Sleep(300 * time.Millisecond)
		}
		if !status.Running || status.Role != "primary" || status.Leader != ha.Member || status.EtcdMemberID == "" {
			t.Fatalf("Patroni/etcd did not bootstrap: %+v", status)
		}
		resp, err := h.request("POST", "/v1/reload", map[string]any{})
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Fatalf("Patroni reload HTTP %d", resp.StatusCode)
		}
		resp, err = h.request("POST", "/v1/postgres/etcd-member", pgspec.EtcdMemberRequest{Op: "list"})
		if err != nil {
			t.Fatal(err)
		}
		var members pgspec.EtcdMemberResponse
		if err = json.NewDecoder(resp.Body).Decode(&members); err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if len(members.Members) != 1 {
			t.Fatalf("etcd member list length=%d", len(members.Members))
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		ha.Postgres.Parameters["max_connections"] = "40"
		ha.Postgres.Parameters["work_mem"] = "8MB"
		if err = patchDynamic(ctx, ha, dataPaths(h.dir)); err != nil {
			t.Fatal(err)
		}
		var dynamic map[string]any
		if err = patroniRequest(ctx, ha, "GET", "/config", nil, &dynamic); err != nil {
			t.Fatal(err)
		}
		if dynamic["postgresql"].(map[string]any)["parameters"].(map[string]any)["max_connections"] != "40" {
			t.Fatal("Patroni dynamic PATCH was not applied")
		}
		delete(ha.Postgres.Parameters, "work_mem")
		if err = patchDynamic(ctx, ha, dataPaths(h.dir)); err != nil {
			t.Fatal(err)
		}
		if err = patroniRequest(ctx, ha, "GET", "/config", nil, &dynamic); err != nil {
			t.Fatal(err)
		}
		if _, exists := dynamic["postgresql"].(map[string]any)["parameters"].(map[string]any)["work_mem"]; exists {
			t.Fatal("retired parameter persisted in Patroni DCS")
		}
		c, err := localConnect(ctx, ha, dataPaths(h.dir))
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close(ctx)
		var roles int
		if err = c.QueryRow(ctx, "SELECT count(*) FROM pg_roles WHERE rolname IN ('kk_monitor','kk_replicator','kk_rewind')").Scan(&roles); err != nil || roles != 3 {
			t.Fatalf("Patroni bootstrap roles=%d: %v", roles, err)
		}
		out, err := command(ctx, "/usr/bin/etcd", "--version")
		if err != nil || !strings.Contains(string(out), "3.5.") {
			t.Fatalf("expected Percona etcd 3.5.x: %v", err)
		}
		c.Close(ctx)
		response, err := h.request("POST", "/v1/postgres/dcs-reset", map[string]any{})
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		if response.StatusCode != http.StatusConflict {
			t.Fatal("DCS reset accepted a running Patroni member")
		}
		response, err = h.request("POST", "/v1/processes/patroni/stop", nil)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		if response.StatusCode != http.StatusOK {
			t.Fatalf("stop Patroni HTTP %d", response.StatusCode)
		}
		key := base64.StdEncoding.EncodeToString([]byte("/karakoram/" + ha.Scope + "/leader"))
		if err = etcdRequest(ctx, ha, "/v3/kv/put", map[string]string{"key": key, "value": base64.StdEncoding.EncodeToString([]byte("held"))}, nil); err != nil {
			t.Fatal(err)
		}
		response, err = h.request("POST", "/v1/postgres/dcs-reset", map[string]any{})
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		if response.StatusCode != http.StatusConflict {
			t.Fatal("DCS reset ignored the leader key")
		}
		if err = etcdRequest(ctx, ha, "/v3/kv/deleterange", map[string]string{"key": key}, nil); err != nil {
			t.Fatal(err)
		}
		response, err = h.request("POST", "/v1/postgres/dcs-reset", map[string]any{})
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		if response.StatusCode != http.StatusOK {
			t.Fatalf("guarded DCS reset HTTP %d", response.StatusCode)
		}
		t.Log("Patroni 4.1.5 + Percona etcd 3.5.x: native TLS client+peer DCS, authenticated REST, primary election, SQL role bootstrap, dynamic PATCH/reload and v3 JSON membership gateway passed")
	})
}
