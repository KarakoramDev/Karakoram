// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

//go:build integration

package server

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agent"
	postgresagent "github.com/KarakoramDev/Karakoram/internal/agent/postgres"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	infrafake "github.com/KarakoramDev/Karakoram/internal/infra/fake"
	"github.com/KarakoramDev/Karakoram/internal/pgbinding"
	"github.com/KarakoramDev/Karakoram/internal/pki"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// The test owns postmaster startup on a dynamic port. This engine adapter checks
// that process instead of asking the supervisor to start a second postmaster.
// Transport, Node locking, status and every access action remain production code.
type nativeBindingEngine struct {
	runner  *postgresagent.Runner
	admin   *pgx.ConnConfig
	version string
}

func (e *nativeBindingEngine) Name() string        { return e.runner.Name() }
func (e *nativeBindingEngine) Processes() []string { return e.runner.Processes() }
func (e *nativeBindingEngine) ping(ctx context.Context) error {
	c, err := pgx.ConnectConfig(ctx, e.admin.Copy())
	if err != nil {
		return err
	}
	defer c.Close(ctx)
	return c.Ping(ctx)
}
func (e *nativeBindingEngine) Apply(ctx context.Context, _ *agent.Node, c agentapi.MemberConfig) ([]string, error) {
	var s pgspec.Spec
	if err := json.Unmarshal(c.EngineSpec, &s); err != nil {
		return nil, err
	}
	if s.Role != pgspec.RoleData || s.Topology != pgspec.TopologySingle {
		return nil, fmt.Errorf("native binding fixture requires a single data member")
	}
	return []string{"externally-owned-postgres"}, e.ping(ctx)
}
func (e *nativeBindingEngine) Readiness(ctx context.Context, _ agent.Options, _ []string) error {
	return e.ping(ctx)
}
func (e *nativeBindingEngine) Version(context.Context) string { return e.version }
func (e *nativeBindingEngine) Metrics(ctx context.Context, n *agent.Node) []agent.Metric {
	return e.runner.Metrics(ctx, n)
}
func (e *nativeBindingEngine) Routes(mux *http.ServeMux, n *agent.Node) { e.runner.Routes(mux, n) }

type nativeBindingDialer struct {
	address string
	ca      []byte
}

func (d nativeBindingDialer) Dial(_ context.Context, ref domain.ClusterRef, member string) (*agentapi.Client, error) {
	if ref.Name != "pgsource" || member != "pgsourcep0" {
		return nil, engine.ErrUnavailable
	}
	return agentapi.NewClient(agentapi.ClientOptions{BaseURL: "https://" + d.address, Token: "native-binding-agent-token", CAPEM: d.ca})
}

func bindingNativeAddress(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := l.Addr().String()
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	return address
}

func startBindingNativePostgres(t *testing.T, bin string, ca *pki.CA) (string, *pgx.ConnConfig, string, string) {
	t.Helper()
	// Keep Unix socket names shorter than the operating system's path limit.
	dir, err := os.MkdirTemp("", "kkbindings-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Error(err)
		}
	})
	data := filepath.Join(dir, "postgres")
	socket := filepath.Join(dir, "karakoram", "postgres", "socket")
	if err := os.MkdirAll(socket, 0700); err != nil {
		t.Fatal(err)
	}
	run := func(args ...string) {
		t.Helper()
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()
		out, err := exec.CommandContext(ctx, filepath.Join(bin, args[0]), args[1:]...).CombinedOutput()
		if err != nil {
			t.Fatalf("native %s failed: %v: %s", args[0], err, out)
		}
	}
	run("initdb", "-D", data, "-U", "postgres", "--auth-local=trust", "--auth-host=scram-sha-256", "--encoding=UTF8", "--locale=C.UTF-8", "--no-sync")
	cert, key, err := ca.Issue(pki.CertRequest{CommonName: "pgsourcep0", DNSNames: []string{"pgsourcep0"}, Server: true})
	if err != nil {
		t.Fatal(err)
	}
	for name, content := range map[string][]byte{"server.crt": cert, "server.key": key, "ca.crt": ca.CertPEM} {
		if err := os.WriteFile(filepath.Join(data, name), content, 0600); err != nil {
			t.Fatal(err)
		}
	}
	address := bindingNativeAddress(t)
	_, port, err := net.SplitHostPort(address)
	if err != nil {
		t.Fatal(err)
	}
	config := fmt.Sprintf("listen_addresses = '127.0.0.1'\nport = %s\nunix_socket_directories = '%s'\nssl = on\nssl_cert_file = 'server.crt'\nssl_key_file = 'server.key'\nssl_ca_file = 'ca.crt'\npassword_encryption = 'scram-sha-256'\nshared_buffers = '16MB'\nmax_connections = 30\nfsync = off\nlog_statement = 'none'\n", port, strings.ReplaceAll(socket, "'", "''"))
	if err := os.WriteFile(filepath.Join(data, "postgresql.conf"), []byte(config), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(data, "pg_hba.conf"), []byte("local all all trust\nhostssl all all 127.0.0.1/32 scram-sha-256\n"), 0600); err != nil {
		t.Fatal(err)
	}
	run("pg_ctl", "-D", data, "-l", filepath.Join(dir, "postgres.log"), "-w", "-t", "20", "start")
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
		defer cancel()
		out, err := exec.CommandContext(ctx, filepath.Join(bin, "pg_ctl"), "-D", data, "-w", "-t", "20", "-m", "immediate", "stop").CombinedOutput()
		if err != nil {
			t.Errorf("native PostgreSQL shutdown: %v: %s", err, out)
		}
	})
	// Production localConnect uses 5432. Alias only this test-owned Unix socket;
	// neither the server's TCP listener nor any system-wide socket uses that port.
	if port != "5432" {
		if err := os.Symlink(filepath.Join(socket, ".s.PGSQL."+port), filepath.Join(socket, ".s.PGSQL.5432")); err != nil {
			t.Fatal(err)
		}
	}
	admin, err := pgx.ParseConfig("")
	if err != nil {
		t.Fatal(err)
	}
	admin.Host, admin.User, admin.Database = socket, "postgres", "postgres"
	n, err := strconv.ParseUint(port, 10, 16)
	if err != nil {
		t.Fatal(err)
	}
	admin.Port = uint16(n)
	admin.TLSConfig = nil
	admin.Fallbacks = nil
	admin.RuntimeParams = map[string]string{}
	admin.ConnectTimeout = 3 * time.Second
	c, err := pgx.ConnectConfig(t.Context(), admin)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(t.Context())
	var version string
	if err := c.QueryRow(t.Context(), "SHOW server_version").Scan(&version); err != nil {
		t.Fatal(err)
	}
	return dir, admin, address, version
}

func startBindingNativeAgent(t *testing.T, dir, bin string, admin *pgx.ConnConfig, version string, ca *pki.CA) nativeBindingDialer {
	t.Helper()
	cert, key, err := ca.Issue(pki.CertRequest{CommonName: "binding-agent", IPs: []net.IP{net.ParseIP("127.0.0.1")}, Server: true})
	if err != nil {
		t.Fatal(err)
	}
	pair, err := tls.X509KeyPair(cert, key)
	if err != nil {
		t.Fatal(err)
	}
	major, err := strconv.Atoi(strings.Split(version, ".")[0])
	if err != nil {
		t.Fatal(err)
	}
	spec := pgspec.Spec{
		Role: pgspec.RoleData, Topology: pgspec.TopologySingle,
		Scope: "pgsource", Member: "pgsourcep0", Version: strings.Fields(version)[0],
		Major: major, BinDir: bin, Postgres: &pgspec.Postgres{},
		Superuser:   pgspec.Credentials{Username: "postgres", Password: "native-local-admin"},
		Replication: pgspec.Credentials{Username: "replicator", Password: "native-replication"},
		Rewind:      pgspec.Credentials{Username: "rewind", Password: "native-rewind"},
		Monitor:     pgspec.Credentials{Username: "monitor", Password: "native-monitor"},
	}
	raw, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	config := agentapi.MemberConfig{Cluster: "pgsource", Member: "pgsourcep0", Engine: "postgres", EngineSpec: raw, ConfigVersion: 1, AgentTLS: agentapi.Certificate{CertPEM: string(cert), KeyPEM: string(key)}}
	cp := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/internal/agent/v1/config" || r.Header.Get("Authorization") != "Bearer native-binding-agent-token" {
			http.Error(w, "unauthorized", 401)
			return
		}
		_ = json.NewEncoder(w).Encode(config)
	}))
	cp.TLS = &tls.Config{Certificates: []tls.Certificate{pair}, MinVersion: tls.VersionTLS12}
	cp.StartTLS()
	t.Cleanup(cp.Close)
	address := bindingNativeAddress(t)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	adapter := &nativeBindingEngine{runner: postgresagent.New(), admin: admin, version: version}
	go func() {
		done <- agent.Run(ctx, agent.Options{ControlPlaneURL: cp.URL, InternalCAPEM: string(ca.CertPEM), Token: "native-binding-agent-token", Cluster: "pgsource", Member: "pgsourcep0", DataDir: dir, Listen: address, Engine: "postgres"}, adapter)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("native binding agent shutdown: %v", err)
			}
		case <-time.After(15 * time.Second):
			t.Error("native binding agent shutdown timed out")
		}
	})
	dialer := nativeBindingDialer{address: address, ca: ca.CertPEM}
	client, err := dialer.Dial(t.Context(), domain.ClusterRef{Name: "pgsource"}, "pgsourcep0")
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	deadline := time.NewTimer(15 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		var status pgspec.StatusResponse
		if err := client.Engine(t.Context(), "postgres", "status", struct{}{}, &status); err == nil && status.Running && status.Role == "standalone" {
			return dialer
		}
		select {
		case err := <-done:
			t.Fatalf("native binding agent exited: %v", err)
		case <-deadline.C:
			t.Fatal("native binding agent did not become ready")
		case <-ticker.C:
		}
	}
}

func TestNativePostgresBindingsAcrossOpenBaoAndTemporal(t *testing.T) {
	bin := os.Getenv("KARAKORAM_PG_BIN_DIR")
	if bin == "" {
		t.Skip("set KARAKORAM_PG_BIN_DIR to run native binding integration")
	}
	for _, name := range []string{"postgres", "initdb", "pg_ctl"} {
		info, err := os.Stat(filepath.Join(bin, name))
		if err != nil || info.Mode()&0111 == 0 {
			t.Fatalf("configured native PostgreSQL binary %s is unavailable: %v", name, err)
		}
	}
	ca, err := pki.NewCA("native-bindings", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	dir, adminConfig, sqlAddress, version := startBindingNativePostgres(t, bin, ca)
	dialer := startBindingNativeAgent(t, dir, bin, adminConfig, version, ca)
	base, _, token := testServer(t)
	var s *Server
	manager := pgbinding.New(pgbinding.Options{Store: base.Store, Source: func(ctx context.Context, ref domain.ClusterRef) (*engine.Context, pgbinding.Access, error) {
		return s.bindingSource(ctx, ref)
	}, Trigger: func(ref domain.ClusterRef) {
		if s.Trigger != nil {
			s.Trigger.Trigger(ref)
		}
	}})
	registry, err := engine.NewRegistry(engineProviders(engineOptions{Store: base.Store, Bindings: manager})...)
	if err != nil {
		t.Fatal(err)
	}
	opts := base.Options
	opts.Bindings = manager
	opts.Registry = registry
	opts.Context.Backend = infrafake.New(infrafake.Options{})
	opts.Context.Agents = dialer
	s, err = New(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	httpServer := httptest.NewServer(s.Handler())
	t.Cleanup(httpServer.Close)
	ctx := t.Context()
	request := func(method, path, body string, want int) {
		t.Helper()
		req, err := http.NewRequestWithContext(ctx, method, httpServer.URL+"/api/v1"+path, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", "application/json")
		response, err := httpServer.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		raw, err := io.ReadAll(response.Body)
		if err != nil {
			t.Fatal(err)
		}
		if response.StatusCode != want {
			t.Fatalf("%s %s: HTTP %d, want %d: %s", method, path, response.StatusCode, want, raw)
		}
	}
	request("POST", "/clusters", `{"name":"pgsource","spec":{"provider":"postgres","topology":{"type":"single"},"components":{"extra":{"proxy":{"replicas":0}}},"tls":{"mode":"requireTLS"}}}`, 201)
	source, err := s.Store.GetCluster(ctx, domain.ClusterRef{Name: "pgsource"})
	if err != nil {
		t.Fatal(err)
	}
	source.Internal = json.RawMessage(`{"postgres":{"uid":"native-source","topology":"single","configVersion":1,"formed":true,"members":[{"name":"pgsourcep0","role":"data","index":0,"stage":"joined"}]}}`)
	if err := s.Store.UpdateClusterState(ctx, source); err != nil {
		t.Fatal(err)
	}
	if err := s.Store.SetClusterSecret(ctx, source.Ref(), "tls/ca.crt", ca.CertPEM); err != nil {
		t.Fatal(err)
	}
	request("POST", "/clusters", `{"name":"workflows","spec":{"provider":"temporal","engineOptions":{"persistence":{"postgres":{"cluster":"pgsource"}}}}}`, 201)
	request("POST", "/clusters", `{"name":"vault","spec":{"provider":"openbao","components":{"engine":{"replicas":1}},"engineOptions":{"storage":{"type":"postgresql","postgresql":{"cluster":"pgsource"}}}}}`, 201)
	for _, name := range []string{"workflows", "vault"} {
		for attempt := range 6 {
			rc, p, err := s.context(ctx, domain.ClusterRef{Name: name})
			if err != nil {
				t.Fatal(err)
			}
			if failure := s.providerContext(rc); failure != nil {
				t.Fatal(failure)
			}
			if _, err := p.Sync(ctx, rc); err != nil {
				t.Fatalf("native %s binding sync: %v", name, err)
			}
			if err := s.Store.UpdateClusterState(ctx, rc.Cluster); err != nil {
				t.Fatal(err)
			}
			rows, err := s.Store.ListPostgresBindings(ctx, source.Ref())
			if err != nil {
				t.Fatal(err)
			}
			ready := false
			for _, row := range rows {
				ready = ready || row.ConsumerName == name && row.State == "ready"
			}
			if ready {
				break
			}
			if attempt == 5 {
				t.Fatalf("%s binding did not become ready", name)
			}
		}
	}
	rows, err := s.Store.ListPostgresBindings(ctx, source.Ref())
	if err != nil || len(rows) != 2 {
		t.Fatalf("native bindings: %d, %v", len(rows), err)
	}
	caPath := filepath.Join(dir, "root-ca.pem")
	if err := os.WriteFile(caPath, ca.CertPEM, 0600); err != nil {
		t.Fatal(err)
	}
	connect := func(connection pgbinding.Connection, database, password string) (*pgx.Conn, error) {
		u := url.URL{Scheme: "postgresql", Host: net.JoinHostPort(connection.Host, strconv.Itoa(connection.Port)), Path: "/" + database, User: url.UserPassword(connection.Username, password)}
		q := url.Values{"sslmode": {"verify-full"}, "sslrootcert": {caPath}}
		u.RawQuery = q.Encode()
		cfg, err := pgx.ParseConfig(u.String())
		if err != nil {
			return nil, err
		}
		cfg.ConnectTimeout = 3 * time.Second
		// Route only transport to the owned dynamic listener. Keep the manager's
		// resolved DNS identity, role, port and CA for verify-full authentication.
		cfg.LookupFunc = func(_ context.Context, host string) ([]string, error) {
			if host != connection.Host {
				return nil, fmt.Errorf("unexpected native binding hostname")
			}
			return []string{"127.0.0.1"}, nil
		}
		cfg.DialFunc = func(ctx context.Context, network, address string) (net.Conn, error) {
			if address != net.JoinHostPort("127.0.0.1", strconv.Itoa(connection.Port)) {
				return nil, fmt.Errorf("unexpected native binding route")
			}
			return (&net.Dialer{}).DialContext(ctx, network, sqlAddress)
		}
		return pgx.ConnectConfig(ctx, cfg)
	}
	connections := map[string]pgbinding.Connection{}
	passwords := map[string]string{}
	for _, row := range rows {
		if row.State != "ready" {
			t.Fatalf("native binding not ready: %s", row.ConsumerName)
		}
		connection, err := manager.Resolve(ctx, row.ID)
		if err != nil {
			t.Fatal(err)
		}
		password, err := manager.Password(ctx, connection.PasswordSecretRef)
		if err != nil {
			t.Fatal(err)
		}
		if connection.TLS.Mode != pgbinding.TLSVerifyFull || string(connection.TLS.CAPEM) != string(ca.CertPEM) {
			t.Fatal("binding did not resolve the source TLS identity")
		}
		connections[row.ConsumerName] = connection
		passwords[row.ConsumerName] = password
		for _, database := range connection.Databases {
			c, err := connect(connection, database, password)
			if err != nil {
				t.Fatalf("%s owner connection: %v", row.ConsumerName, err)
			}
			var ownsDatabase, ownsSchema, canCreate bool
			err = c.QueryRow(ctx, `SELECT pg_catalog.pg_get_userbyid(d.datdba)=current_user,pg_catalog.pg_get_userbyid(n.nspowner)=current_user,pg_catalog.has_schema_privilege(current_user,'public','CREATE') FROM pg_catalog.pg_database d CROSS JOIN pg_catalog.pg_namespace n WHERE d.datname=current_database() AND n.nspname='public'`).Scan(&ownsDatabase, &ownsSchema, &canCreate)
			if err != nil || !ownsDatabase || !ownsSchema || !canCreate {
				c.Close(ctx)
				t.Fatalf("%s database ownership/schema CREATE: %t/%t/%t: %v", database, ownsDatabase, ownsSchema, canCreate, err)
			}
			_, err = c.Exec(ctx, `CREATE TABLE public.binding_private_probe(value text); INSERT INTO public.binding_private_probe VALUES ('owned-data')`)
			c.Close(ctx)
			if err != nil {
				t.Fatalf("%s owned table creation: %v", database, err)
			}
		}
	}
	denied := func(err error) bool { var pg *pgconn.PgError; return errors.As(err, &pg) && pg.Code == "42501" }
	for consumer, connection := range connections {
		for other, otherConnection := range connections {
			if consumer == other {
				continue
			}
			for _, database := range otherConnection.Databases {
				c, err := connect(connection, database, passwords[consumer])
				if err != nil {
					if !denied(err) {
						t.Fatalf("cross-consumer connection failed unexpectedly: %v", err)
					}
					t.Logf("%s -> %s: CONNECT denied", consumer, database)
					continue
				}
				_, readErr := c.Exec(ctx, `SELECT value FROM public.binding_private_probe`)
				_, createErr := c.Exec(ctx, `CREATE TABLE public.foreign_probe(value text)`)
				_, schemaErr := c.Exec(ctx, `CREATE SCHEMA foreign_schema_probe`)
				if !denied(readErr) || !denied(createErr) || !denied(schemaErr) {
					c.Close(ctx)
					t.Fatalf("%s accessed %s objects: read=%v table-create=%v schema-create=%v", consumer, database, readErr, createErr, schemaErr)
				}
				_, tempErr := c.Exec(ctx, `CREATE TEMP TABLE binding_temp_probe(value text)`)
				c.Close(ctx)
				t.Logf("%s -> %s: PUBLIC CONNECT allowed; TEMP allowed=%t; public CREATE, schema CREATE and private-table SELECT denied", consumer, database, tempErr == nil)
			}
		}
	}
	request("DELETE", "/clusters/pgsource", "", 409)
	for _, row := range rows {
		previous := passwords[row.ConsumerName]
		connection := connections[row.ConsumerName]
		request("POST", "/clusters/pgsource/postgres/bindings/"+row.ID+"/rotate", "", 200)
		next, err := manager.Resolve(ctx, row.ID)
		if err != nil {
			t.Fatal(err)
		}
		if next.CredentialGeneration != connection.CredentialGeneration+1 {
			t.Fatal("native rotation did not increment generation")
		}
		password, err := manager.Password(ctx, next.PasswordSecretRef)
		if err != nil || password == previous {
			t.Fatalf("native rotation did not replace sealed password: %v", err)
		}
		for _, database := range next.Databases {
			old, err := connect(next, database, previous)
			if err == nil {
				old.Close(ctx)
				t.Fatal("old binding password still authenticates")
			}
			var pg *pgconn.PgError
			if !errors.As(err, &pg) || pg.Code != "28P01" {
				t.Fatalf("old password failed for a non-authentication reason: %v", err)
			}
			c, err := connect(next, database, password)
			if err != nil {
				t.Fatalf("rotated binding password rejected: %v", err)
			}
			c.Close(ctx)
		}
		connections[row.ConsumerName] = next
		passwords[row.ConsumerName] = password
	}
	for _, consumer := range []struct {
		name string
		keep bool
	}{{"workflows", false}, {"vault", true}} {
		request("DELETE", "/clusters/"+consumer.name+fmt.Sprintf("?keepVolumes=%t", consumer.keep), "", 202)
		for attempt := range 30 {
			rc, p, err := s.context(ctx, domain.ClusterRef{Name: consumer.name})
			if err != nil {
				t.Fatal(err)
			}
			if failure := s.providerContext(rc); failure != nil {
				t.Fatal(failure)
			}
			done, err := p.Cleanup(ctx, rc, rc.Cluster.DeleteOptions)
			if err != nil {
				t.Fatalf("native %s cleanup: %v", consumer.name, err)
			}
			if err := s.Store.UpdateClusterState(ctx, rc.Cluster); err != nil {
				t.Fatal(err)
			}
			if done {
				break
			}
			if attempt == 29 {
				t.Fatal("native consumer cleanup did not finish")
			}
		}
	}
	admin, err := pgx.ConnectConfig(ctx, adminConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close(ctx)
	for name, connection := range connections {
		var roles int
		var login bool
		if err := admin.QueryRow(ctx, `SELECT count(*),COALESCE(bool_or(rolcanlogin),false) FROM pg_catalog.pg_roles WHERE rolname=$1`, connection.Username).Scan(&roles, &login); err != nil {
			t.Fatal(err)
		}
		if name == "workflows" && roles != 0 || name == "vault" && (roles != 1 || login) {
			t.Fatalf("%s release role state: count=%d login=%t", name, roles, login)
		}
		for _, database := range connection.Databases {
			var count int
			if err := admin.QueryRow(ctx, `SELECT count(*) FROM pg_catalog.pg_database WHERE datname=$1`, database).Scan(&count); err != nil {
				t.Fatal(err)
			}
			if name == "workflows" && count != 0 || name == "vault" && count != 1 {
				t.Fatalf("%s release database count=%d", database, count)
			}
		}
	}
	retained := connections["vault"]
	c, err := connect(retained, retained.Databases[0], passwords["vault"])
	if err == nil {
		c.Close(ctx)
		t.Fatal("retained binding role still permits login")
	}
	var loginError *pgconn.PgError
	if !errors.As(err, &loginError) || loginError.Code != "28000" {
		t.Fatalf("retained role login failed for a non-NOLOGIN reason: %v", err)
	}
	retainedConfig := adminConfig.Copy()
	retainedConfig.Database = retained.Databases[0]
	retainedAdmin, err := pgx.ConnectConfig(ctx, retainedConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer retainedAdmin.Close(ctx)
	var value string
	if err := retainedAdmin.QueryRow(ctx, `SELECT value FROM public.binding_private_probe`).Scan(&value); err != nil || value != "owned-data" {
		t.Fatalf("retained database lost its data: %v", err)
	}
	rows, err = s.Store.ListPostgresBindings(ctx, source.Ref())
	if err != nil || len(rows) != 0 {
		t.Fatalf("native binding metadata was not released: %v", err)
	}
	request("DELETE", "/clusters/pgsource", "", 202)
	t.Log("native binding ownership, cross-consumer object isolation, verify-full login, rotation and drop/retain release passed")
}
