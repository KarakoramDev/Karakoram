// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"gopkg.in/yaml.v3"
)

func testSpec() pgspec.Spec {
	return pgspec.Spec{Role: pgspec.RoleData, Topology: pgspec.TopologySingle, Scope: "test-scope", Member: "member0", Major: 17, BinDir: "/usr/lib/postgresql/17/bin", Version: "17.11.1", Superuser: pgspec.Credentials{Username: "postgres", Password: "super-secret"}, Replication: pgspec.Credentials{Username: "kk_replicator", Password: "replication-secret"}, Rewind: pgspec.Credentials{Username: "kk_rewind", Password: "rewind-secret"}, Monitor: pgspec.Credentials{Username: "kk_monitor", Password: "monitor-secret"}, PatroniREST: pgspec.Credentials{Username: "kk_admin", Password: "rest-secret"}, Postgres: &pgspec.Postgres{Synchronous: pgspec.SyncStrict, ClientCIDRs: []string{"127.0.0.0/8"}}}
}
func testTLS(t *testing.T) (string, *pgspec.Certificate) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test-ca"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
	caDER, err := x509.CreateCertificate(rand.Reader, ca, ca, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	leaf := &x509.Certificate{SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "localhost"}, NotBefore: ca.NotBefore, NotAfter: ca.NotAfter, DNSNames: []string{"localhost"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}}
	der, err := x509.CreateCertificate(rand.Reader, leaf, ca, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})), &pgspec.Certificate{CertPEM: string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})), KeyPEM: string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}))}
}
func TestValidation(t *testing.T) {
	base := testSpec()
	if err := validate(base); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*pgspec.Spec){"role": func(s *pgspec.Spec) { s.Role = "other" }, "scope": func(s *pgspec.Spec) { s.Scope = "../escape" }, "binary": func(s *pgspec.Spec) { s.BinDir = "relative" }, "credentials": func(s *pgspec.Spec) { s.Superuser.Password = "" }, "cidr": func(s *pgspec.Spec) { s.Postgres.ClientCIDRs = []string{"not-a-cidr"} }, "guc": func(s *pgspec.Spec) { s.Postgres.Parameters = map[string]string{"x\nport": "1"} }, "ha": func(s *pgspec.Spec) { s.Topology = pgspec.TopologyHA }, "restore": func(s *pgspec.Spec) { s.Bootstrap = &pgspec.Bootstrap{Method: "restore"} }} {
		t.Run(name, func(t *testing.T) {
			s := testSpec()
			mutate(&s)
			if validate(s) == nil {
				t.Fatal("invalid spec accepted")
			}
		})
	}
	b, _ := json.Marshal(base)
	b = append(b[:len(b)-1], []byte(`,"unknown":true}`)...)
	if _, err := decodeSpec(agentapi.MemberConfig{EngineSpec: b}); err == nil {
		t.Fatal("unknown field accepted")
	}
}
func TestSingleRenderer(t *testing.T) {
	s := testSpec()
	s.Postgres.Parameters = map[string]string{"port": "9999", "shared_buffers": "128MB", "application_name": "a'b\\c"}
	s.Postgres.SharedPreloadLibraries = []string{"pg_stat_statements", "pg_cron"}
	p := dataPaths("/volume")
	conf := renderPostgres(s, p)
	for _, want := range []string{"port = '5432'", "archive_command = '/bin/true'", "shared_preload_libraries = 'pg_stat_statements,pg_cron'", "password_encryption = 'scram-sha-256'", "application_name = 'a''b\\\\c'"} {
		if !strings.Contains(conf, want) {
			t.Errorf("missing %q", want)
		}
	}
	lines := strings.Join(hba(s), "\n")
	if !strings.Contains(lines, "host replication kk_replicator 127.0.0.1/32 scram-sha-256") {
		t.Fatal("loopback replication missing")
	}
}
func TestPatroniRenderer(t *testing.T) {
	s := testSpec()
	s.Topology = pgspec.TopologyHA
	s.CAPEM, s.ServerCert = testTLS(t)
	s.Etcd = &pgspec.Etcd{State: "new", Token: "test", InitialCluster: "member0=https://localhost:2380"}
	s.Peers = []pgspec.Peer{{Name: s.Member, Host: "localhost", Etcd: "voter"}}
	if err := validate(s); err != nil {
		t.Fatal(err)
	}
	p := dataPaths("/volume")
	text, err := renderPatroni(s, p)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err = yaml.Unmarshal([]byte(text), &m); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"synchronous_mode: true", "synchronous_mode_strict: true", "use_pg_rewind: true", "use_slots: true", "remove_data_directory_on_rewind_failure: true", "remove_data_directory_on_diverged_timelines: true", "protocol: https", "verify_client: optional", "post_bootstrap:", "- basebackup"} {
		if !strings.Contains(text, want) {
			t.Errorf("missing %s", want)
		}
	}
	initdb := m["bootstrap"].(map[string]any)["initdb"].([]any)
	portableLocale := false
	for _, option := range initdb {
		if value, ok := option.(map[string]any); ok && value["locale"] == "C.UTF-8" {
			portableLocale = true
		}
	}
	if !portableLocale {
		t.Fatal("Patroni initdb must not inherit a locale unavailable in the runtime image")
	}
	process := patroniProcess(p)
	env := strings.Join(process.Env, "\n")
	if !strings.Contains(env, "LANG=C.UTF-8") || !strings.Contains(env, "LC_ALL=C.UTF-8") {
		t.Fatal("Patroni child locale must be deterministic")
	}
	if strings.Contains(text, "- pgbackrest") {
		t.Fatal("backup method enabled before backup")
	}
	proc := etcdProcess(s, p)
	if proc.Binary != "/usr/bin/etcd" {
		t.Fatal(proc.Binary)
	}
	for _, want := range []string{"--client-cert-auth=true", "--peer-client-cert-auth=true", "--initial-cluster-state=new"} {
		found := false
		for _, a := range proc.Args {
			found = found || a == want
		}
		if !found {
			t.Error(want)
		}
	}
}

func TestConversionOnlyOriginalMemberMayBootstrap(t *testing.T) {
	for _, name := range []string{"member0", "member1"} {
		t.Run(name, func(t *testing.T) {
			s := testSpec()
			s.Topology = pgspec.TopologyHA
			s.Member = name
			s.Bootstrap = &pgspec.Bootstrap{Method: "initdb", SeedMember: "member0"}
			text, err := renderPatroni(s, dataPaths("/volume"))
			if err != nil {
				t.Fatal(err)
			}
			var config map[string]any
			if err := yaml.Unmarshal([]byte(text), &config); err != nil {
				t.Fatal(err)
			}
			_, mayBootstrap := config["bootstrap"]
			if mayBootstrap != (name == "member0") {
				t.Fatalf("bootstrap permission for %s = %v", name, mayBootstrap)
			}
			pg := config["postgresql"].(map[string]any)
			if len(pg["create_replica_methods"].([]any)) == 0 {
				t.Fatal("non-seed must remain able to stream from the adopted primary")
			}
		})
	}
}
func TestProxyServerLoadPreservesRuntimeRoles(t *testing.T) {
	columns := []string{"hostname", "hostgroup_id"}
	configured := []tableRow{
		{"hostname": "primary", "hostgroup_id": "10"},
		{"hostname": "replica", "hostgroup_id": "10"},
	}
	desired := []tableRow{
		{"hostname": "primary", "hostgroup_id": "20"},
		{"hostname": "replica", "hostgroup_id": "20"},
		{"hostname": "new-replica", "hostgroup_id": "20"},
	}
	setProxyServerHostgroups(desired, configured)
	if diff := proxyTableDiff("pgsql_servers", "hostname", configured, desired[:2], columns); len(diff) != 0 {
		t.Fatalf("monitor-only role changes churned configuration: %v", diff)
	}
	runtime := []tableRow{
		{"hostname": "primary", "hostgroup_id": "10"},
		{"hostname": "replica", "hostgroup_id": "20"},
	}
	setProxyServerHostgroups(desired, runtime)
	diff := proxyTableDiff("pgsql_servers", "hostname", configured, desired, columns)
	joined := strings.Join(diff, "\n")
	if desired[0]["hostgroup_id"] != "10" || desired[1]["hostgroup_id"] != "20" || desired[2]["hostgroup_id"] != "20" {
		t.Fatalf("unsafe server roles across LOAD: %+v", desired)
	}
	if !strings.Contains(joined, "'new-replica','20'") || !strings.Contains(joined, "'replica','20'") || strings.Contains(joined, "'replica','10'") {
		t.Fatalf("LOAD would route writes to a replica: %v", diff)
	}
}

func TestProxyDiff(t *testing.T) {
	cols := []string{"username", "password", "use_ssl"}
	rows := []tableRow{{"username": "alice", "password": "p'ass", "use_ssl": "1"}}
	if got := proxyTableDiff("pgsql_users", "username", rows, rows, cols); len(got) != 0 {
		t.Fatal("unchanged config churned")
	}
	next := []tableRow{{"username": "alice", "password": "new", "use_ssl": "1"}, {"username": "bob", "password": "b", "use_ssl": "0"}}
	got := proxyTableDiff("pgsql_users", "username", rows, next, cols)
	if len(got) != 3 || !strings.HasPrefix(got[0], "DELETE FROM") || !strings.Contains(got[2], "'bob'") {
		t.Fatalf("unexpected diff: %v", got)
	}
	if sqlQuote("x'y") != "'x''y'" {
		t.Fatal("SQL quoting")
	}
	p := dataPaths("/volume")
	if p.proxy != filepath.Join("/volume", "karakoram", "postgres", "proxy") {
		t.Fatal(p.proxy)
	}
	text := renderProxy(&pgspec.Proxy{Admin: pgspec.Credentials{Username: "admin", Password: "secret"}, ReaderPort: true}, p, "")
	for _, v := range []string{"authentication_method=3", "127.0.0.1:6132", "0.0.0.0:5432;0.0.0.0:5433"} {
		if !strings.Contains(text, v) {
			t.Error(v)
		}
	}
}
func TestDynamicConfigSubset(t *testing.T) {
	if !containsConfig(map[string]any{"x": float64(1), "extra": true}, map[string]any{"x": 1}) {
		t.Fatal("equivalent dynamic config changed")
	}
	if containsConfig(map[string]any{"x": 2}, map[string]any{"x": 1}) {
		t.Fatal("changed config ignored")
	}
}

func TestLocalPathsStayOutsideDCS(t *testing.T) {
	s := testSpec()
	p := dataPaths("/data/member0")
	params := dynamicConfig(s, p)["postgresql"].(map[string]any)["parameters"].(map[string]any)
	for _, name := range localParameters {
		if _, exists := params[name]; exists {
			t.Errorf("node-local %s leaked into DCS", name)
		}
	}
	if params["archive_mode"] != "on" {
		t.Fatal("archiving missing from DCS")
	}
	s.Postgres.Parameters = map[string]string{"wal_level": "logical", "max_wal_senders": "20"}
	local := parameters(s, p)
	if local["wal_level"] != "logical" || local["max_wal_senders"] != "20" {
		t.Fatal("engine extension parameters were overwritten")
	}
}

func TestIPv6ListenersAndReplicationCIDRs(t *testing.T) {
	s := testSpec()
	s.ListenHost = "::"
	s.Topology = pgspec.TopologyHA
	s.CAPEM, s.ServerCert = testTLS(t)
	s.Etcd = &pgspec.Etcd{State: "new", Token: "test", InitialCluster: "member0=https://localhost:2380"}
	s.Peers = []pgspec.Peer{{Name: s.Member, Host: "localhost", Etcd: "voter"}}
	if err := validate(s); err != nil {
		t.Fatal(err)
	}
	p := dataPaths("/volume")
	y, err := renderPatroni(s, p)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(y, "[::]:8008") || !strings.Contains(y, "[::]:5432") {
		t.Fatal("Patroni IPv6 listener missing")
	}
	etcd := strings.Join(etcdProcess(s, p).Args, " ")
	if !strings.Contains(etcd, "https://[::]:2379") || !strings.Contains(etcd, "https://[::]:2380") {
		t.Fatal("etcd IPv6 listener missing")
	}
	if proxyInterfaces(s.ListenHost, true) != "[::]:5432;[::]:5433" || frontendAddress(s.ListenHost) != "[::1]:5432" {
		t.Fatal("proxy IPv6 listener missing")
	}
	if !strings.Contains(strings.Join(hba(s), "\n"), "hostssl replication kk_replicator 127.0.0.0/8 scram-sha-256") {
		t.Fatal("replication HBA must not depend on reverse DNS")
	}
	s.ListenHost = "127.0.0.1"
	if validate(s) == nil {
		t.Fatal("unrecognised listener policy accepted")
	}
}

func TestRestoreAndRepositoryRender(t *testing.T) {
	s := testSpec()
	repo := pgspec.Repository{Stanza: "kk", Path: "/postgres/namespace/cluster", S3: pgspec.S3{Endpoint: "http://storage.example.com", Port: 9000, Bucket: "backups", Region: "region", AccessKey: "access", SecretKey: "secret", PathStyle: true, CAPEM: "test-ca"}}
	s.Backup = &pgspec.Backup{Repository: repo, ArchiveTimeoutSeconds: 30}
	s.Bootstrap = &pgspec.Bootstrap{Method: "restore", Restore: &pgspec.Restore{Source: repo, Set: "20261017-010101F", TargetTime: "2026-10-17T01:01:01Z"}}
	s.Postgres.PgBackRestReplicas = true
	if err := validate(s); err != nil {
		t.Fatal(err)
	}
	p := dataPaths("/volume")
	files, err := render(s, p, nil)
	if err != nil {
		t.Fatal(err)
	}
	found := map[string]string{}
	for _, f := range files {
		found[f.name] = f.content
	}
	if !strings.Contains(found["pgbackrest.conf"], "repo1-s3-endpoint=http://storage.example.com") || !strings.Contains(found["pgbackrest.conf"], "repo1-retention-full=9999999") {
		t.Fatal("repository transport or retention missing")
	}
	if !strings.Contains(found["pgbackrest-restore.conf"], "restore-s3-ca.pem") || found["restore-s3-ca.pem"] != "test-ca" {
		t.Fatal("restore repository CA mixed with destination CA")
	}
	if !strings.Contains(found["bootstrap-restore.sh"], "--type=time") || !strings.Contains(found["bootstrap-restore.sh"], "--target-action=promote") {
		t.Fatal("restore bootstrap targets missing")
	}
}

func TestVersionParsing(t *testing.T) {
	if got := parseVersion("2026-10-17 00:00:00 warning\nProxySQL version 3.0.11-797-gabcdef, codename Truls", true); got != "3.0.11" {
		t.Fatal(got)
	}
	if got := parseVersion("postgres (PostgreSQL) 17.11 - Percona Server for PostgreSQL 17.11.1 - Percona Distribution", false); got != "17.11.1" {
		t.Fatal(got)
	}
	if got := parseVersion("postgres (PostgreSQL) 17.11", false); got != "17.11" {
		t.Fatal(got)
	}
}

func TestEngineActionTransportAndBounds(t *testing.T) {
	mux := http.NewServeMux()
	New().Routes(mux, nil)
	// The shared Engine client uses POST even for observational actions.
	_, pattern := mux.Handler(httptest.NewRequest(http.MethodPost, "/v1/postgres/status", nil))
	if pattern != "POST /v1/postgres/status" {
		t.Fatalf("status does not match Engine client POST: %q", pattern)
	}
	for _, tc := range []struct {
		method, path, body string
		status             int
	}{
		{http.MethodGet, "/v1/postgres/status", "{}", http.StatusMethodNotAllowed},
		{http.MethodPost, "/v1/postgres/status", `{"unknown":true}`, http.StatusBadRequest},
		{http.MethodPost, "/v1/postgres/sql-bootstrap", `{"sql":"SELECT 1"}`, http.StatusBadRequest},
		{http.MethodPost, "/v1/postgres/switchover", `{"candidate":"` + strings.Repeat("a", 17<<10) + `"}`, http.StatusBadRequest},
		{http.MethodPost, "/v1/postgres/not-an-action", "{}", http.StatusNotFound},
	} {
		t.Run(tc.method+tc.path, func(t *testing.T) {
			response := httptest.NewRecorder()
			mux.ServeHTTP(response, httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body)))
			if response.Code != tc.status {
				t.Fatalf("HTTP%d, want%d", response.Code, tc.status)
			}
		})
	}
}

func TestProxyManagedUserIdentifiers(t *testing.T) {
	for _, name := range []string{"app.user", "O'Brien", "应用用户", "123 user"} {
		t.Run(name, func(t *testing.T) {
			s := testSpec()
			s.Role = pgspec.RoleProxy
			s.Proxy = &pgspec.Proxy{
				Admin:   pgspec.Credentials{Username: "admin", Password: "secret"},
				Monitor: s.Monitor, Servers: []string{"localhost"},
				Users: []pgspec.ProxyUser{{Username: name, Password: "user-secret"}},
			}
			if err := validate(s); err != nil {
				t.Fatal("valid access API identifier rejected", err)
			}
			plan := proxyTableDiff("pgsql_users", "username", nil, []tableRow{{"username": name, "password": "user-secret"}}, []string{"username", "password"})
			want := "INSERT INTO pgsql_users (username,password) VALUES (" + "'" + strings.ReplaceAll(name, "'", "''") + "','user-secret')"
			if len(plan) != 1 || plan[0] != want {
				t.Fatal("proxy username not safely quoted", plan)
			}
			s.Proxy.Admin.Username = name
			if validate(s) == nil {
				t.Fatal("internal admin credential validation was relaxed")
			}
		})
	}
	for _, name := range []string{"", "bad\nname", "bad\x00name", strings.Repeat("a", 64), "\xff"} {
		s := testSpec()
		s.Role = pgspec.RoleProxy
		s.Proxy = &pgspec.Proxy{Admin: pgspec.Credentials{Username: "admin", Password: "secret"}, Monitor: s.Monitor, Servers: []string{"localhost"}, Users: []pgspec.ProxyUser{{Username: name, Password: "secret"}}}
		if validate(s) == nil {
			t.Fatalf("invalid access API identifier accepted: %q", name)
		}
	}
}
