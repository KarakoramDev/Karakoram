// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package neo4j

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine/neo4j/neo4jspec"
	"github.com/KarakoramDev/Karakoram/internal/pki"
)

func coreTestSpec(t *testing.T) neo4jspec.Spec {
	t.Helper()
	ca, err := pki.NewCA("test", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	cert, key, err := ca.Issue(pki.CertRequest{CommonName: "localhost", DNSNames: []string{"localhost"}, IPs: []net.IP{net.ParseIP("127.0.0.1")}, Server: true, Validity: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	return neo4jspec.Spec{Version: "2026.10.0", Install: neo4jspec.Install{Neo4jHome: filepath.Join(t.TempDir(), "neo4j"), JavaHome: filepath.Join(t.TempDir(), "java")}, HeapMB: 256, PageCacheMB: 64, CAPEM: string(ca.CertPEM), Server: neo4jspec.Certificate{CertPEM: string(cert), KeyPEM: string(key)}, AdminPassword: "secret-admin-value", AdvertisedHost: "member.test", RestartToken: "token-1"}
}
func TestRenderManagedConfiguration(t *testing.T) {
	s := coreTestSpec(t)
	s.Settings = map[string]string{"db.transaction.timeout": "15s"}
	files, err := render(s, "/volume/karakoram/neo4j", "# stock\nserver.jvm.additional=-XX:+UseG1GC\nserver.jvm.additional=--add-modules=jdk.incubator.vector\nserver.http.enabled=true\n", false, 7687, 7473)
	if err != nil {
		t.Fatal(err)
	}
	conf := string(files["neo4j/conf/neo4j.conf"])
	for _, line := range []string{"server.directories.data=/volume/karakoram/neo4j/data", "server.default_listen_address=0.0.0.0", "server.default_advertised_address=member.test", "server.bolt.tls_level=REQUIRED", "server.http.enabled=false", "server.https.enabled=true", "dbms.ssl.policy.bolt.client_auth=NONE", "dbms.ssl.policy.https.client_auth=NONE", "server.memory.heap.initial_size=256m", "server.memory.heap.max_size=256m", "server.memory.pagecache.size=64m", "dbms.usage_report.enabled=false", "server.fleet_discovery.enabled=false", "dbms.fleet_manager.enabled=false", "server.http.x_forward.enabled=false", "dbms.security.allow_csv_import_from_file_urls=false", "server.config.strict_validation.enabled=true", "server.jvm.additional=-XX:+ExitOnOutOfMemoryError", "server.jvm.additional=-XX:+UseG1GC", "server.jvm.additional=--add-modules=jdk.incubator.vector", "db.transaction.timeout=15s"} {
		if !strings.Contains(conf, line+"\n") {
			t.Errorf("missing managed setting %s", line)
		}
	}
	if strings.Contains(conf, s.AdminPassword) || strings.Contains(conf, "PRIVATE KEY") || strings.Contains(conf, "BEGIN CERTIFICATE") {
		t.Fatal("secret in neo4j.conf")
	}
	if string(files["neo4j/certificates/bolt/private.key"]) != s.Server.KeyPEM {
		t.Fatal("missing private key file")
	}
}
func TestRenderRecoveryLoopbackAndSettingsRejection(t *testing.T) {
	s := coreTestSpec(t)
	files, err := render(s, "/volume/karakoram/neo4j", "", true, 17687, 17473)
	if err != nil {
		t.Fatal(err)
	}
	conf := string(files["neo4j/recovery-conf/neo4j.conf"])
	for _, line := range []string{"server.default_listen_address=127.0.0.1", "server.bolt.listen_address=127.0.0.1:17687", "server.https.listen_address=127.0.0.1:17473", "dbms.security.auth_enabled=false"} {
		if !strings.Contains(conf, line+"\n") {
			t.Fatal("recovery policy missing", line)
		}
	}
	for _, key := range []string{"server.http.enabled", "dbms.security.auth_enabled", "server.jvm.additional", "server.directories.data", "dbms.ssl.policy.bolt.client_auth", "unknown.setting"} {
		s.Settings = map[string]string{key: "unsafe"}
		if _, err := render(s, "/base", "", false, 7687, 7473); err == nil {
			t.Fatal("accepted unsafe setting", key)
		}
	}
	s.Settings = map[string]string{"db.transaction.timeout": "1s\nserver.http.enabled=true"}
	if _, err := render(s, "/base", "", false, 7687, 7473); err == nil {
		t.Fatal("accepted newline injection")
	}
}
func TestSplitDryRunCommand(t *testing.T) {
	for _, tc := range []struct {
		line string
		want []string
	}{
		{`'/opt/java home/bin/java' -cp 'plugins/*:conf/*:lib/*' "-Dkey=a b" '' --console-mode`, []string{"/opt/java home/bin/java", "-cp", "plugins/*:conf/*:lib/*", "-Dkey=a b", "", "--console-mode"}},
		{`java 'a'\''b' escaped\ space "back\slash" "quote\"value"`, []string{"java", "a'b", "escaped space", `back\slash`, `quote"value`}},
	} {
		got, err := splitCommand(tc.line)
		if err != nil || !reflect.DeepEqual(got, tc.want) {
			t.Fatalf("split: got %#v, %v; want %#v", got, err, tc.want)
		}
	}
	for _, bad := range []string{`java 'broken`, `java "broken`, `java trailing\`, `java ; touch /file`, `java | tee /file`} {
		if _, err := splitCommand(bad); err == nil {
			t.Fatal("accepted invalid command", bad)
		}
	}
}
func TestUserValidationAndPasswordParameterization(t *testing.T) {
	secret := "quote' backtick` ; password"
	for _, op := range []string{neo4jspec.UserCreate, neo4jspec.UserPassword} {
		statement, params, err := userStatement(neo4jspec.UserRequest{Op: op, Name: "app-user", Password: secret})
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(statement, secret) || !strings.Contains(statement, "`app-user`") || params["p"] != secret {
			t.Fatal("unsafe Cypher parameterization")
		}
	}
	for _, name := range []string{"neo4j", "NEO4J", "", "x` SET PASSWORD 'x'", "a b", "-option", "line\nnext"} {
		if _, _, err := userStatement(neo4jspec.UserRequest{Op: neo4jspec.UserCreate, Name: name, Password: "password"}); err == nil {
			t.Fatal("accepted invalid name", name)
		}
	}
	for _, r := range []neo4jspec.UserRequest{{Op: "unknown", Name: "valid"}, {Op: neo4jspec.UserCreate, Name: "valid"}, {Op: neo4jspec.UserDelete, Name: "valid", Password: "unexpected"}, {Op: neo4jspec.UserList, Name: "unexpected"}} {
		if _, _, err := userStatement(r); err == nil {
			t.Fatal("accepted invalid request", r.Op)
		}
	}
	statement, params, err := userStatement(neo4jspec.UserRequest{Op: neo4jspec.UserGet, Name: "valid"})
	if err != nil || strings.Contains(statement, "'valid'") || params["name"] != "valid" {
		t.Fatal("get not parameterized")
	}
}
func TestRestartTokenDecisions(t *testing.T) {
	for _, tc := range []struct {
		running     bool
		state       appliedState
		token, want string
	}{
		{false, appliedState{}, "one", "start"},
		{true, appliedState{Applied: true, RestartToken: "one"}, "one", ""},
		{true, appliedState{Applied: true, RestartToken: "one"}, "two", "restart"},
		{true, appliedState{}, "one", ""},
	} {
		if got := restartAction(tc.running, tc.state, tc.token); got != tc.want {
			t.Fatalf("restart action %q != %q", got, tc.want)
		}
	}
}
func TestDecodeStrictSpec(t *testing.T) {
	s := coreTestSpec(t)
	payload, _ := json.Marshal(s)
	c := agentapi.MemberConfig{Engine: neo4jspec.Engine, EngineSpec: payload}
	if _, err := decodeSpec(c); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*neo4jspec.Spec){func(s *neo4jspec.Spec) { s.Install.JavaHome = "relative" }, func(s *neo4jspec.Spec) { s.Install.Neo4jHome = "/home\nserver.http.enabled=true" }, func(s *neo4jspec.Spec) { s.HeapMB = 0 }, func(s *neo4jspec.Spec) { s.PageCacheMB = 0 }, func(s *neo4jspec.Spec) { s.AdminPassword = "" }, func(s *neo4jspec.Spec) { s.Version = "2026.11.0" }, func(s *neo4jspec.Spec) { s.CAPEM = "invalid" }} {
		copy := s
		mutate(&copy)
		c.EngineSpec, _ = json.Marshal(copy)
		if _, err := decodeSpec(c); err == nil {
			t.Fatal("accepted invalid configuration")
		}
	}
	c.EngineSpec = json.RawMessage(strings.TrimSuffix(string(payload), "}") + `,"unknown":true}`)
	if _, err := decodeSpec(c); err == nil {
		t.Fatal("accepted unknown field")
	}
}
func TestReadinessMappingAndCredentials(t *testing.T) {
	s := coreTestSpec(t)
	pair, err := tls.X509KeyPair([]byte(s.Server.CertPEM), []byte(s.Server.KeyPEM))
	if err != nil {
		t.Fatal(err)
	}
	var rejected atomic.Bool
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, password, ok := r.BasicAuth()
		if rejected.Load() || !ok || user != "neo4j" || password != s.AdminPassword {
			w.WriteHeader(401)
			return
		}
		if r.URL.Path == "/" {
			writeJSON(w, 200, map[string]string{"neo4j_version": "2026.10.0", "neo4j_edition": "community"})
			return
		}
		var req struct {
			Statement string `json:"statement"`
		}
		if json.NewDecoder(r.Body).Decode(&req) != nil || req.Statement != "RETURN 1 AS ready" {
			t.Error("unexpected readiness query")
		}
		writeJSON(w, 202, map[string]any{"data": map[string]any{"fields": []string{"ready"}, "values": [][]int{{1}}}})
	}))
	server.TLS = &tls.Config{Certificates: []tls.Certificate{pair}, MinVersion: tls.VersionTLS12}
	server.StartTLS()
	defer server.Close()
	_, p, _ := net.SplitHostPort(server.Listener.Addr().String())
	port, _ := strconv.Atoi(p)
	r, err := observe(context.Background(), s, port)
	if err != nil || !r.Ready || r.Version != "2026.10.0" || r.Edition != "community" {
		t.Fatal(r, err)
	}
	rejected.Store(true)
	if _, err := observe(context.Background(), s, port); !errors.Is(err, errCredentials) {
		t.Fatal("401 not mapped to credential mismatch", err)
	}
}
func TestBoundedStrictActionDecode(t *testing.T) {
	for _, body := range []string{"", `[]`, `{"unknown":true}`, `{} {}`, strings.Repeat(" ", 65537)} {
		r := httptest.NewRequest("POST", "/", strings.NewReader(body))
		if decode(r, &struct{}{}) == nil {
			t.Fatal("accepted invalid request")
		}
	}
}

func TestNativeEnvironmentDoesNotInheritCredentials(t *testing.T) {
	t.Setenv("JAVA_TOOL_OPTIONS", "-Dsecret=ambient")
	t.Setenv("KARAKORAM_AGENT_TOKEN", "ambient-agent-token")
	s := coreTestSpec(t)
	for _, entry := range commandEnv(s, "/private/conf") {
		if strings.Contains(entry, "ambient") || strings.HasPrefix(entry, "JAVA_TOOL_OPTIONS=") || strings.HasPrefix(entry, "KARAKORAM_") {
			t.Fatal("native environment inherited agent state")
		}
	}
}

func TestAdminPasswordRotationIsIdempotent(t *testing.T) {
	s := coreTestSpec(t)
	pair, err := tls.X509KeyPair([]byte(s.Server.CertPEM), []byte(s.Server.KeyPEM))
	if err != nil {
		t.Fatal(err)
	}
	password := &atomic.Value{}
	password.Store("current-password")
	var changes atomic.Int32
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, supplied, ok := r.BasicAuth()
		if !ok || supplied != password.Load().(string) {
			w.WriteHeader(401)
			return
		}
		var req struct {
			Statement  string         `json:"statement"`
			Parameters map[string]any `json:"parameters"`
		}
		if json.NewDecoder(r.Body).Decode(&req) != nil {
			t.Error("invalid rotation JSON")
			w.WriteHeader(400)
			return
		}
		switch req.Statement {
		case "SHOW USERS":
		case "ALTER USER neo4j SET PASSWORD $p CHANGE NOT REQUIRED":
			next, ok := req.Parameters["p"].(string)
			if !ok || next == "" {
				t.Error("missing password parameter")
				w.WriteHeader(400)
				return
			}
			password.Store(next)
			changes.Add(1)
		default:
			t.Error("rotation used invalid system command")
			w.WriteHeader(400)
			return
		}
		writeJSON(w, 202, map[string]any{"data": map[string]any{"fields": []string{}, "values": [][]any{}}})
	}))
	server.TLS = &tls.Config{Certificates: []tls.Certificate{pair}, MinVersion: tls.VersionTLS12}
	server.StartTLS()
	defer server.Close()
	_, rawPort, _ := net.SplitHostPort(server.Listener.Addr().String())
	port, _ := strconv.Atoi(rawPort)
	req := neo4jspec.AdminPasswordRequest{Receipt: "rotation", Current: "current-password", Next: "next-password"}
	for range 2 {
		if err := changeAdminPassword(t.Context(), s, port, req); err != nil {
			t.Fatal("rotation failed", err)
		}
	}
	if changes.Load() != 1 {
		t.Fatal("rotation replay changed password again")
	}
	req.Current, req.Next = "drifted-current", "unused-next"
	if err := changeAdminPassword(t.Context(), s, port, req); !errors.Is(err, errCredentials) {
		t.Fatal("credential drift did not preserve the authentication failure")
	}
}

func TestDryRunAllowsOnlyOwnedRunningDiagnostic(t *testing.T) {
	for _, tc := range []struct {
		status, pid int
		stderr      string
		want        bool
	}{
		{1, 42, "Command Failed\nNeo4j is already running (pid:42).\n", true},
		{1, 42, "Command Failed\nNeo4j is already running (pid: 42).\n", true},
		{1, 43, "Neo4j is already running (pid:42).", false},
		{1, 0, "Neo4j is already running (pid:42).", false},
		{2, 42, "Neo4j is already running (pid:42).", false},
		{1, 42, "Invalid configuration", false},
		{1, 42, "Neo4j is already running (pid:42). Invalid configuration", false},
	} {
		if got := ownedDryRunExit(tc.status, tc.stderr, tc.pid); got != tc.want {
			t.Fatalf("status %d owned PID %d: got %v want %v", tc.status, tc.pid, got, tc.want)
		}
	}
}
