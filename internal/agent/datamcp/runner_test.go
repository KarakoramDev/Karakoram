package datamcp

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agent"
	"github.com/KarakoramDev/Karakoram/internal/datamcp/companionspec"
	"gopkg.in/yaml.v3"
)

func fixture(upstream string) (companionspec.Spec, companionspec.Process) {
	p := companionspec.Process{Level: "read", Port: 18080, Username: "reader", Password: "database-secret", Hosts: []string{"db1:5432", "db2:5432"}, Database: "postgres", URI: "mongodb://reader:database-secret@db/test"}
	return companionspec.Spec{Upstream: upstream, Version: "1", InstallDir: "/opt/upstream", Processes: []companionspec.Process{p}}, p
}
func TestRenderMongo(t *testing.T) {
	s, p := fixture(companionspec.UpstreamMongoDB)
	spec, files, err := Render(s, p, "/data", "local-token")
	if err != nil {
		t.Fatal(err)
	}
	if spec.Binary != "/opt/upstream/bin/start" || len(spec.Args) != 0 {
		t.Fatal(spec)
	}
	env := strings.Join(spec.Env, "\n")
	for _, want := range []string{"MDB_MCP_READ_ONLY=true", "MDB_MCP_HTTP_HEADERS={\"Authorization\":\"Bearer local-token\"}", "MDB_MCP_ALLOW_REQUEST_OVERRIDES=false", "MDB_MCP_DISABLE_SERVER_SIDE_JS=true", "MDB_MCP_TELEMETRY=disabled", "MDB_MCP_MAX_TIME_M_S=10000", "HOME=/data/karakoram/datamcp/mcp-ro", "MDB_MCP_DISABLED_TOOLS=connect,atlas,"} {
		if !strings.Contains(env, want) {
			t.Fatalf("missing %s", want)
		}
	}
	if string(files["datamcp/mcp-ro/local-secret"]) != "local-token" {
		t.Fatal(files)
	}
	p.Level = "readWrite"
	s.Processes = []companionspec.Process{p}
	spec, _, err = Render(s, p, "/data", "token")
	if err != nil || !strings.Contains(strings.Join(spec.Env, "\n"), "MDB_MCP_READ_ONLY=false") {
		t.Fatal(err, spec)
	}
}
func TestRenderPostgres(t *testing.T) {
	s, p := fixture(companionspec.UpstreamPostgres)
	p.TLS = true
	p.CAPEM = "test CA"
	s.Processes = []companionspec.Process{p}
	spec, files, err := Render(s, p, "/data", "local-token")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(strings.Join(spec.Args, " "), "secret") || strings.Contains(strings.Join(spec.Args, " "), "token") {
		t.Fatal(spec.Args)
	}
	var cfg map[string]any
	if err = yaml.Unmarshal(files["datamcp/mcp-ro/config.yaml"], &cfg); err != nil {
		t.Fatal(err)
	}
	db := cfg["databases"].([]any)[0].(map[string]any)
	hosts := db["hosts"].([]any)
	if len(hosts) != 2 || hosts[0].(map[string]any)["host"] != "db1" || hosts[0].(map[string]any)["port"] != 5432 {
		t.Fatal(hosts)
	}
	for key, want := range map[string]any{"sslmode": "verify-full", "target_session_attrs": "prefer-standby", "allow_writes": false, "allow_llm_switching": false} {
		if db[key] != want {
			t.Errorf("%s=%v", key, db[key])
		}
	}
	tokens := string(files["datamcp/mcp-ro/tokens.yaml"])
	if strings.Contains(tokens, "local-token") || !strings.Contains(tokens, "hash:") {
		t.Fatal(tokens)
	}
	var store struct {
		Tokens map[string]struct {
			Hash     string `yaml:"hash"`
			Database string `yaml:"database"`
		} `yaml:"tokens"`
	}
	if err := yaml.Unmarshal([]byte(tokens), &store); err != nil {
		t.Fatal(err)
	}
	if token := store.Tokens["karakoram-local"]; len(token.Hash) != 64 || token.Database != "cluster" {
		t.Fatal(store)
	}
	for _, key := range []string{"llm", "embedding", "knowledgebase"} {
		if cfg[key].(map[string]any)["enabled"] != false {
			t.Fatal(key)
		}
	}
	p.Level = "readWrite"
	s.Processes = []companionspec.Process{p}
	_, files, err = Render(s, p, "/data", "local-token")
	if err != nil {
		t.Fatal(err)
	}
	if err = yaml.Unmarshal(files["datamcp/mcp-rw/config.yaml"], &cfg); err != nil {
		t.Fatal(err)
	}
	db = cfg["databases"].([]any)[0].(map[string]any)
	if db["target_session_attrs"] != "read-write" || db["allow_writes"] != true {
		t.Fatal(db)
	}
}
func TestValidate(t *testing.T) {
	for _, mutate := range []func(*companionspec.Spec){func(s *companionspec.Spec) { s.Upstream = "other" }, func(s *companionspec.Spec) { s.InstallDir = "relative" }, func(s *companionspec.Spec) { s.Processes[0].Port = 0 }, func(s *companionspec.Spec) { s.Processes = append(s.Processes, s.Processes[0]) }, func(s *companionspec.Spec) { s.Processes[0].Level = "root" }, func(s *companionspec.Spec) { s.Processes[0].TLS = true }, func(s *companionspec.Spec) { s.Processes[0].MaxTimeMS = -1 }} {
		s, _ := fixture(companionspec.UpstreamMongoDB)
		mutate(&s)
		if validate(s) == nil {
			t.Fatal(s)
		}
	}
}
func fakeEndpoint(t *testing.T, h http.Handler) endpoint {
	t.Helper()
	server := httptest.NewServer(h)
	t.Cleanup(server.Close)
	port := server.Listener.Addr().(*net.TCPAddr).Port
	return endpoint{Port: port, Token: "private-token", Path: "/mcp"}
}
func TestProxy(t *testing.T) {
	e := fakeEndpoint(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/mcp/sub" || r.URL.RawQuery != "x=1" || r.Header.Get("Authorization") != "Bearer private-token" || r.Header.Get("Cookie") != "" {
			t.Errorf("bad forwarded request: %s %v", r.URL, r.Header)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: first\n\n")
		w.(http.Flusher).Flush()
		io.WriteString(w, "data: second\n\n")
	}))
	req := httptest.NewRequest("POST", "/v1/datamcp/ro/sub?x=1", strings.NewReader("payload"))
	req.Header.Set("Authorization", "Bearer agent-secret")
	req.Header.Set("Cookie", "session=secret")
	w := httptest.NewRecorder()
	proxy(e, "/v1/datamcp/ro").ServeHTTP(w, req)
	if w.Code != 200 || !w.Flushed || !strings.Contains(w.Body.String(), "second") {
		t.Fatal(w)
	}
	e.Port = 0
	w = httptest.NewRecorder()
	proxy(e, "/v1/datamcp/ro").ServeHTTP(w, req)
	if w.Code != 502 || w.Body.String() != "upstream unavailable\n" {
		t.Fatal(w)
	}
}
func TestReadiness(t *testing.T) {
	e := fakeEndpoint(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer private-token" {
			w.WriteHeader(401)
			return
		}
		io.WriteString(w, `{"jsonrpc":"2.0","id":1,"result":{}}`)
	}))
	r := New()
	r.state = state{Version: "3.1.0", ConfigVersion: 7, Processes: map[string]endpoint{"mcp-ro": e}}
	mux := http.NewServeMux()
	r.Routes(mux, nil)
	for _, method := range []string{"POST", "GET"} {
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, httptest.NewRequest(method, "/v1/datamcp/readiness", nil))
		if w.Code != 200 || !strings.Contains(w.Body.String(), `"configVersion":7`) {
			t.Fatal(w)
		}
	}
	dir := t.TempDir()
	os.MkdirAll(filepath.Join(dir, "karakoram/datamcp"), 0700)
	raw, _ := json.Marshal(r.state)
	os.WriteFile(filepath.Join(dir, "karakoram/datamcp/readiness.json"), raw, 0600)
	if err := r.Readiness(context.Background(), agent.Options{DataDir: dir}, nil); err != nil {
		t.Fatal(err)
	}
	e.Token = "wrong"
	r.state.Processes["mcp-ro"] = e
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest("POST", "/v1/datamcp/readiness", nil))
	if w.Code != 503 {
		t.Fatal(w)
	}
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest("POST", "/v1/datamcp/rw", nil))
	if w.Code != 404 {
		t.Fatal(w)
	}
	if r.Version(context.Background()) != "3.1.0" {
		t.Fatal("version")
	}
}

func TestPostgresHosts(t *testing.T) {
	for _, address := range []string{"db", "db:5433", "[::1]:5432", "::1"} {
		if _, err := postgresHosts([]string{address}); err != nil {
			t.Fatal(address, err)
		}
	}
	for _, address := range []string{"", "db:0", "db:70000", "db:bad", ":5432"} {
		if _, err := postgresHosts([]string{address}); err == nil {
			t.Fatal(address)
		}
	}
}
