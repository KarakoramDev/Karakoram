//go:build integration

package datamcp

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	gateway "github.com/KarakoramDev/Karakoram/internal/datamcp"
	"github.com/KarakoramDev/Karakoram/internal/datamcp/companionspec"
	"github.com/jackc/pgx/v5"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func binary(t *testing.T, env, fallback string) string {
	t.Helper()
	path := os.Getenv(env)
	if path == "" {
		path = fallback
	}
	path, e := filepath.Abs(path)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = os.Stat(path); e != nil {
		t.Skipf("set %s: %v", env, e)
	}
	return path
}
func runCommand(t *testing.T, path string, args ...string) {
	t.Helper()
	out, e := exec.Command(path, args...).CombinedOutput()
	if e != nil {
		t.Fatalf("%s: %v\n%s", filepath.Base(path), e, out)
	}
}
func startProcess(t *testing.T, path string, args ...string) {
	t.Helper()
	log, e := os.CreateTemp(t.TempDir(), "process.log")
	if e != nil {
		t.Fatal(e)
	}
	cmd := exec.Command(path, args...)
	cmd.Stdout = log
	cmd.Stderr = log
	if e = cmd.Start(); e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Signal(os.Interrupt)
		done := make(chan error, 1)
		go func() { done <- cmd.Wait() }()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			cmd.Process.Kill()
			<-done
		}
		log.Close()
		if t.Failed() {
			data, _ := os.ReadFile(log.Name())
			t.Logf("%s: %s", path, data)
		}
	})
}

func exercise(t *testing.T, a *integrationAgent, p companionspec.Process, tool string, read, write map[string]any) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	session, e := mcp.NewClient(&mcp.Implementation{Name: "integration", Version: "1"}, nil).Connect(ctx, &mcp.StreamableClientTransport{Endpoint: a.url + "/v1/datamcp/ro", HTTPClient: a.client}, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer session.Close()
	catalogue(t, ctx, session, companionspec.UpstreamPostgres)
	result, e := session.CallTool(ctx, &mcp.CallToolParams{Name: tool, Arguments: read})
	if e != nil || result.IsError {
		t.Fatalf("read tool: %v %+v", e, result)
	}
	result, e = session.CallTool(ctx, &mcp.CallToolParams{Name: tool, Arguments: write})
	if e == nil && !result.IsError {
		t.Fatalf("read level accepted write: %+v", result)
	}
	path := "/mcp"
	if strings.Contains(a.runner.Version(ctx), "pg") {
		path = "/mcp/v1"
	}
	a.runner.mu.RLock()
	for _, ep := range a.runner.state.Processes {
		path = ep.Path
	}
	a.runner.mu.RUnlock()
	req, _ := http.NewRequestWithContext(ctx, "POST", fmt.Sprintf("http://127.0.0.1:%d%s", p.Port, path), strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"tools/list"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	response, e := http.DefaultClient.Do(req)
	if e != nil {
		t.Fatal(e)
	}
	defer response.Body.Close()
	if response.StatusCode != 401 && response.StatusCode != 403 {
		t.Fatalf("unauthenticated upstream status %d", response.StatusCode)
	}
	// Adapter inputs and authentication material are mode 0600. Upstream-owned
	// SQLite files use vendor permissions, confined by the mode-0700 process root.
	base := filepath.Join(a.dir, "karakoram", "datamcp", "mcp-ro")
	info, err := os.Stat(base)
	if err != nil || info.Mode().Perm() != 0700 {
		t.Fatalf("unsafe process directory: %v %v", info, err)
	}
	for _, name := range []string{"config.yaml", "tokens.yaml", "local-secret"} {
		info, err := os.Stat(filepath.Join(base, name))
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm() != 0600 {
			t.Errorf("unsafe mode %s: %v", name, info.Mode())
		}
	}
}

func TestNativePostgresCompanion(t *testing.T) {
	bin := binary(t, "KARAKORAM_PG_BIN_DIR", "../../../.tmp-int/dl/pg/bin")
	upstream := binary(t, "KARAKORAM_PGEDGE_DIR", "../../../.tmp-int/dl/pgedge")
	if os.Geteuid() == 0 {
		t.Fatal("PostgreSQL integration requires an unprivileged user")
	}
	dir := t.TempDir()
	runCommand(t, filepath.Join(bin, "initdb"), "-D", dir, "-A", "trust", "-U", "postgres", "--no-locale")
	port := freePort(t)
	startProcess(t, filepath.Join(bin, "postgres"), "-D", dir, "-h", "127.0.0.1", "-p", strconv.Itoa(port), "-c", "shared_buffers=16MB", "-c", "max_connections=10", "-c", "unix_socket_directories="+dir)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	var conn *pgx.Conn
	for ctx.Err() == nil {
		var err error
		conn, err = pgx.Connect(ctx, fmt.Sprintf("postgres://postgres@127.0.0.1:%d/postgres?sslmode=disable", port))
		if err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if conn == nil {
		t.Fatal("PostgreSQL startup deadline exceeded")
	}
	defer conn.Close(context.Background())
	if _, err := conn.Exec(ctx, `CREATE USER reader PASSWORD 'integration'; ALTER ROLE reader SET default_transaction_read_only = on;`); err != nil {
		t.Fatal(err)
	}
	p := companionspec.Process{Level: "read", Port: freePort(t), Username: "reader", Password: "integration", Database: "postgres", Hosts: []string{fmt.Sprintf("127.0.0.1:%d", port)}}
	a := startAgent(t, companionspec.Spec{Upstream: companionspec.UpstreamPostgres, Version: "pg-integration", InstallDir: upstream, Processes: []companionspec.Process{p}})
	exercise(t, a, p, "query_database", map[string]any{"query": "SELECT 42 AS answer"}, map[string]any{"query": "CREATE TABLE forbidden(id integer)"})
}
func TestNativeMongoCompanion(t *testing.T) {
	mongod := binary(t, "MONGOD_BINARY", "../../../.tmp-int/dl/mongodb/bin/mongod")
	upstream := binary(t, "KARAKORAM_MONGODB_MCP_DIR", "../../../.tmp-int/dl/mongodb-mcp-server")
	port := freePort(t)
	startProcess(t, mongod, "--dbpath", t.TempDir(), "--bind_ip", "127.0.0.1", "--port", strconv.Itoa(port), "--wiredTigerCacheSizeGB", "0.25", "--setParameter", "diagnosticDataCollectionEnabled=false")
	p := companionspec.Process{Level: "read", Port: freePort(t), URI: fmt.Sprintf("mongodb://127.0.0.1:%d/test", port)}
	a := startAgent(t, companionspec.Spec{Upstream: companionspec.UpstreamMongoDB, Version: "3.1.0", InstallDir: upstream, Processes: []companionspec.Process{p}})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	session, e := mcp.NewClient(&mcp.Implementation{Name: "integration", Version: "1"}, nil).Connect(ctx, &mcp.StreamableClientTransport{Endpoint: a.url + "/v1/datamcp/ro", HTTPClient: a.client}, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer session.Close()
	tools := catalogue(t, ctx, session, companionspec.UpstreamMongoDB)
	for _, tool := range tools {
		if tool.Name == "insert-many" {
			t.Fatal("write tool exposed")
		}
	}
	result, e := session.CallTool(ctx, &mcp.CallToolParams{Name: "list-databases", Arguments: map[string]any{"connectionId": "preconfigured"}})
	if e != nil || result.IsError {
		t.Fatalf("read %v %+v", e, result)
	}
	result, e = session.CallTool(ctx, &mcp.CallToolParams{Name: "insert-many", Arguments: map[string]any{"connectionId": "preconfigured", "database": "test", "collection": "forbidden", "documents": []any{map[string]any{"x": 1}}}})
	if e == nil && !result.IsError {
		t.Fatal("write accepted")
	}
	request, _ := http.NewRequestWithContext(ctx, "POST", fmt.Sprintf("http://127.0.0.1:%d/mcp", p.Port), strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"tools/list"}`))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json, text/event-stream")
	response, e := http.DefaultClient.Do(request)
	if e != nil {
		t.Fatal(e)
	}
	response.Body.Close()
	if response.StatusCode != 401 && response.StatusCode != 403 {
		t.Fatalf("unauthenticated upstream status %d", response.StatusCode)
	}
	t.Logf("listed %d tools; read succeeded, write and unauthenticated request refused", len(tools))
}

func catalogue(t *testing.T, ctx context.Context, session *mcp.ClientSession, upstream string) []*mcp.Tool {
	t.Helper()
	manifest, ok := gateway.CompanionManifests()[upstream]
	if !ok {
		t.Fatalf("missing companion manifest for %s", upstream)
	}
	var tools []*mcp.Tool
	var names []string
	listed := make(map[string]bool)
	for tool, err := range session.Tools(ctx, nil) {
		if err != nil {
			t.Fatalf("list %s tools: %v", upstream, err)
		}
		tools = append(tools, tool)
		names = append(names, tool.Name)
		listed[tool.Name] = true
		if _, ok := manifest.Tools[tool.Name]; !ok {
			t.Errorf("%s lists unclassified tool %q", upstream, tool.Name)
		}
	}
	slices.Sort(names)
	t.Logf("%s catalogue (%d): %s", upstream, len(names), strings.Join(names, ", "))
	if len(names) == 0 {
		t.Fatal("empty upstream catalogue")
	}
	for name, rule := range manifest.Tools {
		if rule.Class == gateway.ClassRead && !listed[name] {
			t.Errorf("%s manifest read tool %q absent from upstream catalogue", upstream, name)
		}
	}
	return tools
}
