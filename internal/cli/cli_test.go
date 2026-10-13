// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/api"
	"github.com/spf13/cobra"
)

func TestEveryOperationHasCommand(t *testing.T) {
	seen := map[string]bool{}
	var walk func(*cobra.Command)
	walk = func(c *cobra.Command) {
		if c.RunE != nil {
			for _, id := range strings.Split(c.Annotations[operationAnnotation], ",") {
				if id != "" {
					seen[id] = true
					if _, ok := api.OperationRBAC[id]; !ok {
						t.Errorf("%s annotates unknown operation %s", c.CommandPath(), id)
					}
				}
			}
		}
		for _, child := range c.Commands() {
			walk(child)
		}
	}
	walk(NewCommand(Options{}))
	for id := range api.OperationRBAC {
		if !seen[id] {
			t.Errorf("operation %s has no command", id)
		}
	}
}
func TestConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "config.yaml")
	empty, err := LoadConfig(path)
	if err != nil || len(empty.Contexts) != 0 {
		t.Fatalf("missing config: %+v %v", empty, err)
	}
	c := Config{CurrentContext: "prod", Contexts: map[string]Context{"prod": {Server: "https://example.invalid", Token: "test-token", TokenID: "one"}}}
	if err = SaveConfig(path, c); err != nil {
		t.Fatal(err)
	}
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if st.Mode().Perm() != 0600 {
		t.Fatalf("permissions: %o", st.Mode().Perm())
	}
	loaded, err := LoadConfig(path)
	if err != nil || loaded.Contexts["prod"] != c.Contexts["prod"] {
		t.Fatalf("roundtrip: %+v %v", loaded, err)
	}
	if err = os.Chmod(path, 0644); err != nil {
		t.Fatal(err)
	}
	if err = SaveConfig(path, c); err != nil {
		t.Fatal(err)
	}
	st, _ = os.Stat(path)
	if st.Mode().Perm() != 0600 {
		t.Fatal("overwrite did not restore secure mode")
	}
	if err = os.WriteFile(path, []byte("contexts: [broken"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = LoadConfig(path); err == nil {
		t.Fatal("invalid YAML accepted")
	}
}
func TestScheduleAndRestoreParsing(t *testing.T) {
	s, err := parseSchedule("daily=0 1 * * *,type=physical,retain=4", "s3")
	if err != nil {
		t.Fatal(err)
	}
	if s["name"] != "daily" || s["retainCount"] != 4 || s["type"] != "physical" {
		t.Fatalf("%+v", s)
	}
	for _, input := range []string{"bad", "x=bad", "x=0 1 * * *,retain=0", "x=0 1 * * *,type=unknown", "x=0 1 * * *,other=1"} {
		if _, err = parseSchedule(input, "s3"); err == nil {
			t.Errorf("accepted %q", input)
		}
	}
	if _, err = parseSchedule("x=0 1 * * *", ""); err == nil {
		t.Fatal("missing storage accepted")
	}
	for _, pair := range [][2]string{{"", ""}, {"backup", "2026-01-01T00:00:00Z"}, {"", "bad"}} {
		if _, err = restoreSource("source", pair[0], pair[1]); err == nil {
			t.Fatalf("accepted source %v", pair)
		}
	}
	if _, err = restoreSource("source", "", "2026-01-01T00:00:00Z"); err != nil {
		t.Fatal(err)
	}
}

func execute(t *testing.T, server, config, input string, args ...string) (string, string, error) {
	t.Helper()
	var out, stderr bytes.Buffer
	cmd := NewCommand(Options{ConfigPath: config, In: strings.NewReader(input), Out: &out, Err: &stderr})
	cmd.SetArgs(append([]string{"--server", server}, args...))
	err := cmd.ExecuteContext(context.Background())
	return out.String(), stderr.String(), err
}
func TestCommandsAgainstServer(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/healthz":
			fmt.Fprint(w, `{"status":"ok","database":true}`)
		case "/metrics":
			w.Header().Set("Content-Type", "text/plain")
			fmt.Fprint(w, "karakoram_up 1\n")
		case "/api/v1/clusters":
			if r.Header.Get("Authorization") != "Bearer test-token" {
				t.Error("missing bearer token")
			}
			if r.URL.Query().Get("limit") != "2" {
				t.Error("missing limit")
			}
			fmt.Fprint(w, `{"items":[{"name":"demo","spec":{"version":"8.0"},"status":{"phase":"Ready"}}]}`)
		case "/api/v1/clusters/demo/members/m0":
			fmt.Fprint(w, `{"name":"m0","state":"SECONDARY"}`)
		default:
			t.Errorf("unexpected path %s", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	config := filepath.Join(t.TempDir(), "config.yaml")
	for _, tt := range []struct {
		args []string
		want string
	}{{[]string{"health", "-o", "json"}, `"database": true`}, {[]string{"metrics"}, "karakoram_up 1"}, {[]string{"--token", "test-token", "clusters", "list", "--limit", "2"}, "demo"}, {[]string{"clusters", "members", "demo", "m0", "-o", "yaml"}, "state: SECONDARY"}} {
		out, _, err := execute(t, server.URL+"/api/v1", config, "", tt.args...)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(out, tt.want) {
			t.Fatalf("output %q missing %q", out, tt.want)
		}
	}
}

func TestActivityActorOutputAndEventFilters(t *testing.T) {
	const actor = "admin (token: deploy [token-one])"
	const event = `{"id":12,"cluster":"demo","type":"Normal","actor":"admin (token: deploy [token-one])","resource":"clusters","target":"demo","message":"updated"}`
	const operation = `{"id":"operation-one","cluster":"demo","type":"restart","phase":"Succeeded","actor":"admin (token: deploy [token-one])"}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/v1/events", "/api/v1/events/stream":
			query := r.URL.Query()
			if query.Get("actor") != actor || query.Get("resource") != "clusters" || query.Get("cluster") != "demo" {
				t.Errorf("event filters not preserved: %v", query)
			}
			if r.URL.Path == "/api/v1/events/stream" {
				w.Header().Set("Content-Type", "text/event-stream")
				fmt.Fprintf(w, "event: event\nid: 12\ndata: %s\n\n", event)
			} else {
				fmt.Fprintf(w, `{"items":[%s]}`, event)
			}
		case "/api/v1/operations", "/api/v1/clusters/demo/operations":
			fmt.Fprintf(w, `{"items":[%s]}`, operation)
		case "/api/v1/operations/operation-one":
			fmt.Fprint(w, operation)
		default:
			t.Errorf("unexpected path %s", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	config := filepath.Join(t.TempDir(), "config.yaml")
	for _, tt := range []struct {
		name  string
		args  []string
		table bool
	}{
		{"events", []string{"events", "list", "--cluster", "demo", "--actor", actor, "--resource", "clusters"}, true},
		{"event watch", []string{"events", "watch", "--cluster", "demo", "--actor", actor, "--resource", "clusters"}, false},
		{"global operations", []string{"operations", "list"}, true},
		{"cluster operations", []string{"operations", "cluster", "demo"}, true},
		{"operation detail", []string{"operations", "get", "operation-one"}, false},
		{"operation wait", []string{"operations", "wait", "operation-one"}, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			out, _, err := execute(t, server.URL, config, "", tt.args...)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(out, actor) {
				t.Fatalf("actor missing from output %q", out)
			}
			if tt.table && !strings.Contains(out, "ACTOR") {
				t.Fatalf("actor column missing from output %q", out)
			}
		})
	}
}

func TestOrphanedVolumesList(t *testing.T) {
	const body = `{"items":[{"hostname":"prodm0v","serviceId":"service-one","cluster":"prod","member":"prodm0","attempts":7,"nextAttemptAt":"2024-01-07T10:32:00Z","lastProcessId":"process-one","firstError":"first mounted failure","lastError":"last mounted failure","firstFailedAt":"2024-01-07T10:00:00Z","orphanedAt":"2024-01-07T10:30:00Z","lastAttemptAt":"2024-01-07T10:30:00Z"}]}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/api/v1/orphaned-volumes" || r.URL.RawQuery != "" {
			t.Errorf("unexpected request %s %s", r.Method, r.URL)
		}
		if r.Header.Get("Authorization") != "Bearer test-token" {
			t.Error("missing bearer token")
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, body)
	}))
	defer server.Close()
	config := filepath.Join(t.TempDir(), "config.yaml")
	out, _, err := execute(t, server.URL, config, "", "--token", "test-token", "orphaned-volumes", "list")
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) != 2 {
		t.Fatalf("unexpected table %q", out)
	}
	const header = "HOSTNAME SERVICE ID CLUSTER MEMBER ATTEMPTS NEXT ATTEMPT LAST PROCESS"
	if got := strings.Join(strings.Fields(lines[0]), " "); got != header {
		t.Fatalf("header = %q; want %q", got, header)
	}
	const row = "prodm0v service-one prod prodm0 7 2024-01-07T10:32:00Z process-one"
	if got := strings.Join(strings.Fields(lines[1]), " "); got != row {
		t.Fatalf("row = %q; want %q", got, row)
	}
	out, _, err = execute(t, server.URL, config, "", "--token", "test-token", "orphaned-volumes", "list", "-o", "json")
	if err != nil {
		t.Fatal(err)
	}
	var got, want any
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(body), &want); err != nil {
		t.Fatal(err)
	}
	gotJSON, _ := json.Marshal(got)
	wantJSON, _ := json.Marshal(want)
	if !bytes.Equal(gotJSON, wantJSON) {
		t.Fatalf("JSON output = %s; want %s", gotJSON, wantJSON)
	}
}

func TestOrphanedVolumesListEmpty(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"items":[]}`)
	}))
	defer server.Close()
	config := filepath.Join(t.TempDir(), "config.yaml")
	out, _, err := execute(t, server.URL, config, "", "orphaned-volumes", "list")
	if err != nil || out != "No results.\n" {
		t.Fatalf("output = %q; err = %v", out, err)
	}
	out, _, err = execute(t, server.URL, config, "", "orphaned-volumes", "list", "-o", "json")
	if err != nil || strings.Join(strings.Fields(out), "") != `{"items":[]}` {
		t.Fatalf("JSON output = %q; err = %v", out, err)
	}
}
func TestCreateScaleAndClone(t *testing.T) {
	var bodies []map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == "GET" {
			fmt.Fprint(w, `{"name":"demo","spec":{"provider":"psmdb","version":"8.0","components":{},"topology":{"type":"replicaSet"}}}`)
			return
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		bodies = append(bodies, body)
		if r.Method == "PATCH" && r.Header.Get("Content-Type") != "application/merge-patch+json" {
			t.Error("patch content type")
		}
		fmt.Fprint(w, `{"name":"demo","generation":2}`)
	}))
	defer server.Close()
	config := filepath.Join(t.TempDir(), "config.yaml")
	cases := [][]string{{"clusters", "create", "demo", "--version", "8.0", "--topology", "sharded", "--shards", "2", "--backup-storage", "s3", "--schedule", "daily=0 1 * * *,type=physical,retain=4", "--pitr", "--min-ram", "1.5"}, {"clusters", "scale", "demo", "--members", "5", "--min-ram", "2"}, {"clusters", "upgrade", "demo", "--version", "8.1"}, {"restores", "create", "demo", "--backup", "base", "--into-new", "clone"}}
	for _, args := range cases {
		if _, _, err := execute(t, server.URL, config, "", args...); err != nil {
			t.Fatal(err)
		}
	}
	if nested(bodies[0], "spec", "topology", "shards") != float64(2) {
		t.Fatal(bodies[0])
	}
	if nested(bodies[0], "spec", "components", "engine", "resources", "minRam") != 1.5 {
		t.Fatal(bodies[0])
	}
	if nested(bodies[1], "spec", "components", "engine", "replicas") != float64(5) {
		t.Fatal(bodies[1])
	}
	if nested(bodies[1], "spec", "components", "engine", "resources", "maxRam") != nil {
		t.Fatal("scale patch included unrequested resource")
	}
	if nested(bodies[2], "spec", "version") != "8.1" {
		t.Fatal(bodies[2])
	}
	if nested(bodies[3], "spec", "dataSource", "backup") != "base" || bodies[3]["name"] != "clone" {
		t.Fatal(bodies[3])
	}
}
func TestLoginAndLogout(t *testing.T) {
	calls := []string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls = append(calls, r.Method+" "+r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/v1/auth/login":
			var body map[string]any
			json.NewDecoder(r.Body).Decode(&body)
			if body["password"] != "password-secret" {
				t.Error("password not sent")
			}
			http.SetCookie(w, &http.Cookie{Name: "karakoram_session", Value: "session", Path: "/"})
			fmt.Fprint(w, `{"csrfToken":"csrf","account":{"name":"admin"}}`)
		case "/api/v1/api-tokens":
			if r.Header.Get("X-Karakoram-CSRF") != "csrf" {
				t.Error("missing CSRF")
			}
			if _, err := r.Cookie("karakoram_session"); err != nil {
				t.Error("missing session")
			}
			fmt.Fprint(w, `{"secret":"kk_test-secret","token":{"id":"token-one"}}`)
		case "/api/v1/auth/logout":
			w.WriteHeader(204)
		case "/api/v1/api-tokens/token-one":
			if r.Method != "DELETE" {
				t.Error("not revoking token")
			}
			w.WriteHeader(204)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	config := filepath.Join(t.TempDir(), "config.yaml")
	out, stderr, err := execute(t, server.URL, config, "password-secret\n", "--context", "prod", "login")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out+stderr, "password-secret") || strings.Contains(out+stderr, "kk_test-secret") {
		t.Fatal("credentials printed")
	}
	c, err := LoadConfig(config)
	if err != nil || c.Contexts["prod"].Token != "kk_test-secret" {
		t.Fatalf("saved: %+v %v", c, err)
	}
	if _, _, err = execute(t, server.URL, config, "", "logout"); err != nil {
		t.Fatal(err)
	}
	c, err = LoadConfig(config)
	if err != nil || c.Contexts["prod"].Token != "" {
		t.Fatal("logout did not clear token")
	}
	if len(calls) != 5 {
		t.Fatalf("calls: %v", calls)
	}
}
func TestWaitAndSSE(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "events") {
			w.Header().Set("Content-Type", "text/event-stream")
			fmt.Fprint(w, ": keepalive\n\nevent: cluster.updated\nid: 12\ndata: {\"id\":12,\"message\":\"ready\"}\n\n")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"id":"one","phase":"Succeeded","cluster":"demo"}`)
	}))
	defer server.Close()
	config := filepath.Join(t.TempDir(), "config.yaml")
	for _, args := range [][]string{{"operations", "wait", "one", "--timeout", "1s"}, {"events", "watch", "--cursor", "11", "-o", "json"}} {
		out, _, err := execute(t, server.URL, config, "", args...)
		if err != nil {
			t.Fatal(err)
		}
		if out == "" {
			t.Fatal("empty output")
		}
	}
}
func TestInvalidArguments(t *testing.T) {
	config := filepath.Join(t.TempDir(), "config.yaml")
	for _, args := range [][]string{{"-o", "xml", "health"}, {"clusters", "get"}, {"clusters", "create", "demo"}, {"clusters", "create", "demo", "--version", "8.0", "--topology", "wrong"}, {"clusters", "scale", "demo"}, {"clusters", "connection", "demo"}, {"restores", "create", "demo", "--backup", "a", "--time", "2026-01-01T00:00:00Z"}} {
		if _, _, err := execute(t, "http://127.0.0.1:1", config, "", args...); err == nil {
			t.Fatalf("accepted %v", args)
		}
	}
}
func TestConfigurationPrecedence(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer flag-token" {
			t.Error("flag must win")
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"name":"admin"}`)
	}))
	defer server.Close()
	config := filepath.Join(t.TempDir(), "config.yaml")
	if err := SaveConfig(config, Config{CurrentContext: "prod", Contexts: map[string]Context{"prod": {Server: "http://127.0.0.1:1", Token: "config-token"}}}); err != nil {
		t.Fatal(err)
	}
	t.Setenv("KARAKORAM_SERVER", "http://127.0.0.1:2")
	t.Setenv("KARAKORAM_TOKEN", "env-token")
	if _, _, err := execute(t, server.URL, config, "", "--token", "flag-token", "whoami"); err != nil {
		t.Fatal(err)
	}
}
func TestPollingCancellation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"phase":"Running"}`)
	}))
	defer server.Close()
	config := filepath.Join(t.TempDir(), "config.yaml")
	started := time.Now()
	_, _, err := execute(t, server.URL, config, "", "operations", "wait", "one", "--timeout", "10ms")
	if err == nil || !strings.Contains(err.Error(), "deadline") {
		t.Fatalf("wait error: %v", err)
	}
	if time.Since(started) > time.Second {
		t.Fatal("cancellation too slow")
	}
}

func TestClusterWaitEditorAndFiles(t *testing.T) {
	var replaced map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == "PUT" {
			if err := json.NewDecoder(r.Body).Decode(&replaced); err != nil {
				t.Error(err)
			}
		}
		fmt.Fprint(w, `{"name":"demo","generation":2,"spec":{"provider":"psmdb","version":"8.0","topology":{"type":"replicaSet"},"components":{}},"status":{"phase":"Ready","observedGeneration":2}}`)
	}))
	defer server.Close()
	config := filepath.Join(t.TempDir(), "config.yaml")
	if _, _, err := execute(t, server.URL, config, "", "clusters", "create", "demo", "--version", "8.0", "--wait", "--timeout", "1s"); err != nil {
		t.Fatal(err)
	}
	t.Setenv("EDITOR", "true")
	if _, _, err := execute(t, server.URL, config, "", "clusters", "edit", "demo"); err != nil {
		t.Fatal(err)
	}
	if len(replaced) != 1 || nested(replaced, "spec", "version") != "8.0" {
		t.Fatalf("editor submitted non-spec fields: %+v", replaced)
	}
	if _, _, err := execute(t, server.URL, config, "spec:\n  version: '8.1'\n", "clusters", "patch", "demo", "-f", "-"); err != nil {
		t.Fatal(err)
	}
}

func TestHTTPErrorAndInsecureWarning(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		fmt.Fprint(w, `{"code":"forbidden","message":"permission denied"}`)
	}))
	defer server.Close()
	config := filepath.Join(t.TempDir(), "config.yaml")
	_, stderr, err := execute(t, server.URL, config, "", "--insecure-skip-tls-verify", "health")
	if err == nil || !strings.Contains(err.Error(), "permission denied") {
		t.Fatalf("API error: %v", err)
	}
	if !strings.Contains(stderr, "WARNING") {
		t.Fatal("missing TLS warning")
	}
}
