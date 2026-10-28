// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
)

func runCLI(t *testing.T, server, input string, tty bool, args ...string) (int, string, string) {
	t.Helper()
	var out, stderr bytes.Buffer
	options := Options{ConfigPath: filepath.Join(t.TempDir(), "config.yaml"), In: strings.NewReader(input), Out: &out, Err: &stderr, IsTerminal: func(io.Reader) bool { return tty }}
	if server != "" {
		args = append([]string{"--server", server}, args...)
	}
	code := Run(context.Background(), options, args)
	return code, out.String(), stderr.String()
}

func TestSystemUserRotationHelpStatesPostgresBoundary(t *testing.T) {
	code, out, stderr := runCLI(t, "", "", false, "system-users", "rotate", "--help")
	if code != 0 || stderr != "" {
		t.Fatalf("rotation help failed: code=%d stderr=%q", code, stderr)
	}
	for _, detail := range []string{"provider-supported identities", "enabled Data MCP users", "not postgres"} {
		if !strings.Contains(out, detail) {
			t.Errorf("rotation help omitted %q: %s", detail, out)
		}
	}
}

func TestUsageExitCodesAndOfflineCompletion(t *testing.T) {
	t.Setenv("KARAKORAM_SERVER", "")
	t.Setenv("KARAKORAM_TOKEN", "")
	for _, args := range [][]string{{"auth", "login"}, {"auth"}, {"tokens", "revoke"}, {"tokens", "unknown"}, {"clusters", "get"}, {"config"}, {"completion"}, {"completion", "unknown"}, {"unknown"}, {"clusters", "list", "--unknown"}} {
		code, out, stderr := runCLI(t, "", "", false, args...)
		if code != 2 || out != "" || !strings.Contains(stderr, "Usage:") || !strings.Contains(stderr, "karactl:") {
			t.Errorf("%v: code=%d stdout=%q stderr=%q", args, code, out, stderr)
		}
	}
	for _, shell := range []string{"bash", "zsh", "fish", "powershell"} {
		var out, stderr bytes.Buffer
		code := Run(context.Background(), Options{ConfigPath: "/nonexistent/invalid-config", Out: &out, Err: &stderr}, []string{"--context", "unconfigured", "completion", shell})
		if code != 0 || out.Len() == 0 || stderr.Len() != 0 {
			t.Errorf("%s: code=%d out=%q stderr=%q", shell, code, out.String(), stderr.String())
		}
	}
}

func TestFlagValidationExitCodes(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/api/v1/clusters/demo" {
			t.Errorf("invalid flags submitted a request: %s %s", r.Method, r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"name":"demo","spec":{}}`)
	}))
	defer server.Close()
	cases := [][]string{
		{"health", "--output", "bogus"},
		{"health", "--server", "not-a-url"},
		{"health", "--context", "missing"},
		{"health", "--cookie", "invalid"},
		{"clusters", "create"},
		{"clusters", "scale", "demo", "--component", "invalid"},
		{"clusters", "scale", "demo"},
		{"clusters", "upgrade", "demo"},
		{"clusters", "raise-fcv", "demo"},
		{"backups", "create", "demo", "--type", "invalid"},
		{"backups", "create", "demo"},
		{"restores", "create", "demo"},
		{"restores", "create", "demo", "--backup", "base", "--time", "2026-01-01T00:00:00Z"},
		{"restores", "create", "demo", "--time", "invalid"},
		{"rbac", "can"},
		{"storages", "create"},
		{"namespaces", "create"},
		{"namespaces", "create", "--token-file", "-"},
		{"namespaces", "rotate-token", "demo"},
		{"namespaces", "update", "demo"},
		{"events", "list", "--order", "invalid"},
	}
	for _, flags := range [][]string{
		{"--topology", "invalid"},
		{"--cpu-mode", "invalid"},
		{"--members", "0"},
		{"--tls-mode", "invalid"},
		{"--from-backup", "base"},
		{"--source-cluster", "source", "--from-pitr", "invalid"},
		{"--schedule", "invalid"},
		{"--schedule", "daily=invalid"},
		{"--schedule", "daily=0 1 * * *,invalid"},
		{"--schedule", "daily=0 1 * * *,type=invalid"},
		{"--schedule", "daily=0 1 * * *,retain=0"},
		{"--schedule", "daily=0 1 * * *,other=value"},
		{"--schedule", "daily=0 1 * * *"},
	} {
		cases = append(cases, append([]string{"clusters", "create", "demo", "--version", "8.0"}, flags...))
	}
	for _, shards := range []string{"0", "129"} {
		cases = append(cases,
			[]string{"clusters", "create", "demo", "--version", "8.0", "--topology", "sharded", "--shards", shards},
			[]string{"clusters", "scale", "demo", "--shards", shards},
		)
	}
	for _, args := range cases {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			code, out, stderr := runCLI(t, server.URL, "test-token\n", false, args...)
			if code != 2 || out != "" || !strings.Contains(stderr, "Usage:") {
				t.Fatalf("code=%d stdout=%q stderr=%q", code, out, stderr)
			}
		})
	}
}

func TestTableNumbersUseDecimalNotation(t *testing.T) {
	for _, tt := range []struct {
		value any
		want  string
	}{
		{float64(1234567890), "1234567890"},
		{float64(0.00000001), "0.00000001"},
		{float64(-1234567890.25), "-1234567890.25"},
		{float64(0), "0"},
		{map[string]any{"bytes": float64(1234567890)}, "bytes=1234567890"},
		{[]any{float64(1234567890), float64(0.00000001)}, "1234567890, 0.00000001"},
	} {
		if got := cell(tt.value); got != tt.want {
			t.Errorf("cell(%v)=%q want=%q", tt.value, got, tt.want)
		}
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"items":[{"id":1234567890,"size":1234567890.25,"fraction":0.00000001}]}`)
	}))
	defer server.Close()
	code, out, stderr := runCLI(t, server.URL, "", false, "events", "list")
	if code != 0 {
		t.Fatalf("code=%d stderr=%q", code, stderr)
	}
	for _, want := range []string{"1234567890", "1234567890.25", "0.00000001"} {
		if !strings.Contains(out, want) {
			t.Errorf("table output %q missing %q", out, want)
		}
	}
	if strings.Contains(out, "e+") || strings.Contains(out, "e-") {
		t.Errorf("scientific notation in table output: %q", out)
	}
}

func TestServerErrorExitCodesAndDetails(t *testing.T) {
	for status, want := range map[int]int{400: 6, 401: 5, 403: 5, 404: 3, 409: 4, 422: 6, 500: 1, 504: 7} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(status)
				fmt.Fprint(w, `{"code":"invalid_request","message":"spec.resources is invalid","details":{"field":"spec.resources","reason":"out_of_range","resource":"cluster"}}`)
			}))
			defer server.Close()
			code, out, stderr := runCLI(t, server.URL, "", false, "health")
			if code != want || out != "" {
				t.Fatalf("code=%d stdout=%q stderr=%q", code, out, stderr)
			}
			for _, text := range []string{"spec.resources is invalid", "field: spec.resources", "reason: out_of_range", "resource: cluster"} {
				if !strings.Contains(stderr, text) {
					t.Errorf("stderr %q missing %q", stderr, text)
				}
			}
		})
	}
	code, _, _ := runCLI(t, "http://127.0.0.1:1", "", false, "health")
	if code != 1 {
		t.Fatalf("network failure code: %d", code)
	}
}

func TestTokenRevokeAliasAndConfirmation(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "DELETE" || r.URL.Path != "/api/v1/api-tokens/token-one" {
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		calls++
		w.WriteHeader(204)
	}))
	defer server.Close()
	for _, tt := range []struct {
		command, input     string
		tty, yes, accepted bool
	}{
		{"delete", "", false, false, false},
		{"delete", "\n", false, false, false},
		{"delete", "n\n", false, false, false},
		{"delete", "no\n", false, false, false},
		{"delete", "wrong\n", false, false, false},
		{"delete", "y\n", false, false, true},
		{"delete", "YES\n", false, false, true},
		{"delete", "YeS", false, false, true},
		{"delete", "", false, true, true},
		{"revoke", "token-one\n", true, false, true},
		{"revoke", "wrong\n", true, false, false},
		{"revoke", "", true, true, true},
		{"delete", "", true, false, false},
	} {
		before := calls
		args := []string{"tokens", tt.command, "token-one"}
		if tt.yes {
			args = append(args, "-y")
		}
		code, _, stderr := runCLI(t, server.URL, tt.input, tt.tty, args...)
		if (code == 0) != tt.accepted || (calls > before) != tt.accepted {
			t.Errorf("%+v: code=%d calls=%d stderr=%q", tt, code, calls-before, stderr)
		}
		if !tt.tty && !tt.yes && strings.TrimSpace(tt.input) == "" {
			if code != 2 || !strings.Contains(stderr, "pass --yes for non-interactive use") {
				t.Errorf("missing confirmation: code=%d stderr=%q", code, stderr)
			}
		} else if !tt.accepted && code != 1 {
			t.Errorf("declined confirmation: code=%d stderr=%q", code, stderr)
		}
		prompted := strings.Contains(stderr, "Type the name to confirm")
		if prompted != (tt.tty && !tt.yes) {
			t.Errorf("%+v: prompt=%v stderr=%q", tt, prompted, stderr)
		}
	}
}

func TestDestructiveCommandConfirmationSurface(t *testing.T) {
	root := NewCommand(Options{})
	for _, args := range [][]string{{"clusters", "delete"}, {"backups", "delete"}, {"storages", "delete"}, {"tokens", "delete"}, {"accounts", "delete"}, {"namespaces", "delete"}, {"db-users", "delete"}, {"db-roles", "delete"}, {"restores", "create"}, {"clusters", "replace-member"}, {"clusters", "raise-fcv"}, {"system-users", "rotate"}, {"accounts", "reset-password"}} {
		cmd, _, err := root.Find(args)
		if err != nil || cmd.Flags().Lookup("yes") == nil || cmd.Flags().Lookup("yes").Shorthand != "y" {
			t.Errorf("%v missing --yes/-y: %v", args, err)
		}
	}
}

func TestStorageVerificationFailureExit(t *testing.T) {
	for _, verified := range []bool{false, true} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, `{"verified":%t,"message":"Check bucket access"}`, verified)
		}))
		code, out, stderr := runCLI(t, server.URL, "", false, "storages", "verify", "store", "-o", "json")
		server.Close()
		if (code == 0) != verified || !strings.Contains(out, "Check bucket access") {
			t.Errorf("verified=%v code=%d out=%q stderr=%q", verified, code, out, stderr)
		}
		if !verified && !strings.Contains(stderr, "Check bucket access") {
			t.Error("verification failure message absent from stderr")
		}
	}
}

func TestConnectionPEMAndPasswordFreeSummary(t *testing.T) {
	const pem = "-----BEGIN CERTIFICATE-----\nYWJj\n-----END CERTIFICATE-----\n"
	const uri = "mongodb://app:secret@node:27017/?tls=true&authSource=app"
	sensitiveRequests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.HasSuffix(r.URL.Path, "/connection") {
			sensitiveRequests++
			json.NewEncoder(w).Encode(map[string]any{"caCertificate": pem, "connection": map[string]any{"hosts": []string{"node:27017"}}, "credentials": []any{map[string]any{"username": "app", "password": "secret", "uri": uri}}})
		} else {
			fmt.Fprint(w, `{"name":"demo","spec":{"tls":{"mode":"requireTLS"}},"status":{"connection":{"hosts":["node:27017"],"publicHosts":[]}}}`)
		}
	}))
	defer server.Close()
	code, out, stderr := runCLI(t, server.URL, "", false, "clusters", "connection", "demo", "--reveal")
	if code != 0 || !strings.Contains(out, pem) || !strings.Contains(out, uri) {
		t.Fatalf("revealed output code=%d out=%q stderr=%q", code, out, stderr)
	}
	code, out, stderr = runCLI(t, server.URL, "", false, "-n", "project", "clusters", "ca", "demo", "-o", "json")
	if code != 0 || out != pem || stderr != "" {
		t.Fatalf("CA output code=%d out=%q stderr=%q", code, out, stderr)
	}
	before := sensitiveRequests
	code, out, stderr = runCLI(t, server.URL, "", false, "clusters", "connection", "demo")
	if code != 0 || !strings.Contains(out, "node:27017") || !strings.Contains(out, "requireTLS") || !strings.Contains(out, "caAvailability") || !strings.Contains(stderr, "--reveal") || strings.Contains(out, "secret") || sensitiveRequests != before {
		t.Fatalf("summary code=%d out=%q stderr=%q", code, out, stderr)
	}
}

func TestEventsOrderAndUserHelp(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("order"); got != "desc" && got != "asc" {
			t.Errorf("order=%q", got)
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"items":[]}`)
	}))
	defer server.Close()
	for _, order := range []string{"asc", "desc", "sideways"} {
		code, _, _ := runCLI(t, server.URL, "", false, "events", "list", "--order", order)
		if order == "sideways" && code != 2 || order != "sideways" && code != 0 {
			t.Errorf("%s code=%d", order, code)
		}
	}
	code, out, stderr := runCLI(t, "", "", false, "clusters", "create", "--help")
	if code != 0 || strings.Contains(out, "createCluster") || !strings.Contains(out, "CPU cores") || !strings.Contains(out, "presets") || !strings.Contains(out, "providers get <provider> -o json") || !strings.Contains(out, "psmdb") {
		t.Fatalf("help code=%d out=%q stderr=%q", code, out, stderr)
	}
	code, out, _ = runCLI(t, "", "", false, "login", "--help")
	if code != 0 || !strings.Contains(out, "non-terminal stdin") {
		t.Fatalf("login help=%q", out)
	}
}

func TestReloginRevokesOnlyAfterSaving(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprint(fail), func(t *testing.T) {
			config := filepath.Join(t.TempDir(), "config.yaml")
			revoked := false
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch r.URL.Path {
				case "/api/v1/auth/login":
					if r.Header.Get("Authorization") != "" {
						t.Error("old bearer overrides login")
					}
					fmt.Fprint(w, `{"csrfToken":"csrf"}`)
				case "/api/v1/api-tokens":
					fmt.Fprint(w, `{"secret":"new-secret","token":{"id":"new-id"}}`)
				case "/api/v1/api-tokens/old-id":
					c, err := LoadConfig(config)
					if err != nil || c.Contexts["prod"].Token != "new-secret" {
						t.Error("revoked before saving replacement")
					}
					if r.Method != "DELETE" || r.Header.Get("Authorization") != "Bearer old-secret" {
						t.Error("wrong revocation credentials")
					}
					revoked = true
					if fail {
						w.WriteHeader(403)
						fmt.Fprint(w, `{"code":"forbidden","message":"Permission denied"}`)
					} else {
						w.WriteHeader(204)
					}
				case "/api/v1/auth/logout":
					w.WriteHeader(204)
				default:
					t.Errorf("unexpected path %s", r.URL.Path)
					http.NotFound(w, r)
				}
			}))
			defer server.Close()
			if err := SaveConfig(config, Config{CurrentContext: "prod", Contexts: map[string]Context{"prod": {Server: server.URL, Token: "old-secret", TokenID: "old-id"}}}); err != nil {
				t.Fatal(err)
			}
			var out, stderr bytes.Buffer
			code := Run(context.Background(), Options{ConfigPath: config, In: strings.NewReader("password\n"), Out: &out, Err: &stderr}, []string{"login"})
			if code != 0 || !revoked || strings.Contains(out.String()+stderr.String(), "new-secret") || strings.Contains(out.String()+stderr.String(), "old-secret") {
				t.Fatalf("code=%d revoked=%v out=%q stderr=%q", code, revoked, out.String(), stderr.String())
			}
			if strings.Contains(stderr.String(), "WARNING") != fail {
				t.Fatalf("warning=%q fail=%v", stderr.String(), fail)
			}
		})
	}
}

func TestHumanNestedTablesAndStructuredOutput(t *testing.T) {
	responses := map[string]string{
		"/api/v1/providers/psmdb":       `{"name":"psmdb","versions":[{"version":"8.0"}],"presets":[{"name":"dev","spec":{"components":{"engine":{"replicas":1}}}}]}`,
		"/api/v1/clusters/demo/status":  `{"phase":"Ready","conditions":[{"type":"Ready","status":"True","message":"Available"}],"members":[{"name":"m0","resources":{"cpuCores":2,"memoryGB":4,"diskGB":10}}]}`,
		"/api/v1/clusters/demo/members": `{"items":[{"name":"m0","resources":{"cpuCores":2,"memoryGB":4,"diskGB":10},"autoscaling":{"minCpu":1,"maxCpu":2}}]}`,
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, responses[r.URL.Path])
	}))
	defer server.Close()
	for _, args := range [][]string{{"providers", "get", "psmdb"}, {"clusters", "status", "demo"}, {"clusters", "members", "demo"}} {
		code, out, stderr := runCLI(t, server.URL, "", false, args...)
		if code != 0 || strings.ContainsAny(out, "{}[]") {
			t.Errorf("%v code=%d out=%q stderr=%q", args, code, out, stderr)
		}
		if args[1] == "status" && (!strings.Contains(out, "conditions:") || !strings.Contains(out, "Available")) {
			t.Errorf("condition rows absent: %q", out)
		}
		code, out, _ = runCLI(t, server.URL, "", false, append(args, "-o", "json")...)
		if code != 0 || !json.Valid([]byte(out)) {
			t.Errorf("JSON changed: %q", out)
		}
		if args[0] == "providers" && !strings.Contains(out, "replicas") {
			t.Error("JSON lost preset spec")
		}
	}
}

func TestWaitAcceptanceProgressTimeoutAndResume(t *testing.T) {
	for _, operation := range []bool{false, true} {
		t.Run(fmt.Sprint(operation), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if r.Method == "POST" {
					if operation {
						w.Header().Set("Karakoram-Operation-Id", "op-one")
					}
					fmt.Fprint(w, `{"name":"demo","generation":2,"status":{"phase":"Creating"}}`)
				} else if strings.Contains(r.URL.Path, "operations") {
					fmt.Fprint(w, `{"id":"op-one","phase":"Running","message":"Provisioning resources"}`)
				} else {
					fmt.Fprint(w, `{"name":"demo","generation":2,"status":{"phase":"Creating","observedGeneration":1,"message":"Provisioning resources"}}`)
				}
			}))
			defer server.Close()
			code, _, stderr := runCLI(t, server.URL, "", false, "clusters", "create", "demo", "--version", "8.0", "--wait", "--timeout", "20ms")
			if code != 7 || !strings.Contains(stderr, "Accepted creation") || !strings.Contains(stderr, "Provisioning resources") || !strings.Contains(stderr, "work continues server-side") {
				t.Fatalf("code=%d stderr=%q", code, stderr)
			}
			want := `clusters wait "demo"`
			if operation {
				want = `operations wait "op-one"`
			}
			if !strings.Contains(stderr, want) {
				t.Fatalf("resume missing %q: %q", want, stderr)
			}
			code, _, stderr = runCLI(t, server.URL, "", false, "clusters", "wait", "demo", "--timeout", "20ms")
			if code != 7 || !strings.Contains(stderr, "generation 1/2") {
				t.Fatalf("resume code=%d stderr=%q", code, stderr)
			}
		})
	}
}

func TestReplacementFailureMessageAndRepeatedDelete(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "replace"):
			fmt.Fprint(w, `{"id":"op-one","phase":"Pending"}`)
		case strings.Contains(r.URL.Path, "operations"):
			fmt.Fprint(w, `{"id":"op-one","phase":"Failed","message":"Single-member clusters cannot replace members; use restore"}`)
		case r.Method == "DELETE":
			w.Header().Set("Karakoram-Operation-Id", "delete-existing")
			w.WriteHeader(202)
			fmt.Fprint(w, `{"id":"delete-existing","type":"delete","phase":"Running","message":"Deleting resources"}`)
		default:
			fmt.Fprint(w, `{"name":"demo","status":{"phase":"Deleting"}}`)
		}
	}))
	defer server.Close()
	code, _, stderr := runCLI(t, server.URL, "", false, "clusters", "replace-member", "demo", "m0", "--yes", "--wait", "--timeout", "100ms")
	if code != 1 || !strings.Contains(stderr, "Single-member clusters cannot replace members") {
		t.Fatalf("failure code=%d stderr=%q", code, stderr)
	}
	code, out, stderr := runCLI(t, server.URL, "", false, "clusters", "delete", "demo", "--yes")
	if code != 0 || !strings.Contains(stderr, "already being deleted") || !strings.Contains(out, "delete-existing") || !strings.Contains(out, "Running") {
		t.Fatalf("delete code=%d stdout=%q stderr=%q", code, out, stderr)
	}
}

func TestRepeatedDeleteWaitsForReturnedOperation(t *testing.T) {
	waited := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/api/v1/operations/delete-existing":
			waited = true
			fmt.Fprint(w, `{"id":"delete-existing","type":"delete","phase":"Succeeded","message":"Deletion complete"}`)
		case r.Method == "DELETE":
			w.Header().Set("Karakoram-Operation-Id", "delete-existing")
			w.WriteHeader(202)
			fmt.Fprint(w, `{"id":"delete-existing","type":"delete","phase":"Running","message":"Deleting resources"}`)
		case waited:
			w.WriteHeader(404)
			fmt.Fprint(w, `{"code":"not_found","message":"Cluster deleted"}`)
		default:
			fmt.Fprint(w, `{"name":"demo","status":{"phase":"Deleting"}}`)
		}
	}))
	defer server.Close()
	code, out, stderr := runCLI(t, server.URL, "", false, "clusters", "delete", "demo", "--yes", "--wait", "--timeout", "1s")
	if code != 0 || !waited || !strings.Contains(out, "deleted") {
		t.Fatalf("code=%d waited=%t stdout=%q stderr=%q", code, waited, out, stderr)
	}
	for _, message := range []string{"already being deleted", "operation delete-existing", "Deletion complete"} {
		if !strings.Contains(stderr, message) {
			t.Errorf("stderr %q missing %q", stderr, message)
		}
	}
}

func TestEveryDestructiveCommandUsesTerminalConfirmation(t *testing.T) {
	root := NewCommand(Options{})
	for _, spec := range commandSpecs {
		if !destructive(spec.operation) {
			continue
		}
		words := strings.Fields(spec.path)
		end := len(words)
		for i, word := range words {
			if strings.HasPrefix(word, "<") || strings.HasPrefix(word, "[") {
				end = i
				break
			}
		}
		cmd, _, err := root.Find(words[:end])
		if err != nil || cmd.Flags().Lookup("yes") == nil {
			t.Fatalf("%s: missing confirmation flag", spec.path)
		}
		args := make([]string, spec.args)
		for i := range args {
			args[i] = fmt.Sprintf("resource-%d", i)
		}
		name := args[len(args)-1]
		if spec.operation == "createRestore" {
			name = args[0]
		}
		var stderr bytes.Buffer
		a := &app{opts: Options{In: strings.NewReader(name + "\n"), Err: &stderr, IsTerminal: func(io.Reader) bool { return true }}}
		if err := a.confirm(cmd, spec.operation, args); err != nil || !strings.Contains(stderr.String(), "Type the name to confirm") {
			t.Errorf("%s: err=%v stderr=%q", spec.path, err, stderr.String())
		}
		if spec.operation == "createRestore" {
			cmd.Flags().Set("into-new", "clone")
			a.opts.In = strings.NewReader("")
			stderr.Reset()
			if err := a.confirm(cmd, spec.operation, args); err != nil || stderr.Len() != 0 {
				t.Errorf("clone prompts: %v %q", err, stderr.String())
			}
		}
	}
}

func TestOperationTerminalMessages(t *testing.T) {
	for _, phase := range []string{"Failed", "Cancelled", "Canceled"} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, `{"phase":%q,"message":"","error":"Resource replacement unavailable"}`, phase)
		}))
		code, _, stderr := runCLI(t, server.URL, "", false, "operations", "wait", "op", "--timeout", "1s")
		server.Close()
		if code != 1 || !strings.Contains(stderr, phase) || !strings.Contains(stderr, "Resource replacement unavailable") {
			t.Errorf("%s code=%d stderr=%q", phase, code, stderr)
		}
	}
}

func TestOperationProgressChangesAndDeleteHeaderTracking(t *testing.T) {
	stage := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.Contains(r.URL.Path, "operations") {
			stage++
			if stage == 1 {
				fmt.Fprint(w, `{"id":"op","phase":"Running","message":"Removing runtime"}`)
			} else {
				fmt.Fprint(w, `{"id":"op","phase":"Succeeded","message":"Deletion complete"}`)
			}
		} else if r.Method == "DELETE" {
			w.Header().Set("Karakoram-Operation-Id", "op")
			w.WriteHeader(202)
		} else {
			w.WriteHeader(404)
			fmt.Fprint(w, `{"code":"not_found","message":"Cluster deleted"}`)
		}
	}))
	defer server.Close()
	code, out, stderr := runCLI(t, server.URL, "", false, "clusters", "delete", "demo", "--yes", "--wait", "--timeout", "3s")
	if code != 0 || !strings.Contains(out, "deleted") {
		t.Fatalf("code=%d out=%q stderr=%q", code, out, stderr)
	}
	for _, text := range []string{"Accepted deletion", "operation op", "Removing runtime", "Deletion complete"} {
		if !strings.Contains(stderr, text) {
			t.Errorf("stderr %q missing %q", stderr, text)
		}
	}
}

func TestCARequiresConnectionPermissionAndNamespace(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/connection") || r.URL.Query().Get("namespace") != "project" {
			t.Errorf("request=%s", r.URL)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(403)
		fmt.Fprint(w, `{"code":"forbidden","message":"cluster-credentials read permission required"}`)
	}))
	defer server.Close()
	code, out, stderr := runCLI(t, server.URL, "", false, "-n", "project", "clusters", "ca", "demo")
	if code != 5 || out != "" || !strings.Contains(stderr, "cluster-credentials read permission required") {
		t.Fatalf("code=%d out=%q stderr=%q", code, out, stderr)
	}
}
