// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package cli

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestClusterCreateFileRejectsConvenienceFlags(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		t.Errorf("conflicting file/flags reached API: %s %s", r.Method, r.URL)
	}))
	defer server.Close()
	file := filepath.Join(t.TempDir(), "cluster.json")
	if err := os.WriteFile(file, []byte(`{"name":"target","spec":{"provider":"psmdb"}}`), 0600); err != nil {
		t.Fatal(err)
	}
	cases := [][]string{
		{"--from-backup", "base"}, {"--from-pitr", "2026-10-21T20:00:00Z"},
		{"--source-cluster", "source"}, {"--source-namespace", "archive"},
		{"--from-backup", "base", "--source-cluster", "source", "--source-namespace", "archive"},
		{"--provider", "psmdb"}, {"--version", "8.0"}, {"--topology", "replicaSet"},
		{"--members", "3"}, {"--replicas", "3"}, {"--shards", "1"}, {"--config-servers", "3"}, {"--routers", "2"},
		{"--component", "engine"}, {"--cpu-mode", "SHARED"},
		{"--min-cpu", "1"}, {"--max-cpu", "2"}, {"--min-ram", "1"}, {"--max-ram", "2"},
		{"--min-disk", "5"}, {"--max-disk", "10"}, {"--start-cpu", "1"},
		{"--backup-storage", "archive"}, {"--schedule", "daily=0 1 * * *,type=physical,retain=4"},
		{"--pitr=false"}, {"--oplog-span-minutes", "10"}, {"--tls-mode", "requireTLS"},
		{"--public=false"}, {"--ip-type", "IPV6"}, {"--allow", "192.0.2.0/24"},
		{"--engine-config", "not-read.conf"}, {"--engine-options", "{}"}, {"--monitoring=true"},
	}
	for _, flags := range cases {
		t.Run(strings.Join(flags, " "), func(t *testing.T) {
			args := append([]string{"clusters", "create", "--file", file}, flags...)
			code, out, stderr := runCLI(t, server.URL, "", false, args...)
			flag := strings.SplitN(flags[0], "=", 2)[0]
			if code != 2 || out != "" || !strings.Contains(stderr, "--file cannot be combined with") || !strings.Contains(stderr, flag) || !strings.Contains(stderr, "spec.dataSource") {
				t.Fatalf("code=%d stdout=%q stderr=%q", code, out, stderr)
			}
		})
	}
	// Short file alias and stdin have identical conflict semantics; the invalid
	// input must not be read before rejecting the dropped source flag.
	code, _, stderr := runCLI(t, server.URL, "not JSON", false, "clusters", "create", "-f", "-", "--from-backup", "base")
	if code != 2 || !strings.Contains(stderr, "--file cannot be combined with --from-backup") {
		t.Fatalf("code=%d stderr=%q", code, stderr)
	}
	if requests != 0 {
		t.Fatalf("sent %d requests for conflicting flags", requests)
	}
}

func TestBackupAndRestoreCreateFileRejectsSourceOverrides(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		t.Errorf("conflicting file/flags reached API: %s %s", r.Method, r.URL)
	}))
	defer server.Close()
	for _, flags := range [][]string{
		{"backups", "create", "target", "--type", "physical"},
		{"backups", "create", "target", "--storage", "archive"},
		{"restores", "create", "target", "--backup", "base", "--yes"},
		{"restores", "create", "target", "--time", "2026-10-21T20:00:00Z", "--yes"},
		{"restores", "create", "target", "--source-cluster", "source", "--into-new", "copy"},
		{"restores", "create", "target", "--source-namespace", "archive", "--into-new", "copy"},
	} {
		t.Run(strings.Join(flags, " "), func(t *testing.T) {
			args := append(append([]string{}, flags...), "-f", "-")
			code, out, stderr := runCLI(t, server.URL, "not JSON", false, args...)
			if code != 2 || out != "" || !strings.Contains(stderr, "--file cannot be combined with "+flags[3]) {
				t.Fatalf("code=%d stdout=%q stderr=%q", code, out, stderr)
			}
		})
	}
	if requests != 0 {
		t.Fatalf("sent %d requests for conflicting flags", requests)
	}
}

func TestClusterCreateFilePreservesSourceAndWaitControls(t *testing.T) {
	for _, source := range []string{`{"cluster":"source","namespace":"archive","backup":"base"}`, `{"cluster":"source","namespace":"archive","pitrTime":"2026-10-21T20:00:00Z"}`} {
		t.Run(source, func(t *testing.T) {
			posts := 0
			var got map[string]any
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch r.Method + " " + r.URL.Path {
				case "POST /api/v1/clusters":
					posts++
					if r.URL.Query().Get("namespace") != "targetns" {
						t.Error("lost target namespace")
					}
					if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
						t.Error(err)
					}
					w.Header().Set("Karakoram-Operation-Id", "create-one")
					w.WriteHeader(http.StatusAccepted)
					fmt.Fprint(w, `{"name":"target","generation":1}`)
				case "GET /api/v1/operations/create-one":
					fmt.Fprint(w, `{"id":"create-one","phase":"Succeeded"}`)
				case "GET /api/v1/clusters/target":
					fmt.Fprint(w, `{"name":"target","generation":1,"status":{"phase":"Ready","observedGeneration":1}}`)
				default:
					t.Errorf("unexpected request %s %s", r.Method, r.URL)
				}
			}))
			defer server.Close()
			body := `{"name":"target","spec":{"provider":"psmdb","dataSource":` + source + `}}`
			code, out, stderr := runCLI(t, server.URL, body, false, "clusters", "create", "-f", "-", "-n", "targetns", "-o", "json", "--wait", "--timeout", "2s")
			var want map[string]any
			if err := json.Unmarshal([]byte(source), &want); err != nil {
				t.Fatal(err)
			}
			actual, _ := nested(got, "spec", "dataSource").(map[string]any)
			if code != 0 || posts != 1 || len(actual) != len(want) || !strings.Contains(out, "Ready") {
				t.Fatalf("code=%d posts=%d body=%v stdout=%q stderr=%q", code, posts, got, out, stderr)
			}
			for key, value := range want {
				if actual[key] != value {
					t.Errorf("source %s=%v want %v", key, actual[key], value)
				}
			}
		})
	}
}

func TestRestoreFileIntoNewUsesRecordedSource(t *testing.T) {
	posts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method + " " + r.URL.Path {
		case "GET /api/v1/clusters/source":
			if r.URL.Query().Get("namespace") != "archive" {
				t.Error("lost source namespace")
			}
			fmt.Fprint(w, `{"name":"source","spec":{"provider":"psmdb"}}`)
		case "POST /api/v1/clusters":
			posts++
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Error(err)
			}
			if body["name"] != "copy" || nested(body, "spec", "dataSource", "backup") != "base" || nested(body, "spec", "dataSource", "cluster") != "source" || nested(body, "spec", "dataSource", "namespace") != "archive" {
				t.Errorf("lost clone source: %v", body)
			}
			w.WriteHeader(http.StatusAccepted)
			fmt.Fprint(w, `{"name":"copy","operationId":"clone-one"}`)
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL)
		}
	}))
	defer server.Close()
	code, out, stderr := runCLI(t, server.URL, `{"sourceNamespace":"archive","source":{"sourceCluster":"source","backup":"base"}}`, false, "restores", "create", "target", "-f", "-", "--into-new", "copy", "-o", "json")
	if code != 0 || posts != 1 || !strings.Contains(out, "clone-one") {
		t.Fatalf("code=%d posts=%d stdout=%q stderr=%q", code, posts, out, stderr)
	}
}

func TestCompleteFileRejectsAllOtherBodyOverrides(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		t.Errorf("conflicting file/flags reached API: %s %s", r.Method, r.URL)
	}))
	defer server.Close()
	type conflictCase struct {
		command []string
		flags   []string
	}
	var cases []conflictCase
	for _, flags := range [][]string{
		{"--members", "3"}, {"--replicas", "3"}, {"--shards", "1"},
		{"--config-servers", "3"}, {"--routers", "2"}, {"--component", "engine"},
		{"--cpu-mode", "SHARED"}, {"--min-cpu", "1"}, {"--max-cpu", "2"},
		{"--min-ram", "1"}, {"--max-ram", "2"}, {"--min-disk", "5"},
		{"--max-disk", "10"}, {"--start-cpu", "1"},
		{"--replicas", "3", "--min-ram", "1", "--component", "engine"},
	} {
		cases = append(cases, conflictCase{[]string{"clusters", "scale", "demo"}, flags})
	}
	cases = append(cases,
		conflictCase{[]string{"clusters", "upgrade", "demo"}, []string{"--version", "8.0"}},
		conflictCase{[]string{"clusters", "raise-fcv", "demo", "--yes"}, []string{"--version", "8.0"}},
		conflictCase{[]string{"clusters", "balancer", "set", "demo"}, []string{"--enabled=false"}},
		conflictCase{[]string{"rbac", "can"}, []string{"--resource", "clusters"}},
		conflictCase{[]string{"rbac", "can"}, []string{"--action", "read"}},
		conflictCase{[]string{"rbac", "can"}, []string{"--object", "*"}},
		conflictCase{[]string{"rbac", "can"}, []string{"--subject", "role:admin"}},
	)
	for _, tc := range cases {
		t.Run(strings.Join(append(append([]string{}, tc.command...), tc.flags...), " "), func(t *testing.T) {
			args := append(append(append([]string{}, tc.command...), "-f", "-"), tc.flags...)
			// Invalid input proves the conflict is rejected before file parsing.
			code, out, stderr := runCLI(t, server.URL, "not JSON", false, args...)
			if code != 2 || out != "" || !strings.Contains(stderr, "--file cannot be combined with") {
				t.Fatalf("code=%d stdout=%q stderr=%q", code, out, stderr)
			}
			for _, flag := range tc.flags {
				if strings.HasPrefix(flag, "--") && !strings.Contains(stderr, strings.SplitN(flag, "=", 2)[0]) {
					t.Errorf("missing conflicting flag %q in %q", flag, stderr)
				}
			}
		})
	}
	if requests != 0 {
		t.Fatalf("sent %d requests for conflicting flags", requests)
	}
}
