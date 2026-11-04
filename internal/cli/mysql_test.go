// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package cli

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
)

func TestMySQLCommands(t *testing.T) {
	for _, tc := range []struct {
		resource, verb, method, suffix string
		body                           bool
	}{
		{"users", "list", "GET", "", false},
		{"users", "get", "GET", "/app%2Fa", false},
		{"users", "create", "POST", "", true},
		{"users", "update", "PATCH", "/app%2Fa", true},
		{"users", "delete", "DELETE", "/app%2Fa", false},
		{"users", "rotate-password", "POST", "/app%2Fa/password", false},
		{"users", "discard-old-password", "POST", "/app%2Fa/password/discard-old", false},
		{"databases", "list", "GET", "", false},
		{"databases", "get", "GET", "/app%2Fa", false},
		{"databases", "create", "POST", "", true},
		{"databases", "delete", "DELETE", "/app%2Fa", false},
		{"grants", "list", "GET", "", false},
		{"grants", "grant", "POST", "", true},
		{"grants", "revoke", "DELETE", "", true},
	} {
		t.Run(tc.resource+"/"+tc.verb, func(t *testing.T) {
			called := false
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				called = true
				if r.Method != tc.method || r.URL.EscapedPath() != "/api/v1/clusters/mysql/mysql/"+tc.resource+tc.suffix {
					t.Errorf("unexpected request %s %s", r.Method, r.URL.EscapedPath())
				}
				if r.URL.Query().Get("namespace") != "production" {
					t.Error("namespace missing")
				}
				if tc.resource == "databases" && tc.verb == "delete" && r.URL.Query().Get("force") != "true" {
					t.Error("force missing")
				}
				if tc.resource == "grants" && tc.verb == "list" && r.URL.Query().Get("user") != "app/a" {
					t.Error("user filter missing")
				}
				if tc.body || tc.verb == "rotate-password" {
					var body map[string]any
					if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
						t.Errorf("invalid request body: %v", err)
					}
					if tc.verb == "rotate-password" {
						if body["retainCurrent"] != true {
							t.Error("retain-current missing")
						}
					} else if body["name"] != "app/a" {
						t.Errorf("file body not preserved: %v", body)
					}
				}
				w.Header().Set("Content-Type", "application/json")
				if tc.method == "DELETE" || tc.verb == "grant" || tc.verb == "discard-old-password" {
					w.WriteHeader(http.StatusNoContent)
				} else if tc.verb == "list" {
					fmt.Fprint(w, `{"items":[]}`)
				} else {
					fmt.Fprint(w, `{"name":"app/a","generatedPassword":"returned-once"}`)
				}
			}))
			defer server.Close()
			args := []string{"mysql", tc.resource, tc.verb, "mysql"}
			if tc.suffix != "" {
				args = append(args, "app/a")
			}
			if tc.body {
				args = append(args, "-f", "-")
			}
			if tc.verb == "delete" || tc.verb == "revoke" {
				args = append(args, "--yes")
			}
			if tc.resource == "databases" && tc.verb == "delete" {
				args = append(args, "--force")
			}
			if tc.resource == "grants" && tc.verb == "list" {
				args = append(args, "--user", "app/a")
			}
			if tc.verb == "rotate-password" {
				args = append(args, "--retain-current")
			}
			args = append(args, "-n", "production", "-o", "json")
			out, _, err := execute(t, server.URL, filepath.Join(t.TempDir(), "config.yaml"), `{"name":"app/a"}`, args...)
			if err != nil || !called {
				t.Fatal("command failed", err, called)
			}
			if (tc.resource == "users" && tc.verb == "create" || tc.verb == "rotate-password") && strings.Count(out, "returned-once") != 1 {
				t.Fatalf("generated password not rendered exactly once: %q", out)
			}
		})
	}
}

func TestMySQLDestructiveConfirmation(t *testing.T) {
	for _, args := range [][]string{
		{"mysql", "recover-quorum", "mysql", "-f", "-"},
		{"mysql", "users", "delete", "mysql", "app"},
		{"mysql", "databases", "delete", "mysql", "app"},
		{"mysql", "grants", "revoke", "mysql", "-f", "-"},
	} {
		_, _, err := execute(t, "http://127.0.0.1:1", filepath.Join(t.TempDir(), "config.yaml"), "", args...)
		if err == nil || !strings.Contains(err.Error(), "no request submitted") {
			t.Fatal("destructive command lacked local confirmation", args, err)
		}
	}
}

func TestMySQLGrantsListFilters(t *testing.T) {
	for _, flags := range [][]string{nil, {"--user", ""}, {"--database", " "}, {"--user", " ", "--database", ""}, {"--user", "app"}, {"--database", "app"}, {"--user", "app", "--database", "app"}} {
		t.Run(fmt.Sprint(flags), func(t *testing.T) {
			valid := false
			for _, flag := range flags {
				valid = valid || flag == "app"
			}
			called := false
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				called = true
				for i := 0; i < len(flags); i += 2 {
					if r.URL.Query().Get(strings.TrimPrefix(flags[i], "--")) != flags[i+1] {
						t.Errorf("filter missing: %v", r.URL.Query())
					}
				}
				fmt.Fprint(w, `{"items":[]}`)
			}))
			defer server.Close()
			args := append([]string{"mysql", "grants", "list", "mysql"}, flags...)
			_, _, err := execute(t, server.URL, filepath.Join(t.TempDir(), "config.yaml"), "", args...)
			if valid {
				if err != nil || !called {
					t.Fatal("filtered command failed", err, called)
				}
			} else if err == nil || ExitCode(err) != 2 || !strings.Contains(err.Error(), "--user or --database is required") || called {
				t.Fatalf("expected local usage error without request, got %v (called %v)", err, called)
			}
		})
	}
}

func TestMySQLDatabaseCreationFlags(t *testing.T) {
	for _, flags := range [][]string{nil, {"--character-set", "latin1"}, {"--collation", "utf8mb4_bin"}, {"--character-set", "latin1", "--collation", "latin1_bin"}} {
		t.Run(fmt.Sprint(flags), func(t *testing.T) {
			want := map[string]any{"name": "app", "characterSet": "utf8mb4", "collation": "utf8mb4_0900_ai_ci"}
			for i := 0; i < len(flags); i += 2 {
				field := "collation"
				if flags[i] == "--character-set" {
					field = "characterSet"
				}
				want[field] = flags[i+1]
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var body map[string]any
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Error(err)
				}
				for field, value := range want {
					if body[field] != value {
						t.Errorf("%s = %v, want %v", field, body[field], value)
					}
				}
				fmt.Fprint(w, `{"name":"app"}`)
			}))
			defer server.Close()
			args := append([]string{"mysql", "databases", "create", "mysql", "-f", "-"}, flags...)
			_, _, err := execute(t, server.URL, filepath.Join(t.TempDir(), "config.yaml"), `{"name":"app","characterSet":"utf8mb4","collation":"utf8mb4_0900_ai_ci"}`, args...)
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestMySQLRotationDefault(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		if body["retainCurrent"] != false {
			t.Errorf("default rotation retained current password: %v", body)
		}
		fmt.Fprint(w, `{"name":"app","generatedPassword":"new-password"}`)
	}))
	defer server.Close()
	out, _, err := execute(t, server.URL, filepath.Join(t.TempDir(), "config.yaml"), "", "mysql", "users", "rotate-password", "mysql", "app", "-o", "json")
	if err != nil || strings.Count(out, "new-password") != 1 {
		t.Fatal("default rotation failed", err, out)
	}
}

func TestMySQLQuorumRecoveryCommand(t *testing.T) {
	called := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		if r.Method != "POST" || r.URL.Path != "/api/v1/clusters/pxcone/mysql/recover-quorum" || r.URL.Query().Get("namespace") != "production" {
			t.Errorf("unexpected recovery request: %s %s", r.Method, r.URL)
		}
		var body map[string]string
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body["confirmation"] != "all-absent-members-fenced" {
			t.Errorf("missing explicit fencing confirmation: %v %v", body, err)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		fmt.Fprint(w, `{"id":"recovery-one","type":"recover-quorum","phase":"Pending"}`)
	}))
	defer server.Close()
	out, _, err := execute(t, server.URL, filepath.Join(t.TempDir(), "config.yaml"), `{"confirmation":"all-absent-members-fenced"}`, "mysql", "recover-quorum", "pxcone", "-f", "-", "--yes", "-n", "production", "-o", "json")
	if err != nil || !called || !strings.Contains(out, "recovery-one") {
		t.Fatalf("recovery command: called=%v err=%v output=%s", called, err, out)
	}
}
