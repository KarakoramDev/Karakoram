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

func TestPostgresCommands(t *testing.T) {
	for _, tc := range []struct {
		resource, verb, method, suffix string
		body                           bool
	}{
		{"roles", "list", "GET", "", false},
		{"bindings", "list", "GET", "", false},
		{"bindings", "rotate", "POST", "/app%2Fa/rotate", false},
		{"roles", "get", "GET", "/app%2Fa", false},
		{"roles", "create", "POST", "", true},
		{"roles", "update", "PUT", "/app%2Fa", true},
		{"roles", "patch", "PATCH", "/app%2Fa", true},
		{"roles", "delete", "DELETE", "/app%2Fa", false},
		{"roles", "rotate-password", "POST", "/app%2Fa/password", false},
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
				if r.Method != tc.method || r.URL.EscapedPath() != "/api/v1/clusters/pg/postgres/"+tc.resource+tc.suffix {
					t.Errorf("unexpected request %s %s", r.Method, r.URL.EscapedPath())
				}
				if r.URL.Query().Get("namespace") != "production" {
					t.Error("namespace missing")
				}
				if tc.resource == "databases" && tc.verb == "delete" && r.URL.Query().Get("force") != "true" {
					t.Error("force missing")
				}
				if tc.resource == "grants" && tc.verb == "list" && r.URL.Query().Get("database") != "app" {
					t.Error("database scope missing")
				}
				w.Header().Set("Content-Type", "application/json")
				if tc.method == "DELETE" || tc.verb == "grant" {
					w.WriteHeader(204)
				} else if tc.verb == "list" {
					fmt.Fprint(w, `{"items":[]}`)
				} else {
					fmt.Fprint(w, `{"name":"app/a","generatedPassword":"returned-once"}`)
				}
			}))
			defer server.Close()
			args := []string{"postgres", tc.resource, tc.verb, "pg"}
			if tc.suffix != "" {
				args = append(args, "app/a")
			}
			if tc.body {
				args = append(args, "-f", "-")
			}
			if tc.verb == "delete" || tc.verb == "revoke" || tc.verb == "rotate-password" || tc.verb == "rotate" {
				args = append(args, "--yes")
			}
			if tc.resource == "databases" && tc.verb == "delete" {
				args = append(args, "--force")
			}
			if tc.resource == "grants" && tc.verb == "list" {
				args = append(args, "--database", "app")
			}
			args = append(args, "-n", "production", "-o", "json")
			out, _, err := execute(t, server.URL, filepath.Join(t.TempDir(), "config.yaml"), `{"name":"app/a"}`, args...)
			if err != nil || !called {
				t.Fatal("command failed", err, called)
			}
			if tc.verb == "rotate-password" && !strings.Contains(out, "returned-once") {
				t.Fatal("rotation password not rendered")
			}
		})
	}
}

func TestPostgresDestructiveConfirmation(t *testing.T) {
	for _, args := range [][]string{
		{"postgres", "roles", "delete", "pg", "app"},
		{"postgres", "roles", "rotate-password", "pg", "app"},
		{"postgres", "bindings", "rotate", "pg", "binding"},
		{"postgres", "databases", "delete", "pg", "app"},
		{"postgres", "grants", "revoke", "pg", "-f", "-"},
	} {
		_, _, err := execute(t, "http://127.0.0.1:1", filepath.Join(t.TempDir(), "config.yaml"), "", args...)
		if err == nil || !strings.Contains(err.Error(), "no request submitted") {
			t.Fatal("destructive command lacked local confirmation", args, err)
		}
	}
}

func TestPostgresGrantsListRequiresDatabase(t *testing.T) {
	for _, flags := range [][]string{nil, {"--database", ""}, {"--database", " "}} {
		t.Run(fmt.Sprint(flags), func(t *testing.T) {
			called := false
			server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
				called = true
			}))
			defer server.Close()
			args := append([]string{"postgres", "grants", "list", "pg"}, flags...)
			_, _, err := execute(t, server.URL, filepath.Join(t.TempDir(), "config.yaml"), "", args...)
			if ExitCode(err) != 2 || err == nil || !strings.Contains(err.Error(), "--database is required") {
				t.Fatalf("expected local usage error, got %v (exit %d)", err, ExitCode(err))
			}
			if called {
				t.Fatal("missing database submitted an API request")
			}
		})
	}
}

func TestPostgresDatabaseExtensionFlags(t *testing.T) {
	called := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		var body struct {
			Extensions []string `json:"extensions"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		if strings.Join(body.Extensions, ",") != "hstore,pgcrypto,uuid-ossp" {
			t.Errorf("extensions=%v", body.Extensions)
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"name":"app"}`)
	}))
	defer server.Close()
	_, _, err := execute(t, server.URL, filepath.Join(t.TempDir(), "config.yaml"), `{"name":"app","owner":"appuser","extensions":["hstore"]}`, "postgres", "databases", "create", "pg", "-f", "-", "--extension", "pgcrypto", "--extension", "uuid-ossp")
	if err != nil || !called {
		t.Fatal("extension flags failed", err)
	}
}

func TestRestoreBoundConsumerAcknowledgementFlag(t *testing.T) {
	called := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if body["acknowledgeBoundConsumers"] != true {
			t.Errorf("missing acknowledgement: %+v", body)
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"name":"restore","phase":"Pending"}`)
	}))
	defer server.Close()
	_, _, err := execute(t, server.URL, filepath.Join(t.TempDir(), "config.yaml"), "", "restores", "create", "pg", "--backup", "saved", "--acknowledge-bound-consumers", "--yes", "-o", "json")
	if err != nil || !called {
		t.Fatal("restore failed", err, called)
	}
}
