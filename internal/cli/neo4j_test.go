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

func TestNeo4jCommands(t *testing.T) {
	for _, tc := range []struct {
		verb, method, suffix string
		stdin                bool
	}{
		{"list", "GET", "", false}, {"get", "GET", "/app%2Fa", false},
		{"create", "POST", "", false}, {"create", "POST", "", true},
		{"delete", "DELETE", "/app%2Fa", false},
		{"password", "POST", "/app%2Fa/password", false}, {"password", "POST", "/app%2Fa/password", true},
	} {
		t.Run(fmt.Sprintf("%s/stdin=%v", tc.verb, tc.stdin), func(t *testing.T) {
			called := false
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				called = true
				if r.Method != tc.method || r.URL.EscapedPath() != "/api/v1/clusters/graph/neo4j/users"+tc.suffix || r.URL.Query().Get("namespace") != "production" {
					t.Errorf("unexpected request %s %s", r.Method, r.URL)
				}
				if tc.verb == "create" || tc.verb == "password" {
					var body map[string]any
					if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
						t.Error(err)
					}
					if tc.verb == "create" && body["name"] != "app/a" {
						t.Error("missing name", body)
					}
					if tc.stdin && body["password"] != "private-password" {
						t.Error("stdin password not submitted")
					}
					if !tc.stdin && body["password"] != nil {
						t.Error("unexpected password")
					}
				}
				w.Header().Set("Content-Type", "application/json")
				if tc.method == "DELETE" {
					w.WriteHeader(204)
				} else if tc.verb == "list" {
					fmt.Fprint(w, `{"items":[]}`)
				} else {
					fmt.Fprint(w, `{"user":{"name":"app/a","passwordChangeRequired":false},"generatedPassword":"returned-once"}`)
				}
			}))
			defer server.Close()
			args := []string{"neo4j", "users", tc.verb, "graph"}
			if tc.verb != "list" {
				args = append(args, "app/a")
			}
			if tc.stdin {
				args = append(args, "--password-stdin")
			}
			if tc.verb == "delete" {
				args = append(args, "--yes")
			}
			args = append(args, "-n", "production", "-o", "json")
			out, stderr, err := execute(t, server.URL, filepath.Join(t.TempDir(), "config.yaml"), "private-password\n", args...)
			if err != nil || !called {
				t.Fatal(err, called)
			}
			if strings.Contains(out+stderr, "private-password") {
				t.Fatal("input password leaked")
			}
			if (tc.verb == "create" || tc.verb == "password") && !strings.Contains(out, "returned-once") {
				t.Fatal("generated password not rendered")
			}
		})
	}
}

func TestNeo4jPasswordInputRejected(t *testing.T) {
	for _, args := range [][]string{
		{"neo4j", "users", "create", "graph", "app", "--password", "secret"},
		{"neo4j", "users", "password", "graph", "app", "--password-stdin"},
	} {
		_, _, err := execute(t, "http://127.0.0.1:1", filepath.Join(t.TempDir(), "config.yaml"), "", args...)
		if err == nil {
			t.Fatal("invalid password input accepted")
		}
	}
}
