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

func TestWeaviateCommands(t *testing.T) {
	for _, resource := range []string{"users", "roles"} {
		verbs := []string{"list", "get", "create", "update", "delete"}
		if resource == "users" {
			verbs = append(verbs, "rotate-key")
		}
		for _, verb := range verbs {
			t.Run(resource+"/"+verb, func(t *testing.T) {
				args := []string{"weaviate", resource, verb, "vectors"}
				path := "/api/v1/clusters/vectors/weaviate/" + resource
				if verb == "get" || verb == "update" || verb == "delete" || verb == "rotate-key" {
					args = append(args, "app/a")
					path += "/app%2Fa"
				}
				if verb == "rotate-key" {
					path += "/rotate-key"
				}
				body := `{"name":"app/a","permissions":[{"action":"read_data","future":{"enabled":true}}]}`
				if verb == "create" || verb == "update" {
					if resource == "roles" {
						args = append(args, "-f", "-")
					} else {
						if verb == "create" {
							args = append(args, "app/a")
						}
						args = append(args, "--role", "viewer", "--role", "read-only")
						if verb == "update" {
							args = append(args, "--inactive")
						}
					}
				}
				if verb == "delete" || verb == "rotate-key" {
					args = append(args, "--yes")
				}
				args = append(args, "-n", "production", "-o", "json")
				called := false
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					called = true
					method := map[string]string{"list": "GET", "get": "GET", "create": "POST", "update": "PUT", "delete": "DELETE", "rotate-key": "POST"}[verb]
					if r.Method != method || r.URL.EscapedPath() != path || r.URL.Query().Get("namespace") != "production" {
						t.Errorf("%s %s", r.Method, r.URL.String())
					}
					if verb == "create" || verb == "update" {
						var v map[string]any
						if err := json.NewDecoder(r.Body).Decode(&v); err != nil {
							t.Error(err)
						}
						if resource == "users" {
							if len(v["roles"].([]any)) != 2 {
								t.Error(v)
							}
							if verb == "update" && v["active"] != false {
								t.Error(v)
							}
						}
					}
					w.Header().Set("Content-Type", "application/json")
					if verb == "delete" || resource == "users" && verb == "update" {
						w.WriteHeader(204)
					} else if verb == "list" {
						fmt.Fprint(w, `{"items":[]}`)
					} else if resource == "users" && (verb == "create" || verb == "rotate-key") {
						fmt.Fprint(w, `{"apiKey":"one-time"}`)
					} else {
						fmt.Fprint(w, `{"name":"app/a"}`)
					}
				}))
				defer srv.Close()
				out, _, err := execute(t, srv.URL, filepath.Join(t.TempDir(), "config.yaml"), body, args...)
				if err != nil || !called {
					t.Fatal(err, called)
				}
				if resource == "users" && (verb == "create" || verb == "rotate-key") && strings.Count(out, "one-time") != 1 {
					t.Fatal(out)
				}
			})
		}
	}
}
func TestWeaviateActivationFlagsExclusive(t *testing.T) {
	_, _, err := execute(t, "http://127.0.0.1:1", filepath.Join(t.TempDir(), "config.yaml"), "", "weaviate", "users", "update", "vectors", "app", "--active", "--inactive")
	if err == nil || !strings.Contains(err.Error(), "active") {
		t.Fatal(err)
	}
}
