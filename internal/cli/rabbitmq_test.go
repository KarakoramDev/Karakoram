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

func TestRabbitMQCommands(t *testing.T) {
	for _, resource := range []string{"vhosts", "users", "permissions", "topic-permissions", "policies", "operator-policies"} {
		for _, verb := range []string{"list", "get", "create", "update", "delete"} {
			t.Run(resource+"/"+verb, func(t *testing.T) {
				method := map[string]string{"list": "GET", "get": "GET", "create": "POST", "update": "PUT", "delete": "DELETE"}[verb]
				args := []string{"rabbitmq", resource, verb, "brokers"}
				path := "/api/v1/clusters/brokers/rabbitmq/" + resource
				if verb == "get" || verb == "update" || verb == "delete" {
					if resource == "vhosts" || resource == "users" {
						args = append(args, "app/a")
						path += "/app%2Fa"
					} else {
						args = append(args, "/", "app/a")
						path += "/%2F/app%2Fa"
					}
				}
				body := `{"name":"app/a","tags":[]}`
				if verb == "create" || verb == "update" {
					args = append(args, "-f", "-")
				}
				args = append(args, "-n", "production", "-o", "json")
				called := false
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					called = true
					if r.Method != method || r.URL.EscapedPath() != path {
						t.Errorf("%s %s want %s %s", r.Method, r.URL.EscapedPath(), method, path)
					}
					if r.URL.Query().Get("namespace") != "production" {
						t.Error("namespace missing")
					}
					if method == "POST" || method == "PUT" {
						var v map[string]any
						if e := json.NewDecoder(r.Body).Decode(&v); e != nil {
							t.Error(e)
						}
						if v["name"] != "app/a" {
							t.Error(v)
						}
					}
					w.Header().Set("Content-Type", "application/json")
					if method == "DELETE" {
						w.WriteHeader(204)
					} else if verb == "list" {
						fmt.Fprint(w, `{"items":[]}`)
					} else if resource == "users" && verb == "create" {
						w.WriteHeader(201)
						fmt.Fprint(w, `{"name":"app/a","tags":[],"generatedPassword":"one-time"}`)
					} else {
						fmt.Fprint(w, `{"name":"app/a","tags":[]}`)
					}
				}))
				defer srv.Close()
				out, _, e := execute(t, srv.URL, filepath.Join(t.TempDir(), "config.yaml"), body, args...)
				if e != nil {
					t.Fatal(e)
				}
				if !called {
					t.Fatal("request not sent")
				}
				if verb == "create" && resource == "users" && !strings.Contains(out, "one-time") {
					t.Fatal(out)
				}
			})
		}
	}
}
