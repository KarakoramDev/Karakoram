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
					if resource == "vhosts" {
						args = append(args, "/")
						path += "/%2F"
					} else if resource == "users" {
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
				if verb == "delete" {
					args = append(args, "--yes")
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

func TestRabbitMQVhostNameIsNotPreencoded(t *testing.T) {
	for name, encoded := range map[string]string{"/": "%2F", "%2F": "%252F"} {
		t.Run(encoded, func(t *testing.T) {
			called := false
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				called = true
				if r.URL.EscapedPath() != "/api/v1/clusters/brokers/rabbitmq/vhosts/"+encoded {
					t.Error(r.URL.EscapedPath(), r.URL.RawPath)
				}
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(map[string]any{"name": name, "tags": []string{}})
			}))
			defer server.Close()
			_, _, err := execute(t, server.URL, filepath.Join(t.TempDir(), "config.yaml"), "", "rabbitmq", "vhosts", "get", "brokers", name, "-o", "json")
			if err != nil || !called {
				t.Fatal(err, called)
			}
		})
	}
}

func TestRabbitMQActionableErrors(t *testing.T) {
	for _, tc := range []struct {
		status                int
		body, message, detail string
	}{
		{400, `{"code":"invalid_request","message":"configure, write or read contains a regular expression rejected by RabbitMQ","details":{"field":"configure,write,read","reason":"invalid"}}`, "regular expression rejected by RabbitMQ", "field: configure,write,read"},
		{409, `{"code":"conflict","message":"RabbitMQ user admin is managed by Karakoram; rotate with system-users rotate","details":{"resource":"rabbitmq-user","name":"admin","reason":"protected"}}`, "rotate with system-users rotate", "reason: protected"},
	} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(tc.status)
			fmt.Fprint(w, tc.body)
		}))
		_, _, err := execute(t, server.URL, filepath.Join(t.TempDir(), "config.yaml"), "", "rabbitmq", "permissions", "delete", "brokers", "/", "admin", "--yes")
		server.Close()
		if err == nil || !strings.Contains(err.Error(), tc.message) || !strings.Contains(err.Error(), tc.detail) {
			t.Fatal(err)
		}
	}
}
