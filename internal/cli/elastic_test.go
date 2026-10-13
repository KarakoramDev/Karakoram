// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package cli

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestElasticCLIRequests(t *testing.T) {
	roleFile := filepath.Join(t.TempDir(), "role.json")
	if err := os.WriteFile(roleFile, []byte(`{"cluster":["monitor"],"indices":[{"names":["logs-*"],"privileges":["read"],"allowRestrictedIndices":false}],"applications":[{"application":"apm","privileges":["event:write","config_agent:read"],"resources":["*"]}]}`), 0600); err != nil {
		t.Fatal(err)
	}
	descriptors := filepath.Join(t.TempDir(), "descriptors.json")
	if err := os.WriteFile(descriptors, []byte(`{"apm":{"cluster":[],"indices":[],"applications":[{"application":"apm","privileges":["event:write"],"resources":["*"]}]}}`), 0600); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		args                []string
		method, path, input string
		check               func(*testing.T, map[string]any)
	}{
		{args: []string{"users", "list", "search", "--limit", "10", "--cursor", "next"}, method: "GET", path: "users?cursor=next&limit=10&namespace=project"},
		{args: []string{"users", "get", "search", "app"}, method: "GET", path: "users/app?namespace=project"},
		{args: []string{"users", "create", "search", "app", "--roles", "viewer,ingest", "--full-name", "Analyst", "--email", "app@example.com", "--disabled", "--password-stdin"}, method: "POST", path: "users?namespace=project", input: "private-password\n", check: func(t *testing.T, b map[string]any) {
			if b["username"] != "app" || b["password"] != "private-password" || b["fullName"] != "Analyst" || b["enabled"] != false || len(b["roles"].([]any)) != 2 {
				t.Fatal(b)
			}
		}},
		{args: []string{"users", "update", "search", "app", "--disabled=false", "--email", ""}, method: "PUT", path: "users/app?namespace=project", check: func(t *testing.T, b map[string]any) {
			if b["username"] != "app" || b["fullName"] != "Existing name" || b["email"] != "" || b["enabled"] != true || b["password"] != nil {
				t.Fatal(b)
			}
		}},
		{args: []string{"users", "delete", "search", "app"}, method: "DELETE", path: "users/app?namespace=project"},
		{args: []string{"roles", "list", "search"}, method: "GET", path: "roles?namespace=project"},
		{args: []string{"roles", "get", "search", "ingest"}, method: "GET", path: "roles/ingest?namespace=project"},
		{args: []string{"roles", "create", "search", "ingest", "-f", roleFile}, method: "POST", path: "roles?namespace=project", check: func(t *testing.T, b map[string]any) {
			if b["name"] != "ingest" || b["applications"].([]any)[0].(map[string]any)["application"] != "apm" {
				t.Fatal(b)
			}
		}},
		{args: []string{"roles", "update", "search", "ingest", "-f", roleFile}, method: "PUT", path: "roles/ingest?namespace=project"},
		{args: []string{"roles", "delete", "search", "ingest"}, method: "DELETE", path: "roles/ingest?namespace=project"},
		{args: []string{"api-keys", "list", "search"}, method: "GET", path: "api-keys?namespace=project"},
		{args: []string{"api-keys", "create", "search", "--name", "agent", "--expiration", "1d", "-f", descriptors}, method: "POST", path: "api-keys?namespace=project", check: func(t *testing.T, b map[string]any) {
			if b["name"] != "agent" || b["expiration"] != "1d" || b["roleDescriptors"].(map[string]any)["apm"] == nil {
				t.Fatal(b)
			}
		}},
		{args: []string{"api-keys", "invalidate", "search", "key"}, method: "DELETE", path: "api-keys/key?namespace=project"},
	}
	for _, tc := range cases {
		t.Run(strings.Join(tc.args, " "), func(t *testing.T) {
			seen := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Authorization") != "Bearer test-token" {
					t.Error("token missing")
				}
				w.Header().Set("Content-Type", "application/json")
				if tc.args[1] == "update" && tc.args[0] == "users" && r.Method == "GET" {
					if r.URL.String() != "/api/v1/clusters/search/elasticsearch/users/app?namespace=project" {
						t.Error(r.URL)
					}
					w.Write([]byte(`{"username":"app","roles":["viewer"],"fullName":"Existing name","email":"old@example.com","enabled":false}`))
					return
				}
				seen++
				if r.Method != tc.method || r.URL.String() != "/api/v1/clusters/search/elasticsearch/"+tc.path {
					t.Error(r.Method, r.URL)
				}
				if tc.check != nil {
					var body map[string]any
					if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
						t.Error(err)
					}
					tc.check(t, body)
				}
				if r.Method == "DELETE" {
					w.WriteHeader(204)
					return
				}
				if tc.args[0] == "api-keys" && tc.args[1] == "create" {
					w.WriteHeader(201)
					w.Write([]byte(`{"id":"key","name":"agent","apiKey":"secret","encoded":"encoded-secret"}`))
					return
				}
				w.Write([]byte(`{"items":[]}`))
			}))
			defer server.Close()
			var out, errs bytes.Buffer
			command := NewCommand(Options{ConfigPath: filepath.Join(t.TempDir(), "config.yaml"), In: strings.NewReader(tc.input), Out: &out, Err: &errs})
			args := append([]string{"--server", server.URL, "--token", "test-token", "--namespace", "project", "-o", "json", "elasticsearch"}, tc.args...)
			command.SetArgs(args)
			if err := command.ExecuteContext(t.Context()); err != nil {
				t.Fatal(err, errs.String())
			}
			if seen != 1 || !json.Valid(out.Bytes()) {
				t.Fatal(seen, out.String())
			}
			if strings.Contains(errs.String(), "private-password") {
				t.Fatal("password leaked")
			}
		})
	}
}
func TestElasticCLIInputValidation(t *testing.T) {
	for _, args := range [][]string{{"users", "create", "search", "app"}, {"roles", "create", "search", "ingest"}, {"api-keys", "create", "search"}, {"users", "get", "search"}, {"api-keys", "invalidate", "search"}} {
		var out bytes.Buffer
		cmd := NewCommand(Options{ConfigPath: filepath.Join(t.TempDir(), "config.yaml"), Out: &out, Err: &out, In: strings.NewReader("")})
		cmd.SetArgs(append([]string{"--server", "http://example.invalid", "elasticsearch"}, args...))
		if err := cmd.ExecuteContext(t.Context()); err == nil {
			t.Fatal("invalid arguments accepted", args)
		}
	}
}
func TestElasticCLIRoleNameMismatch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "role.json")
	if err := os.WriteFile(path, []byte(`{"name":"other"}`), 0600); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	cmd := NewCommand(Options{ConfigPath: filepath.Join(t.TempDir(), "config.yaml"), Out: &out, Err: &out})
	cmd.SetArgs([]string{"--server", "http://example.invalid", "elasticsearch", "roles", "create", "search", "ingest", "-f", path})
	if err := cmd.ExecuteContext(t.Context()); err == nil || !strings.Contains(err.Error(), "must match") {
		t.Fatal(err)
	}
}
