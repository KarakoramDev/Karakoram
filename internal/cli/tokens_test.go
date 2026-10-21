// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package cli

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestTokenCreateFlags(t *testing.T) {
	for _, expiry := range []string{"2099-01-02T03:04:05Z", "720h"} {
		t.Run(expiry, func(t *testing.T) {
			before := time.Now()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPost || r.URL.Path != "/api/v1/api-tokens" {
					t.Errorf("unexpected request: %s %s", r.Method, r.URL)
				}
				// Token issuance is global; persistent -n must neither become
				// a token restriction nor add an unsupported REST query field.
				if got := r.URL.Query().Get("namespace"); got != "" {
					t.Errorf("global token endpoint received namespace = %q", got)
				}
				var body map[string]any
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Error(err)
					return
				}
				want := map[string]any{"readOnly": true, "namespaces": []any{"dev", "staging"}, "clusters": []any{"dev/orders", "staging/orders"}, "permissions": []any{"clusters:read", "backups:*"}, "allowCredentialReveal": true}
				if body["name"] != "automation" || !reflect.DeepEqual(body["restrictions"], want) {
					t.Errorf("unexpected body: %#v", body)
				}
				value, _ := body["expiresAt"].(string)
				expires, err := time.Parse(time.RFC3339Nano, value)
				if err != nil {
					t.Errorf("expiry is not a timestamp: %q", value)
				} else if expiry == "720h" {
					duration := 720 * time.Hour
					if expires.Before(before.Add(duration)) || expires.After(time.Now().Add(duration)) {
						t.Errorf("duration expiry out of bounds: %s", expires)
					}
				} else if value != expiry {
					t.Errorf("expiry = %q, want %q", value, expiry)
				}
				w.Header().Set("Content-Type", "application/json")
				fmt.Fprint(w, `{"token":{"id":"token-one","name":"automation"},"secret":"synthetic-secret"}`)
			}))
			defer server.Close()
			_, _, err := execute(t, server.URL, filepath.Join(t.TempDir(), "config.yaml"), "", "tokens", "create", "--name", "automation", "--expires", expiry, "--read-only", "--scope-namespace", "dev", "--scope-namespace", "staging", "--scope-cluster", "dev/orders", "--scope-cluster", "staging/orders", "--permission", "clusters:read", "--permission", "backups:*", "--allow-credential-reveal", "-n", "request-scope")
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestTokenCreateUsageErrors(t *testing.T) {
	cases := [][]string{{}, {"--name", " "}, {"--name", "automation", "--expires", "tomorrow"}}
	for _, flag := range []string{"--name=automation", "--expires=720h", "--read-only", "--read-only=false", "--scope-namespace=dev", "--scope-cluster=dev/orders", "--permission=clusters:read", "--allow-credential-reveal", "--allow-credential-reveal=false"} {
		cases = append(cases, []string{"-f", "-", flag})
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("usage error submitted a request")
	}))
	defer server.Close()
	for _, args := range cases {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			_, _, err := execute(t, server.URL, filepath.Join(t.TempDir(), "config.yaml"), `{}`, append([]string{"tokens", "create"}, args...)...)
			if ExitCode(err) != 2 {
				t.Fatalf("exit = %d, error = %v", ExitCode(err), err)
			}
		})
	}
}

func TestTokenCreateFileAndUnrestricted(t *testing.T) {
	for _, fromFile := range []bool{false, true} {
		t.Run(fmt.Sprint(fromFile), func(t *testing.T) {
			want := map[string]any{"name": "automation"}
			input := `{"name":"automation","restrictions":{"readOnly":true}}`
			args := []string{"tokens", "create", "--name", "automation"}
			if fromFile {
				args = []string{"tokens", "create", "-f", "-"}
				want["restrictions"] = map[string]any{"readOnly": true}
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var body map[string]any
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Error(err)
				}
				if !reflect.DeepEqual(body, want) {
					t.Errorf("body = %#v, want %#v", body, want)
				}
				w.Header().Set("Content-Type", "application/json")
				fmt.Fprint(w, `{}`)
			}))
			defer server.Close()
			if _, _, err := execute(t, server.URL, filepath.Join(t.TempDir(), "config.yaml"), input, args...); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestTokenRestrictionsOutput(t *testing.T) {
	for _, command := range [][]string{{"list"}, {"get", "token-one"}, {"create", "--name", "automation"}} {
		for _, restrictions := range []string{"", `,"restrictions":{}`, `,"restrictions":{"readOnly":true,"namespaces":["dev"],"clusters":["dev/orders"],"permissions":["clusters:read"],"allowCredentialReveal":true}`} {
			t.Run(strings.Join(command, " ")+restrictions, func(t *testing.T) {
				token := `{"id":"token-one","name":"automation"` + restrictions + `}`
				response := token
				switch command[0] {
				case "list":
					response = `{"items":[` + token + `],"nextCursor":"next"}`
				case "create":
					response = `{"token":` + token + `,"secret":"synthetic-secret"}`
				}
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("Content-Type", "application/json")
					fmt.Fprint(w, response)
				}))
				defer server.Close()
				for _, format := range []string{"table", "json", "yaml"} {
					args := append([]string{"tokens"}, command...)
					args = append(args, "-o", format)
					out, stderr, err := execute(t, server.URL, filepath.Join(t.TempDir(), "config.yaml"), "", args...)
					if err != nil {
						t.Fatal(err)
					}
					if format != "table" {
						if restrictions != "" && !strings.Contains(out, "restrictions") {
							t.Fatalf("missing structured restrictions: %s", out)
						}
						continue
					}
					if !strings.Contains(strings.ToLower(out), "restrictions") {
						t.Fatalf("missing restrictions: %s", out)
					}
					if strings.Contains(restrictions, "readOnly") && !strings.Contains(out, "read-only, ns=dev, clusters=dev/orders, perms=clusters:read, credential-reveal") {
						t.Fatalf("missing compact restrictions: %s", out)
					}
					if restrictions == `,"restrictions":{}` && !strings.Contains(out, "no-credential-reveal") {
						t.Fatalf("empty restrictions shown as unrestricted: %s", out)
					}
					if restrictions == "" && !strings.Contains(out, "-") {
						t.Fatalf("missing unrestricted marker: %s", out)
					}
					if command[0] == "list" && !strings.Contains(stderr, "--cursor next") {
						t.Fatal("lost pagination hint")
					}
					if command[0] == "create" && !strings.Contains(out, "synthetic-secret") {
						t.Fatal("lost newly created token secret")
					}
				}
			})
		}
	}
}
