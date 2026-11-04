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
	"reflect"
	"strings"
	"testing"
)

const openBaoRevealResponse = `{"threshold":2,"sealType":"shamir","shares":["share-one","share-two","share-three"],"initialRootToken":"root-secret","recoveryShares":false,"initializedAt":"2026-01-01T00:00:00Z"}`

func TestOpenBaoReveal(t *testing.T) {
	for _, format := range []string{"table", "json"} {
		t.Run(format, func(t *testing.T) {
			called := false
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				called = true
				if r.Method != "POST" || r.URL.EscapedPath() != "/api/v1/clusters/bao/openbao/unseal-keys/reveal" || r.URL.Query().Get("namespace") != "production" {
					t.Errorf("unexpected request %s %s", r.Method, r.URL)
				}
				var body map[string]any
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil || !reflect.DeepEqual(body, map[string]any{"confirm": "bao"}) {
					t.Errorf("unexpected reveal body: %v %v", body, err)
				}
				w.Header().Set("Content-Type", "application/json")
				fmt.Fprint(w, openBaoRevealResponse)
			}))
			defer server.Close()
			out, warning, err := execute(t, server.URL, filepath.Join(t.TempDir(), "config.yaml"), "", "openbao", "unseal-keys", "reveal", "bao", "--confirm", "bao", "-n", "production", "-o", format)
			if err != nil || !called {
				t.Fatal(err, called)
			}
			for _, text := range []string{"WARNING", "unseal shares", "initial root token", "offline"} {
				if !strings.Contains(warning, text) {
					t.Errorf("warning missing %q: %s", text, warning)
				}
			}
			if strings.Contains(out, "WARNING") || strings.Contains(warning, "root-secret") || strings.Contains(warning, "share-one") {
				t.Fatal("warning and secret output not separated")
			}
			if format == "json" {
				var got, want any
				if err := json.Unmarshal([]byte(out), &got); err != nil {
					t.Fatal(err)
				}
				if err := json.Unmarshal([]byte(openBaoRevealResponse), &want); err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("JSON response altered: %s", out)
				}
			} else {
				want := "Threshold: 2\nSeal type: shamir\nShare 1: share-one\nShare 2: share-two\nShare 3: share-three\nInitial root token: root-secret\n"
				if out != want {
					t.Fatalf("unexpected text output: %q", out)
				}
			}
		})
	}
}

func TestOpenBaoRevealRequiresConfirmation(t *testing.T) {
	for _, flags := range [][]string{nil, {"--confirm", ""}, {"--confirm", "other"}} {
		t.Run(fmt.Sprint(flags), func(t *testing.T) {
			called := false
			server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { called = true }))
			defer server.Close()
			args := append([]string{"openbao", "unseal-keys", "reveal", "bao"}, flags...)
			_, warning, err := execute(t, server.URL, filepath.Join(t.TempDir(), "config.yaml"), "", args...)
			if err == nil || ExitCode(err) != 2 || !strings.Contains(err.Error(), "confirm") {
				t.Fatalf("expected local confirmation error: %v", err)
			}
			if called || strings.Contains(warning, "WARNING") {
				t.Fatal("unconfirmed reveal reached execution")
			}
		})
	}
}

func TestOpenBaoDatabaseConnectionCommands(t *testing.T) {
	for _, tc := range []struct{ verb, method, suffix string }{
		{"list", "GET", ""}, {"get", "GET", "/app%2Fa"}, {"create", "POST", ""}, {"delete", "DELETE", "/app%2Fa"},
	} {
		t.Run(tc.verb, func(t *testing.T) {
			called := false
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				called = true
				if r.Method != tc.method || r.URL.EscapedPath() != "/api/v1/clusters/bao/openbao/database-connections"+tc.suffix {
					t.Errorf("unexpected request %s %s", r.Method, r.URL)
				}
				if r.URL.Query().Get("namespace") != "production" {
					t.Error("namespace missing")
				}
				if tc.verb == "create" {
					var body map[string]any
					if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
						t.Fatal(err)
					}
					if body["name"] != "app" || body["targetCluster"] != "pg" {
						t.Errorf("body not forwarded: %v", body)
					}
				}
				w.Header().Set("Content-Type", "application/json")
				if tc.verb == "delete" {
					w.WriteHeader(204)
				} else if tc.verb == "list" {
					fmt.Fprint(w, `{"items":[]}`)
				} else {
					fmt.Fprint(w, `{"name":"app"}`)
				}
			}))
			defer server.Close()
			args := []string{"openbao", "database-connections", tc.verb, "bao"}
			if tc.suffix != "" {
				args = append(args, "app/a")
			}
			if tc.verb == "create" {
				args = append(args, "-f", "-")
			}
			if tc.verb == "delete" {
				args = append(args, "--yes")
			}
			args = append(args, "-n", "production", "-o", "json")
			_, _, err := execute(t, server.URL, filepath.Join(t.TempDir(), "config.yaml"), `{"name":"app","targetCluster":"pg","roles":[{"name":"reader","access":"readonly"}]}`, args...)
			if err != nil || !called {
				t.Fatal(err, called)
			}
		})
	}
}

func TestOpenBaoConnectionRoleFlagsAndTTLs(t *testing.T) {
	for _, file := range []bool{false, true} {
		t.Run(fmt.Sprint(file), func(t *testing.T) {
			called := false
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				called = true
				var body map[string]any
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Fatal(err)
				}
				want := `{"name":"app","targetCluster":"pg","roles":[{"name":"reader","access":"readonly","database":"app","defaultTtlSeconds":3600,"maxTtlSeconds":86400},{"name":"writer","access":"readwrite","defaultTtlSeconds":3600,"maxTtlSeconds":86400}]}`
				if file {
					want = `{"name":"app","targetCluster":"pg","roles":[{"name":"reader","access":"readonly","database":"app","defaultTtlSeconds":60,"maxTtlSeconds":7200},{"name":"writer","access":"readwrite","defaultTtlSeconds":3600,"maxTtlSeconds":86400}]}`
				}
				var expected any
				if err := json.Unmarshal([]byte(want), &expected); err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(body, expected) {
					t.Errorf("unexpected body: %#v", body)
				}
				w.Header().Set("Content-Type", "application/json")
				fmt.Fprint(w, `{"name":"app"}`)
			}))
			defer server.Close()
			args := []string{"openbao", "database-connections", "create", "bao", "--default-ttl", "1h", "--max-ttl", "24h"}
			if file {
				path := filepath.Join(t.TempDir(), "connection.json")
				payload := `{"name":"app","targetCluster":"pg","roles":[{"name":"reader","access":"readonly","database":"app","defaultTtlSeconds":60,"maxTtlSeconds":7200},{"name":"writer","access":"readwrite"}]}`
				if err := os.WriteFile(path, []byte(payload), 0600); err != nil {
					t.Fatal(err)
				}
				args = append(args, "-f", path)
			} else {
				args = append(args, "--name", "app", "--target", "pg", "--role", "reader:readonly:app", "--role", "writer:readwrite")
			}
			_, _, err := execute(t, server.URL, filepath.Join(t.TempDir(), "config.yaml"), "", args...)
			if err != nil || !called {
				t.Fatal(err, called)
			}
		})
	}
}

func TestOpenBaoInvalidRoleFlags(t *testing.T) {
	for _, value := range []string{"reader", "reader:admin", "reader:readonly:", "reader:readonly:db:extra", "Bad:readonly"} {
		if _, err := parseOpenBaoRole(value); err == nil {
			t.Errorf("accepted invalid role %q", value)
		}
	}
	for _, value := range []string{"reader:readonly", "writer:readwrite:app"} {
		if _, err := parseOpenBaoRole(value); err != nil {
			t.Errorf("rejected valid role %q: %v", value, err)
		}
	}
}

func TestOpenBaoInvalidCreateFlags(t *testing.T) {
	for _, flags := range [][]string{
		nil,
		{"--name", "app", "--target", "pg", "--role", "reader:admin"},
		{"--name", "app", "--target", "pg", "--role", "reader:readonly", "--default-ttl", "0s"},
		{"--name", "app", "--target", "pg", "--role", "reader:readonly", "--max-ttl", "-1s"},
		{"--name", "app", "--target", "pg", "--role", "reader:readonly", "--default-ttl", "1500ms"},
	} {
		called := false
		server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { called = true }))
		args := append([]string{"openbao", "database-connections", "create", "bao"}, flags...)
		_, _, err := execute(t, server.URL, filepath.Join(t.TempDir(), "config.yaml"), "", args...)
		server.Close()
		if err == nil || ExitCode(err) != 2 || called {
			t.Fatalf("invalid create submitted request: %v %v %v", flags, err, called)
		}
	}
}

func TestOpenBaoDeleteConfirmation(t *testing.T) {
	for _, input := range []string{"", "no\n", "yes\n"} {
		t.Run(fmt.Sprintf("input=%q", input), func(t *testing.T) {
			called := false
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { called = true; w.WriteHeader(204) }))
			defer server.Close()
			_, _, err := execute(t, server.URL, filepath.Join(t.TempDir(), "config.yaml"), input, "openbao", "database-connections", "delete", "bao", "app")
			if input == "yes\n" {
				if err != nil || !called {
					t.Fatal(err, called)
				}
			} else if err == nil || !strings.Contains(err.Error(), "no request submitted") || called {
				t.Fatalf("missing local confirmation: %v %v", err, called)
			}
		})
	}
}

func TestOpenBaoHelpAndOperationAnnotations(t *testing.T) {
	root := NewCommand(Options{})
	for _, spec := range openBaoCommandSpecs() {
		words := strings.Fields(spec.path)
		var path []string
		for _, word := range words {
			if strings.HasPrefix(word, "<") {
				break
			}
			path = append(path, word)
		}
		command, _, err := root.Find(path)
		if err != nil {
			t.Fatal(err)
		}
		if command.Annotations[operationAnnotation] != spec.operation || command.Short == "" {
			t.Fatalf("missing command annotation or help for %s", spec.path)
		}
		for parent := command.Parent(); parent != root; parent = parent.Parent() {
			if parent.Short == "" {
				t.Fatalf("missing group description for %s", parent.CommandPath())
			}
		}
	}
}
