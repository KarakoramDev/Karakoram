// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

func TestDataMCPConfig(t *testing.T) {
	for _, client := range []string{"json", "cursor", "vscode", "claude-code"} {
		for _, write := range []bool{false, true} {
			for _, suffix := range []string{"", "/api/v1/"} {
				t.Run(fmt.Sprintf("%s/write=%t/suffix=%s", client, write, suffix), func(t *testing.T) {
					var requests int
					before := time.Now()
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						requests++
						if r.Method != "POST" || r.URL.Path != "/install/api/v1/api-tokens" || r.URL.RawQuery != "" {
							t.Errorf("request: %s %s", r.Method, r.URL)
						}
						if r.Header.Get("Authorization") != "Bearer admin-token" || r.Header.Get("Content-Type") != "application/json" {
							t.Errorf("headers: %v", r.Header)
						}
						var body struct {
							Name         string    `json:"name"`
							ExpiresAt    time.Time `json:"expiresAt"`
							Restrictions struct {
								Namespaces, Clusters, Permissions []string
								ReadOnly                          bool
							} `json:"restrictions"`
						}
						if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
							t.Error(err)
						}
						if body.Name != "data-client" || body.ExpiresAt.Before(before.Add(90*24*time.Hour)) || body.ExpiresAt.After(time.Now().Add(90*24*time.Hour)) {
							t.Errorf("name/expiry: %+v", body)
						}
						permissions := []string{"cluster-data:read"}
						if write {
							permissions = append(permissions, "cluster-data:write")
						}
						rules := body.Restrictions
						if !reflect.DeepEqual(rules.Namespaces, []string{"prod"}) || !reflect.DeepEqual(rules.Clusters, []string{"prod/orders"}) || !reflect.DeepEqual(rules.Permissions, permissions) || rules.ReadOnly != !write {
							t.Errorf("restrictions: %+v", rules)
						}
						w.Header().Set("Content-Type", "application/json")
						w.WriteHeader(http.StatusCreated)
						fmt.Fprint(w, `{"secret":"once-only-secret","token":{"id":"test-id"}}`)
					}))
					defer server.Close()
					path := filepath.Join(t.TempDir(), "config.yaml")
					if err := SaveConfig(path, Config{CurrentContext: "prod", Contexts: map[string]Context{"prod": {Server: server.URL + "/install" + suffix, Token: "admin-token"}}}); err != nil {
						t.Fatal(err)
					}
					var out, stderr bytes.Buffer
					cmd := NewCommand(Options{ConfigPath: path, Out: &out, Err: &stderr})
					args := []string{"--context", "prod", "mcp", "config", "--cluster", "prod/orders", "--client", client, "--token-name", "data-client"}
					if write {
						args = append(args, "--write")
					}
					cmd.SetArgs(args)
					if err := cmd.ExecuteContext(context.Background()); err != nil {
						t.Fatal(err)
					}
					if requests != 1 || strings.Count(out.String(), "once-only-secret") != 1 || strings.Contains(stderr.String(), "once-only-secret") || !strings.Contains(stderr.String(), "credential") {
						t.Fatalf("requests=%d out=%s stderr=%s", requests, out.String(), stderr.String())
					}
					endpoint := server.URL + "/install/mcp/clusters/prod/orders"
					if client == "claude-code" {
						want := "claude mcp add --transport http 'karakoram-prod-orders' '" + endpoint + "' --header 'Authorization: Bearer once-only-secret'\n"
						if out.String() != want {
							t.Fatalf("command: %s", out.String())
						}
						return
					}
					var got map[string]any
					if err := json.Unmarshal(out.Bytes(), &got); err != nil {
						t.Fatal(err)
					}
					switch client {
					case "cursor":
						got = got["mcpServers"].(map[string]any)["karakoram-prod-orders"].(map[string]any)
					case "vscode":
						got = got["servers"].(map[string]any)["karakoram-prod-orders"].(map[string]any)
						if got["type"] != "http" {
							t.Errorf("type: %v", got)
						}
						delete(got, "type")
					}
					want := map[string]any{"url": endpoint, "headers": map[string]any{"Authorization": "Bearer once-only-secret"}}
					if !reflect.DeepEqual(got, want) {
						t.Errorf("config: %v", got)
					}
				})
			}
		}
	}
}

func TestDataMCPValidationBeforeMint(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { requests++ }))
	defer server.Close()
	for _, flags := range [][]string{
		{"--cluster", "orders"}, {"--cluster", "prod/"}, {"--cluster", "../orders"}, {"--cluster", "prod/orders/other"},
		{"--cluster", "prod/orders", "--client", "unknown"},
		{"--cluster", "prod/orders", "--expires-in", "0d"},
		{"--cluster", "prod/orders", "--expires-in", "-1h"},
		{"--cluster", "prod/orders", "--expires-in", "999999999999d"},
		{"--cluster", "prod/orders", "--token-name", "  "},
		{"--cluster", "prod/orders", "--token-name", strings.Repeat("a", 129)},
	} {
		out, _, err := execute(t, server.URL, filepath.Join(t.TempDir(), "config"), "", append([]string{"mcp", "config"}, flags...)...)
		if err == nil || out != "" {
			t.Errorf("flags %v: out=%s err=%v", flags, out, err)
		}
	}
	if requests != 0 {
		t.Fatalf("minted %d tokens for invalid input", requests)
	}
}

func TestDataMCPAPIErrors(t *testing.T) {
	for _, tc := range []struct {
		status     int
		body, want string
	}{
		{400, `{"code":"invalid_body","message":"unknown field restrictions"}`, "unknown field restrictions"},
		{403, `{"code":"forbidden","message":"permission denied"}`, "permission denied"},
		{201, `{"token":{"id":"one"}}`, "did not contain a secret"},
	} {
		t.Run(fmt.Sprint(tc.status, tc.want), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tc.status)
				fmt.Fprint(w, tc.body)
			}))
			defer server.Close()
			out, _, err := execute(t, server.URL, filepath.Join(t.TempDir(), "config"), "", "mcp", "config", "--cluster", "prod/orders")
			if err == nil || !strings.Contains(err.Error(), tc.want) || out != "" {
				t.Fatalf("out=%s err=%v", out, err)
			}
			if tc.status >= 400 && !strings.Contains(err.Error(), "server must support token restrictions") {
				t.Errorf("unclear error: %v", err)
			}
		})
	}
}

func TestDataMCPDurationAndName(t *testing.T) {
	for input, want := range map[string]time.Duration{"90d": 90 * 24 * time.Hour, "1d": 24 * time.Hour, "1h30m": 90 * time.Minute} {
		got, err := dataMCPDuration(input)
		if err != nil || got != want {
			t.Errorf("%s: %v %v", input, got, err)
		}
	}
	for _, input := range []string{"", "0", "-1d", "1.5d", "bad", "999999999999999h"} {
		if _, err := dataMCPDuration(input); err == nil {
			t.Errorf("accepted %s", input)
		}
	}
	if got := dataMCPTokenName("orders", "my host\n"); got != "mcp-data-orders-my-host-" {
		t.Errorf("name: %q", got)
	}
	if got := dataMCPTokenName(strings.Repeat("界", 128), "host"); utf8.RuneCountInString(got) != 128 || !utf8.ValidString(got) {
		t.Errorf("truncated name: %q", got)
	}
}

func TestDataMCPRejectsManagementOnlyFlags(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("unexpected token request %s %s", r.Method, r.URL)
	}))
	defer server.Close()
	for _, flags := range [][]string{{"--stdio"}, {"--output", filepath.Join(t.TempDir(), "mcp.json")}, {"-n", "prod"}} {
		args := append([]string{"mcp", "config", "--cluster", "prod/orders"}, flags...)
		if _, _, err := execute(t, server.URL, filepath.Join(t.TempDir(), "config"), "", args...); err == nil || !strings.Contains(err.Error(), "cannot be combined with --cluster") {
			t.Fatalf("%v: err=%v", flags, err)
		}
	}
}

func TestDataMCPInsecureAndExplicitServer(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		name, _ := body["name"].(string)
		if !strings.HasPrefix(name, "mcp-data-orders-") {
			t.Errorf("default name: %s", name)
		}
		expiry, err := time.Parse(time.RFC3339Nano, body["expiresAt"].(string))
		if err != nil || time.Until(expiry) < 23*time.Hour || time.Until(expiry) > 24*time.Hour {
			t.Errorf("expiry: %v %v", expiry, err)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		fmt.Fprint(w, `{"secret":"tls-test-secret"}`)
	}))
	defer server.Close()
	path := filepath.Join(t.TempDir(), "config")
	if err := SaveConfig(path, Config{CurrentContext: "prod", Contexts: map[string]Context{"prod": {Server: "https://unused.invalid"}}}); err != nil {
		t.Fatal(err)
	}
	out, stderr, err := execute(t, server.URL+"/api/v1", path, "", "--insecure-skip-tls-verify", "mcp", "config", "--cluster", "prod/orders", "--expires-in", "24h")
	if err != nil || !strings.Contains(out, server.URL+"/mcp/clusters/prod/orders") || !strings.Contains(stderr, "TLS certificate verification is disabled") {
		t.Fatalf("out=%s stderr=%s err=%v", out, stderr, err)
	}
}
