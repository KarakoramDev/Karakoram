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
	"strings"
	"testing"
)

func TestSupabaseCredentialsMaskAndReveal(t *testing.T) {
	secrets := []string{"anon-value", "service-value", "publishable-value", "secret-value", "dashboard-value", "jwt-value"}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/clusters/app/supabase/credentials" {
			t.Error(r.URL)
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"apiUrl":"https://api.example","studioUrl":"https://api.example","anonKey":"anon-value","serviceRoleKey":"service-value","publishableKey":"publishable-value","secretKey":"secret-value","dashboardUsername":"admin","dashboardPassword":"dashboard-value","jwtSecret":"jwt-value"}`)
	}))
	defer server.Close()
	for _, format := range []string{"json", "yaml", "table"} {
		for _, show := range []bool{false, true} {
			args := []string{"supabase", "credentials", "app", "-o", format}
			if show {
				args = append(args, "--show-secrets")
			}
			out, stderr, err := execute(t, server.URL, filepath.Join(t.TempDir(), "config.yaml"), "", args...)
			if err != nil {
				t.Fatal(err)
			}
			for _, secret := range secrets {
				if strings.Contains(out, secret) != show {
					t.Fatal("credential reveal mismatch", format, show)
				}
			}
			if !show && !strings.Contains(stderr, "--show-secrets") {
				t.Fatal("missing reveal guidance")
			}
			if !strings.Contains(out, "https://api.example") {
				t.Fatal("missing endpoint")
			}
		}
	}
}

func TestSupabaseCLIRoutes(t *testing.T) {
	source := filepath.Join(t.TempDir(), "index.ts")
	if err := os.WriteFile(source, []byte("Deno.serve(() => new Response('ok'))"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		args                []string
		input, method, path string
		status              int
	}{
		{[]string{"smtp-password", "set", "app"}, "private-smtp\n", "PUT", "smtp-password", 204},
		{[]string{"functions", "list", "app"}, "", "GET", "functions", 200},
		{[]string{"functions", "get", "app", "hello"}, "", "GET", "functions/hello", 200},
		{[]string{"functions", "deploy", "app", "hello", source}, "", "PUT", "functions/hello", 204},
		{[]string{"functions", "delete", "app", "hello", "--yes"}, "", "DELETE", "functions/hello", 204},
		{[]string{"rotate-jwt-secret", "app", "--yes"}, "", "POST", "rotate-jwt-secret", 202},
	} {
		t.Run(strings.Join(tc.args[:2], " "), func(t *testing.T) {
			requests := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests++
				if r.Method != tc.method || r.URL.Path != "/api/v1/clusters/app/supabase/"+tc.path || r.URL.Query().Get("namespace") != "project" {
					t.Error(r.Method, r.URL)
				}
				if tc.method == "PUT" {
					var body map[string]string
					if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
						t.Error(err)
					}
					if tc.path == "smtp-password" && body["password"] != "private-smtp" {
						t.Error("SMTP input not forwarded")
					}
					if tc.path == "functions/hello" && !strings.Contains(body["index.ts"], "Deno.serve") {
						t.Error("source not forwarded")
					}
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tc.status)
				if tc.status != 204 {
					fmt.Fprint(w, `{ "items": [] }`)
				}
			}))
			defer server.Close()
			args := append([]string{"--namespace", "project", "supabase"}, tc.args...)
			out, _, err := execute(t, server.URL, filepath.Join(t.TempDir(), "config.yaml"), tc.input, args...)
			if err != nil || requests != 1 {
				t.Fatal(err, requests)
			}
			if strings.Contains(out, "private-smtp") {
				t.Fatal("password echoed")
			}
		})
	}
}

func TestSupabaseConfirmationAndArguments(t *testing.T) {
	for _, args := range [][]string{{"functions", "delete", "app", "hello"}, {"rotate-jwt-secret", "app"}} {
		for _, input := range []string{"", "n\n", "yes\n"} {
			requests := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { requests++; w.WriteHeader(204) }))
			_, _, err := execute(t, server.URL, filepath.Join(t.TempDir(), "config.yaml"), input, append([]string{"supabase"}, args...)...)
			server.Close()
			if input == "yes\n" {
				if err != nil || requests != 1 {
					t.Fatal(err, requests)
				}
			} else if err == nil || requests != 0 {
				t.Fatal("unconfirmed mutation", err, requests)
			}
		}
	}
	for _, args := range [][]string{{"smtp-password", "set", "app", "argv-password"}, {"functions", "deploy", "app", "Bad", "index.ts"}, {"functions", "get", "app", "../bad"}, {"credentials"}} {
		_, _, err := execute(t, "http://example.invalid", filepath.Join(t.TempDir(), "config.yaml"), "", append([]string{"supabase"}, args...)...)
		if err == nil {
			t.Fatal("invalid args accepted")
		}
	}
}

func TestReadSupabaseFunctionSources(t *testing.T) {
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "lib"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "index.ts"), []byte("export {}"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "lib", "helper.ts"), []byte("export const answer = 42"), 0600); err != nil {
		t.Fatal(err)
	}
	files, err := readSupabaseFunction(dir)
	if err != nil || len(files) != 2 || files["lib/helper.ts"] == "" {
		t.Fatal(err, files)
	}
	if err := os.Symlink(filepath.Join(dir, "index.ts"), filepath.Join(dir, "linked.ts")); err != nil {
		t.Fatal(err)
	}
	if _, err := readSupabaseFunction(dir); err == nil {
		t.Fatal("symlink accepted")
	}
	large := filepath.Join(t.TempDir(), "index.ts")
	if err := os.WriteFile(large, []byte(strings.Repeat("x", (1<<20)+1)), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := readSupabaseFunction(large); err == nil {
		t.Fatal("oversized source accepted")
	}
	if _, err := readSupabaseFunction(t.TempDir()); err == nil {
		t.Fatal("empty directory accepted")
	}
}

func TestSupabaseHelpAndCompletion(t *testing.T) {
	for _, args := range [][]string{{"supabase", "--help"}, {"supabase", "functions", "deploy", "--help"}, {"supabase", "smtp-password", "set", "--help"}} {
		code, out, stderr := runCLI(t, "", "", false, args...)
		if code != 0 || out == "" || stderr != "" {
			t.Fatal(code, stderr)
		}
	}
	code, out, stderr := runCLI(t, "", "", false, "completion", "fish")
	if code != 0 || stderr != "" || out == "" {
		t.Fatal("shell completion generation failed", code, stderr)
	}
	for _, tc := range []struct {
		args []string
		want string
	}{
		{[]string{"__complete", ""}, "supabase"},
		{[]string{"__complete", "supabase", ""}, "rotate-jwt-secret"},
		{[]string{"__complete", "supabase", "functions", ""}, "deploy"},
	} {
		code, out, stderr := runCLI(t, "", "", false, tc.args...)
		if code != 0 || !strings.Contains(out, tc.want) {
			t.Fatal("missing dynamic Supabase completion", tc.want, code, stderr)
		}
	}
}
