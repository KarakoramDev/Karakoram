// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package cli

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestOAuthGrantCommands(t *testing.T) {
	for _, command := range []string{"list", "delete"} {
		t.Run(command, func(t *testing.T) {
			requests := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests++
				if r.Header.Get("Authorization") != "Bearer management-token" || r.URL.RawQuery != "" {
					t.Errorf("unexpected authentication or namespace query: %s %v", r.URL, r.Header)
				}
				if command == "list" {
					if r.Method != http.MethodGet || r.URL.Path != "/api/v1/oauth-grants" {
						t.Errorf("list request: %s %s", r.Method, r.URL)
					}
					w.Header().Set("Content-Type", "application/json")
					fmt.Fprint(w, `{"enabled":true,"items":[{"id":"grant-one","clientName":"Agent"}]}`)
				} else {
					if r.Method != http.MethodDelete || r.URL.Path != "/api/v1/oauth-grants/grant-one" {
						t.Errorf("delete request: %s %s", r.Method, r.URL)
					}
					w.WriteHeader(http.StatusNoContent)
				}
			}))
			defer server.Close()
			args := []string{"--token", "management-token", "--namespace", "dev", "oauth-grants", command}
			if command == "delete" {
				args = append(args, "grant-one", "--yes")
			} else {
				args = append(args, "-o", "json")
			}
			code, out, stderr := runCLI(t, server.URL, "", false, args...)
			if code != 0 || requests != 1 {
				t.Fatalf("exit=%d requests=%d stdout=%s stderr=%s", code, requests, out, stderr)
			}
			if command == "list" && !strings.Contains(out, "grant-one") {
				t.Fatalf("grant list missing from output: %s", out)
			}
		})
	}
}

func TestOAuthGrantDeleteConfirmation(t *testing.T) {
	for _, tc := range []struct {
		name, input            string
		tty                    bool
		wantRequests, wantCode int
	}{
		{"noninteractive-empty", "", false, 0, 2},
		{"noninteractive-decline", "no\n", false, 0, 1},
		{"terminal-name", "grant-one\n", true, 1, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			requests := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { requests++; w.WriteHeader(http.StatusNoContent) }))
			defer server.Close()
			code, _, stderr := runCLI(t, server.URL, tc.input, tc.tty, "oauth-grants", "delete", "grant-one")
			if code != tc.wantCode || requests != tc.wantRequests {
				t.Fatalf("exit=%d requests=%d stderr=%s", code, requests, stderr)
			}
			if tc.tty && !strings.Contains(stderr, "Type the name to confirm") {
				t.Fatalf("missing terminal confirmation: %s", stderr)
			}
		})
	}
}
