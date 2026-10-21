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

func TestControlPlaneBackupCommands(t *testing.T) {
	for _, test := range []struct {
		args         []string
		method, path string
	}{
		{[]string{"backup-policy", "get"}, "GET", "/api/v1/control-plane/backup-policy"},
		{[]string{"backup-policy", "set", "--enabled", "--storage", "ops/archive", "--schedule", "0 4 * * *", "--retain-count", "7"}, "PUT", "/api/v1/control-plane/backup-policy"},
		{[]string{"backups", "list", "--limit", "9", "--cursor", "next"}, "GET", "/api/v1/control-plane/backups"},
		{[]string{"backups", "run"}, "POST", "/api/v1/control-plane/backups"},
	} {
		t.Run(strings.Join(test.args[:2], "-"), func(t *testing.T) {
			calls := 0
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				if r.Method != test.method || r.URL.Path != test.path {
					t.Errorf("unexpected request %s %s", r.Method, r.URL)
				}
				if r.Method == "PUT" {
					var body map[string]any
					if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
						t.Error(err)
					}
					storage, _ := body["storage"].(map[string]any)
					if body["enabled"] != true || body["schedule"] != "0 4 * * *" || body["retainCount"] != float64(7) || storage["namespace"] != "ops" || storage["name"] != "archive" {
						t.Errorf("body: %#v", body)
					}
				}
				if test.args[1] == "list" && (r.URL.Query().Get("limit") != "9" || r.URL.Query().Get("cursor") != "next") {
					t.Errorf("pagination: %s", r.URL)
				}
				w.Header().Set("Content-Type", "application/json")
				if r.Method == "POST" {
					w.WriteHeader(202)
				}
				fmt.Fprint(w, `{"id":"state-one","items":[],"enabled":true}`)
			}))
			defer srv.Close()
			args := append([]string{"control-plane"}, test.args...)
			args = append(args, "-o", "json")
			out, stderr, err := execute(t, srv.URL, filepath.Join(t.TempDir(), "config"), "", args...)
			if err != nil || stderr != "" || !strings.Contains(out, "state-one") || calls != 1 {
				t.Fatalf("calls=%d out=%s stderr=%s err=%v", calls, out, stderr, err)
			}
		})
	}
}

func TestControlPlaneBackupPolicyValidation(t *testing.T) {
	for _, args := range [][]string{{"--storage", "archive", "--retain-count", "0"}, {"--storage", "archive", "--schedule", "bad"}, {"--storage", "bad/ref/name"}, {"--enabled"}} {
		command := append([]string{"control-plane", "backup-policy", "set"}, args...)
		_, _, err := execute(t, "http://127.0.0.1:1", filepath.Join(t.TempDir(), "config"), "", command...)
		if err == nil {
			t.Fatalf("accepted invalid flags %v", args)
		}
	}
}
