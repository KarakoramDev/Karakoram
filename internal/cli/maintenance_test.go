// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package cli

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
)

func TestMaintenanceOverrideFlags(t *testing.T) {
	for _, command := range [][]string{{"restart", "demo"}, {"rotate-tls", "demo"}, {"replace-member", "demo", "demom0"}, {"raise-fcv", "demo", "--version", "8.0"}} {
		t.Run(command[0], func(t *testing.T) {
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				if r.URL.Query().Get("ignoreMaintenanceWindow") != "true" {
					t.Errorf("override missing: %s", r.URL)
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(202)
				fmt.Fprint(w, `{"id":"work","phase":"Pending"}`)
			}))
			defer server.Close()
			args := append([]string{"clusters"}, command...)
			args = append(args, "--ignore-maintenance-window", "--yes") // Confirmation is used only by disruptive commands.
			if command[0] == "restart" {
				args = args[:len(args)-1]
			}
			_, stderr, err := execute(t, server.URL, filepath.Join(t.TempDir(), "config"), "", args...)
			if err != nil || calls != 1 {
				t.Fatal(stderr, err, calls)
			}
		})
	}
}
func TestMaintenanceCreateFlags(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		spec, _ := body["spec"].(map[string]any)
		policy, _ := spec["maintenance"].(map[string]any)
		if policy["timeZone"] != "Europe/Prague" {
			t.Error(policy)
		}
		windows, _ := policy["windows"].([]any)
		if len(windows) != 2 {
			t.Error(windows)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(201)
		fmt.Fprint(w, `{"name":"demo","status":{"phase":"Creating"}}`)
	}))
	defer server.Close()
	_, stderr, err := execute(t, server.URL, filepath.Join(t.TempDir(), "config"), "", "clusters", "create", "demo", "--maintenance-time-zone", "Europe/Prague", "--maintenance-window", "0 2 * * 0;2h", "--maintenance-window", "0 23 * * 3;45m")
	if err != nil {
		t.Fatal(stderr, err)
	}
}
