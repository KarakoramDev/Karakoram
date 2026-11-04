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

func TestMaintenanceOverrideWithRequestFile(t *testing.T) {
	for _, tc := range []struct {
		command, method, path, body string
	}{
		{"edit", "PUT", "/api/v1/clusters/demo", `{"spec":{"version":"8.0","engineOptions":{"custom":true},"optional":null,"members":[]}}`},
		{"patch", "PATCH", "/api/v1/clusters/demo", `{"spec":{"version":"8.0","engineOptions":{"custom":true},"optional":null,"members":[]}}`},
		{"scale", "PATCH", "/api/v1/clusters/demo", `{"spec":{"components":{"engine":{"replicas":5}}}}`},
		{"upgrade", "PATCH", "/api/v1/clusters/demo", `{"spec":{"version":"8.0"}}`},
		{"raise-fcv", "POST", "/api/v1/clusters/demo/raise-fcv", `{"version":"8.0"}`},
		{"upgrade-agents", "POST", "/api/v1/clusters/demo/upgrade-agents", `{"includeCurrent":true}`},
	} {
		t.Run(tc.command, func(t *testing.T) {
			var want map[string]any
			if err := json.Unmarshal([]byte(tc.body), &want); err != nil {
				t.Fatal(err)
			}
			for _, source := range []string{"stdin", "file"} {
				for _, override := range []string{"true", "false", ""} {
					t.Run(source+"/override="+override, func(t *testing.T) {
						requests := 0
						server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
							requests++
							if r.Method != tc.method || r.URL.Path != tc.path {
								t.Errorf("request: %s %s", r.Method, r.URL)
							}
							query := r.URL.Query()
							if query.Get("ignoreMaintenanceWindow") != override || query.Has("ignoreMaintenanceWindow") != (override != "") {
								t.Errorf("override query: %s", r.URL)
							}
							if query.Get("namespace") != "targetns" {
								t.Errorf("namespace query: %s", r.URL)
							}
							var got map[string]any
							if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
								t.Error(err)
							}
							if !reflect.DeepEqual(got, want) {
								t.Errorf("body changed: got %#v, want %#v", got, want)
							}
							w.Header().Set("Content-Type", "application/json")
							w.WriteHeader(http.StatusAccepted)
							fmt.Fprint(w, `{"name":"demo","id":"work","phase":"Pending"}`)
						}))
						defer server.Close()
						file := "-"
						input := tc.body
						if source == "file" {
							file = filepath.Join(t.TempDir(), "body.json")
							if err := os.WriteFile(file, []byte(tc.body), 0600); err != nil {
								t.Fatal(err)
							}
							input = ""
						}
						args := []string{"clusters", tc.command, "demo", "-f", file, "-n", "targetns"}
						if override == "true" {
							args = append(args, "--ignore-maintenance-window")
						} else if override == "false" {
							args = append(args, "--ignore-maintenance-window=false")
						}
						if tc.command == "raise-fcv" {
							args = append(args, "--yes")
						}
						code, _, stderr := runCLI(t, server.URL, input, false, args...)
						if code != 0 || requests != 1 {
							t.Fatalf("code=%d requests=%d stderr=%q", code, requests, stderr)
						}
					})
				}
			}
		})
	}
}

func TestMaintenanceOverrideWithFileStillRejectsBodyFlags(t *testing.T) {
	for _, tc := range []struct {
		command string
		flags   []string
	}{
		{"edit", []string{"--maintenance-time-zone", "UTC"}},
		{"patch", []string{"--maintenance-window", "0 2 * * 0;2h"}},
		{"scale", []string{"--replicas", "5"}},
		{"upgrade", []string{"--version", "8.0"}},
		{"raise-fcv", []string{"--version", "8.0", "--yes"}},
		{"upgrade-agents", []string{"--include-current"}},
	} {
		t.Run(tc.command, func(t *testing.T) {
			requests := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests++
				t.Errorf("conflicting flags reached API: %s %s", r.Method, r.URL)
			}))
			defer server.Close()
			args := append([]string{"clusters", tc.command, "demo", "-f", "-", "--ignore-maintenance-window"}, tc.flags...)
			code, _, stderr := runCLI(t, server.URL, "not JSON", false, args...)
			if code != 2 || requests != 0 || !strings.Contains(stderr, "--file cannot be combined with "+tc.flags[0]) {
				t.Fatalf("code=%d requests=%d stderr=%q", code, requests, stderr)
			}
			if strings.Contains(stderr, "--file cannot be combined with --ignore-maintenance-window") || strings.Contains(stderr, ", --ignore-maintenance-window") {
				t.Fatalf("query control included in body conflict hint: %q", stderr)
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
