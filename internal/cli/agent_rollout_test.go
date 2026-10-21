// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package cli

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestAgentRolloutCommands(t *testing.T) {
	for _, tc := range []struct {
		args         []string
		method, path string
		body         map[string]any
		override     string
	}{
		{[]string{"clusters", "upgrade-agents", "demo"}, "POST", "/api/v1/clusters/demo/upgrade-agents", map[string]any{"includeCurrent": false}, ""},
		{[]string{"clusters", "upgrade-agents", "demo", "--include-current", "--ignore-maintenance-window"}, "POST", "/api/v1/clusters/demo/upgrade-agents", map[string]any{"includeCurrent": true}, "true"},
		{[]string{"clusters", "upgrade", "demo", "--version", "8.0"}, "PATCH", "/api/v1/clusters/demo", map[string]any{"spec": map[string]any{"version": "8.0"}}, ""},
		{[]string{"settings", "agent-rollout", "get"}, "GET", "/api/v1/settings/agent-rollout", nil, ""},
		{[]string{"settings", "agent-rollout", "set", "--automatic=true", "--max-concurrent-clusters", "5"}, "PUT", "/api/v1/settings/agent-rollout", map[string]any{"automatic": true, "maxConcurrentClusters": float64(5)}, ""},
		{[]string{"settings", "agent-rollout", "set", "--automatic=false"}, "PUT", "/api/v1/settings/agent-rollout", map[string]any{"automatic": false, "maxConcurrentClusters": float64(1)}, ""},
	} {
		t.Run(strings.Join(tc.args, " "), func(t *testing.T) {
			called := false
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				called = true
				if r.Method != tc.method || r.URL.Path != tc.path || r.URL.Query().Get("ignoreMaintenanceWindow") != tc.override {
					t.Errorf("request %s %s", r.Method, r.URL)
				}
				if strings.HasPrefix(tc.path, "/api/v1/clusters/") && r.URL.Query().Get("namespace") != "project" {
					t.Errorf("namespace missing: %s", r.URL)
				}
				if tc.body != nil {
					var body map[string]any
					if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
						t.Error(err)
					}
					if !reflect.DeepEqual(body, tc.body) {
						t.Errorf("body %#v want %#v", body, tc.body)
					}
				}
				w.Header().Set("Content-Type", "application/json")
				if tc.method == "POST" {
					w.WriteHeader(http.StatusAccepted)
				}
				fmt.Fprint(w, `{"id":"rollout-one"}`)
			}))
			defer server.Close()
			args := append([]string{"-n", "project"}, tc.args...)
			code, out, stderr := runCLI(t, server.URL, "", false, args...)
			if code != 0 || !called {
				t.Fatalf("code=%d out=%q err=%q called=%v", code, out, stderr, called)
			}
		})
	}
}

func TestAgentRolloutFlags(t *testing.T) {
	for _, args := range [][]string{
		{"settings", "agent-rollout", "set", "--max-concurrent-clusters", "0"},
		{"settings", "agent-rollout", "set", "--max-concurrent-clusters", "6"},
		{"clusters", "upgrade-agents", "demo", "--version", "8.0"},
		{"clusters", "upgrade", "demo"},
	} {
		code, _, _ := runCLI(t, "http://127.0.0.1:1", "", false, args...)
		if code != 2 {
			t.Errorf("%v: code=%d want usage error", args, code)
		}
	}
}

func TestAgentRolloutWait(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == "POST":
			w.WriteHeader(http.StatusAccepted)
			fmt.Fprint(w, `{"id":"rollout-one","type":"upgrade-agents","phase":"Pending"}`)
		case strings.Contains(r.URL.Path, "/operations/"):
			fmt.Fprint(w, `{"id":"rollout-one","phase":"Succeeded"}`)
		default:
			fmt.Fprint(w, `{"name":"demo","generation":1,"status":{"phase":"Ready","observedGeneration":1}}`)
		}
	}))
	defer server.Close()
	code, out, stderr := runCLI(t, server.URL, "", false, "clusters", "upgrade-agents", "demo", "--wait", "--timeout", "1s")
	if code != 0 || !strings.Contains(out, "Ready") || !strings.Contains(stderr, "Accepted agent rollout") {
		t.Fatalf("code=%d out=%q err=%q", code, out, stderr)
	}
	if mutationDescription("upgrade-agents") != "agent rollout" {
		t.Fatal("missing operation kind label")
	}
}

func TestAgentIdentityOutput(t *testing.T) {
	const member = `{"name":"demo-0","agent":{"version":"v1.2.3","commit":"abc","build":"a-long-agent-build-identity-that-must-not-be-summarized"}}`
	const status = `{"phase":"Ready","agents":{"targetBuild":"new-build","targetVersion":"v1.2.4","outdated":["demo-0"],"unknown":["demo-1"]},"members":[` + member + `]}`
	for _, tc := range []struct {
		args     []string
		document string
	}{
		{[]string{"clusters", "get", "demo"}, `{"name":"demo","status":` + status + `}`},
		{[]string{"clusters", "status", "demo"}, status},
		{[]string{"clusters", "members", "demo"}, `{"items":[` + member + `]}`},
		{[]string{"clusters", "members", "demo", "demo-0"}, member},
	} {
		t.Run(strings.Join(tc.args, " "), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				fmt.Fprint(w, tc.document)
			}))
			defer server.Close()
			code, out, stderr := runCLI(t, server.URL, "", false, tc.args...)
			if code != 0 {
				t.Fatalf("code=%d err=%q", code, stderr)
			}
			for _, text := range []string{"v1.2.3", "a-long-agent-build-identity-that-must-not-be-summarized"} {
				if !strings.Contains(out, text) {
					t.Errorf("missing %q: %s", text, out)
				}
			}
			if tc.args[1] == "get" {
				for _, text := range []string{"outdated", "unknown", "demo-1", "new-build"} {
					if !strings.Contains(out, text) {
						t.Errorf("missing %q: %s", text, out)
					}
				}
			}
			var expected any
			if err := json.Unmarshal([]byte(tc.document), &expected); err != nil {
				t.Fatal(err)
			}
			for _, format := range []string{"json", "yaml"} {
				args := append(append([]string{}, tc.args...), "-o", format)
				code, out, stderr := runCLI(t, server.URL, "", false, args...)
				if code != 0 {
					t.Fatalf("%s: %s", format, stderr)
				}
				var actual any
				var err error
				if format == "json" {
					err = json.Unmarshal([]byte(out), &actual)
				} else {
					err = yaml.Unmarshal([]byte(out), &actual)
				}
				if err != nil || !reflect.DeepEqual(expected, actual) {
					t.Errorf("%s document changed: %s (%v)", format, out, err)
				}
			}
		})
	}
}
