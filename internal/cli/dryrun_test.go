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

const dryRunPlan = `{"operation":"patchCluster","cluster":{"name":"demo"},"changes":[{"path":"/spec/replicas","before":3,"after":5},{"path":"/spec/config","after":{"enabled":true}}],"effects":[{"kind":"deployment","message":"Resize members"}],"warnings":["Capacity may be limited"]}`

func TestDryRunDispatch(t *testing.T) {
	for _, tt := range []struct {
		name, method, path, body string
		args                     []string
	}{
		{"create", "POST", "/dry-run/clusters", `{"name":"demo","spec":{"provider":"psmdb"}}`, []string{"clusters", "create"}},
		{"replace", "PUT", "/dry-run/clusters/demo", `{"spec":{"provider":"psmdb","version":"8.0"}}`, []string{"clusters", "edit", "demo"}},
		{"patch", "PATCH", "/dry-run/clusters/demo", `{"spec":{"replicas":5,"backup":null}}`, []string{"clusters", "patch", "demo"}},
		{"scale", "PATCH", "/dry-run/clusters/demo", `{"spec":{"replicas":5}}`, []string{"clusters", "scale", "demo"}},
		{"upgrade", "PATCH", "/dry-run/clusters/demo", `{"spec":{"version":"8.0"}}`, []string{"clusters", "upgrade", "demo"}},
		{"restore", "POST", "/dry-run/clusters/demo/restores", `{"name":"recover","source":{"sourceCluster":"demo","backup":"base"}}`, []string{"restores", "create", "demo"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			requests := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests++
				if r.Method != tt.method || r.URL.Path != "/api/v1"+tt.path || r.URL.Query().Get("namespace") != "project" {
					t.Errorf("unexpected request: %s %s", r.Method, r.URL)
				}
				contentType := "application/json"
				if tt.method == "PATCH" {
					contentType = "application/merge-patch+json"
				}
				if r.Header.Get("Content-Type") != contentType {
					t.Errorf("Content-Type = %q", r.Header.Get("Content-Type"))
				}
				var got, want any
				if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
					t.Error(err)
				}
				if err := json.Unmarshal([]byte(tt.body), &want); err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(got, want) {
					t.Errorf("body = %#v, want %#v", got, want)
				}
				w.Header().Set("Content-Type", "application/json")
				fmt.Fprint(w, dryRunPlan)
			}))
			defer server.Close()
			// Restore stdin bodies deliberately omit --yes even with terminal stdin.
			args := append(append([]string{}, tt.args...), "--dry-run", "-f", "-", "-n", "project", "-o", "json")
			code, out, stderr := runCLI(t, server.URL, tt.body, true, args...)
			if code != 0 || stderr != "" || requests != 1 {
				t.Fatalf("code=%d requests=%d stderr=%q out=%q", code, requests, stderr, out)
			}
			var got, want any
			if err := json.Unmarshal([]byte(out), &got); err != nil {
				t.Fatal(err)
			}
			json.Unmarshal([]byte(dryRunPlan), &want)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("result changed: %s", out)
			}
		})
	}
}

func TestDryRunOutput(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, dryRunPlan)
	}))
	defer server.Close()
	for _, format := range []string{"table", "yaml"} {
		t.Run(format, func(t *testing.T) {
			code, out, stderr := runCLI(t, server.URL, `{"spec":{"replicas":5}}`, false, "clusters", "patch", "demo", "-f", "-", "--dry-run", "-o", format)
			if code != 0 || stderr != "" {
				t.Fatalf("code=%d stderr=%q", code, stderr)
			}
			if format == "yaml" {
				var got, want any
				if err := yaml.Unmarshal([]byte(out), &got); err != nil {
					t.Fatal(err)
				}
				if err := yaml.Unmarshal([]byte(dryRunPlan), &want); err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("result changed: %s", out)
				}
				return
			}
			for _, text := range []string{"Changes:", "PATH", "BEFORE → AFTER", "/spec/replicas", "3 → 5", `null → {"enabled":true}`, "Effects:", "KIND", "MESSAGE", "deployment", "Resize members", "Warnings:", "Capacity may be limited"} {
				if !strings.Contains(out, text) {
					t.Errorf("missing %q in %q", text, out)
				}
			}
		})
	}
}

func TestDryRunWaitConflict(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("conflicting flags submitted %s %s", r.Method, r.URL)
	}))
	defer server.Close()
	for _, command := range []string{"create", "edit", "patch", "scale", "upgrade"} {
		t.Run(command, func(t *testing.T) {
			code, _, stderr := runCLI(t, server.URL, "", false, "clusters", command, "demo", "--dry-run", "--wait")
			if code != 2 || !strings.Contains(stderr, "--dry-run cannot be combined with --wait") {
				t.Fatalf("code=%d stderr=%q", code, stderr)
			}
		})
	}
}

func TestDryRunValidationError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnprocessableEntity)
		fmt.Fprint(w, `{"code":"validation_failed","message":"invalid replicas"}`)
	}))
	defer server.Close()
	code, out, stderr := runCLI(t, server.URL, `{"spec":{"replicas":-1}}`, false, "clusters", "patch", "demo", "-f", "-", "--dry-run")
	if code != 6 || out != "" || !strings.Contains(stderr, "invalid replicas") {
		t.Fatalf("code=%d stdout=%q stderr=%q", code, out, stderr)
	}
}

func TestDryRunClone(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == "GET" && r.URL.Path == "/api/v1/clusters/source":
			if r.URL.Query().Get("namespace") != "origin" {
				t.Errorf("source namespace: %s", r.URL)
			}
			fmt.Fprint(w, `{"spec":{"provider":"psmdb","backup":{"storage":"old"}}}`)
		case r.Method == "POST" && r.URL.Path == "/api/v1/dry-run/clusters":
			if r.URL.Query().Get("namespace") != "target" {
				t.Errorf("target namespace: %s", r.URL)
			}
			var body any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Error(err)
			}
			var want any
			json.Unmarshal([]byte(`{"name":"clone","spec":{"provider":"psmdb","dataSource":{"cluster":"source","namespace":"origin","backup":"base"}}}`), &want)
			if !reflect.DeepEqual(body, want) {
				t.Errorf("clone body: %#v", body)
			}
			fmt.Fprint(w, dryRunPlan)
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL)
		}
	}))
	defer server.Close()
	code, out, stderr := runCLI(t, server.URL, "", true, "restores", "create", "source", "--backup", "base", "--source-namespace", "origin", "--into-new", "clone", "-n", "target", "--dry-run")
	if code != 0 || stderr != "" || requests != 2 || !strings.Contains(out, "Changes:") {
		t.Fatalf("code=%d requests=%d stdout=%q stderr=%q", code, requests, out, stderr)
	}
}
