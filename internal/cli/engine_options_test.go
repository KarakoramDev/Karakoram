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
	"testing"
)

func TestProviderNeutralCreate(t *testing.T) {
	var body map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" || r.URL.Path != "/api/v1/clusters" {
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(201)
		fmt.Fprint(w, `{"name":"demo","status":{"phase":"Creating"}}`)
	}))
	defer server.Close()
	file := filepath.Join(t.TempDir(), "options.json")
	if err := os.WriteFile(file, []byte(`{"nested":{"value":2}}`), 0600); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name     string
		flags    []string
		topology string
		replicas float64
		options  bool
	}{
		{"provider defaults", nil, "", 0, false},
		{"generic flags", []string{"--topology", "cluster", "--replicas", "5", "--engine-options", `{"nested":{"value":2}}`}, "cluster", 5, true},
		{"options file", []string{"--engine-options", "@" + file}, "", 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			args := append([]string{"clusters", "create", "demo", "--provider", "fake", "--version", "1.0"}, tc.flags...)
			if _, _, err := execute(t, server.URL+"/api/v1", filepath.Join(t.TempDir(), "config.yaml"), "", args...); err != nil {
				t.Fatal(err)
			}
			spec := body["spec"].(map[string]any)
			topology := spec["topology"].(map[string]any)
			if tc.topology == "" {
				if len(topology) != 0 {
					t.Fatal("Mongo topology defaults leaked", topology)
				}
			} else if topology["type"] != tc.topology {
				t.Fatal(topology)
			}
			components := spec["components"].(map[string]any)
			if len(components) != 1 {
				t.Fatal("Mongo components leaked", components)
			}
			engine := components["engine"].(map[string]any)
			if tc.replicas == 0 {
				if len(engine) != 0 {
					t.Fatal("Mongo component defaults leaked", engine)
				}
			} else if engine["replicas"] != tc.replicas || len(engine) != 1 {
				t.Fatal(engine)
			}
			if tc.options && nested(spec, "engineOptions", "nested", "value") != float64(2) {
				t.Fatal(spec)
			}
		})
	}
}

func TestEngineOptionsRequiresObject(t *testing.T) {
	for _, options := range []string{"[]", "null", "true", "{broken", "@missing-options.json"} {
		if _, _, err := execute(t, "https://example.invalid/api/v1", filepath.Join(t.TempDir(), "config.yaml"), "", "clusters", "create", "demo", "--version", "8.0", "--engine-options", options); err == nil {
			t.Fatalf("accepted %q", options)
		}
	}
}
