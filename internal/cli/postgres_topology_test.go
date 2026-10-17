// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package cli

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
)

func TestPostgresTopologyFlags(t *testing.T) {
	for _, topology := range []string{"single", "ha"} {
		t.Run(topology, func(t *testing.T) {
			seen := false
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != "POST" || r.URL.Path != "/api/v1/clusters" {
					t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
				}
				var body struct {
					Spec struct {
						Provider string `json:"provider"`
						Topology struct {
							Type string `json:"type"`
						} `json:"topology"`
					} `json:"spec"`
				}
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Error(err)
				}
				if body.Spec.Provider != "postgres" || body.Spec.Topology.Type != topology {
					t.Error("topology flag not preserved")
				}
				seen = true
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusAccepted)
				w.Write([]byte(`{"operationId":"operation"}`))
			}))
			defer server.Close()
			var output bytes.Buffer
			command := NewCommand(Options{ConfigPath: filepath.Join(t.TempDir(), "config.yaml"), In: strings.NewReader(""), Out: &output, Err: &output})
			command.SetArgs([]string{"--server", server.URL, "--token", "test-token", "-o", "json", "clusters", "create", "example", "--provider", "postgres", "--topology", topology})
			if err := command.ExecuteContext(t.Context()); err != nil {
				t.Fatal(err)
			}
			if !seen {
				t.Fatal("create request not submitted")
			}
		})
	}
}
