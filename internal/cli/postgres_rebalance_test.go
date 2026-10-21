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

func TestPostgresRebalance(t *testing.T) {
	for _, strategy := range []string{"", "by_shard_count"} {
		t.Run(strategy, func(t *testing.T) {
			submitted, waited := false, false
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch r.URL.Path {
				case "/api/v1/clusters/distdb/actions/rebalance":
					if r.Method != http.MethodPost || r.URL.Query().Get("namespace") != "dev" {
						t.Errorf("unexpected request %s %s", r.Method, r.URL)
					}
					var body map[string]string
					if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
						t.Error(err)
					}
					if body["strategy"] != strategy {
						t.Errorf("body=%v strategy=%q", body, strategy)
					}
					submitted = true
					w.WriteHeader(http.StatusAccepted)
					fmt.Fprint(w, `{"id":"rebalance-one","type":"rebalance","phase":"Pending","cluster":"distdb"}`)
				case "/api/v1/operations/rebalance-one":
					waited = true
					fmt.Fprint(w, `{"id":"rebalance-one","type":"rebalance","phase":"Succeeded","cluster":"distdb"}`)
				default:
					t.Errorf("unexpected request %s", r.URL)
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			defer server.Close()
			args := []string{"-n", "dev", "postgres", "rebalance", "distdb", "--wait", "--timeout", "1s"}
			if strategy != "" {
				args = append(args, "--strategy", strategy)
			}
			_, _, err := execute(t, server.URL, filepath.Join(t.TempDir(), "config.yaml"), "", args...)
			if err != nil {
				t.Fatal(err)
			}
			if !submitted || !waited {
				t.Fatalf("submitted=%t waited=%t", submitted, waited)
			}
		})
	}
}
