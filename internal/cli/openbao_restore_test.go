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

func TestOpenBaoClonePreservesWriteOnlyShares(t *testing.T) {
	for _, translate := range []bool{false, true} {
		t.Run(map[bool]string{false: "cluster-file", true: "restore-into-new"}[translate], func(t *testing.T) {
			called := false
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if r.Method == http.MethodGet {
					fmt.Fprint(w, `{"name":"bao","spec":{"provider":"openbao","version":"2.7.1","topology":{"type":"cluster"},"components":{"engine":{"replicas":1}}}}`)
					return
				}
				called = true
				var body map[string]any
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Error(err)
				}
				shares, ok := body["openbaoUnsealShares"].(map[string]any)
				if !ok || shares["threshold"] != float64(1) {
					t.Error("write-only shares were dropped", body)
				}
				spec, _ := body["spec"].(map[string]any)
				if _, exists := spec["openbaoUnsealShares"]; exists {
					t.Error("shares moved into persisted spec")
				}
				fmt.Fprint(w, `{"name":"cloned"}`)
			}))
			defer server.Close()
			body := `{"name":"cloned","spec":{"provider":"openbao","dataSource":{"cluster":"bao","backup":"snapshot"}},"openbaoUnsealShares":{"shares":["private-share"],"threshold":1}}`
			args := []string{"clusters", "create", "-f", "-"}
			if translate {
				body = `{"name":"restore","source":{"backup":"snapshot"},"openbaoUnsealShares":{"shares":["private-share"],"threshold":1}}`
				args = []string{"restores", "create", "bao", "--into-new", "cloned", "-f", "-"}
			}
			if _, _, err := execute(t, server.URL, filepath.Join(t.TempDir(), "config.yaml"), body, args...); err != nil {
				t.Fatal(err)
			}
			if !called {
				t.Fatal("no creation request")
			}
		})
	}
}
