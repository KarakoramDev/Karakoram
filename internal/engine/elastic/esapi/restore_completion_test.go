// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package esapi

import (
	"errors"
	"fmt"
	"net/http"
	"testing"
)

func TestRestoreRequiresPositiveNativeCompletion(t *testing.T) {
	for _, tc := range []struct {
		name, response string
		ok, failed     bool
	}{
		{"accepted-only", `{"accepted":true}`, false, false},
		{"missing-shard-result", `{"snapshot":{}}`, false, false},
		{"aborted-shard", `{"snapshot":{"shards":{"total":2,"successful":1,"failed":1}}}`, false, true},
		{"incomplete-shards", `{"snapshot":{"shards":{"total":2,"successful":1,"failed":0}}}`, false, false},
		{"completed", `{"snapshot":{"shards":{"total":2,"successful":2,"failed":0}}}`, true, false},
		{"empty-snapshot-completed", `{"snapshot":{"shards":{"total":0,"successful":0,"failed":0}}}`, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, _ := testClient(t, func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Query().Get("wait_for_completion") != "true" {
					t.Error("restore did not request native completion")
				}
				w.Header().Set("Content-Type", "application/json")
				fmt.Fprint(w, tc.response)
			})
			err := c.RestoreSnapshot(t.Context(), "repo", "snapshot", RestoreRequest{Indices: "*"})
			if (err == nil) != tc.ok || errors.Is(err, ErrRestoreFailed) != tc.failed {
				t.Fatalf("native completion result: %v", err)
			}
		})
	}
}
