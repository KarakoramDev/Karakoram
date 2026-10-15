//go:build e2e

// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package e2e

import (
	"net/http"
	"net/http/httptest"
	"testing"

	api "github.com/KarakoramDev/Karakoram/pkg/client"
)

func TestElasticDeletedBackupInventoryReturnsNotFound(t *testing.T) {
	t.Setenv("KARAKORAM_E2E_EVIDENCE_DIR", "")
	seen := map[string]bool{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen[r.URL.Path] = true
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()
	client, err := api.NewClient(server.URL + "/api/v1/")
	if err != nil {
		t.Fatal(err)
	}
	f := &elasticFixture{name: "deleted", s: &suite{t: t, ctx: t.Context(), client: client}}
	f.assertBackupsDeleted(map[string]api.Backup{"saved": {Name: "saved"}})
	for _, path := range []string{"/api/v1/clusters/deleted/backups/saved", "/api/v1/clusters/deleted/backups"} {
		if !seen[path] {
			t.Fatalf("deletion assertion did not check %s", path)
		}
	}
}
