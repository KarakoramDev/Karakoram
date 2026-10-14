// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package cli

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
)

func TestExplicitZeroShardsRejected(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("invalid shards reached server")
		w.WriteHeader(500)
	}))
	defer server.Close()
	for _, args := range [][]string{
		{"clusters", "create", "sample", "--topology", "sharded", "--version", "8.0.32-14", "--shards", "0"},
		{"clusters", "scale", "sample", "--shards", "0"},
		{"clusters", "scale", "sample", "--shards", "129"},
	} {
		_, _, err := execute(t, server.URL, filepath.Join(t.TempDir(), "config.yaml"), "", args...)
		if err == nil || !strings.Contains(err.Error(), "--shards must be from 1 to 128") {
			t.Fatal(err)
		}
	}
}
