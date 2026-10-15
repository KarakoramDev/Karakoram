// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package cli

import (
	"errors"
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

func TestShardingFlagsRequireShardedTopology(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("replica-set create with sharding flags reached server")
		w.WriteHeader(500)
	}))
	defer server.Close()
	for flag, value := range map[string]string{"--shards": "-1", "--config-servers": "3", "--routers": "2"} {
		_, _, err := execute(t, server.URL, filepath.Join(t.TempDir(), "config.yaml"), "", "clusters", "create", "sample", "--provider", "psmdb", flag, value)
		var cmdErr *commandError
		if !errors.As(err, &cmdErr) || cmdErr.code != 2 || !strings.Contains(err.Error(), flag+" requires --topology sharded") {
			t.Fatalf("%s: %v", flag, err)
		}
	}
}
