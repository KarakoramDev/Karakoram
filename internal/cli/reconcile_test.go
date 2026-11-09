// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package cli

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
)

func TestClustersReconcile(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Method != "POST" || r.URL.Path != "/api/v1/clusters/orders/reconcile" || r.URL.Query().Get("namespace") != "dev" {
			t.Errorf("unexpected request %s %s", r.Method, r.URL)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		fmt.Fprint(w, `{"name":"orders","namespace":"dev","generation":2,"status":{"phase":"Ready"}}`)
	}))
	defer server.Close()
	for _, format := range []string{"json", "yaml"} {
		out, stderr, err := execute(t, server.URL, filepath.Join(t.TempDir(), "config"), "", "clusters", "reconcile", "orders", "--namespace", "dev", "-o", format)
		if err != nil || stderr != "" || !strings.Contains(out, "orders") {
			t.Fatal(out, stderr, err)
		}
	}
	if calls != 2 {
		t.Fatal(calls)
	}
	_, _, err := execute(t, server.URL, filepath.Join(t.TempDir(), "config"), "", "clusters", "reconcile", "orders", "--wait")
	if err == nil || !strings.Contains(err.Error(), "unknown flag: --wait") || calls != 2 {
		t.Fatal("reconcile must not offer operation-based waiting", calls, err)
	}
}
