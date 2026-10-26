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

func TestClustersRetryDeployments(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Method != "POST" || r.URL.Path != "/api/v1/clusters/orders/retry-deployments" || r.URL.Query().Get("namespace") != "dev" {
			t.Errorf("unexpected request %s %s", r.Method, r.URL)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(202)
		fmt.Fprint(w, `{"name":"orders","namespace":"dev","generation":2,"spec":{"retryEpoch":1},"status":{"phase":"Updating"}}`)
	}))
	defer server.Close()
	for _, format := range []string{"json", "yaml"} {
		out, stderr, err := execute(t, server.URL, filepath.Join(t.TempDir(), "config"), "", "clusters", "retry-deployments", "orders", "--namespace", "dev", "-o", format)
		if err != nil || stderr != "" || !strings.Contains(out, "orders") || !strings.Contains(out, "retryEpoch") {
			t.Fatal(out, stderr, err)
		}
	}
	if calls != 2 {
		t.Fatal(calls)
	}
}

func TestClustersRetryDeploymentsWait(t *testing.T) {
	for _, observed := range []int{1, 2} {
		t.Run(fmt.Sprintf("observed-%d", observed), func(t *testing.T) {
			var calls []string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls = append(calls, r.Method+" "+r.URL.Path)
				if strings.Contains(r.URL.Path, "/clusters/") && r.URL.Query().Get("namespace") != "dev" {
					t.Errorf("missing namespace: %s", r.URL)
				}
				w.Header().Set("Content-Type", "application/json")
				switch r.Method + " " + r.URL.Path {
				case "POST /api/v1/clusters/orders/retry-deployments":
					w.Header().Set("Karakoram-Operation-Id", "retry-one")
					w.WriteHeader(http.StatusAccepted)
					fmt.Fprint(w, `{"name":"orders","namespace":"dev","generation":2,"spec":{"retryEpoch":1},"status":{"phase":"Updating"}}`)
				case "GET /api/v1/operations/retry-one":
					fmt.Fprint(w, `{"id":"retry-one","phase":"Succeeded"}`)
				case "GET /api/v1/clusters/orders":
					fmt.Fprintf(w, `{"name":"orders","namespace":"dev","generation":2,"status":{"phase":"Ready","observedGeneration":%d}}`, observed)
				default:
					t.Errorf("unexpected request: %s %s", r.Method, r.URL)
					http.NotFound(w, r)
				}
			}))
			defer server.Close()
			code, out, stderr := runCLI(t, server.URL, "", false, "clusters", "retry-deployments", "orders", "--namespace", "dev", "--wait", "--timeout", "50ms")
			wantCode := 0
			if observed < 2 {
				wantCode = 7
			}
			if code != wantCode || !strings.Contains(stderr, "operation retry-one") || !strings.Contains(stderr, fmt.Sprintf("generation %d/2", observed)) {
				t.Fatalf("code=%d stdout=%q stderr=%q", code, out, stderr)
			}
			want := []string{"POST /api/v1/clusters/orders/retry-deployments", "GET /api/v1/operations/retry-one", "GET /api/v1/clusters/orders"}
			if strings.Join(calls, "\n") != strings.Join(want, "\n") {
				t.Fatalf("requests=%v; want %v", calls, want)
			}
		})
	}
}
