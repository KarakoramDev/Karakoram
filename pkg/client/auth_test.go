// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package client_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/pkg/client"
)

func TestWithBearerToken(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer synthetic-test-token" {
			t.Errorf("Authorization = %q", got)
		}
		if got := r.Header.Get("X-Custom"); got != "preserved" {
			t.Errorf("X-Custom = %q", got)
		}
		if r.URL.Path != "/api/v1/clusters" {
			t.Errorf("path = %q", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"items":[]}`))
	}))
	defer server.Close()
	c, err := client.NewClientWithResponses(server.URL+"/api/v1",
		client.WithRequestEditorFn(func(_ context.Context, req *http.Request) error {
			req.Header.Set("Authorization", "Bearer replaced")
			req.Header.Set("X-Custom", "preserved")
			return nil
		}),
		client.WithBearerToken("synthetic-test-token"))
	if err != nil {
		t.Fatal(err)
	}
	response, err := c.ListClustersWithResponse(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if response.JSON200 == nil || len(response.JSON200.Items) != 0 {
		t.Fatalf("unexpected cluster response: %+v", response)
	}
}

func TestNoInternalDependencies(t *testing.T) {
	// Check the transitive production dependency graph, not only direct imports.
	output, err := exec.Command("go", "list", "-deps", "-f", "{{.ImportPath}}", ".").CombinedOutput()
	if err != nil {
		t.Fatalf("go list -deps: %v\n%s", err, output)
	}
	for _, dependency := range strings.Fields(string(output)) {
		if strings.HasPrefix(dependency, "github.com/KarakoramDev/Karakoram/internal/") {
			t.Errorf("public client depends on private package %s", dependency)
		}
	}
}

func TestMetricsExample(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/clusters/demo/metrics" {
			t.Errorf("path = %q", r.URL.Path)
		}
		for key, want := range map[string]string{"namespace": "default", "range": "1h", "step": "30s"} {
			if got := r.URL.Query().Get(key); got != want {
				t.Errorf("%s = %q, want %q", key, got, want)
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"series":[]}`))
	}))
	defer server.Close()
	c, err := client.NewClientWithResponses(server.URL + "/api/v1")
	if err != nil {
		t.Fatal(err)
	}
	metrics, err := clusterMetrics(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	if metrics.Series == nil || len(metrics.Series) != 0 {
		t.Fatalf("unexpected metrics response: %+v", metrics)
	}
}
