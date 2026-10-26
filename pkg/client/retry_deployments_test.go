// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package client

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRetryClusterDeployments(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" || r.URL.Path != "/api/v1/clusters/orders/retry-deployments" || r.URL.Query().Get("namespace") != "dev" || r.Header.Get("Idempotency-Key") != "rearm" {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(202)
		_, _ = w.Write([]byte(`{"name":"orders","namespace":"dev","generation":2,"spec":{"retryEpoch":1}}`))
	}))
	defer server.Close()
	c, err := NewClientWithResponses(server.URL + "/api/v1")
	if err != nil {
		t.Fatal(err)
	}
	ns, key := "dev", "rearm"
	got, err := c.RetryClusterDeploymentsWithResponse(context.Background(), "orders", &RetryClusterDeploymentsParams{Namespace: &ns, IdempotencyKey: &key})
	if err != nil || got.JSON202 == nil || got.JSON202.Spec.RetryEpoch == nil || *got.JSON202.Spec.RetryEpoch != 1 {
		t.Fatal(got, err)
	}
}
