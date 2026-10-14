// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package cli

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/KarakoramDev/Karakoram/pkg/client"
)

func TestOperationCancellationDispatch(t *testing.T) {
	called := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		if r.Method != "POST" || r.URL.Path != "/api/v1/operations/task/cancel" {
			t.Errorf("unexpected route %s %s", r.Method, r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"id":"task","phase":"Cancelled"}`))
	}))
	defer server.Close()
	client, err := client.NewClient(server.URL + "/api/v1")
	if err != nil {
		t.Fatal(err)
	}
	response, err := dispatch(context.Background(), client, client, "cancelOperation", []string{"task"}, map[string]any{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if !called || response.StatusCode != http.StatusAccepted {
		t.Fatal(called, response.StatusCode)
	}
}
