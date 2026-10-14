// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package client_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/pkg/client"
)

func TestConsumerWorkflow(t *testing.T) {
	for _, test := range []struct {
		name      string
		listCode  int
		listBody  string
		phase     string
		wantError string
	}{
		{name: "poll to success", listCode: 200, listBody: `{"items":[]}`, phase: "Succeeded"},
		{name: "structured error", listCode: 400, listBody: `{"code":"invalid_request","message":"limit must be between 1 and 500","details":{"field":"limit","reason":"out_of_range"}}`, wantError: "invalid_request: limit must be between 1 and 500 (details: map[field:limit reason:out_of_range])"},
		{name: "terminal failure", listCode: 200, listBody: `{"items":[]}`, phase: "Failed", wantError: "operation op-test Failed: maintenance did not complete"},
		{name: "terminal cancellation", listCode: 200, listBody: `{"items":[]}`, phase: "Cancelled", wantError: "operation op-test Cancelled: maintenance did not complete"},
	} {
		t.Run(test.name, func(t *testing.T) {
			polls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch r.URL.Path {
				case "/api/v1/clusters":
					w.WriteHeader(test.listCode)
					_, _ = w.Write([]byte(test.listBody))
				case "/api/v1/operations/op-test":
					polls++
					if polls == 1 && test.phase == "Succeeded" {
						_, _ = w.Write([]byte(`{"id":"op-test","phase":"Running"}`))
						return
					}
					_, _ = w.Write([]byte(`{"id":"op-test","phase":"` + test.phase + `","message":"maintenance did not complete"}`))
				default:
					t.Errorf("unexpected path %s", r.URL.Path)
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			defer server.Close()
			c, err := client.NewClientWithResponses(server.URL+"/api/v1", client.WithBearerToken("synthetic-test-token"))
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			err = listAndWait(ctx, c, "op-test")
			if test.wantError == "" {
				if err != nil {
					t.Fatal(err)
				}
				if polls != 2 {
					t.Fatalf("operation polled %d times, want 2", polls)
				}
			} else if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("error = %v, want %q", err, test.wantError)
			}
		})
	}
}
