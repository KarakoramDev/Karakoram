// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package agentapi

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHeartbeatClient(t *testing.T) {
	for _, code := range []int{http.StatusOK, http.StatusNotFound, http.StatusServiceUnavailable} {
		t.Run(http.StatusText(code), func(t *testing.T) {
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPost || r.URL.Path != HeartbeatPath || !AuthenticateBearer(r, "test-bearer") {
					t.Error("invalid heartbeat request")
				}
				var request HeartbeatRequest
				if err := json.NewDecoder(r.Body).Decode(&request); err != nil || request.Session != "session" {
					t.Errorf("invalid request: %+v, %v", request, err)
				}
				w.WriteHeader(code)
				if code == http.StatusOK {
					_ = json.NewEncoder(w).Encode(HeartbeatResponse{Token: "reply", IntervalMillis: 3000})
				}
			}))
			defer server.Close()
			ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
			client, err := NewClient(ClientOptions{BaseURL: server.URL, Token: "test-bearer", CAPEM: ca})
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			response, err := client.Heartbeat(context.Background(), HeartbeatRequest{Session: "session"})
			if code == http.StatusOK {
				if err != nil || response.Token != "reply" || response.IntervalMillis != 3000 {
					t.Fatalf("response=%+v, error=%v", response, err)
				}
			} else {
				var remote *HTTPError
				if !errors.As(err, &remote) || remote.StatusCode != code {
					t.Fatalf("expected HTTPError %d, got %v", code, err)
				}
			}
		})
	}
}

func TestHeartbeatClientBoundsReply(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"token":"`))
		chunk := strings.Repeat("x", 1<<20)
		for range 17 {
			if _, err := w.Write([]byte(chunk)); err != nil {
				return
			}
		}
		_, _ = w.Write([]byte(`"}`))
	}))
	defer server.Close()
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	client, err := NewClient(ClientOptions{BaseURL: server.URL, Token: "test-bearer", CAPEM: ca})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if _, err := client.Heartbeat(context.Background(), HeartbeatRequest{Session: "session"}); err == nil {
		t.Fatal("accepted oversized reply")
	}
}
