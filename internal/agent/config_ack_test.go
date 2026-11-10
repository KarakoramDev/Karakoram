// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package agent

import (
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
)

func TestConfig304AcknowledgementStopsStaleStampLoop(t *testing.T) {
	a, cfg, _ := fakeNode(t, "/bin/true")
	cfg.DesiredProcesses = []string{}
	cfg.PullRevision = "certified-payload"
	// A full response began while nonsteady but certified the same payload
	// after steady re-entry; only its pre-render notification stamp is stale.
	cfg.ChangeRevision = "before-steady"
	if _, err := a.apply(t.Context(), cfg); err != nil {
		t.Fatal(err)
	}
	var gets atomic.Int32
	remote := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == agentapi.HeartbeatPath {
			var request agentapi.HeartbeatRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Error(err)
			}
			json.NewEncoder(w).Encode(agentapi.HeartbeatResponse{Token: "token", IntervalMillis: 3000, ReloadConfig: request.ConfigRevision != "after-steady"})
			return
		}
		gets.Add(1)
		w.Header().Set("X-Karakoram-Config-Change-Ack", "after-steady")
		w.WriteHeader(http.StatusNotModified)
	}))
	defer remote.Close()
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: remote.Certificate().Raw})
	client, err := agentapi.NewClient(agentapi.ClientOptions{BaseURL: remote.URL, Token: "token", CAPEM: ca})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	a.client = client
	for range 4 {
		a.mu.RLock()
		stamp := a.changeRevision
		a.mu.RUnlock()
		notice, err := client.Heartbeat(t.Context(), agentapi.HeartbeatRequest{Session: "session", ConfigRevision: stamp})
		if err != nil {
			t.Fatal(err)
		}
		if notice.ReloadConfig {
			if _, err := a.reload(t.Context()); err != nil {
				t.Fatal(err)
			}
		}
	}
	if gets.Load() != 1 || a.changeRevision != "after-steady" {
		t.Fatalf("notification-driven config GETs did not stop after safe304 ack: GETs=%d stamp=%q", gets.Load(), a.changeRevision)
	}
}
