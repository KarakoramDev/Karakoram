// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package agent

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestHeartbeatConfigNotificationPullsBeforeMinute(t *testing.T) {
	a, c, _ := fakeNode(t, "/bin/true")
	c.DesiredProcesses = []string{}
	c.PullRevision = "old"
	c.ChangeRevision = "old"
	if _, err := a.apply(t.Context(), c); err != nil {
		t.Fatal(err)
	}
	var gets, posts atomic.Int32
	remote := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == agentapi.HeartbeatPath {
			posts.Add(1)
			var request agentapi.HeartbeatRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Error(err)
			}
			json.NewEncoder(w).Encode(agentapi.HeartbeatResponse{Token: "token", IntervalMillis: 3000, ReloadConfig: request.ConfigRevision != "new"})
			return
		}
		gets.Add(1)
		newer := c
		newer.PullRevision = "new"
		newer.ChangeRevision = "new"
		newer.EngineSpec = json.RawMessage(`{"port":1235}`)
		json.NewEncoder(w).Encode(newer)
	}))
	defer remote.Close()
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: remote.Certificate().Raw})
	client, err := agentapi.NewClient(agentapi.ClientOptions{BaseURL: remote.URL, Token: "token", CAPEM: ca})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	a.client = client
	waits := 0
	a.heartbeatLoop(t.Context(), a.handler(), func(ctx context.Context, interval time.Duration) bool {
		waits++
		if waits > 1 {
			return false
		}
		if interval != 3*time.Second {
			t.Fatal("notification waits for wrong interval", interval)
		}
		deadline := time.NewTimer(3 * time.Second)
		defer deadline.Stop()
		tick := time.NewTicker(time.Millisecond)
		defer tick.Stop()
		for {
			a.mu.RLock()
			revision := a.changeRevision
			a.mu.RUnlock()
			if revision == "new" {
				return true
			}
			select {
			case <-ctx.Done():
				return false
			case <-deadline.C:
				t.Error("notified config waited for minute ticker")
				return false
			case <-tick.C:
			}
		}
	})
	if gets.Load() != 1 || posts.Load() != 2 || a.pullRevision != "new" {
		t.Fatal("notification was not promptly coalesced", gets.Load(), posts.Load(), a.pullRevision)
	}
}
