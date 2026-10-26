// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package agent

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
)

func TestRecoveryCPOutagePreservesLocalRestart(t *testing.T) {
	a, c, _ := fakeNode(t, buildFake(t))
	a.sup.backoff = 10 * time.Millisecond
	c.ConfigVersion = 2
	if _, err := a.apply(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	pid := workerState(a).PID
	var online atomic.Bool
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !online.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		json.NewEncoder(w).Encode(c)
	}))
	defer server.Close()
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	client, err := agentapi.NewClient(agentapi.ClientOptions{BaseURL: server.URL, Token: "token", CAPEM: ca})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	a.client = client
	if _, err := a.reload(context.Background()); err == nil {
		t.Fatal("outage not observed")
	}
	if workerState(a).PID != pid || a.config().ConfigVersion != 2 {
		t.Fatal("failed pull changed running intent")
	}
	if err := syscall.Kill(pid, syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		state := workerState(a)
		if state.PID != 0 && state.PID != pid {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("offline local restart did not recover", state)
		}
		time.Sleep(5 * time.Millisecond)
	}
	online.Store(true)
	c.ConfigVersion = 3
	if _, err := a.reload(context.Background()); err != nil {
		t.Fatal(err)
	}
	if a.config().ConfigVersion != 3 {
		t.Fatal("restored CP intent not consumed")
	}
}
