// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package agent

import (
	"encoding/json"
	"encoding/pem"
	"errors"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestConfigPullRevisionRequiresSuccessfulApply(t *testing.T) {
	a, c, e := fakeNode(t, "")
	c.PullRevision = "candidate"
	e.onApply = func(*Node, agentapi.MemberConfig) error { return errors.New("apply refused") }
	sent := make(chan string, 1)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		revision := r.URL.Query().Get("revision")
		sent <- revision
		if revision == "applied" {
			w.WriteHeader(http.StatusNotModified)
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
	for range 2 {
		if _, err = a.reload(t.Context()); err == nil {
			t.Fatal("failed apply accepted")
		}
		if revision := <-sent; revision != "" {
			t.Fatal("unapplied revision advertised", revision)
		}
	}
	if a.pullRevision != "" {
		t.Fatal("unapplied revision retained", a.pullRevision)
	}
	a.pullRevision = "applied"
	if _, err = a.reload(t.Context()); err != nil {
		t.Fatal("unchanged config reapplied", err)
	}
	if revision := <-sent; revision != "applied" {
		t.Fatal(revision)
	}
}
