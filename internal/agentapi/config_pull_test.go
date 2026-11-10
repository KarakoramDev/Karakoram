// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package agentapi

import (
	"encoding/pem"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestConditionalConfigClient(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("revision") == "applied" {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Write([]byte(`{"cluster":"demo","pullRevision":"applied"}`))
	}))
	defer server.Close()
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	client, err := NewClient(ClientOptions{BaseURL: server.URL, Token: "token", CAPEM: ca})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	cfg, err := client.Config(t.Context(), "")
	if err != nil || cfg.PullRevision != "applied" {
		t.Fatal(cfg, err)
	}
	if _, err = client.Config(t.Context(), cfg.PullRevision); !errors.Is(err, ErrConfigUnchanged) {
		t.Fatal(err)
	}
	// An older server ignores the revision and returns the ordinary payload.
	if _, err = client.Config(t.Context(), "old-server-ignored"); err != nil {
		t.Fatal(err)
	}
}
