// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package agentapi

import (
	"bytes"
	"encoding/json"
	"encoding/pem"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestConfigAcknowledgementHeaderVersionSkew(t *testing.T) {
	for _, ack := range []string{"", "certified-change", strings.Repeat("x", 129)} {
		t.Run(ackLabel(ack), func(t *testing.T) {
			remote := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set(ConfigChangeAckHeader, ack)
				w.WriteHeader(http.StatusNotModified)
			}))
			defer remote.Close()
			ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: remote.Certificate().Raw})
			client, err := NewClient(ClientOptions{BaseURL: remote.URL, Token: "token", CAPEM: ca})
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			cfg, err := client.Config(t.Context(), "applied-payload")
			if !errors.Is(err, ErrConfigUnchanged) {
				t.Fatal(err)
			}
			want := ack
			if len(want) > 128 {
				want = ""
			}
			if cfg.ChangeRevision != want {
				t.Fatal("missing/oversized/valid acknowledgement mishandled", cfg.ChangeRevision)
			}
		})
	}
}

func ackLabel(value string) string {
	if value == "" {
		return "legacy-no-header"
	}
	if len(value) > 128 {
		return "oversized"
	}
	return "new-header"
}

func TestHeartbeatContentStampDoesNotChangeLegacyJSON(t *testing.T) {
	request := HeartbeatRequest{Session: "session", ConfigRevision: "change", ConfigContentRevision: "content"}
	body, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	var legacy struct {
		Session        string            `json:"session"`
		Token          string            `json:"token,omitempty"`
		Instance       InstanceIdentity  `json:"instance"`
		Samples        []HeartbeatSample `json:"samples,omitempty"`
		ConfigRevision string            `json:"configRevision,omitempty"`
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&legacy); err != nil {
		t.Fatal("new agent broke old CP's strict heartbeat envelope", err)
	}
	remote := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get(ConfigContentRevisionHeader) != "content" {
			t.Error("content capability missing from skew-safe header")
		}
		w.Write([]byte(`{"token":"next","intervalMillis":3000,"reloadConfig":true}`))
	}))
	defer remote.Close()
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: remote.Certificate().Raw})
	client, err := NewClient(ClientOptions{BaseURL: remote.URL, Token: "token", CAPEM: ca})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	response, err := client.Heartbeat(t.Context(), request)
	if err != nil || !response.ReloadConfig || response.ConfigRevision != "" {
		t.Fatal("legacy boolean notice compatibility changed", response, err)
	}
}
