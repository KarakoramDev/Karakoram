// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package agentapi

import (
	"context"
	"encoding/pem"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func TestLiveSessionHeaderBinding(t *testing.T) {
	source := &testSamples{}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/status":
			w.Header().Set(HeartbeatSessionHeader, "status-session")
		case "/v1/mysql/status":
			w.Header().Set(HeartbeatSessionHeader, "engine-session")
		case "/v1/mysql/fail":
			w.Header().Set(HeartbeatSessionHeader, "failed-session")
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		_, _ = w.Write([]byte(`{}`))
	}))
	defer server.Close()
	client, err := NewClient(ClientOptions{BaseURL: server.URL, Token: "member-bearer", CAPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), Samples: source})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if _, err := client.Status(context.Background()); err != nil || source.bound != "status-session" {
		t.Fatal("live Status did not bind", err, source.bound)
	}
	if err := client.Engine(context.Background(), "mysql", "status", nil, nil); err != nil || source.bound != "engine-session" {
		t.Fatal("live Engine with discarded output did not bind", err, source.bound)
	}
	if err := client.Engine(context.Background(), "mysql", "fail", nil, nil); err == nil || source.bound != "engine-session" {
		t.Fatal("failed request changed binding", err, source.bound)
	}
	pinned, err := client.ForInstance(server.Listener.Addr().(*net.TCPAddr).IP.String())
	if err != nil {
		t.Fatal(err)
	}
	defer pinned.Close()
	if _, err := pinned.Status(context.Background()); err != nil || source.bound != "engine-session" {
		t.Fatal("pinned request changed logical binding", err, source.bound)
	}
	if err := client.Engine(context.Background(), "mysql", "proxy-status", nil, nil); err != nil || source.bound != "" {
		t.Fatal("legacy response did not clear previous binding", err, source.bound)
	}
	source.ok, source.body = true, []byte(`{}`)
	if _, err := client.Status(infra.WithSteadyObservation(infra.ObservationContext(context.Background()))); err != nil || source.bound != "" {
		t.Fatal("pushed sample changed logical binding", err, source.bound)
	}
}
