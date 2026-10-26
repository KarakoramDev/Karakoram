// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package agent

import (
	"context"
	"encoding/pem"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
)

func TestRecoveryCachedStartupServesDuringPublicationFailure(t *testing.T) {
	a, c := recoveryAgent(t)
	if _, err := a.apply(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	// Deterministic publication refusal, without changing the host mount or rlimit.
	path := filepath.Join(a.o.DataDir, "karakoram", "mongod.conf")
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
	cp := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusServiceUnavailable) }))
	defer cp.Close()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	listener.Close()
	o := a.o
	o.Token = "token"
	o.Listen = address
	o.ControlPlaneURL = cp.URL
	o.InternalCAPEM = string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cp.Certificate().Raw}))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, o) }()
	client, err := agentapi.NewClient(agentapi.ClientOptions{BaseURL: "https://member", Token: "token", CAPEM: []byte(c.AgentTLS.CertPEM), Timeout: 100 * time.Millisecond, DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, address)
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	deadline := time.Now().Add(3 * time.Second)
	for {
		select {
		case err := <-done:
			t.Fatal("cached startup exited before serving storage diagnosis", err)
		default:
		}
		status, err := client.Status(context.Background())
		if err == nil {
			if status.Error == "" || status.ConfigVersion == c.ConfigVersion {
				t.Fatal("failed storage publication acknowledged", status)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("authenticated status unavailable during storage failure", err)
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("member shutdown stalled")
	}
}
