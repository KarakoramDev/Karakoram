// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package agentapi

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestPinnedClient(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !AuthenticateBearer(r, "member-token") {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		switch r.URL.Path {
		case "/v1/status":
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"engineVersion":"8.0","configVersion":7}`))
		case "/v1/metrics":
			w.Write([]byte("mongo_up 1\n"))
		case "/v1/logs":
			w.Write([]byte(`{"lines":[]}`))
		case "/v1/pbm":
			w.Write([]byte(`{"output":{"ok":true},"exitCode":0}`))
		case "/v1/bootstrap":
			w.Write([]byte(`{"initialized":true,"userCreated":true}`))
		case "/internal/agent/v1/config":
			w.Write([]byte(`{"role":"mongod-rs","configVersion":7}`))
		default:
			w.WriteHeader(http.StatusNoContent)
		}
	}))
	defer server.Close()
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	client, err := NewClient(ClientOptions{BaseURL: server.URL, Token: "member-token", CAPEM: ca, Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ctx := context.Background()
	status, err := client.Status(ctx)
	if err != nil || status.ConfigVersion != 7 {
		t.Fatal(status, err)
	}
	config, err := client.Config(ctx)
	if err != nil || config.Role != RoleReplicaSet {
		t.Fatal(config, err)
	}
	if err = client.Reload(ctx); err != nil {
		t.Fatal(err)
	}
	if err = client.Process(ctx, ProcessAction{Process: "mongod", Action: "restart"}); err != nil {
		t.Fatal(err)
	}
	if err = client.Process(ctx, ProcessAction{Process: "../shell", Action: "start"}); err == nil {
		t.Fatal("invalid process")
	}
	if err = client.Maintenance(ctx, Maintenance{Enabled: true}); err != nil {
		t.Fatal(err)
	}
	bootstrap, err := client.Bootstrap(ctx, BootstrapRequest{})
	if err != nil || !bootstrap.Initialized {
		t.Fatal(bootstrap, err)
	}
	pbm, err := client.PBM(ctx, PBMRequest{Command: "status"})
	if err != nil || string(pbm.Output) != `{"ok":true}` {
		t.Fatal(pbm, err)
	}
	if _, err = client.Logs(ctx, "mongod", 20); err != nil {
		t.Fatal(err)
	}
	metrics, err := client.Metrics(ctx)
	if err != nil || string(metrics) != "mongo_up 1\n" {
		t.Fatal(string(metrics), err)
	}
	bad, err := NewClient(ClientOptions{BaseURL: server.URL, Token: "wrong", CAPEM: ca})
	if err != nil {
		t.Fatal(err)
	}
	defer bad.Close()
	if _, err = bad.Status(ctx); err == nil {
		t.Fatal("wrong token accepted")
	}
	for _, opts := range []ClientOptions{{BaseURL: "http://localhost", Token: "t", CAPEM: ca}, {BaseURL: server.URL, CAPEM: ca}, {BaseURL: server.URL, Token: "t", CAPEM: []byte("invalid")}} {
		if _, err = NewClient(opts); err == nil {
			t.Fatal("invalid options accepted")
		}
	}
}
func TestBearerAuthentication(t *testing.T) {
	for _, test := range []struct {
		header, token string
		want          bool
	}{{"Bearer secret", "secret", true}, {"Bearer secrets", "secret", false}, {"bearer secret", "secret", false}, {"", "secret", false}, {"Bearer ", "", false}} {
		r := httptest.NewRequest("GET", "/", nil)
		r.Header.Set("Authorization", test.header)
		if got := AuthenticateBearer(r, test.token); got != test.want {
			t.Errorf("authentication mismatch %v", test.want)
		}
	}
	sum := sha256.Sum256([]byte("secret"))
	request := httptest.NewRequest("GET", "/", nil)
	request.Header.Set("Authorization", "Bearer secret")
	if !AuthenticateBearerHash(request, hex.EncodeToString(sum[:])) {
		t.Fatal("stored token hash not accepted")
	}
	if AuthenticateBearerHash(request, "invalid") {
		t.Fatal("invalid stored hash accepted")
	}
}
func TestNoRedirectCredentialLeak(t *testing.T) {
	target := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { t.Error("redirect followed") }))
	defer target.Close()
	source := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	defer source.Close()
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: source.Certificate().Raw})
	c, err := NewClient(ClientOptions{BaseURL: source.URL, Token: "secret", CAPEM: ca})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err = c.Status(context.Background()); err == nil {
		t.Fatal("redirect accepted")
	}
}

func TestRejectUnpinnedCA(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("request reached server with untrusted CA")
	}))
	defer server.Close()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	certificate, err := x509.CreateCertificate(rand.Reader, template, template, public, private)
	if err != nil {
		t.Fatal(err)
	}
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate})
	client, err := NewClient(ClientOptions{BaseURL: server.URL, Token: "member-token", CAPEM: ca})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if _, err := client.Status(context.Background()); err == nil {
		t.Fatal("untrusted certificate accepted")
	}
}
