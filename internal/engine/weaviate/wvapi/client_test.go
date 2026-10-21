// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package wvapi

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func plainClient(t *testing.T, handler http.HandlerFunc, options ...func(*Config)) (*Client, *httptest.Server) {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	cfg := Config{Endpoints: []string{server.Listener.Addr().String()}, Scheme: "http"}
	for _, option := range options {
		option(&cfg)
	}
	c, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c, server
}

func TestTLSHostnameAndNamespaceDialer(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.TLS.ServerName != "example.com" {
			t.Errorf("SNI = %q", r.TLS.ServerName)
		}
		w.WriteHeader(200)
	}))
	defer server.Close()
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	var dialed string
	dial := func(ctx context.Context, network, address string) (net.Conn, error) {
		dialed = address
		return (&net.Dialer{}).DialContext(ctx, network, server.Listener.Addr().String())
	}
	cfg := Config{Endpoints: []string{"example.com:8080"}, CAPEM: ca, DialContext: dial}
	c, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err = c.Ready(context.Background()); err != nil {
		t.Fatal(err)
	}
	if dialed != "example.com:8080" {
		t.Fatalf("namespace dial address %q", dialed)
	}
	cfg.Endpoints = []string{"wrong.example:8080"}
	bad, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer bad.Close()
	if err = bad.Ready(context.Background()); err == nil {
		t.Fatal("accepted wrong hostname")
	}
	cfg.Endpoints = []string{"example.com:8080"}
	cfg.CAPEM = []byte("invalid PEM")
	if _, err = New(cfg); err == nil {
		t.Fatal("accepted invalid CA")
	}
	if calls.Load() != 1 {
		t.Fatalf("wrong-hostname request reached handler: %d", calls.Load())
	}
}

func TestFailoverAndNoHTTPReplay(t *testing.T) {
	for _, status := range []int{200, 400, 401, 404, 500} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			var second atomic.Int32
			a := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(status) }))
			defer a.Close()
			b := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { second.Add(1); w.WriteHeader(200) }))
			defer b.Close()
			c, err := New(Config{Endpoints: []string{a.URL, b.URL}, Scheme: "http"})
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			err = c.Ready(context.Background())
			if (err == nil) != (status == 200) {
				t.Fatalf("status %d: %v", status, err)
			}
			if second.Load() != 0 {
				t.Fatal("replayed HTTP response")
			}
		})
	}
	c, _ := plainClient(t, func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) }, func(cfg *Config) {
		cfg.Endpoints = append([]string{"offline.invalid:8080"}, cfg.Endpoints...)
		cfg.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
			if address == "offline.invalid:8080" {
				return nil, errors.New("connection refused")
			}
			return (&net.Dialer{}).DialContext(ctx, network, address)
		}
	})
	if err := c.Ready(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestNoReplayAfterConnection(t *testing.T) {
	var second atomic.Int32
	a := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		conn.Close()
	}))
	defer a.Close()
	b := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { second.Add(1) }))
	defer b.Close()
	c, err := New(Config{Scheme: "http", Endpoints: []string{a.URL, b.URL}})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err = c.DeleteCollection(context.Background(), "Article"); err == nil {
		t.Fatal("expected uncertain connection failure")
	}
	if second.Load() != 0 {
		t.Fatal("replayed uncertain mutation")
	}
}

func TestAuthAndRedaction(t *testing.T) {
	key := "secret-bootstrap-api-key-0123456789"
	c, _ := plainClient(t, func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer "+key {
			t.Errorf("wrong bearer header")
		}
		w.WriteHeader(403)
		json.NewEncoder(w).Encode(map[string]any{"error": []map[string]string{{"message": "denied " + key + " apiKey: shortsecret otherOpaqueKey012345678901234567890"}}})
	}, func(cfg *Config) { cfg.APIKey = key })
	err := c.Ready(context.Background())
	var native *Error
	if !errors.As(err, &native) || native.Status != 403 {
		t.Fatalf("wrong error: %v", err)
	}
	for _, secret := range []string{key, "shortsecret", "otherOpaqueKey012345678901234567890"} {
		if strings.Contains(err.Error(), secret) || strings.Contains(native.Message, secret) {
			t.Fatalf("credential leaked")
		}
	}
	if !strings.Contains(err.Error(), "denied") {
		t.Fatal("lost native message")
	}
}

func TestInternalRemoveAndBasicAuth(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, password, ok := r.BasicAuth()
		if !ok || user != "cluster" || password != "cluster-secret" {
			t.Error("missing basic auth")
		}
		if r.Method != "POST" || r.URL.Path != "/v1/cluster/remove" {
			t.Errorf("wrong route: %s %s", r.Method, r.URL.Path)
		}
		var body map[string]string
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body["node"] != "member-3" {
			t.Error("wrong removal body")
		}
	}))
	defer server.Close()
	c, err := NewInternal(Config{Endpoints: []string{server.URL}, Scheme: "http", APIKey: "must-not-be-sent"}, "cluster", "cluster-secret")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err = c.RemoveNode(context.Background(), "member-3"); err != nil {
		t.Fatal(err)
	}
}

func TestBoundsAndTimeout(t *testing.T) {
	for _, status := range []int{200, 400} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			c, _ := plainClient(t, func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(status)
				fmt.Fprint(w, strings.Repeat("x", 65))
			}, func(cfg *Config) { cfg.MaxResponseBytes = 64 })
			if err := c.Ready(context.Background()); err == nil || !strings.Contains(err.Error(), "exceeds") {
				t.Fatalf("body limit not enforced: %v", err)
			}
		})
	}
	c, _ := plainClient(t, func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() }, func(cfg *Config) { cfg.Timeout = 20 * time.Millisecond })
	if err := c.Ready(context.Background()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("timeout: %v", err)
	}
}

func TestConcurrentRequests(t *testing.T) {
	c, _ := plainClient(t, func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, `{"version":"1.40.1"}`) })
	var group sync.WaitGroup
	for range 20 {
		group.Add(1)
		go func() {
			defer group.Done()
			v, err := c.Meta(context.Background())
			if err != nil || v.Version != "1.40.1" {
				t.Errorf("concurrent request: %v", err)
			}
		}()
	}
	group.Wait()
}

func TestRedirectDoesNotForwardCredentials(t *testing.T) {
	var followed atomic.Bool
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { followed.Store(true) }))
	defer target.Close()
	c, _ := plainClient(t, func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}, func(cfg *Config) { cfg.APIKey = "secret" })
	if err := c.Ready(context.Background()); err == nil {
		t.Fatal("redirect was successful")
	}
	if followed.Load() {
		t.Fatal("followed redirect")
	}
}
