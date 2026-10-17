// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package supabase

import (
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
)

func TestMetaProxyAuthentication(t *testing.T) {
	var calls atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Method != "POST" || r.URL.RequestURI() != "/query?mode=test" {
			t.Error("metadata method/path was not preserved")
		}
		body, _ := io.ReadAll(r.Body)
		if string(body) != `{"query":"select 1"}` || r.Header.Get("apikey") != "" {
			t.Error("metadata request body changed or credential reached upstream")
		}
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte("forwarded"))
	}))
	defer upstream.Close()
	target, _ := url.Parse(upstream.URL)
	s := testSpec("meta")
	s.ServiceAsymmetricJWT = "internal-service-jwt"
	proxy := httptest.NewServer(privateHandler(s, target, false))
	defer proxy.Close()
	for _, tc := range []struct {
		name   string
		keys   []string
		status int
	}{
		{"missing", nil, http.StatusUnauthorized},
		{"anonymous", []string{s.AnonKey}, http.StatusUnauthorized},
		{"incorrect", []string{"wrong"}, http.StatusUnauthorized},
		{"duplicate", []string{s.ServiceRoleKey, s.ServiceRoleKey}, http.StatusUnauthorized},
		{"legacy service", []string{s.ServiceRoleKey}, http.StatusCreated},
		{"translated opaque service", []string{s.ServiceAsymmetricJWT}, http.StatusCreated},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req, _ := http.NewRequest("POST", proxy.URL+"/query?mode=test", strings.NewReader(`{"query":"select 1"}`))
			for _, key := range tc.keys {
				req.Header.Add("apikey", key)
			}
			resp, err := proxy.Client().Do(req)
			if err != nil {
				t.Fatal(err)
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode != tc.status {
				t.Fatalf("status %d, want %d", resp.StatusCode, tc.status)
			}
		})
	}
	if calls.Load() != 2 {
		t.Fatalf("forwarded %d requests, want only two authenticated requests", calls.Load())
	}
}
func TestStudioMetaBridgeInjectsServiceCredential(t *testing.T) {
	s := testSpec("studio")
	keys := make(chan string, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		keys <- r.Header.Get("apikey")
		w.WriteHeader(http.StatusNoContent)
	}))
	defer upstream.Close()
	target, _ := url.Parse(upstream.URL)
	bridge := httptest.NewServer(privateHandler(s, target, true))
	defer bridge.Close()
	req, _ := http.NewRequest("GET", bridge.URL+"/tables", nil)
	req.Header.Set("apikey", "untrusted-client-key")
	resp, err := bridge.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent || <-keys != s.ServiceRoleKey {
		t.Fatal("Studio bridge did not overwrite metadata transport credential")
	}
}
func TestMetaAndStudioLoopbackRendering(t *testing.T) {
	p, _, err := Render(testSpec("meta"), "/data")
	if err != nil {
		t.Fatal(err)
	}
	if !contains(p.Env, "PG_META_HOST=127.0.0.1") || !contains(p.Env, "PG_META_PORT=8081") {
		t.Fatal("postgres-meta must bind only its protected loopback listener")
	}
	p, _, err = Render(testSpec("studio"), "/data")
	if err != nil {
		t.Fatal(err)
	}
	if !contains(p.Env, "HOSTNAME=127.0.0.1") || !contains(p.Env, "STUDIO_PG_META_URL=http://127.0.0.1:8083") {
		t.Fatal("Studio must use its local authenticated metadata bridge")
	}
}

func TestStudioMetaBridgeAvoidsMetaListeners(t *testing.T) {
	p, _, err := Render(testSpec("meta"), "/data")
	if err != nil {
		t.Fatal(err)
	}
	metaPort := 0
	for _, entry := range p.Env {
		if value, found := strings.CutPrefix(entry, "PG_META_PORT="); found {
			metaPort, err = strconv.Atoi(value)
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	host, port, err := net.SplitHostPort(studioMetaBridgeAddress)
	if err != nil || host != "127.0.0.1" {
		t.Fatal("Studio bridge must remain loopback-only")
	}
	studioPort, err := strconv.Atoi(port)
	if err != nil || metaPort == 0 || studioPort == metaPort || studioPort == metaPort+1 {
		t.Fatal("Studio bridge conflicts with postgres-meta HTTP or adjacent admin listener")
	}
}

func TestStudioPrivateSQLRequiresServiceCredential(t *testing.T) {
	s := testSpec("studio")
	var calls atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		body, _ := io.ReadAll(r.Body)
		if r.Method != "POST" || r.RequestURI != "/api/platform/pg-meta/default/query" || string(body) != `{"query":"SELECT current_user"}` || r.Header.Get("apikey") != "" {
			t.Error("Studio SQL request changed or transport credential leaked")
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer upstream.Close()
	target, _ := url.Parse(upstream.URL)
	proxy := httptest.NewServer(privateHandler(s, target, false))
	defer proxy.Close()
	for _, keys := range [][]string{nil, {s.AnonKey}, {"wrong"}, {s.ServiceRoleKey, s.ServiceRoleKey}, {s.ServiceRoleKey}} {
		req, _ := http.NewRequest("POST", proxy.URL+"/api/platform/pg-meta/default/query", strings.NewReader(`{"query":"SELECT current_user"}`))
		for _, key := range keys {
			req.Header.Add("apikey", key)
		}
		resp, err := proxy.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		want := http.StatusUnauthorized
		if len(keys) == 1 && keys[0] == s.ServiceRoleKey {
			want = http.StatusNoContent
		}
		if resp.StatusCode != want {
			t.Fatalf("Studio private SQL status %d, want %d", resp.StatusCode, want)
		}
	}
	if calls.Load() != 1 {
		t.Fatal("unauthenticated Studio request reached raw application")
	}
}

func TestPrivateHTTPListenerAddressFamily(t *testing.T) {
	for _, ipv6 := range []bool{false, true} {
		name := "ipv4"
		if ipv6 {
			name = "ipv6"
		}
		t.Run(name, func(t *testing.T) {
			network, address := privateHTTPListener(ipv6, "0")
			server, err := startHTTPProxy(network, address, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusNoContent)
			}))
			if err != nil {
				t.Fatal(err)
			}
			defer server.Close()
			_, port, err := net.SplitHostPort(server.Addr)
			if err != nil {
				t.Fatal(err)
			}
			client := &http.Client{Transport: &http.Transport{Proxy: nil}}
			defer client.CloseIdleConnections()
			for _, host := range []string{"127.0.0.1", "::1"} {
				resp, err := client.Get("http://" + net.JoinHostPort(host, port))
				if !ipv6 && host == "::1" {
					if err == nil {
						resp.Body.Close()
						t.Fatal("IPv4 backend unexpectedly accepts IPv6")
					}
					continue
				}
				if err != nil {
					t.Fatalf("%s listener cannot accept %s: %v", name, host, err)
				}
				resp.Body.Close()
				if resp.StatusCode != http.StatusNoContent {
					t.Fatal("private listener did not serve expected response")
				}
			}
		})
	}
}
