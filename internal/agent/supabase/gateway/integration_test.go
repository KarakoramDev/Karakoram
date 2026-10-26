//go:build integration

// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
)

type observed struct {
	Component, URI, Host string
	Headers              http.Header
}

func TestEnvoyConformance(t *testing.T) {
	bin := os.Getenv("KARAKORAM_ENVOY_BIN")
	if bin == "" {
		t.Skip("set KARAKORAM_ENVOY_BIN to the official Envoy 1.39.1 amd64 executable")
	}
	executable, err := os.Open(bin)
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.New()
	_, err = io.Copy(hash, executable)
	executable.Close()
	if err != nil {
		t.Fatal(err)
	}
	if hex.EncodeToString(hash.Sum(nil)) != "002c6e1c69ed0fa0ea381887247cadadfaec9481375fa8d8d2b1731eeabf40b8" {
		t.Fatal("official Envoy 1.39.1 checksum mismatch")
	}
	version, err := exec.Command(bin, "--version").CombinedOutput()
	if err != nil || !bytes.Contains(version, []byte("1.39.1")) {
		t.Fatal("unexpected Envoy version")
	}
	upstreams := map[string]string{}
	disconnected := make(chan struct{})
	for _, name := range []string{"auth", "rest", "storage", "meta", "studio", "realtime", "functions"} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if name == "realtime" && strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
				conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
				if err != nil {
					return
				}
				defer conn.CloseNow()
				message, _ := json.Marshal(observed{name, r.RequestURI, r.Host, r.Header})
				if conn.Write(r.Context(), websocket.MessageText, message) != nil {
					return
				}
				kind, payload, err := conn.Read(r.Context())
				if err == nil {
					conn.Write(r.Context(), kind, payload)
				}
				return
			}
			if name == "functions" && r.URL.Path == "/slow" {
				select {
				case <-time.After(151 * time.Second):
				case <-r.Context().Done():
					return
				}
			}
			if name == "functions" && r.URL.Path == "/disconnect" {
				w.WriteHeader(200)
				w.(http.Flusher).Flush()
				<-r.Context().Done()
				close(disconnected)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(observed{name, r.RequestURI, r.Host, r.Header})
		}))
		t.Cleanup(server.Close)
		upstreams[name] = strings.TrimPrefix(server.URL, "http://")
	}
	in := fixture()
	in.Upstreams = upstreams
	in.ListenAddress = "127.0.0.1"
	in.ListenPort = freePort(t)
	endpoint := startEnvoy(t, bin, in)
	client := &http.Client{Timeout: 190 * time.Second}
	request := func(path, key, auth string, extra http.Header) (int, observed, http.Header) {
		t.Helper()
		req, err := http.NewRequest("GET", endpoint+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		if key != "" {
			req.Header.Set("apikey", key)
		}
		if auth != "" {
			req.Header.Set("Authorization", auth)
		}
		for key, values := range extra {
			req.Header[key] = values
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		data, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		var seen observed
		if resp.StatusCode == 200 {
			if err := json.Unmarshal(data, &seen); err != nil {
				t.Fatalf("invalid upstream response on %s", path)
			}
		}
		return resp.StatusCode, seen, resp.Header
	}
	t.Run("route-auth-matrix", func(t *testing.T) {
		routes := []struct {
			path, component, uri string
			secured, admin       bool
		}{{"/auth/v1/settings", "auth", "/settings", true, false}, {"/rest/v1/table", "rest", "/table", true, false}, {"/rest/v1/", "rest", "/", true, true}, {"/rest/v1/?x=1", "rest", "/?x=1", true, true}, {"/graphql/v1", "rest", "/rpc/graphql", true, false}, {"/realtime/v1/api/broadcast", "realtime", "/api/broadcast", true, false}, {"/realtime/v1/websocket", "realtime", "/socket/websocket", true, false}, {"/pg/tables", "meta", "/tables", true, true}, {"/storage/v1/object", "storage", "/object", false, false}, {"/functions/v1/hello", "functions", "/hello", false, false}}
		for _, route := range routes {
			for _, key := range []string{"", "invalid", in.AnonKey, in.ServiceRoleKey, "sb_publishable_test", "sb_secret_test"} {
				status, seen, _ := request(route.path, key, "", nil)
				want := 200
				if route.secured && (key == "" || key == "invalid") {
					want = 401
				} else if route.admin && (key == in.AnonKey || key == "sb_publishable_test") {
					want = 403
				}
				if status != want {
					t.Errorf("%s credential class %d got %d want %d", route.path, keyClass(key, in), status, want)
				}
				if status == 200 && (seen.Component != route.component || seen.URI != route.uri) {
					t.Errorf("route %s went to %s %s", route.path, seen.Component, seen.URI)
				}
			}
		}
	})
	t.Run("open-auth", func(t *testing.T) {
		for _, suffix := range []string{"verify", "callback", "authorize", ".well-known/jwks.json", ".well-known/oauth-authorization-server", ".well-known/openid-configuration", "sso/saml/acs", "sso/saml/metadata"} {
			for _, key := range []string{"", "invalid", in.AnonKey, in.ServiceRoleKey, "sb_publishable_test", "sb_secret_test"} {
				status, seen, _ := request("/auth/v1/"+suffix, key, "", nil)
				if status != 200 || seen.URI != "/"+suffix {
					t.Errorf("open route %s credential class %d: %d", suffix, keyClass(key, in), status)
				}
			}
		}
		status, seen, _ := request("/.well-known/oauth-authorization-server/auth/v1", "", "", nil)
		if status != 200 || seen.URI != "/.well-known/oauth-authorization-server" {
			t.Fatal("authorization-server metadata")
		}
	})
	t.Run("header-query-precedence", func(t *testing.T) {
		status, seen, _ := request("/rest/v1/table?apikey="+in.AnonKey, "", "", nil)
		if status != 200 || seen.Headers.Get("Authorization") != "Bearer "+in.AnonKey {
			t.Fatal("query key not synthesized")
		}
		status, _, _ = request("/rest/v1/table?apikey="+in.ServiceRoleKey, "bad", "", nil)
		if status != 401 {
			t.Fatal("query bypassed invalid header")
		}
		status, _, _ = request("/rest/v1/?apikey=sb_secret_test", in.AnonKey, "", nil)
		if status != 403 {
			t.Fatal("query elevated anon to admin")
		}
		status, seen, _ = request("/rest/v1/table?apikey=sb_secret_test", in.AnonKey, "", nil)
		if status != 200 || seen.Headers.Get("Authorization") != "Bearer "+in.AnonKey {
			t.Fatal("query overwrote header identity")
		}
	})
	t.Run("opaque-user-jwt-graphql", func(t *testing.T) {
		status, seen, _ := request("/graphql/v1", "sb_publishable_test", "Bearer user.jwt.signature", http.Header{"Content-Profile": {"spoof"}, "Accept-Profile": {"spoof"}, "X-Forwarded-Host": {"spoof"}})
		if status != 200 || seen.Headers.Get("Authorization") != "Bearer user.jwt.signature" || seen.Headers.Get("Apikey") != "internal-anon" || seen.Headers.Get("Content-Profile") != "graphql_public" || seen.Headers.Get("Accept-Profile") != "graphql_public" || seen.Headers.Get("X-Forwarded-Host") != "api.example.test" {
			t.Fatal("JWT/profile/forwarded translation")
		}
		status, seen, _ = request("/rest/v1/table", "sb_secret_test", "", nil)
		if status != 200 || seen.Headers.Get("Authorization") != "Bearer internal-service" {
			t.Fatal("opaque role mapping")
		}
		status, _, _ = request("/rest/v1/table", "internal-service", "", nil)
		if status != 401 {
			t.Fatal("internal JWT accepted as external API credential")
		}
	})
	t.Run("storage-sigv4-presigned-raw", func(t *testing.T) {
		const sig = "AWS4-HMAC-SHA256 Credential=test/20260101/region/s3/aws4_request, SignedHeaders=host, Signature=abc"
		path := "/storage/v1/s3/a%2Fb//c?X-Amz-Signature=Ab%2FC&x=1&x=2"
		status, seen, _ := request(path, "sb_publishable_test", sig, nil)
		if status != 200 || seen.Headers.Get("Authorization") != sig || seen.URI != strings.TrimPrefix(path, "/storage/v1") || seen.Host != strings.TrimPrefix(endpoint, "http://") {
			t.Fatal("SigV4 request modified")
		}
		status, seen, _ = request(path, "", "   ", nil)
		if status != 200 || seen.Headers.Get("Authorization") != "" {
			t.Fatal("presigned request Authorization")
		}
	})
	t.Run("functions-anti-spoof-conflict", func(t *testing.T) {
		for _, tc := range []struct {
			key, auth, wantHeader string
			status                int
		}{{"", "", "", 200}, {"invalid", "Bearer user.jwt", "", 200}, {in.AnonKey, "", "", 200}, {"sb_publishable_test", "Bearer user.jwt", "internal-anon", 200}, {"", "Bearer sb_secret_test", "internal-service", 200}, {"sb_unknown", "", "", 401}, {"sb_publishable_test", "Bearer sb_secret_test", "", 401}} {
			status, seen, _ := request("/functions/v1/hello", tc.key, tc.auth, http.Header{"Sb-Api-Key": {"spoof-admin"}})
			if status != tc.status {
				t.Errorf("function status got %d want %d", status, tc.status)
			}
			if status == 200 && (seen.Headers.Get("Sb-Api-Key") != tc.wantHeader || seen.Headers.Get("Authorization") != tc.auth) {
				t.Fatal("function authorization spoof/translation")
			}
		}
		status, seen, _ := request("/functions/v1/hello?apikey=sb_secret_test", "", "", nil)
		if status != 200 || seen.Headers.Get("Sb-Api-Key") != "" {
			t.Fatal("functions used query key")
		}
	})
	t.Run("studio-basic-stripping", func(t *testing.T) {
		status, _, _ := request("/project/default", "", "", nil)
		if status != 401 {
			t.Fatal("Studio open")
		}
		req, _ := http.NewRequest("GET", endpoint+"/project/default", nil)
		req.SetBasicAuth(in.DashboardUsername, in.DashboardPassword)
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var seen observed
		json.NewDecoder(resp.Body).Decode(&seen)
		if resp.StatusCode != 200 || seen.Component != "studio" || seen.Headers.Get("Authorization") != "" || seen.Headers.Get("Apikey") != in.ServiceRoleKey {
			t.Fatal("dashboard credentials leaked")
		}
		status, _, _ = request("/project/default", "", "Basic invalid", nil)
		if status != 401 {
			t.Fatal("invalid basic auth accepted")
		}
	})
	t.Run("blocked-admin", func(t *testing.T) {
		for _, path := range []string{"/mcp", "/mcp/foo", "/api/mcp", "/api/mcp/secret", "/realtime/v1/api/tenants", "/realtime/v1/api/tenants/foo", "/realtime/v1/api/openapi.json"} {
			status, _, _ := request(path, in.ServiceRoleKey, "", nil)
			if status != 403 {
				t.Errorf("blocked route %s: %d", path, status)
			}
		}
	})
	t.Run("realtime-canonical-paths", func(t *testing.T) {
		for _, method := range []string{"GET", "DELETE", "PUT"} {
			for _, key := range []string{in.AnonKey, in.ServiceRoleKey, "sb_publishable_test", "sb_secret_test"} {
				for _, path := range []string{"/realtime/v1/api//tenants", "/realtime//v1/api/tenants", "/realtime/v1//api/tenants/realtime-dev", "/realtime/v1/api/./tenants", "/realtime/v1/api/%2e%2e/api/tenants", "/realtime/v1/api%2ftenants", "/realtime/v1/api/broadcast/../tenants/realtime-dev", "/realtime/v1/api/broadcast/a%2fb/events/event", "/realtime/v1/api/broadcast/./events/event"} {
					req, _ := http.NewRequest(method, endpoint+path, nil)
					req.Header.Set("apikey", key)
					req.Header.Set("Authorization", "Bearer user.jwt.signature")
					resp, err := client.Do(req)
					if err != nil {
						t.Fatal(err)
					}
					resp.Body.Close()
					if resp.StatusCode != http.StatusForbidden {
						t.Fatalf("%s %s credential class %d: %d", method, path, keyClass(key, in), resp.StatusCode)
					}
				}
			}
		}
		for _, tc := range []struct{ path, uri string }{
			{"/realtime/v1/api/ping", "/api/ping"},
			{"/realtime/v1/healthcheck", "/healthcheck"},
		} {
			status, seen, _ := request(tc.path, in.AnonKey, "", nil)
			if status != http.StatusOK || seen.Component != "realtime" || seen.URI != tc.uri {
				t.Errorf("tenant route %s: status %d, upstream %s %s", tc.path, status, seen.Component, seen.URI)
			}
		}
		// Both broadcast APIs are POST routes in the pinned Realtime router.
		for _, tc := range []struct{ path, uri, contentType, body string }{
			{"/realtime/v1/api/broadcast", "/api/broadcast", "application/json", `{"messages":[{"topic":"topic","event":"event","payload":{"ok":true},"private":false}]}`},
			{"/realtime/v1/api/broadcast/topic/events/event", "/api/broadcast/topic/events/event", "application/octet-stream", "broadcast payload"},
			{"/realtime/v1/api/broadcast/topic%20name/events/event?private=false", "/api/broadcast/topic%20name/events/event?private=false", "application/octet-stream", "broadcast payload"},
		} {
			req, _ := http.NewRequest("POST", endpoint+tc.path, strings.NewReader(tc.body))
			req.Header.Set("apikey", in.AnonKey)
			req.Header.Set("Content-Type", tc.contentType)
			resp, err := client.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			var seen observed
			err = json.NewDecoder(resp.Body).Decode(&seen)
			resp.Body.Close()
			if resp.StatusCode != http.StatusOK || err != nil || seen.Component != "realtime" || seen.URI != tc.uri {
				t.Errorf("broadcast route %s: status %d, upstream %s %s, decode %v", tc.path, resp.StatusCode, seen.Component, seen.URI, err)
			}
		}
	})
	t.Run("cors-before-auth", func(t *testing.T) {
		req, _ := http.NewRequest("OPTIONS", endpoint+"/rest/v1/table", nil)
		req.Header.Set("Origin", "https://app.example.test")
		req.Header.Set("Access-Control-Request-Method", "POST")
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != 200 || resp.Header.Get("Access-Control-Allow-Origin") != "https://app.example.test" || resp.Header.Get("Access-Control-Allow-Credentials") == "true" {
			t.Fatal("CORS preflight")
		}
	})
	t.Run("websocket-tenant", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		conn, resp, err := websocket.Dial(ctx, strings.Replace(endpoint, "http://", "ws://", 1)+"/realtime/v1/websocket?apikey=sb_publishable_test&vsn=1.0.0", nil)
		if err != nil {
			if resp != nil {
				t.Fatalf("websocket status %d", resp.StatusCode)
			}
			t.Fatal(err)
		}
		defer conn.CloseNow()
		_, data, err := conn.Read(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var seen observed
		if err := json.Unmarshal(data, &seen); err != nil {
			t.Fatal(err)
		}
		if seen.Host != "realtime-dev.supabase-realtime" || seen.Headers.Get("X-Api-Key") != "internal-anon" || seen.URI != "/socket/websocket?apikey=internal-anon&vsn=1.0.0" {
			t.Fatal("websocket translation/tenant")
		}
		if err := conn.Write(ctx, websocket.MessageText, []byte("ping")); err != nil {
			t.Fatal(err)
		}
		_, data, err = conn.Read(ctx)
		if err != nil || string(data) != "ping" {
			t.Fatal("websocket echo")
		}
	})
	t.Run("disabled-deny", func(t *testing.T) {
		disabled := fixture()
		disabled.ListenAddress = "127.0.0.1"
		disabled.ListenPort = freePort(t)
		disabled.Upstreams = map[string]string{"studio": upstreams["studio"]}
		disabled.GraphQL = false
		base := startEnvoy(t, bin, disabled)
		for _, path := range []string{"/auth/v1/verify", "/auth/v1/settings", "/rest/v1/table", "/graphql/v1", "/storage/v1/object", "/functions/v1/hello", "/realtime/v1/websocket", "/pg/tables", "/rest/v1"} {
			req, _ := http.NewRequest("GET", base+path, nil)
			req.SetBasicAuth(disabled.DashboardUsername, disabled.DashboardPassword)
			resp, err := client.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			resp.Body.Close()
			if resp.StatusCode != 404 {
				t.Errorf("disabled route %s: %d", path, resp.StatusCode)
			}
		}
		disabled.Upstreams = nil
		disabled.DashboardUsername, disabled.DashboardPassword = "", ""
		disabled.ListenPort = freePort(t)
		base = startEnvoy(t, bin, disabled)
		resp, err := client.Get(base + "/anything")
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != 404 {
			t.Fatal("disabled Studio catch-all")
		}
	})
	t.Run("function-client-disconnect", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		req, _ := http.NewRequestWithContext(ctx, "GET", endpoint+"/functions/v1/disconnect", nil)
		resp, err := client.Do(req)
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		cancel()
		resp.Body.Close()
		select {
		case <-disconnected:
		case <-time.After(5 * time.Second):
			t.Fatal("client cancellation did not reach the upstream stream")
		}
	})
	t.Run("legacy-only-functions", func(t *testing.T) {
		legacy := fixture()
		legacy.OpaqueKeys = nil
		legacy.ListenAddress = "127.0.0.1"
		legacy.ListenPort = freePort(t)
		legacy.Upstreams = map[string]string{"functions": upstreams["functions"]}
		base := startEnvoy(t, bin, legacy)
		req, _ := http.NewRequest("GET", base+"/functions/v1/hello", nil)
		req.Header.Set("apikey", "sb_unregistered")
		req.Header.Set("sb-api-key", "spoof")
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var seen observed
		if err := json.NewDecoder(resp.Body).Decode(&seen); err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode != 200 || seen.Headers.Get("Sb-Api-Key") != "" {
			t.Fatal("legacy-only functions must leave API verification to the runtime")
		}
	})
	t.Run("rollout-refusing-host-and-write-safety", func(t *testing.T) {
		var writes atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body, err := io.ReadAll(r.Body)
			if err != nil || string(body) != "non-idempotent-write" {
				t.Error("write payload changed", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			writes.Add(1)
			switch r.URL.Path {
			case "/fail":
				w.WriteHeader(http.StatusServiceUnavailable)
			case "/reset":
				conn, _, err := w.(http.Hijacker).Hijack()
				if err != nil {
					t.Error(err)
					return
				}
				conn.Close()
			default:
				w.WriteHeader(http.StatusOK)
			}
		}))
		defer server.Close()
		host, port, err := net.SplitHostPort(strings.TrimPrefix(server.URL, "http://"))
		if err != nil {
			t.Fatal(err)
		}
		deadPort := freePort(t)
		testInput := fixture()
		testInput.ListenAddress, testInput.ListenPort = "127.0.0.1", freePort(t)
		testInput.Upstreams = map[string]string{"storage": net.JoinHostPort(host, port)}
		base := startEnvoy(t, bin, testInput, func(bootstrap object) {
			cluster := bootstrap["static_resources"].(map[string]any)["clusters"].([]any)[0].(map[string]any)
			endpoints := cluster["load_assignment"].(map[string]any)["endpoints"].([]any)[0].(map[string]any)
			endpoints["lb_endpoints"] = append(endpoints["lb_endpoints"].([]any), object{"endpoint": object{"address": socket("127.0.0.1", uint64(deadPort))}})
		})
		for i := range 40 {
			resp, err := client.Post(base+"/storage/v1/object", "application/octet-stream", strings.NewReader("non-idempotent-write"))
			if err != nil {
				t.Fatal(err)
			}
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode != http.StatusOK || writes.Load() != int32(i+1) {
				t.Fatalf("request %d: status=%d writes=%d", i, resp.StatusCode, writes.Load())
			}
		}
		for _, path := range []string{"fail", "reset"} {
			before := writes.Load()
			resp, err := client.Post(base+"/storage/v1/"+path, "application/octet-stream", strings.NewReader("non-idempotent-write"))
			if err != nil {
				t.Fatal(err)
			}
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode != http.StatusServiceUnavailable || writes.Load() != before+1 {
				t.Fatalf("%s: a sent write was retried: status=%d writes=%d", path, resp.StatusCode, writes.Load()-before)
			}
		}
	})

	t.Run("long-function-response", func(t *testing.T) {
		start := time.Now()
		status, seen, _ := request("/functions/v1/slow", "", "", nil)
		if status != 200 || seen.Component != "functions" || time.Since(start) < 150*time.Second {
			t.Fatal("long function response interrupted")
		}
	})
}
func keyClass(key string, in Input) int {
	switch key {
	case "":
		return 0
	case "invalid":
		return 1
	case in.AnonKey:
		return 2
	case in.ServiceRoleKey:
		return 3
	case "sb_publishable_test":
		return 4
	default:
		return 5
	}
}
func freePort(t *testing.T) uint16 {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	listener.Close()
	return uint16(port)
}
func startEnvoy(t *testing.T, bin string, in Input, customize ...func(object)) string {
	t.Helper()
	config, err := Render(in)
	if err != nil {
		t.Fatal(err)
	}
	// Conformance cases keep several Envoy processes alive simultaneously.
	// Only the isolated test bootstrap varies the production loopback admin port.
	var bootstrap object
	if err := json.Unmarshal(config, &bootstrap); err != nil {
		t.Fatal(err)
	}
	bootstrap["admin"].(map[string]any)["address"] = socket("127.0.0.1", uint64(freePort(t)))
	for _, change := range customize {
		change(bootstrap)
	}
	config, err = json.Marshal(bootstrap)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "envoy.json")
	if err := os.WriteFile(path, config, 0600); err != nil {
		t.Fatal(err)
	}
	// Keep native logs private: Envoy may include configuration fragments in errors.
	logfile, err := os.OpenFile(filepath.Join(t.TempDir(), "envoy.log"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(bin, "-c", path, "--concurrency", "1", "--log-level", "error", "--disable-hot-restart")
	cmd.Stdout = logfile
	cmd.Stderr = logfile
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	t.Cleanup(func() {
		cmd.Process.Signal(os.Interrupt)
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			cmd.Process.Kill()
			<-done
		}
		logfile.Close()
	})
	address := fmt.Sprintf("127.0.0.1:%d", in.ListenPort)
	deadline := time.NewTimer(15 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case err := <-done:
			done <- err
			t.Fatalf("Envoy failed to start: %v (private log %s)", err, logfile.Name())
		case <-deadline.C:
			t.Fatal("Envoy readiness timeout")
		case <-ticker.C:
			conn, err := net.DialTimeout("tcp", address, 100*time.Millisecond)
			if err == nil {
				conn.Close()
				return "http://" + address
			}
		}
	}
}
