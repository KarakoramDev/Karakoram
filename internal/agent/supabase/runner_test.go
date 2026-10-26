// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package supabase

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agent"
	"github.com/KarakoramDev/Karakoram/internal/engine/supabase/sbspec"
)

func TestPreparationTracksPreparedDigestIndependentlyOfAcceptedConfig(t *testing.T) {
	s := testSpec("realtime")
	s.JWTSecret = "first"
	s.InstallDir = t.TempDir()
	bin := filepath.Join(s.InstallDir, "app", "bin")
	if err := os.MkdirAll(bin, 0700); err != nil {
		t.Fatal(err)
	}
	trace := filepath.Join(s.InstallDir, "prepared")
	for _, name := range []string{"migrate", "realtime"} {
		if err := os.WriteFile(filepath.Join(bin, name), []byte("#!/bin/sh\nif [ \"$1\" = eval ]; then\nprintf '%s:%s\\n' \"$API_JWT_SECRET\" \"$KARAKORAM_REALTIME_TENANT_JWT_SECRET\" >> \"$TRACE\"\nelse\nprintf '%s\\n' \"$API_JWT_SECRET\" >> \"$TRACE\"\nfi\n"), 0700); err != nil {
			t.Fatal(err)
		}
	}
	process := agent.ProcessSpec{Dir: filepath.Dir(bin), Env: []string{"TRACE=" + trace, "API_JWT_SECRET=admin-secret"}}
	first := sha256.Sum256([]byte("first"))
	r := &Runner{}
	if err := r.prepare(t.Context(), s, process, s.InstallDir, first); err != nil {
		t.Fatal(err)
	}
	if !r.prepared || r.preparedDigest != first {
		t.Fatal("successful preparation was not recorded")
	}
	if err := r.prepare(t.Context(), s, process, s.InstallDir, first); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(trace)
	if err != nil || string(data) != "admin-secret\nadmin-secret:first\n" {
		t.Fatal("unchanged digest repeated migrations/seeding", err)
	}
	// Apply accepts a new config while held without preparing it. Accepted and
	// prepared digests must remain distinct until the next permitted start.
	second := sha256.Sum256([]byte("second"))
	s.JWTSecret = "rotated"
	r.spec, r.digest, r.applied = s, second, true
	process.Env = []string{"TRACE=" + trace, "API_JWT_SECRET=admin-secret"}
	if err := r.prepare(t.Context(), s, process, s.InstallDir, second); err != nil {
		t.Fatal(err)
	}
	data, err = os.ReadFile(trace)
	if err != nil || string(data) != "admin-secret\nadmin-secret:first\nadmin-secret\nadmin-secret:rotated\n" || r.preparedDigest != second {
		t.Fatal("accepted held config skipped current tenant preparation", err)
	}
}

func TestFailedPreparationDoesNotAdvancePreparedDigest(t *testing.T) {
	s := testSpec("analytics")
	s.InstallDir = t.TempDir()
	bin := filepath.Join(s.InstallDir, "app", "bin")
	if err := os.MkdirAll(bin, 0700); err != nil {
		t.Fatal(err)
	}
	launcher := filepath.Join(bin, "logflare")
	if err := os.WriteFile(launcher, []byte("#!/bin/sh\nexit 1\n"), 0700); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256([]byte("current"))
	r := &Runner{applied: true, digest: digest}
	process := agent.ProcessSpec{Dir: filepath.Dir(bin)}
	if err := r.prepare(t.Context(), s, process, s.InstallDir, digest); err == nil || !strings.Contains(err.Error(), "database preparation failed") {
		t.Fatal("failed migration was accepted", err)
	}
	if r.prepared || r.preparedDigest == digest {
		t.Fatal("failed preparation advanced digest")
	}
	if err := os.WriteFile(launcher, []byte("#!/bin/sh\nexit 0\n"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := r.prepare(t.Context(), s, process, s.InstallDir, digest); err != nil {
		t.Fatal("preparation retry failed", err)
	}
	if !r.prepared || r.preparedDigest != digest {
		t.Fatal("retry did not record prepared digest")
	}
}

func TestRealtimeAdminCredentialIsIndependent(t *testing.T) {
	s := testSpec("realtime")
	process, _, err := Render(s, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if !contains(process.Env, "API_JWT_SECRET="+s.RealtimeAdminSecret) || contains(process.Env, "API_JWT_SECRET="+s.JWTSecret) {
		t.Fatal("raw Realtime admin API must not accept tenant JWT signatures")
	}
	start := strings.Index(realtimeSeed, "Application.ensure_all_started(:realtime)")
	replace := strings.Index(realtimeSeed, `System.put_env("API_JWT_SECRET"`)
	seed := strings.Index(realtimeSeed, "Realtime.Release.seeds(Realtime.Repo)")
	if start < 0 || replace <= start || seed <= replace {
		t.Fatal("seeding must start HTTP with independent admin auth before changing seed environment")
	}
	for _, secret := range []string{"", s.JWTSecret} {
		s.RealtimeAdminSecret = secret
		if validateService(s) == nil {
			t.Fatal("missing/shared Realtime administration secret was accepted")
		}
	}
}

func TestAuthenticatedReadinessProbes(t *testing.T) {
	for _, component := range []string{"realtime", "studio"} {
		t.Run(component, func(t *testing.T) {
			s := testSpec(component)
			address := "127.0.0.1:3002"
			if component == "realtime" {
				address = "127.0.0.1:4000"
			}
			server, err := startHTTPProxy("tcp4", address, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				if component == "studio" {
					if req.Header.Get("apikey") != s.ServiceRoleKey || req.URL.Path != "/api/platform/profile" {
						t.Error("Studio health did not use authenticated private ingress")
						w.WriteHeader(http.StatusUnauthorized)
						return
					}
				} else {
					parts := strings.Split(strings.TrimPrefix(req.Header.Get("Authorization"), "Bearer "), ".")
					if len(parts) != 3 || req.Method != "GET" || req.URL.Path != "/api/tenants/realtime-dev/health" {
						t.Error("invalid Realtime readiness request")
						w.WriteHeader(http.StatusForbidden)
						return
					}
					signature, _ := base64.RawURLEncoding.DecodeString(parts[2])
					mac := hmac.New(sha256.New, []byte(s.RealtimeAdminSecret))
					_, _ = mac.Write([]byte(parts[0] + "." + parts[1]))
					if !hmac.Equal(signature, mac.Sum(nil)) {
						t.Error("Realtime readiness used a tenant signature")
						w.WriteHeader(http.StatusForbidden)
						return
					}
				}
				w.WriteHeader(http.StatusOK)
				if component == "realtime" {
					_, _ = w.Write([]byte(`{"data":{"healthy":true,"db_connected":false}}`))
				}
			}))
			if err != nil {
				t.Fatal(err)
			}
			defer server.Close()
			if err := healthy(t.Context(), s); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestGatewayBootstrapReadinessDoesNotWaitForUpstreamDiscovery(t *testing.T) {
	listener, err := net.Listen("tcp4", "127.0.0.1:8000")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	s := testSpec("gateway")
	s.Upstreams = map[string]string{"studio": "not-yet-created.invalid:3002"}
	if err := healthy(t.Context(), s); err != nil {
		t.Fatalf("gateway-first bootstrap waited for a later component: %v", err)
	}
}

func TestRESTReadinessRequiresServingDatabase(t *testing.T) {
	admin, err := startHTTPProxy("tcp4", "127.0.0.1:3001", http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path != "/ready" {
			t.Error("REST admin readiness path changed")
		}
		w.WriteHeader(http.StatusOK)
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	s := testSpec("rest")
	var code atomic.Int32
	code.Store(http.StatusServiceUnavailable)
	api, err := startHTTPProxy("tcp4", "127.0.0.1:3000", http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodGet || req.URL.Path != "/" || req.Header.Get("Authorization") != "Bearer "+s.ServiceRoleKey {
			t.Error("REST serving probe must request authenticated, read-only OpenAPI")
		}
		status := int(code.Load())
		w.WriteHeader(status)
		if status == http.StatusServiceUnavailable {
			_, _ = w.Write([]byte(`{"code":"PGRST002","message":"Could not query the database for the schema cache. Retrying."}`))
		}
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer api.Close()
	if err := healthy(t.Context(), s); err == nil || !strings.Contains(err.Error(), "REST database/schema cache") {
		t.Fatalf("admin /ready=200 hid REST 503 PGRST002: %v", err)
	}
	code.Store(http.StatusOK)
	if err := healthy(t.Context(), s); err != nil {
		t.Fatalf("REST readiness did not recover after database restoration: %v", err)
	}
}

// Exercise the production secret projection, not just an unfiltered testSpec:
// native serving endpoints reject the empty bearer token that projection used
// to leave in the Auth and REST member payloads.
func TestProjectedDatabaseReadinessCredentials(t *testing.T) {
	for _, tc := range []struct {
		component, address, path string
	}{
		{"rest", "127.0.0.1:3000", "/"},
		{"auth", "127.0.0.1:9999", "/admin/users/00000000-0000-0000-0000-000000000000"},
		{"storage", "127.0.0.1:5000", "/bucket"},
		{"meta", "127.0.0.1:8081", "/schemas"},
	} {
		t.Run(tc.component, func(t *testing.T) {
			full := testSpec(tc.component)
			if tc.component == "rest" {
				admin, err := startHTTPProxy("tcp4", "127.0.0.1:3001", http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
					w.WriteHeader(http.StatusOK)
				}))
				if err != nil {
					t.Fatal(err)
				}
				defer admin.Close()
			}
			var unavailable atomic.Bool
			server, err := startHTTPProxy("tcp4", tc.address, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				if req.Method != http.MethodGet || req.URL.RequestURI() != tc.path {
					t.Error("unexpected database serving request")
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				if tc.component != "meta" && req.Header.Get("Authorization") != "Bearer "+full.ServiceRoleKey {
					w.WriteHeader(http.StatusUnauthorized)
					return
				}
				if unavailable.Load() {
					w.WriteHeader(http.StatusServiceUnavailable)
					return
				}
				if tc.component == "auth" {
					w.WriteHeader(http.StatusNotFound)
					_, _ = w.Write([]byte(`{"error_code":"user_not_found"}`))
					return
				}
				w.WriteHeader(http.StatusOK)
			}))
			if err != nil {
				t.Fatal(err)
			}
			defer server.Close()
			projected := full.ProjectSecrets()
			// Also exercise the standalone readiness command's persisted input.
			dataDir := t.TempDir()
			dir := filepath.Join(dataDir, "karakoram/supabase")
			if err := os.MkdirAll(dir, 0700); err != nil {
				t.Fatal(err)
			}
			data, err := json.Marshal(sbspec.Spec{Component: projected.Component, ServiceRoleKey: projected.ServiceRoleKey})
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, "readiness.json"), data, 0600); err != nil {
				t.Fatal(err)
			}
			checks := []func() error{
				func() error { return healthy(t.Context(), projected) },
				func() error { return (&Runner{}).Readiness(t.Context(), agent.Options{DataDir: dataDir}, nil) },
			}
			for _, check := range checks {
				if err := check(); err != nil {
					t.Fatalf("projected serving credential was rejected: %v", err)
				}
				unavailable.Store(true)
				if err := check(); err == nil || !strings.Contains(err.Error(), "HTTP 503") {
					t.Fatalf("database partition did not withdraw readiness: %v", err)
				}
				unavailable.Store(false)
				if err := check(); err != nil {
					t.Fatalf("database restoration did not restore readiness: %v", err)
				}
			}
		})
	}
}

func TestDatabaseReadinessDoesNotTrustLiveness(t *testing.T) {
	for _, tc := range []struct {
		component, address, path string
	}{
		{"auth", "127.0.0.1:9999", "/admin/users/00000000-0000-0000-0000-000000000000"},
		{"storage", "127.0.0.1:5000", "/bucket"},
		{"meta", "127.0.0.1:8081", "/schemas"},
	} {
		t.Run(tc.component, func(t *testing.T) {
			s := testSpec(tc.component)
			var unavailable atomic.Bool
			unavailable.Store(true)
			server, err := startHTTPProxy("tcp4", tc.address, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				if req.URL.Path == "/health" || req.URL.Path == "/status" {
					w.WriteHeader(http.StatusOK)
					return
				}
				if req.Method != http.MethodGet || req.URL.RequestURI() != tc.path {
					t.Errorf("unexpected serving probe: %s %s", req.Method, req.URL.RequestURI())
				}
				if tc.component != "meta" && req.Header.Get("Authorization") != "Bearer "+s.ServiceRoleKey {
					t.Error("serving probe lacks service authorization")
				}
				if unavailable.Load() {
					w.WriteHeader(http.StatusServiceUnavailable)
					return
				}
				w.WriteHeader(http.StatusOK)
			}))
			if err != nil {
				t.Fatal(err)
			}
			defer server.Close()
			if err := healthy(t.Context(), s); err == nil || !strings.Contains(err.Error(), "database readiness") {
				t.Fatalf("liveness hid a database serving failure: %v", err)
			}
			unavailable.Store(false)
			if err := healthy(t.Context(), s); err != nil {
				t.Fatalf("database restoration did not restore readiness: %v", err)
			}
		})
	}
}

func TestRealtimeReadinessRejectsUnhealthyHTTP200(t *testing.T) {
	var body atomic.Value
	body.Store(`{"data":{"healthy":false,"db_connected":false}}`)
	server, err := startHTTPProxy("tcp4", "127.0.0.1:4000", http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(body.Load().(string)))
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	for _, payload := range []string{`{"data":{"healthy":false}}`, `{}`, `invalid`} {
		body.Store(payload)
		if err := healthy(t.Context(), testSpec("realtime")); err == nil {
			t.Fatalf("unhealthy or invalid tenant health accepted: %s", payload)
		}
	}
	// Native Realtime lazily opens an idle tenant's database connection.
	// Requiring db_connected would prevent healthy empty stacks from joining.
	body.Store(`{"data":{"healthy":true,"db_connected":false}}`)
	if err := healthy(t.Context(), testSpec("realtime")); err != nil {
		t.Fatalf("healthy idle tenant blocked bootstrap: %v", err)
	}
}

func TestAuthReadinessUsesBoundedPinnedUserLookup(t *testing.T) {
	// GoTrue 2.197.0 api.loadUser parses a UUID, looks it up by primary key,
	// and maps absent rows to 404 user_not_found; DB errors map to 500.
	// Generic/validation 404s must not be mistaken for a completed lookup.
	var status atomic.Int32
	var body atomic.Value
	status.Store(http.StatusNotFound)
	body.Store(`{"error_code":"user_not_found"}`)
	server, err := startHTTPProxy("tcp4", "127.0.0.1:9999", http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path != "/admin/users/00000000-0000-0000-0000-000000000000" || req.URL.RawQuery != "" {
			t.Error("Auth probe must use the indexed lookup, never offset pagination or COUNT(*)")
		}
		w.WriteHeader(int(status.Load()))
		_, _ = w.Write([]byte(body.Load().(string)))
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	for _, tc := range []struct {
		name  string
		code  int
		body  string
		ready bool
	}{
		{"empty-database", 404, `{"error_code":"user_not_found"}`, true},
		{"existing-user", 200, `{}`, true},
		{"database-down", 500, `{"error_code":"unexpected_failure"}`, false},
		{"invalid-uuid", 404, `{"error_code":"validation_failed"}`, false},
		{"missing-route", 404, `404 page not found`, false},
		{"missing-code", 404, `{}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status.Store(int32(tc.code))
			body.Store(tc.body)
			if err := healthy(t.Context(), testSpec("auth")); (err == nil) != tc.ready {
				t.Fatalf("pinned Auth lookup result: ready=%v err=%v", tc.ready, err)
			}
		})
	}
}
