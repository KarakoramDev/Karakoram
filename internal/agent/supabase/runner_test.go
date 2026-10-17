// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package supabase

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agent"
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
					if len(parts) != 3 || req.Method != "HEAD" || req.URL.Path != "/api/tenants/realtime-dev/health" {
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
