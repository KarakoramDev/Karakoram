// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package rabbitmq

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
)

func TestObservationChecksDesiredAdminCredentialFresh(t *testing.T) {
	code := http.StatusUnauthorized
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/whoami":
			user, password, ok := r.BasicAuth()
			if !ok || user != "admin" || password != "desired-admin" {
				t.Error("probe did not use desired admin credential")
			}
			w.WriteHeader(code)
			_, _ = w.Write([]byte(`{"name":"admin"}`))
		case "/api/nodes":
			_, _ = w.Write([]byte(`[{"name":"rabbit@rmqone","type":"disc","running":true}]`))
		default:
			_, _ = w.Write([]byte(`{"status":"ok"}`))
		}
	}))
	defer server.Close()
	s := testSpec()
	s.ManagementPort = server.Listener.Addr().(*net.TCPAddr).Port
	s.PrometheusPort = observationMetrics(t, runtimeBuildInfo)
	c := memberConfig(s)
	c.SystemUsers = map[string]agentapi.Credentials{"karakoram": {Password: "management-password"}, "admin": {Password: "desired-admin"}}
	for _, step := range []struct {
		code int
		want string
	}{
		{401, "rejected"}, {503, "unknown"}, {403, "unknown"}, {302, "unknown"}, {200, "accepted"}, {401, "rejected"},
	} {
		code = step.code
		st, err := observe(t.Context(), c)
		if err != nil {
			t.Fatal(err)
		}
		data, _ := json.Marshal(st)
		var fields map[string]any
		_ = json.Unmarshal(data, &fields)
		if fields["adminCredentialStatus"] != step.want {
			t.Fatalf("HTTP %d: credential status=%v, want %s", code, fields["adminCredentialStatus"], step.want)
		}
	}
}

func TestAdminCredentialProbeDoesNotConfuseTransportOrInvalidIdentityWithDrift(t *testing.T) {
	for _, body := range []string{`{`, `{}`, `{"name":"other"}`} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte(body))
		}))
		if got := observeAdminCredential(t.Context(), server.Client(), server.URL, "desired-admin"); got != "unknown" {
			t.Fatalf("invalid identity: got %q", got)
		}
		client := server.Client()
		server.Close()
		if got := observeAdminCredential(t.Context(), client, server.URL, "desired-admin"); got != "unknown" {
			t.Fatalf("connection failure: got %q", got)
		}
	}
}

func TestStalledAdminProbeDoesNotConsumeHealthDeadline(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/whoami":
			<-r.Context().Done()
		case "/api/nodes":
			_, _ = w.Write([]byte(`[{"name":"rabbit@rmqone","running":true}]`))
		default:
			_, _ = w.Write([]byte(`{"status":"ok"}`))
		}
	}))
	defer server.Close()
	s := testSpec()
	s.ManagementPort = server.Listener.Addr().(*net.TCPAddr).Port
	s.PrometheusPort = observationMetrics(t, runtimeBuildInfo)
	c := memberConfig(s)
	c.SystemUsers = map[string]agentapi.Credentials{"karakoram": {Password: "management-password"}, "admin": {Password: "desired-admin"}}
	ctx, cancel := context.WithTimeout(t.Context(), 2500*time.Millisecond)
	defer cancel()
	start := time.Now()
	st, err := observe(ctx, c)
	if err != nil || ctx.Err() != nil || time.Since(start) > 1500*time.Millisecond || st.AdminCredentialStatus != "unknown" {
		t.Fatalf("optional probe consumed health deadline: elapsed=%s status=%+v err=%v context=%v", time.Since(start), st, err, ctx.Err())
	}
}
