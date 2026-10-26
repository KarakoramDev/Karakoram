// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package rabbitmq

import (
	"context"
	"encoding/json"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestObservationDoesNotWaitForAggregateOverview(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/nodes":
			_, _ = w.Write([]byte(`[{"name":"rabbit@rmqtwo","rabbitmq_version":"4.3.5","running":true},{"name":"rabbit@rmqone","rabbitmq_version":"4.3.6","type":"disc","running":true,"partitions":[]}]`))
		case "/api/overview":
			<-r.Context().Done()
		default:
			_, _ = w.Write([]byte(`{"status":"ok"}`))
		}
	}))
	defer server.Close()
	s := testSpec()
	s.ManagementPort = server.Listener.Addr().(*net.TCPAddr).Port
	s.PrometheusPort = observationMetrics(t, runtimeBuildInfo)
	c := memberConfig(s)
	c.EngineSpec, _ = json.Marshal(s)
	c.SystemUsers = map[string]agentapi.Credentials{"karakoram": {Password: "test-password"}}
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	status, err := observe(ctx, c)
	if err != nil || status.Version != "4.3.6" || len(status.RunningNodes) != 2 {
		t.Fatalf("aggregate stats blocked fresh local broker health: %+v %v", status, err)
	}
}

func observationMetrics(t *testing.T, body string) int {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/metrics" || r.Header.Get("Authorization") != "" {
			t.Error("runtime identity scrape used the wrong route or leaked management credentials")
		}
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(server.Close)
	return server.Listener.Addr().(*net.TCPAddr).Port
}

const runtimeBuildInfo = "# TYPE rabbitmq_build_info untyped\nrabbitmq_build_info{rabbitmq_version=\"4.3.6\"} 1\n# TYPE rabbitmq_identity_info untyped\nrabbitmq_identity_info{rabbitmq_node=\"rabbit@rmqone\"} 1\n"

func TestObservationAvoidsAggregateNodeStatistics(t *testing.T) {
	var alarm atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/nodes":
			if r.URL.Query().Get("disable_stats") != "true" {
				<-r.Context().Done()
				return
			}
			// RabbitMQ's live membership response intentionally omits all
			// statistics, including version and alarm fields.
			_, _ = w.Write([]byte(`[{"name":"rabbit@rmqone","type":"disc","running":true,"being_drained":false},{"name":"rabbit@rmqtwo","type":"disc","running":false,"being_drained":false}]`))
		case "/api/health/checks/local-alarms":
			if alarm.Load() {
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = w.Write([]byte(`{"status":"failed","alarms":[{"node":"rabbit@rmqone","resource":"memory"}]}`))
				return
			}
			_, _ = w.Write([]byte(`{"status":"ok"}`))
		case "/api/health/checks/is-in-service":
			_, _ = w.Write([]byte(`{"status":"ok"}`))
		default:
			t.Errorf("unexpected observation request %s", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	s := testSpec()
	s.Version = "4.3.5"
	s.ManagementPort = server.Listener.Addr().(*net.TCPAddr).Port
	s.PrometheusPort = observationMetrics(t, runtimeBuildInfo)
	c := memberConfig(s)
	c.SystemUsers = map[string]agentapi.Credentials{"karakoram": {Password: "test-password"}}
	ctx, cancel := context.WithTimeout(t.Context(), 250*time.Millisecond)
	defer cancel()
	status, err := observe(ctx, c)
	if err != nil || status.Version != "4.3.6" || len(status.RunningNodes) != 1 || status.RunningNodes[0] != "rabbit@rmqone" {
		t.Fatalf("aggregate statistics blocked fresh membership/runtime version: %+v %v", status, err)
	}
	alarm.Store(true)
	status, err = observe(t.Context(), c)
	if err != nil || string(status.Alarms) != `[{"node":"rabbit@rmqone","resource":"memory"}]` {
		t.Fatalf("live alarm identity was lost without statistics: %+v %v", status, err)
	}
}

func TestRuntimeVersionRequiresFreshLocalIdentity(t *testing.T) {
	for name, body := range map[string]string{
		"missing-version": strings.ReplaceAll(runtimeBuildInfo, `rabbitmq_version="4.3.6"`, `rabbitmq_version=""`),
		"wrong-node":      strings.ReplaceAll(runtimeBuildInfo, "rabbit@rmqone", "rabbit@rmqtwo"),
		"missing-family":  "unrelated_metric 1\n",
		"malformed":       "rabbitmq_build_info{\n",
		"zero-value":      strings.ReplaceAll(runtimeBuildInfo, "} 1", "} 0"),
		"oversized":       runtimeBuildInfo + strings.Repeat("# padding\n", 300000),
	} {
		t.Run(name, func(t *testing.T) {
			port := observationMetrics(t, body)
			if version, err := observeRuntimeVersion(t.Context(), http.DefaultClient, port, "rabbit@rmqone"); err == nil || version != "" {
				t.Fatalf("invalid native runtime identity established a version: %q %v", version, err)
			}
		})
	}
}
