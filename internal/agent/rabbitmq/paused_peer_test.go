// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package rabbitmq

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
)

func TestPausedPeerDoesNotInvalidateLocalHealth(t *testing.T) {
	for _, blocked := range []string{"membership", "cluster-alarms", "local-service"} {
		t.Run(blocked, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/api/nodes":
					if blocked == "membership" {
						<-r.Context().Done()
						return
					}
					_, _ = w.Write([]byte(`[{"name":"rabbit@rmqone","type":"disc","running":true}]`))
				case "/api/health/checks/alarms":
					// The cluster-wide health route must never determine local health.
					<-r.Context().Done()
				case "/api/health/checks/is-in-service":
					if blocked == "local-service" {
						<-r.Context().Done()
						return
					}
					_, _ = w.Write([]byte(`{"status":"ok"}`))
				default:
					_, _ = w.Write([]byte(`{"status":"ok"}`))
				}
			}))
			defer server.Close()
			s := testSpec()
			s.ManagementPort = server.Listener.Addr().(*net.TCPAddr).Port
			s.PrometheusPort = observationMetrics(t, runtimeBuildInfo)
			c := memberConfig(s)
			c.SystemUsers = map[string]agentapi.Credentials{"karakoram": {Password: "test-password"}}
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			start := time.Now()
			status, err := observe(ctx, c)
			if blocked == "local-service" {
				if err == nil {
					t.Fatalf("unresponsive local broker established health: %+v", status)
				}
				return
			}
			if err != nil || status.Version != "4.3.6" || status.Maintenance || string(status.Alarms) != "[]" {
				t.Fatalf("paused peer erased fresh local health: %+v %v", status, err)
			}
			if time.Since(start) > 2500*time.Millisecond {
				t.Fatal("membership consumed control-plane observation budget")
			}
			if blocked == "membership" && (status.MembershipError == "" || len(status.RunningNodes) != 0 || len(status.DiskNodes) != 0 || len(status.Partitions) != 0 || !strings.Contains(status.MembershipError, "peer connectivity")) {
				t.Fatalf("unobservable membership was invented or silent: %+v", status)
			}
		})
	}
}
