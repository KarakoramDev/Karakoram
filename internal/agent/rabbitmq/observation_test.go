// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package rabbitmq

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/pki"
)

func TestHTTPObservationAvoidsColdErlangAndKeepsFreshHealth(t *testing.T) {
	// This executable would consume six seconds if observation accidentally
	// invoked the CLI. HTTP probes must complete within the shorter context.
	fakeCLI(t, "rabbitmqctl", "exec sleep 6\n")
	for _, secure := range []bool{false, true} {
		t.Run(strconv.FormatBool(secure), func(t *testing.T) {
			var unavailable, alarm atomic.Bool
			server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				user, password, ok := r.BasicAuth()
				if !ok || user != "karakoram" || password != "test-password" {
					t.Error("missing local management authentication")
					http.Error(w, "unauthorized", http.StatusUnauthorized)
					return
				}
				// Independent 0.5s API calls must not serialize.
				timer := time.NewTimer(500 * time.Millisecond)
				defer timer.Stop()
				select {
				case <-r.Context().Done():
					return
				case <-timer.C:
				}
				switch r.URL.Path {
				case "/api/nodes":
					// Deliberately keep statistics healthy during later faults.
					_, _ = w.Write([]byte(`[{"name":"rabbit@rmqone","rabbitmq_version":"4.3.6","type":"disc","running":true},{"name":"rabbit@rmqtwo","rabbitmq_version":"4.3.5","type":"disc","running":false}]`))
				case "/api/health/checks/is-in-service":
					if unavailable.Load() {
						w.WriteHeader(http.StatusServiceUnavailable)
					}
					_, _ = w.Write([]byte(`{"status":"ok"}`))
				case "/api/health/checks/local-alarms":
					if alarm.Load() {
						w.WriteHeader(http.StatusServiceUnavailable)
					}
					_, _ = w.Write([]byte(`{"status":"ok"}`))
				default:
					t.Errorf("unexpected management request %s", r.URL.Path)
					http.NotFound(w, r)
				}
			}))
			s := testSpec()
			s.PrometheusPort = observationMetrics(t, runtimeBuildInfo)
			c := memberConfig(s)
			c.SystemUsers = map[string]agentapi.Credentials{"karakoram": {Password: "test-password"}}
			if secure {
				ca, err := pki.NewCA("test", time.Hour)
				if err != nil {
					t.Fatal(err)
				}
				cert, key, err := ca.Issue(pki.CertRequest{CommonName: "rmqone", DNSNames: []string{"rmqone"}, Server: true})
				if err != nil {
					t.Fatal(err)
				}
				pair, err := tls.X509KeyPair(cert, key)
				if err != nil {
					t.Fatal(err)
				}
				server.TLS = &tls.Config{Certificates: []tls.Certificate{pair}, MinVersion: tls.VersionTLS12}
				server.StartTLS()
				s.AMQPPort, s.ManagementPort = 0, 0
				s.AMQPSPort = 5671
				s.ManagementTLSPort = server.Listener.Addr().(*net.TCPAddr).Port
				c.TLS = agentapi.TLSConfig{Mode: "requireTLS", CAPEM: string(ca.CertPEM), CertPEM: string(cert), KeyPEM: string(key)}
			} else {
				server.Start()
				s.ManagementPort = server.Listener.Addr().(*net.TCPAddr).Port
			}
			defer server.Close()
			c.EngineSpec, _ = json.Marshal(s)
			ctx, cancel := context.WithTimeout(t.Context(), 2500*time.Millisecond)
			defer cancel()
			status, err := observe(ctx, c)
			if err != nil || status.Version != "4.3.6" || status.Maintenance || len(status.RunningNodes) != 1 || len(status.DiskNodes) != 2 || string(status.Alarms) != "[]" || string(status.Partitions) != "{}" {
				t.Fatalf("healthy native HTTP observation failed: %+v, %v", status, err)
			}
			unavailable.Store(true)
			alarm.Store(true)
			status, err = observe(t.Context(), c)
			if err != nil || !status.Maintenance || string(status.Alarms) == "[]" {
				t.Fatalf("cached healthy node statistics hid live maintenance/alarm: %+v, %v", status, err)
			}
		})
	}
}

func TestHTTPObservationRefusesUnavailableOrInvalidManagement(t *testing.T) {
	for _, fault := range []string{"unauthorized", "redirect", "malformed", "missing-local", "missing-runtime-version", "partition", "alarm"} {
		t.Run(fault, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if strings.HasPrefix(r.URL.Path, "/api/health/") {
					_, _ = w.Write([]byte(`{"status":"ok"}`))
					return
				}
				if r.URL.Path == "/api/nodes" {
					switch fault {
					case "unauthorized":
						http.Error(w, "test-password", http.StatusUnauthorized)
						return
					case "redirect":
						http.Redirect(w, r, "http://untrusted.invalid/", http.StatusFound)
						return
					case "malformed":
						_, _ = w.Write([]byte(`{"test-password":`))
						return
					case "missing-local":
						_, _ = w.Write([]byte(`[]`))
						return
					case "missing-runtime-version":
						_, _ = w.Write([]byte(`[{"name":"rabbit@rmqone","running":true},{"name":"rabbit@rmqtwo","rabbitmq_version":"4.3.6","running":true}]`))
						return
					case "partition":
						_, _ = w.Write([]byte(`[{"name":"rabbit@rmqone","rabbitmq_version":"4.3.6","running":true,"partitions":["rabbit@rmqtwo"]}]`))
						return
					case "alarm":
						_, _ = w.Write([]byte(`[{"name":"rabbit@rmqone","rabbitmq_version":"4.3.6","running":true,"mem_alarm":true,"disk_free_alarm":true}]`))
						return
					}
				}
				_, _ = w.Write([]byte(`{"rabbitmq_version":"4.3.6"}`))
			}))
			defer server.Close()
			s := testSpec()
			s.ManagementPort = server.Listener.Addr().(*net.TCPAddr).Port
			metrics := runtimeBuildInfo
			if fault == "missing-runtime-version" {
				metrics = strings.ReplaceAll(metrics, `rabbitmq_version="4.3.6"`, `rabbitmq_version=""`)
			}
			s.PrometheusPort = observationMetrics(t, metrics)
			c := memberConfig(s)
			c.SystemUsers = map[string]agentapi.Credentials{"karakoram": {Password: "test-password"}}
			status, err := observe(t.Context(), c)
			if fault == "partition" || fault == "alarm" {
				if err != nil || fault == "partition" && string(status.Partitions) == "{}" || fault == "alarm" && string(status.Alarms) == "[]" {
					t.Fatalf("native negative diagnostic was lost: %+v, %v", status, err)
				}
			} else if fault != "missing-runtime-version" {
				if err != nil || status.MembershipError == "" || status.Version == "" || len(status.RunningNodes) != 0 || strings.Contains(status.MembershipError, "test-password") {
					t.Fatalf("invalid membership erased local health or leaked credentials: %+v, %v", status, err)
				}
			} else if err == nil || strings.Contains(err.Error(), "test-password") {
				t.Fatalf("invalid management response accepted or leaked credentials: %+v, %v", status, err)
			}
		})
	}
}

func TestHTTPObservationRequiresCompleteHealthResponse(t *testing.T) {
	for _, path := range []string{"/api/health/checks/is-in-service", "/api/health/checks/local-alarms"} {
		for _, fault := range []string{"truncated", "cancelled", "oversized", "malformed", "failed-status", "missing-status"} {
			t.Run(path+"/"+fault, func(t *testing.T) {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					switch r.URL.Path {
					case "/api/nodes":
						_, _ = w.Write([]byte(`[{"name":"rabbit@rmqone","rabbitmq_version":"4.3.6","type":"disc","running":true}]`))
					default:
						if r.URL.Path != path {
							_, _ = w.Write([]byte(`{"status":"ok"}`))
							return
						}
						switch fault {
						case "truncated":
							w.Header().Set("Content-Length", "1024")
							_, _ = w.Write([]byte(`{"status":`))
						case "cancelled":
							w.WriteHeader(http.StatusOK)
							w.(http.Flusher).Flush()
							<-r.Context().Done()
						case "oversized":
							_, _ = w.Write([]byte(strings.Repeat(" ", (64<<10)+1)))
						case "malformed":
							_, _ = w.Write([]byte(`{"status":`))
						case "failed-status":
							_, _ = w.Write([]byte(`{"status":"failed"}`))
						case "missing-status":
							_, _ = w.Write([]byte(`{}`))
						}
					}
				}))
				defer server.Close()
				s := testSpec()
				s.ManagementPort = server.Listener.Addr().(*net.TCPAddr).Port
				s.PrometheusPort = observationMetrics(t, runtimeBuildInfo)
				c := memberConfig(s)
				c.SystemUsers = map[string]agentapi.Credentials{"karakoram": {Password: "test-password"}}
				ctx, cancel := context.WithTimeout(t.Context(), 250*time.Millisecond)
				defer cancel()
				if _, err := observe(ctx, c); err == nil {
					t.Fatal("incomplete health response established positive availability")
				}
			})
		}
	}
}

func TestHTTPObservationTimeoutExplainsUnavailableManagement(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/nodes" {
			<-r.Context().Done()
			return
		}
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	}))
	defer server.Close()
	s := testSpec()
	s.ManagementPort = server.Listener.Addr().(*net.TCPAddr).Port
	s.PrometheusPort = observationMetrics(t, runtimeBuildInfo)
	c := memberConfig(s)
	c.SystemUsers = map[string]agentapi.Credentials{"karakoram": {Password: "test-password"}}
	status, err := observe(t.Context(), c)
	if err != nil || status.Version == "" || status.MembershipError == "" || len(status.RunningNodes) != 0 {
		t.Fatalf("membership timeout erased local health or invented membership: %+v %v", status, err)
	}
	for _, detail := range []string{"management API unresponsive", "nodes", "peer connectivity", "do not reset"} {
		if !strings.Contains(status.MembershipError, detail) {
			t.Fatalf("missing %q from timeout diagnosis: %v", detail, status.MembershipError)
		}
	}
	if strings.Contains(status.MembershipError, "test-password") {
		t.Fatal("timeout diagnosis leaked credentials")
	}
}
