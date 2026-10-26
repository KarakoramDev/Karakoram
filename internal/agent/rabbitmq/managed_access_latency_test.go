// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package rabbitmq

import (
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
)

func TestBootstrapHealthyManagedAccessAvoidsCLILaunches(t *testing.T) {
	t.Setenv("KARAKORAM_RABBITMQ_BIN", t.TempDir())
	fakeCLI(t, "rabbitmqctl", "exit 73\n")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, password, ok := r.BasicAuth()
		if !ok || password != "desired-"+user {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		switch r.URL.Path {
		case "/api/whoami":
			_ = json.NewEncoder(w).Encode(map[string]any{"name": user, "tags": []string{"administrator"}})
		case "/api/permissions/%2F/" + user, "/api/permissions///" + user:
			_ = json.NewEncoder(w).Encode(map[string]string{"user": user, "vhost": "/", "configure": ".*", "write": ".*", "read": ".*"})
		case "/api/users/guest":
			w.WriteHeader(http.StatusNotFound)
		default:
			t.Errorf("unexpected managed access read %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	s := testSpec()
	s.ManagementPort = server.Listener.Addr().(*net.TCPAddr).Port
	cfg := memberConfig(s)
	cfg.SystemUsers = map[string]agentapi.Credentials{"admin": {Password: "desired-admin"}, "karakoram": {Password: "desired-karakoram"}}
	if err := New().bootstrap(t.Context(), cfg); err != nil {
		t.Fatalf("healthy managed access launched a CLI VM: %v", err)
	}
}

func TestManagedAccessNoOpRequiresEveryFreshPredicate(t *testing.T) {
	for _, fault := range []string{"admin-password", "control-password", "tags", "permissions", "vhost", "guest", "partial", "unavailable"} {
		t.Run(fault, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				user, _, _ := r.BasicAuth()
				if fault == "unavailable" {
					w.WriteHeader(http.StatusServiceUnavailable)
					return
				}
				if fault == "partial" {
					_, _ = w.Write([]byte(`{"name":`))
					return
				}
				if r.URL.Path == "/api/whoami" {
					if fault == "admin-password" && user == "admin" || fault == "control-password" && user == "karakoram" {
						w.WriteHeader(http.StatusUnauthorized)
						return
					}
					tags := []string{"administrator"}
					if fault == "tags" {
						tags = []string{"monitoring"}
					}
					_ = json.NewEncoder(w).Encode(map[string]any{"name": user, "tags": tags})
					return
				}
				if r.URL.Path == "/api/users/guest" {
					if fault != "guest" {
						w.WriteHeader(http.StatusNotFound)
					}
					return
				}
				if fault == "vhost" {
					w.WriteHeader(http.StatusNotFound)
					return
				}
				read := ".*"
				if fault == "permissions" {
					read = "^$"
				}
				_ = json.NewEncoder(w).Encode(map[string]string{"user": user, "vhost": "/", "configure": ".*", "write": ".*", "read": read})
			}))
			defer server.Close()
			s := testSpec()
			s.ManagementPort = server.Listener.Addr().(*net.TCPAddr).Port
			cfg := memberConfig(s)
			cfg.SystemUsers = map[string]agentapi.Credentials{"admin": {Password: "admin"}, "karakoram": {Password: "control"}}
			if managedAccessCurrent(t.Context(), cfg) {
				t.Fatalf("%s drift was incorrectly treated as a no-op repair", fault)
			}
		})
	}
}

func TestManagedClusterNameAvoidsNativeHostnameDiscovery(t *testing.T) {
	c := memberConfig(testSpec())
	c.Cluster = "orders"
	files, _ := render(t.TempDir(), testSpec(), c)
	for _, file := range files {
		if file.name == "rabbitmq.conf" {
			if !strings.Contains(file.content, "cluster_name = orders\n") {
				t.Fatal("native cluster_status would resolve the host FQDN on every membership observation")
			}
			return
		}
	}
	t.Fatal("RabbitMQ configuration missing")
}
