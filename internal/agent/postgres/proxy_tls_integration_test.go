// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

//go:build integration

package postgres

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"github.com/jackc/pgx/v5"
)

func TestNativeProxyBackendCertificateVerification(t *testing.T) {
	if os.Getenv("KARAKORAM_PG_BIN_DIR") == "" {
		t.Skip("set KARAKORAM_PG_BIN_DIR for native backend TLS acceptance")
	}
	if os.Geteuid() == 0 {
		t.Fatal("native PostgreSQL requires an unprivileged runtime user")
	}
	s := testSpec()
	s.BinDir = os.Getenv("KARAKORAM_PG_BIN_DIR")
	s.Postgres.Parameters = map[string]string{"shared_buffers": "32MB", "max_connections": "30"}
	s.CAPEM, s.ServerCert = testTLS(t)
	h := startNativeAgent(t, s)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	admin, err := localConnect(ctx, s, dataPaths(h.dir))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = admin.Exec(ctx, "ALTER SYSTEM SET listen_addresses='127.0.0.2'"); err != nil {
		t.Fatal(err)
	}
	_ = admin.Close(ctx)
	response, err := h.request("POST", "/v1/processes/postgres/restart", nil)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != 200 {
		t.Fatal("could not isolate native backend listener")
	}
	untrustedCA, _ := testTLS(t)
	for _, tc := range []struct {
		name, ca string
		trusted  bool
	}{{"trusted", s.CAPEM, true}, {"untrusted", untrustedCA, false}} {
		t.Run(tc.name, func(t *testing.T) {
			proxy := &pgspec.Proxy{Servers: []string{"127.0.0.2"}, Monitor: s.Monitor, Admin: pgspec.Credentials{Username: "tls_admin", Password: "native-proxy-admin"}, Users: []pgspec.ProxyUser{{Username: s.Superuser.Username, Password: s.Superuser.Password}}, Threads: 1, MaxConnections: 16, BackendTLS: true}
			proxyPaths := nativeProxy(t, proxy, "127.0.0.1", tc.ca)
			control, err := proxyConnect(ctx, proxy)
			if err != nil {
				t.Fatal(err)
			}
			defer control.Close(ctx)
			// Simulate an existing proxy with the old DNS settings. Apply must
			// load the short cache policy into runtime, not only write the cnf.
			for name, value := range map[string]string{
				"pgsql-monitor_local_dns_cache_ttl":              "300000",
				"pgsql-monitor_local_dns_cache_refresh_interval": "60000",
				"pgsql-monitor_local_dns_resolver_queue_maxsize": "256",
			} {
				if _, err := control.Exec(ctx, "UPDATE global_variables SET variable_value="+sqlQuote(value)+" WHERE variable_name="+sqlQuote(name), pgx.QueryExecModeSimpleProtocol); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := control.Exec(ctx, "LOAD PGSQL VARIABLES TO RUNTIME", pgx.QueryExecModeSimpleProtocol); err != nil {
				t.Fatal(err)
			}
			if _, err := reconcileProxy(ctx, control, proxy, "127.0.0.1", filepath.Join(proxyPaths.root, "ca.pem")); err != nil {
				t.Fatal(err)
			}
			dns, err := queryTable(ctx, control, "SELECT variable_name,variable_value FROM runtime_global_variables WHERE variable_name LIKE 'pgsql-monitor_local_dns_%'")
			if err != nil {
				t.Fatal(err)
			}
			actual := keyRows(dns, "variable_name")
			for name, want := range map[string]string{
				"pgsql-monitor_local_dns_cache_ttl":              "5000",
				"pgsql-monitor_local_dns_cache_refresh_interval": "1000",
				"pgsql-monitor_local_dns_resolver_queue_maxsize": "128",
			} {
				if got := actual[name]["variable_value"]; got != want {
					t.Errorf("runtime %s=%q, want %q", name, got, want)
				}
			}
			if !tc.trusted {
				// Check the monitor as well as application traffic. The monitor
				// must not promote an impersonating backend into the writer pool.
				deadline := time.Now().Add(15 * time.Second)
				rejected := false
				for time.Now().Before(deadline) {
					rows, err := queryTable(ctx, control, "SELECT connect_error FROM monitor.pgsql_server_connect_log WHERE connect_error IS NOT NULL")
					if err != nil {
						t.Fatal(err)
					}
					for _, row := range rows {
						// OpenSSL reports a CA with the trusted subject but a different
						// key as an EVP signature failure, not "certificate verify failed".
						msg := strings.ToLower(row["connect_error"])
						rejected = rejected || strings.Contains(msg, "certificate verify failed") || strings.Contains(msg, "ssl error")
					}
					if rejected {
						break
					}
					time.Sleep(100 * time.Millisecond)
				}
				if !rejected {
					t.Fatal("monitor did not reject the untrusted backend certificate")
				}
				// Force a writer entry to exercise data-path certificate checks
				// independently of the monitor's safe routing exclusion.
				for _, sql := range []string{"UPDATE global_variables SET variable_value='false' WHERE variable_name='pgsql-monitor_enabled'", "LOAD PGSQL VARIABLES TO RUNTIME", "UPDATE pgsql_servers SET hostgroup_id=10,status='ONLINE'", "DELETE FROM pgsql_replication_hostgroups", "LOAD PGSQL SERVERS TO RUNTIME"} {
					if _, err := control.Exec(ctx, sql, pgx.QueryExecModeSimpleProtocol); err != nil {
						t.Fatal(err)
					}
				}
			}
			attempt, stop := context.WithTimeout(ctx, 10*time.Second)
			defer stop()
			var lastErr error
			for {
				client, err := userConnect(attempt, "127.0.0.1", "postgres", s.Superuser.Username, s.Superuser.Password)
				if err == nil {
					var one int
					err = client.QueryRow(attempt, "SELECT 1", pgx.QueryExecModeSimpleProtocol).Scan(&one)
					_ = client.Close(ctx)
					if err == nil {
						if !tc.trusted {
							t.Fatal("application query accepted an untrusted backend certificate")
						}
						return
					}
				}
				lastErr = err
				if !tc.trusted {
					return
				}
				select {
				case <-attempt.Done():
					t.Fatalf("trusted backend did not accept traffic: %v", lastErr)
				case <-time.After(100 * time.Millisecond):
				}
			}
		})
	}
}
