// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
)

func TestProxyRestartStormSettings(t *testing.T) {
	p := &pgspec.Proxy{Servers: []string{"db"}, MaxConnections: 89}
	config := renderProxy(p, dataPaths(t.TempDir()), "")
	runtime := proxyRuntimeSettings(p, "")
	// 3.0.11 counts at most retries+1 errors before shunning. Lowering retries
	// must not be mistaken for disabling shunning. Keep it short and recover
	// in 1-2s, bound pending attempts, and throttle the first TLS reconnect wave.
	for name, want := range map[string]string{
		"connect_retries_on_failure": "2", "connect_retries_delay": "100",
		"connect_timeout_server": "1000", "connect_timeout_server_max": "3000",
		"shun_on_failures": "3", "shun_recovery_time_sec": "1",
		"throttle_connections_per_sec_to_hostgroup": "20",
		"free_connections_pct":                      "10", "connection_warming": "false",
		"ping_interval_server_msec": "1000", "ping_timeout_server": "500",
		"monitor_enabled": "true", "monitor_read_only_interval": "500",
		"monitor_read_only_timeout": "500", "monitor_read_only_max_timeout_count": "2",
		"monitor_ping_interval": "1000", "monitor_ping_timeout": "500", "monitor_ping_max_failures": "2",
		"monitor_connect_interval": "1000", "monitor_connect_timeout": "500",
		"monitor_writer_is_also_reader": "false",
	} {
		if runtime["pgsql-"+name] != want || !strings.Contains(config, "\n "+name+"="+want+";\n") {
			t.Errorf("bootstrap/runtime recovery setting %s must equal %s", name, want)
		}
	}
	// This is intentionally a MySQL variable: pinned 3.0.11's PostgreSQL
	// shun_and_killall mistakenly uses it to forward-date the recovery clock.
	if !strings.Contains(config, "mysql_variables={\n monitor_ping_interval=1000;\n}") {
		t.Fatal("cross-module recovery timestamp workaround missing")
	}
	c := proxyDriftFixture(t, p, false)
	plan, err := proxyPlan(t.Context(), c, p, "", "")
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(plan, "\n")
	for _, want := range []string{
		"'89'", "UPDATE global_variables SET variable_value='1000' WHERE variable_name='mysql-monitor_ping_interval'",
		"LOAD MYSQL VARIABLES TO RUNTIME", "SAVE MYSQL VARIABLES TO DISK",
	} {
		if !strings.Contains(joined, want) {
			t.Errorf("runtime plan missing %s", want)
		}
	}
}

func TestProxyHealthRequiresOnlineWriter(t *testing.T) {
	p := &pgspec.Proxy{Servers: []string{"db"}}
	for _, tc := range []struct {
		name     string
		backends []pgspec.ProxyBackend
		want     bool
	}{
		{"empty-runtime", nil, false},
		{"shunned-writer", []pgspec.ProxyBackend{{Host: "db", Hostgroup: 10, Status: "SHUNNED"}}, false},
		{"offline-writer", []pgspec.ProxyBackend{{Host: "db", Hostgroup: 10, Status: "OFFLINE_HARD"}}, false},
		{"only-online-reader", []pgspec.ProxyBackend{{Host: "db", Hostgroup: 20, Status: "ONLINE"}}, false},
		{"shunned-writer-online-reader", []pgspec.ProxyBackend{{Host: "db", Hostgroup: 10, Status: "SHUNNED"}, {Host: "replica", Hostgroup: 20, Status: "ONLINE"}}, false},
		{"online-writer", []pgspec.ProxyBackend{{Host: "db", Hostgroup: 10, Status: "ONLINE"}}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := proxyWriterReady(p, tc.backends); got != tc.want {
				t.Fatalf("writer readiness=%t want=%t", got, tc.want)
			}
		})
	}
	if !proxyWriterReady(&pgspec.Proxy{}, nil) {
		t.Fatal("bootstrap without published backends must remain possible")
	}
}
