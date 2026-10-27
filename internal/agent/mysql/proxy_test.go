// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	"strings"
	"testing"
)

func testProxySpec() myspec.Spec {
	return myspec.Spec{Proxy: &myspec.Proxy{Admin: myspec.Credentials{Username: "manager", Password: "admin-secret"}, Monitor: myspec.Credentials{Username: "kk_monitor", Password: "monitor-secret"}, Backends: []myspec.Peer{{Name: "db1", Host: "db1.example", Port: 3306}}, Users: []myspec.ProxyUser{{Username: "app", Password: "app-secret", RequireTLS: true}}, MaxConnections: 100}}
}
func TestRenderProxyConfig(t *testing.T) {
	s := testProxySpec()
	text, err := RenderProxyConfig(s, "/config", "/data")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"127.0.0.1:6032", "0.0.0.0:3306", "writer_hostgroup=10", "backup_writer_hostgroup=11", "reader_hostgroup=20", "offline_hostgroup=30", "max_writers=1", "writer_is_also_reader=0", "use_ssl=1", "have_ssl=true", "monitor_username=\"kk_monitor\"", "default_hostgroup=10"} {
		if !strings.Contains(text, want) {
			t.Errorf("missing %s", want)
		}
	}
	if !strings.Contains(text, `pgsql_ifaces="/data/pgsql-admin.sock"`) || !strings.Contains(text, "pgsql_variables={\n interfaces=\"/data/pgsql-client.sock\"\n}") {
		t.Fatal("unused PostgreSQL listeners were not confined to Unix sockets")
	}
	if strings.Contains(text, "^SELECT") {
		t.Fatal("implicit split routing")
	}
	s.Proxy.ReaderPort = true
	s.Proxy.ReadWriteSplit = true
	text, err = RenderProxyConfig(s, "/config", "/data")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(text, "proxy_port=3307") || !strings.Contains(text, "FOR UPDATE") {
		t.Fatal(text)
	}
	rules := proxyRules(s.Proxy)
	if len(rules) != 3 || rules[0].group != 10 || rules[1].port != 3307 || rules[2].group != 20 {
		t.Fatalf("unsafe rules: %+v", rules)
	}
}
func TestProxySafeDiagnostics(t *testing.T) {
	for input, want := range map[string]string{"Access denied for user secret": "authentication", "certificate secret path": "tls", "timed out secret host": "timeout", "Unknown host private": "dns", "Too many connections": "capacity", "connection refused": "refused", "connection reset": "connection", "secret-password-token": "authentication", "opaque native error with secret": "other"} {
		if got := proxyErrorClass(input); got != want {
			t.Errorf("class %q want %q", got, want)
		}
	}
}
func TestProxySQLQuoting(t *testing.T) {
	if got := proxyQuote("a'\\b"); got != "'a''\\b'" {
		t.Fatal(got)
	}
	if groupQuote("a'\\b") != "'a''\\\\b'" {
		t.Fatal("MySQL escaping")
	}
}
func TestSingletonProxyHasNoGroupMonitor(t *testing.T) {
	s := testProxySpec()
	s.Topology = myspec.TopologySingle
	s.Proxy.ReaderPort = true
	text, err := RenderProxyConfig(s, "/config", "/data")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(text, "mysql_group_replication_hostgroups") {
		t.Fatal("singleton would be taken offline by GR monitor")
	}
	if !strings.Contains(text, "hostgroup=20") {
		t.Fatal("singleton reader listener has no backend")
	}
}

func TestProxyTLSRotationDetection(t *testing.T) {
	old := testProxySpec()
	old.CAPEM = "ca"
	old.ServerCert = &myspec.Certificate{CertPEM: "cert", KeyPEM: "key"}
	if proxyTLSChanged(myspec.Spec{}, old) || proxyTLSChanged(old, old) {
		t.Fatal("initial or unchanged configuration requested TLS rotation")
	}
	next := old
	next.CAPEM = "new-ca"
	if !proxyTLSChanged(old, next) {
		t.Fatal("CA rotation ignored")
	}
	next = old
	next.ServerCert = &myspec.Certificate{CertPEM: "new-cert", KeyPEM: "new-key"}
	if !proxyTLSChanged(old, next) {
		t.Fatal("certificate rotation ignored")
	}
}

func TestProxyTLSReloadStatements(t *testing.T) {
	statements := proxyTLSStatements("/sealed/proxy's")
	if len(statements) != 6 {
		t.Fatalf("TLS reload statements: %v", statements)
	}
	for i, value := range []struct{ variable, file string }{
		{"mysql-ssl_p2s_ca", "ca.pem"},
		{"mysql-ssl_p2s_cert", "server-cert.pem"},
		{"mysql-ssl_p2s_key", "server-key.pem"},
	} {
		if !strings.Contains(statements[i], value.variable) || !strings.Contains(statements[i], "/sealed/proxy''s/"+value.file) {
			t.Fatalf("backend TLS statement: %s", statements[i])
		}
	}
	if statements[3] != "LOAD MYSQL VARIABLES TO RUNTIME" || statements[4] != "SAVE MYSQL VARIABLES TO DISK" || statements[5] != "PROXYSQL RELOAD TLS" {
		t.Fatalf("TLS reload order: %v", statements)
	}
}

func TestProxySQLFrontendAddressFamilies(t *testing.T) {
	for _, host := range []string{"", "0.0.0.0", "::"} {
		for _, reader := range []bool{false, true} {
			s := testProxySpec()
			s.ListenHost = host
			s.Proxy.ReaderPort = reader
			text, err := RenderProxyConfig(s, "/config", "/data")
			if err != nil {
				t.Fatal(err)
			}
			want := "0.0.0.0:3306"
			if host == "::" {
				want += ";[::]:3306"
			}
			if reader {
				want += ";0.0.0.0:3307"
				if host == "::" {
					want += ";[::]:3307"
				}
			}
			if !strings.Contains(text, " interfaces=\""+want+"\"") {
				t.Errorf("host=%q reader=%t missing %q", host, reader, want)
			}
			if !strings.Contains(text, `mysql_ifaces="127.0.0.1:6032"`) {
				t.Fatal("admin interface no longer loopback-only")
			}
		}
	}
}
