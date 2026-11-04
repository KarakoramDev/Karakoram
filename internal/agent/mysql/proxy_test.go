// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"encoding/json"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	"regexp"
	"strconv"
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

func TestRestoreProxyCandidateUsesStrictAgentDecoder(t *testing.T) {
	s := coreTestSpec()
	s.Member, s.Role, s.Topology, s.Version, s.AllowInitialize = "proxy1", myspec.RoleProxy, myspec.TopologyCluster, "8.4.11-11.1", false
	s.Proxy = testProxySpec().Proxy
	s.Proxy.Mode = "galera"
	s.Proxy.Backends = []myspec.Peer{{Name: "seed1", Host: "seed1", Port: 3306}}
	for _, desired := range [][]string{{}, {"proxysql"}} {
		raw, err := json.Marshal(s)
		if err != nil {
			t.Fatal(err)
		}
		config := agentapi.MemberConfig{Engine: myspec.Engine, Member: s.Member, Role: s.Role, EngineSpec: raw, DesiredProcesses: desired}
		if _, err := decodeSpec(config); err != nil {
			t.Fatalf("valid restored seed candidate rejected: %v", err)
		}
		empty := s
		proxy := *s.Proxy
		proxy.Backends = nil
		empty.Proxy = &proxy
		config.EngineSpec, _ = json.Marshal(empty)
		if _, err := decodeSpec(config); err == nil || !strings.Contains(err.Error(), "proxy backends and positive connection limit required") {
			t.Fatalf("empty backend config bypassed real agent validation: %v", err)
		}
		proxy.Backends, proxy.MaxConnections = s.Proxy.Backends, 0
		config.EngineSpec, _ = json.Marshal(empty)
		if _, err := decodeSpec(config); err == nil {
			t.Fatal("zero proxy connection limit bypassed real validation")
		}
	}
}

func assertProxyBackendDefaultJSON(t *testing.T, raw string, maxConnections int) {
	t.Helper()
	var defaults map[string]int
	if err := json.Unmarshal([]byte(raw), &defaults); err != nil {
		t.Fatal("hostgroup backend defaults are not valid numeric JSON", raw, err)
	}
	if len(defaults) != 3 || defaults["weight"] != 1 || defaults["max_connections"] != maxConnections || defaults["use_ssl"] != 1 {
		t.Fatal("monitor-discovered backend defaults lost TLS, capacity or weight", defaults)
	}
}

func TestRenderProxyManagedHostgroupsCarryTLSBackendDefaults(t *testing.T) {
	rows := regexp.MustCompile(`hostgroup_id\s*=\s*([0-9]+)\s*,\s*servers_defaults\s*=\s*("(?:\\.|[^"\\])*")`)
	for _, mode := range []string{"gr", "galera", "single"} {
		for _, maxConnections := range []int{100, 731} {
			t.Run(mode+"/"+strconv.Itoa(maxConnections), func(t *testing.T) {
				s := testProxySpec()
				s.Proxy.MaxConnections = maxConnections
				if mode == "galera" {
					s.Proxy.Mode = mode
				}
				if mode == "single" {
					s.Topology = myspec.TopologySingle
				}
				text, err := RenderProxyConfig(s, "/config", "/data")
				if err != nil {
					t.Fatal(err)
				}
				if mode == "single" {
					if strings.Contains(text, "mysql_hostgroup_attributes") {
						t.Fatal("singleton cold config unexpectedly manages monitor hostgroup attributes")
					}
					return
				}
				if !strings.Contains(text, "mysql_hostgroup_attributes") {
					t.Fatal("cluster cold config omitted monitor-discovered server defaults")
				}
				found := map[int]bool{}
				for _, match := range rows.FindAllStringSubmatch(text, -1) {
					group, err := strconv.Atoi(match[1])
					if err != nil {
						t.Fatal(err)
					}
					if found[group] {
						t.Fatal("duplicate managed hostgroup defaults", group)
					}
					found[group] = true
					raw, err := strconv.Unquote(match[2])
					if err != nil {
						t.Fatal("backend defaults were not safely quoted in the cold config", err)
					}
					assertProxyBackendDefaultJSON(t, raw, maxConnections)
				}
				if len(found) != 4 || !found[10] || !found[11] || !found[20] || !found[30] {
					t.Fatal("cold configuration did not protect every monitor-managed hostgroup", found)
				}
			})
		}
	}
}
