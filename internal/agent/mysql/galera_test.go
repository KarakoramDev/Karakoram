// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

func testGaleraSpec() myspec.Spec {
	s := coreTestSpec()
	s.Topology = myspec.TopologyCluster
	s.ServerDir = "/opt/karakoram/percona-xtradb-cluster-8.4.11-11.1"
	s.Version = "8.4.11-11.1"
	s.Galera = &myspec.Galera{ClusterName: "test-cluster", FormationEpoch: 1, Peers: []myspec.Peer{{Name: s.Member, Host: "127.0.0.1", Port: 4567}}, GaleraPort: 4567, ISTPort: 4568, SSTPort: 4444, GCacheSize: "32M", Bootstrap: true}
	s.Galera.TrafficCert = s.ServerCert
	s.Galera.TrafficCAPEM = "-----BEGIN CERTIFICATE-----\nYg==\n-----END CERTIFICATE-----"
	return s
}
func TestGaleraRendering(t *testing.T) {
	s := testGaleraSpec()
	p := dataPaths(t.TempDir())
	text := renderConfig(s, p)
	for _, want := range []string{"wsrep_provider=", "gcomm://127.0.0.1:4567", "ist.recv_addr=127.0.0.1:4568", "wsrep_sst_receive_address=\"127.0.0.1:4444\"", "pxc_encrypt_cluster_traffic=\"OFF\"", "socket.ssl=true", "socket.ssl_cert=" + filepath.Join(p.root, "galera-cert.pem"), "socket.ssl_key=" + filepath.Join(p.root, "galera-key.pem"), "socket.ssl_ca=" + filepath.Join(p.root, "galera-ca.pem"), "pxc_strict_mode=\"ENFORCING\"", "[sst]", "encrypt=4", "donor-timeout=3", "ssl-cert=" + optionQuote(filepath.Join(p.root, "galera-cert.pem")), "ssl-key=" + optionQuote(filepath.Join(p.root, "galera-key.pem")), "ssl-ca=" + optionQuote(filepath.Join(p.root, "galera-ca.pem")), "require_secure_transport=\"ON\""} {
		if !strings.Contains(text, want) {
			t.Errorf("missing %q in %s", want, text)
		}
	}
	if strings.Contains(text, "cpat=") {
		t.Fatal("agent must not override upstream SST keep-lists")
	}
	if strings.Contains(text, "group_replication") {
		t.Fatal("Galera rendered GR settings")
	}
	env := strings.Join(galeraEnv(s), "\n")
	if !strings.Contains(env, "/bin/pxc_extra/pxb-8.4/bin:") {
		t.Fatal(env)
	}
}

func TestGaleraListenerUsesNumericBindNotDNSIdentity(t *testing.T) {
	for _, tc := range []struct{ host, policy, bind string }{
		{"nmypxc6m0", "", "0.0.0.0"},
		{"pxcm0.internal.example", "", "0.0.0.0"},
		{"nmypxc6m0", "0.0.0.0", "0.0.0.0"},
		{"zeropspxcm0", "::", "::"},
		{"10.65.3.13", "", "10.65.3.13"},
		{"127.0.0.1", "", "127.0.0.1"},
		{"2001:db8::13", "", "2001:db8::13"},
	} {
		t.Run(tc.host, func(t *testing.T) {
			s := testGaleraSpec()
			s.Galera.Peers[0].Host = tc.host
			s.Galera.ListenHost = tc.policy
			opts := RenderGaleraOptions(s, t.TempDir())
			provider := opts["wsrep_provider_options"]
			listener := strings.Split(strings.TrimPrefix(provider, "gmcast.listen_addr="), ";")[0]
			u, err := url.Parse(listener)
			if err != nil {
				t.Fatal(err)
			}
			if ip := net.ParseIP(u.Hostname()); ip == nil || u.Hostname() != tc.bind || u.Port() != "4567" {
				t.Fatalf("listener %q is not the expected numeric bind %s", listener, tc.bind)
			}
			// Advertised peer, node, IST and SST addresses keep the stable
			// identity; changing a bind must not publish wildcard endpoints.
			for key, want := range map[string]string{
				"wsrep_cluster_address":     "gcomm://" + net.JoinHostPort(tc.host, "4567"),
				"wsrep_node_address":        net.JoinHostPort(tc.host, "4567"),
				"wsrep_sst_receive_address": net.JoinHostPort(tc.host, "4444"),
			} {
				if opts[key] != want {
					t.Fatalf("%s=%s, want %s", key, opts[key], want)
				}
			}
			if !strings.Contains(provider, ";ist.recv_addr="+net.JoinHostPort(tc.host, "4568")+";") || !strings.Contains(provider, ";socket.ssl=true;") {
				t.Fatal("bind policy changed IST identity or disabled encryption", provider)
			}
		})
	}
}

func TestGaleraListenerRejectsHostnameBindPolicy(t *testing.T) {
	s := testGaleraSpec()
	s.Galera.ListenHost = s.Member
	if err := s.Galera.Validate(); err == nil {
		t.Fatal("accepted DNS name as a numeric Galera bind policy")
	}
}
func TestGaleraBootstrapLauncherOnce(t *testing.T) {
	withTransferHoldoff(t, 0) // models launch arguments, not unsynced relaunch timing
	s := testGaleraSpec()
	p := dataPaths(t.TempDir())
	if err := privateDir(p.root); err != nil {
		t.Fatal(err)
	}
	s.ServerDir = t.TempDir()
	if err := os.MkdirAll(filepath.Join(s.ServerDir, "bin"), 0700); err != nil {
		t.Fatal(err)
	}
	fake := "#!/bin/sh\nprintf '%s\\n' \"$@\"\n"
	if err := os.WriteFile(filepath.Join(s.ServerDir, "bin", "mysqld"), []byte(fake), 0700); err != nil {
		t.Fatal(err)
	}
	launcher := filepath.Join(p.root, "launch")
	if err := os.WriteFile(launcher, []byte(galeraLauncher(s, p)), 0700); err != nil {
		t.Fatal(err)
	}
	first, err := exec.Command(launcher).CombinedOutput()
	if err != nil {
		t.Fatalf("%v %s", err, first)
	}
	second, err := exec.Command(launcher).CombinedOutput()
	if err != nil {
		t.Fatalf("%v %s", err, second)
	}
	if !strings.Contains(string(first), "--wsrep-new-cluster") || strings.Contains(string(first), "--wsrep-start-position=") {
		t.Fatal(string(first))
	}
	if strings.Contains(string(second), "--wsrep-new-cluster") || strings.Contains(string(second), "--wsrep-start-position") {
		t.Fatal("supervisor restart reused formation arguments", string(second))
	}
	s.Galera.Bootstrap = false
	s.Galera.FormationEpoch = 2
	if err := os.WriteFile(launcher, []byte(galeraLauncher(s, p)), 0700); err != nil {
		t.Fatal(err)
	}
	first, err = exec.Command(launcher).CombinedOutput()
	if err != nil {
		t.Fatalf("%v %s", err, first)
	}
	second, err = exec.Command(launcher).CombinedOutput()
	if err != nil {
		t.Fatalf("%v %s", err, second)
	}
	if strings.Contains(string(first), "--wsrep-new-cluster") || strings.Contains(string(first), "--wsrep-start-position=") {
		t.Fatal("new SST join reused a formation position or bootstrapped", string(first))
	}
	if strings.Contains(string(second), "--wsrep-start-position=") {
		t.Fatal("new SST join used a stale recovered position", string(second))
	}
}
func TestGaleraSSTIdentityAuthorization(t *testing.T) {
	s := testGaleraSpec()
	s.Galera.Bootstrap = false
	p := dataPaths(t.TempDir())
	for _, d := range []string{p.root, p.data} {
		if err := privateDir(d); err != nil {
			t.Fatal(err)
		}
	}
	fresh, err := guardInitialization(s, p)
	if err != nil || fresh {
		t.Fatalf("SST guard fresh=%v err=%v", fresh, err)
	}
	if err = os.Mkdir(filepath.Join(p.data, "mysql"), 0700); err != nil {
		t.Fatal(err)
	}
	if fresh, err = guardInitialization(s, p); err != nil || fresh {
		t.Fatalf("SST-provisioned identity rejected: %v %v", fresh, err)
	}
	s.Member = "foreign"
	if _, err = guardInitialization(s, p); err == nil {
		t.Fatal("foreign epoch accepted SST data")
	}
}

func TestGaleraProxyRendering(t *testing.T) {
	s := testProxySpec()
	s.Topology = myspec.TopologyCluster
	s.Proxy.Mode = "galera"
	text, err := RenderProxyConfig(s, "/config", "/data")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"mysql_galera_hostgroups", "writer_is_also_reader=2", "max_writers=1", "monitor_galera_healthcheck_interval"} {
		if !strings.Contains(text, want) {
			t.Errorf("missing %q", want)
		}
	}
	if strings.Contains(text, "group_replication_hostgroups") || strings.Contains(text, "groupreplication_healthcheck") {
		t.Fatal("Galera proxy configured a GR monitor")
	}
}

func TestGaleraSpecValidation(t *testing.T) {
	s := testGaleraSpec()
	if err := s.Validate(); err != nil {
		t.Fatal(err)
	}
	s.Topology = myspec.TopologySingle
	if s.Validate() == nil {
		t.Fatal("single topology accepted Galera block")
	}
	s.Topology = myspec.TopologyCluster
	s.Galera.Peers[0].Name = "foreign"
	if s.Validate() == nil {
		t.Fatal("missing local Galera peer accepted")
	}
}

func TestGaleraOperatorConfirmationAndBootstrapIdentity(t *testing.T) {
	s := testGaleraSpec()
	e := &Runner{current: s}
	for _, token := range []string{"", "yes", "all-members-fenced"} {
		err := e.galeraForcePrimary(context.Background(), myspec.GaleraForcePrimaryRequest{
			ClusterName: s.Galera.ClusterName, FormationEpoch: 1, Confirmation: token,
		})
		if err == nil || !strings.Contains(err.Error(), "operator confirmation") {
			t.Fatalf("unconfirmed force-primary admitted: %v", err)
		}
	}
	err := e.galeraBootstrap(context.Background(), nil, myspec.GaleraBootstrapRequest{
		ClusterName: s.Galera.ClusterName, FormationEpoch: 2,
	})
	if err == nil || !strings.Contains(err.Error(), "intent mismatch") {
		t.Fatalf("foreign bootstrap epoch admitted: %v", err)
	}
	if _, err := e.offlineGTID(context.Background(), nil); err == nil || !strings.Contains(err.Error(), "wsrep-recover") {
		t.Fatalf("Galera admitted legacy offline server startup: %v", err)
	}
}

func TestGaleraSSTRequiresExplicitNewMemberAuthorization(t *testing.T) {
	s := testGaleraSpec()
	s.Galera.Bootstrap = false
	s.AllowInitialize = false
	p := dataPaths(t.TempDir())
	for _, d := range []string{p.root, p.data} {
		if err := privateDir(d); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := guardInitialization(s, p); err == nil {
		t.Fatal("SST admitted without CP new-member authorization")
	}
	s.AllowInitialize = true
	if err := os.Mkdir(filepath.Join(p.data, "mysql"), 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := guardInitialization(s, p); err == nil {
		t.Fatal("foreign data admitted without durable SST intent")
	}
}

func TestGaleraTrafficTLSCannotFallBack(t *testing.T) {
	s := testGaleraSpec()
	s.Galera.TrafficCert = nil
	if err := s.Validate(); err == nil {
		t.Fatal("Galera accepted missing shared traffic TLS material")
	}
	s.Galera.TrafficCert = &myspec.Certificate{}
	if err := s.Validate(); err == nil {
		t.Fatal("Galera accepted invalid shared traffic TLS material")
	}
	s = testGaleraSpec()
	s.Galera.TrafficCAPEM = ""
	if err := s.Validate(); err == nil {
		t.Fatal("Galera accepted missing traffic trust")
	}
	s.Galera.TrafficCAPEM = s.CAPEM
	if err := s.Validate(); err == nil {
		t.Fatal("Galera accepted SQL trust for peer admission")
	}
}

func TestGaleraTrafficTrustRejectsAdditionalAnchors(t *testing.T) {
	for _, kind := range []string{"SQL-anchor", "duplicate", "trailing", "leading"} {
		t.Run(kind, func(t *testing.T) {
			s := testGaleraSpec()
			switch kind {
			case "SQL-anchor":
				s.Galera.TrafficCAPEM += s.CAPEM
			case "duplicate":
				s.Galera.TrafficCAPEM += s.Galera.TrafficCAPEM
			case "trailing":
				s.Galera.TrafficCAPEM += "unexpected trust data"
			case "leading":
				s.Galera.TrafficCAPEM = "unexpected trust data\n" + s.Galera.TrafficCAPEM
			}
			if err := s.Validate(); err == nil {
				t.Fatal("Galera accepted non-single dedicated trust anchor")
			}
		})
	}
}

func TestGaleraTrafficRotationRequiresStoppedMember(t *testing.T) {
	s := testGaleraSpec()
	p := dataPaths(t.TempDir())
	if err := privateDir(p.root); err != nil {
		t.Fatal(err)
	}
	for file, text := range map[string]string{"galera-ca.pem": s.Galera.TrafficCAPEM, "galera-cert.pem": s.Galera.TrafficCert.CertPEM, "galera-key.pem": s.Galera.TrafficCert.KeyPEM} {
		if err := os.WriteFile(filepath.Join(p.root, file), []byte(text), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := guardGaleraTrafficRotation(s, p, true); err != nil {
		t.Fatal(err)
	}
	s.Galera.TrafficCAPEM += "\n"
	if err := guardGaleraTrafficRotation(s, p, true); err == nil {
		t.Fatal("live traffic TLS rotation accepted")
	}
	if err := guardGaleraTrafficRotation(s, p, false); err != nil {
		t.Fatal("stopped traffic TLS rotation refused", err)
	}
}

func TestGaleraWireRejectsOversizedProtocolIdentity(t *testing.T) {
	s := testGaleraSpec()
	s.Galera.ClusterName = "9347c031-40ab-4a90-b950-ec3525ae4321"
	if err := s.Validate(); err == nil {
		t.Fatal("36-byte UUID accepted for fixed-width gmcast cluster name")
	}
	s.Galera.ClusterName = "9347c03140ab4a90b950ec3525ae4321"
	if err := s.Validate(); err != nil {
		t.Fatal("32-byte numeric-leading PXC identity rejected", err)
	}
}
