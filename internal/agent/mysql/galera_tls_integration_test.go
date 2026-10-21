//go:build integration

// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	mysqldriver "github.com/go-sql-driver/mysql"
)

type nativeGaleraConfig struct {
	config agentapi.MemberConfig
	spec   myspec.Spec
}

func nativeGaleraConfigs(t *testing.T, file string) []nativeGaleraConfig {
	t.Helper()
	dir := os.Getenv("KK_PXC_NATIVE_CONFIG_DIR")
	if dir == "" {
		t.Skip("export production Core.Sync/MemberConfig fixtures with KK_PXC_NATIVE_CONFIG_DIR")
	}
	raw, err := os.ReadFile(filepath.Join(dir, file))
	if err != nil {
		t.Fatal(err)
	}
	var configs []agentapi.MemberConfig
	if err = json.Unmarshal(raw, &configs); err != nil {
		t.Fatal(err)
	}
	version := os.Getenv("KARAKORAM_PXC_VERSION")
	if version == "" {
		version = "8.4.11-11.1"
	}
	if version != "8.4.11-11.1" && version != "8.0.46-38.1" {
		t.Fatalf("unsupported native PXC catalog version %s", version)
	}
	var data, proxy []nativeGaleraConfig
	for _, c := range configs {
		var s myspec.Spec
		if err = json.Unmarshal(c.EngineSpec, &s); err != nil {
			t.Fatal(err)
		}
		if err = s.Validate(); err != nil {
			t.Fatalf("invalid CP exported %s config: %v", c.Member, err)
		}
		if s.Version != version {
			t.Fatalf("CP fixture version=%s requested native version=%s", s.Version, version)
		}
		entry := nativeGaleraConfig{config: c, spec: s}
		if s.Role == myspec.RoleProxy {
			proxy = append(proxy, entry)
		} else if s.Galera != nil {
			data = append(data, entry)
		}
	}
	count := 3
	if file == "added-members.json" {
		count = 5
	}
	if len(data) != count || len(proxy) != 1 {
		t.Fatalf("CP export %s must contain %d data and one proxy: data=%d proxy=%d", file, count, len(data), len(proxy))
	}
	for i := range data {
		if data[i].spec.Galera.Bootstrap {
			data[0], data[i] = data[i], data[0]
			break
		}
	}
	if file == "members.json" && !data[0].spec.Galera.Bootstrap {
		t.Fatal("CP export lacks initial bootstrap intent")
	}
	return append(data, proxy[0])
}

// Preserve the exported security/resource policy and change only native
// listener locations. Recovery history is changed separately by real actions.
func nativeGaleraLocalConfig(next, current myspec.Spec) myspec.Spec {
	next.Peers = current.Peers
	next.Galera.Peers = current.Galera.Peers
	next.Galera.GaleraPort = current.Galera.GaleraPort
	next.Galera.ISTPort = current.Galera.ISTPort
	next.Galera.SSTPort = current.Galera.SSTPort
	return next
}

func nativeCertificateName(t *testing.T, raw string) string {
	t.Helper()
	block, _ := pem.Decode([]byte(raw))
	if block == nil {
		t.Fatal("CP certificate missing")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	if len(cert.DNSNames) > 0 {
		return cert.DNSNames[0]
	}
	if len(cert.IPAddresses) > 0 {
		return cert.IPAddresses[0].String()
	}
	t.Fatal("CP certificate has no valid DNS/IP SAN")
	return ""
}

// Inspect effective runtime options, upstream SST config and the actual SQL TLS
// handshake: manual mode is encrypted, not merely an OFF convenience flag.
func nativeGaleraTLSProof(t *testing.T, ctx context.Context, nodes []*groupRunnerAgent, dir string, traffic myspec.Certificate) {
	t.Helper()
	seenSQL, seenAgent := map[string]bool{}, map[string]bool{}
	for _, a := range nodes {
		s := a.spec
		if s.ServerCert == nil || s.Galera.TrafficCert == nil || s.Galera.TrafficCAPEM == s.CAPEM || s.Galera.TrafficCert.KeyPEM != traffic.KeyPEM || s.ServerCert.KeyPEM == traffic.KeyPEM || s.ServerCert.KeyPEM == a.config.AgentTLS.KeyPEM || seenSQL[s.ServerCert.KeyPEM] || seenAgent[a.config.AgentTLS.KeyPEM] || a.config.AgentTLS.KeyPEM == traffic.KeyPEM {
			t.Fatal("PXC must use distinct SQL/agent leaves and one shared traffic leaf")
		}
		seenSQL[s.ServerCert.KeyPEM] = true
		seenAgent[a.config.AgentTLS.KeyPEM] = true
		p := dataPaths(filepath.Join(dir, s.Member))
		for file, want := range map[string]string{"galera-ca.pem": s.Galera.TrafficCAPEM, "galera-cert.pem": traffic.CertPEM, "galera-key.pem": traffic.KeyPEM, "server-cert.pem": s.ServerCert.CertPEM, "server-key.pem": s.ServerCert.KeyPEM} {
			raw, err := os.ReadFile(filepath.Join(p.root, file))
			if err != nil || string(raw) != want {
				t.Fatalf("%s TLS file %s mismatch: %v", s.Member, file, err)
			}
		}
		db, err := a.runner.LocalAccessDB(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var actualVersion string
		if err = db.QueryRowContext(ctx, "SELECT VERSION()").Scan(&actualVersion); err != nil || !strings.HasPrefix(actualVersion, s.Version) {
			t.Fatalf("native PXC binary version=%s CP spec=%s err=%v", actualVersion, s.Version, err)
		}
		var auto, secure bool
		var options, certPath, keyPath, scratch string
		if err = db.QueryRowContext(ctx, "SELECT @@global.pxc_encrypt_cluster_traffic,@@global.require_secure_transport,@@global.wsrep_provider_options,@@global.ssl_cert,@@global.ssl_key,@@global.tmpdir").Scan(&auto, &secure, &options, &certPath, &keyPath, &scratch); err != nil {
			t.Fatal(err)
		}
		if filepath.Clean(scratch) != filepath.Clean(os.TempDir()) {
			t.Fatalf("%s native server did not retain the caller's temporary directory: %s", s.Member, scratch)
		}
		effective := map[string]string{}
		for _, part := range strings.Split(options, ";") {
			key, value, ok := strings.Cut(part, "=")
			if ok {
				effective[strings.TrimSpace(key)] = strings.TrimSpace(value)
			}
		}
		ssl := strings.ToLower(effective["socket.ssl"])
		if auto || !secure || ssl != "true" && ssl != "yes" && ssl != "1" || effective["socket.ssl_cert"] != filepath.Join(p.root, "galera-cert.pem") || effective["socket.ssl_key"] != filepath.Join(p.root, "galera-key.pem") || effective["socket.ssl_ca"] != filepath.Join(p.root, "galera-ca.pem") || certPath != filepath.Join(p.root, "server-cert.pem") || keyPath != filepath.Join(p.root, "server-key.pem") {
			t.Fatalf("%s effective encrypted-channel policy invalid: auto=%v secure=%v options=%s SQLcert=%s", s.Member, auto, secure, options, certPath)
		}
		config, err := os.ReadFile(p.config)
		if err != nil {
			t.Fatal(err)
		}
		for _, want := range []string{"[sst]", "encrypt=4", "ssl-cert=" + optionQuote(filepath.Join(p.root, "galera-cert.pem")), "ssl-key=" + optionQuote(filepath.Join(p.root, "galera-key.pem")), "ssl-ca=" + optionQuote(filepath.Join(p.root, "galera-ca.pem"))} {
			if !strings.Contains(string(config), want) {
				t.Fatalf("%s missing encrypted SST option %s", s.Member, want)
			}
		}
		block, _ := pem.Decode([]byte(s.ServerCert.CertPEM))
		if block == nil {
			t.Fatal("SQL leaf missing")
		}
		parsed, err := x509.ParseCertificate(block.Bytes)
		if err != nil || parsed.VerifyHostname(s.Member) != nil {
			t.Fatalf("SQL leaf lacks correct member SAN: %s %v", s.Member, err)
		}
		expectedDER := block.Bytes
		tlsName := "native-galera-direct-" + s.Member
		policy := tlsConfig(x509Pool(s.CAPEM), nativeCertificateName(t, s.ServerCert.CertPEM))
		policy.VerifyConnection = func(state tls.ConnectionState) error {
			if len(state.PeerCertificates) == 0 || !bytes.Equal(state.PeerCertificates[0].Raw, expectedDER) {
				return errors.New("SQL listener did not present own member leaf")
			}
			return nil
		}
		if err = mysqldriver.RegisterTLSConfig(tlsName, policy); err != nil {
			t.Fatal(err)
		}
		cfg := mysqldriver.NewConfig()
		cfg.User, cfg.Passwd, cfg.Net, cfg.TLSConfig = s.Admin.Username, s.Admin.Password, "tcp", tlsName
		for _, peer := range s.Peers {
			if peer.Name == s.Member {
				cfg.Addr = galeraAddress(peer.Host, peer.Port)
			}
		}
		cfg.Timeout, cfg.ReadTimeout, cfg.WriteTimeout = 3*time.Second, 3*time.Second, 3*time.Second
		connection, err := sql.Open("mysql", cfg.FormatDSN())
		if err == nil {
			err = connection.PingContext(ctx)
		}
		if err == nil {
			// MySQL exposes the disabling sentinel as the literal string
			// "NULL", not protocol SQL NULL; empty string would allow files.
			var restricted string
			if err = connection.QueryRowContext(ctx, "SELECT @@global.secure_file_priv").Scan(&restricted); err != nil || restricted != "NULL" {
				connection.Close()
				t.Fatalf("%s SQL admin file boundary not enforced: %v", s.Member, err)
			}
			for _, sensitive := range []string{filepath.Join(p.root, "galera-key.pem"), filepath.Join(p.root, "server-key.pem"), filepath.Join(filepath.Dir(p.root), "config.json")} {
				if info, statErr := os.Stat(sensitive); statErr != nil || !info.Mode().IsRegular() || info.Size() == 0 {
					connection.Close()
					t.Fatalf("%s sensitive runtime file unavailable for denial proof: %v", s.Member, statErr)
				}
				var keyBytes sql.NullString
				if err = connection.QueryRowContext(ctx, "SELECT LOAD_FILE(?)", sensitive).Scan(&keyBytes); err != nil || keyBytes.Valid {
					connection.Close()
					t.Fatalf("%s SQL admin read a protected runtime key (no bytes logged): %v", s.Member, err)
				}
			}
			if _, grantErr := connection.ExecContext(ctx, "GRANT FILE ON *.* TO "+account(s.Admin.Username)); grantErr == nil {
				connection.Close()
				t.Fatal("SQL admin regained FILE privilege")
			}
		}
		if connection != nil {
			connection.Close()
		}
		mysqldriver.DeregisterTLSConfig(tlsName)
		if err != nil {
			t.Fatalf("%s distinct SQL leaf handshake: %v", s.Member, err)
		}
		wrongCA, _ := coreTLS(t)
		wrongName := tlsName + "-wrong-ca"
		if err = mysqldriver.RegisterTLSConfig(wrongName, tlsConfig(x509Pool(wrongCA), nativeCertificateName(t, s.ServerCert.CertPEM))); err != nil {
			t.Fatal(err)
		}
		cfg.TLSConfig = wrongName
		denied, openErr := sql.Open("mysql", cfg.FormatDSN())
		if openErr != nil {
			t.Fatal(openErr)
		}
		deniedErr := denied.PingContext(ctx)
		denied.Close()
		mysqldriver.DeregisterTLSConfig(wrongName)
		var unknown x509.UnknownAuthorityError
		if !errors.As(deniedErr, &unknown) {
			t.Fatalf("%s wrong CA did not fail certificate verification: %v", s.Member, deniedErr)
		}
	}
	t.Log("manual TLS: effective encrypted replication/IST + encrypt=4 SST with shared traffic leaf; verified distinct per-member SQL listener leaves")
}

// Galera's TLS admission must reject both a self-signed stranger and a valid
// SQL/agent CA leaf: client-plane credentials are not replication credentials.
func nativeGaleraRejectUnauthorizedPeers(t *testing.T, ctx context.Context, node *groupRunnerAgent, retired ...myspec.Certificate) {
	t.Helper()
	s := node.spec
	local := galeraLocal(s)
	address := galeraAddress(local.Host, s.Galera.GaleraPort)
	strangerCA, stranger := coreTLS(t)
	candidates := map[string]myspec.Certificate{
		"self-signed":     {CertPEM: strangerCA, KeyPEM: stranger.KeyPEM},
		"SQL-CA-signed":   *s.ServerCert,
		"agent-CA-signed": {CertPEM: node.config.AgentTLS.CertPEM, KeyPEM: node.config.AgentTLS.KeyPEM},
	}
	if len(retired) > 0 {
		candidates["retired-traffic-CA"] = retired[0]
	}
	var before myspec.GaleraStatusResponse
	node.action(ctx, myspec.ActionGaleraStatus, struct{}{}, &before)
	for name, candidate := range candidates {
		raw, err := (&net.Dialer{Timeout: 3 * time.Second}).DialContext(ctx, "tcp", address)
		if err != nil {
			t.Fatalf("%s peer TCP admission probe: %v", name, err)
		}
		conn := tls.Client(raw, nativeTrafficPeerPolicy(t, s, candidate))
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
		err = conn.HandshakeContext(ctx)
		if err == nil {
			// TLS1.3 can report the local handshake complete before receiving the
			// remote client-certificate alert. Read that alert before declaring success.
			_, err = conn.Read(make([]byte, 1))
		}
		conn.Close()
		if !nativeRemoteCertificateRejection(err) {
			t.Fatalf("%s unauthorized Galera peer lacked explicit TLS rejection: %v", name, err)
		}
	}
	var after myspec.GaleraStatusResponse
	node.action(ctx, myspec.ActionGaleraStatus, struct{}{}, &after)
	if !after.Eligible() || after.ClusterSize != before.ClusterSize || after.StateUUID != before.StateUUID {
		t.Fatalf("unauthorized peer changed component: before=%+v after=%+v", before, after)
	}
	t.Log("Galera TLS rejected self-signed, SQL-CA and agent-CA peer credentials; component membership unchanged")
}

func nativeGaleraVerifiedSQL(t *testing.T, ctx context.Context, s myspec.Spec) {
	t.Helper()
	name := "native-galera-first-recovered-SQL"
	if err := mysqldriver.RegisterTLSConfig(name, tlsConfig(x509Pool(s.CAPEM), nativeCertificateName(t, s.ServerCert.CertPEM))); err != nil {
		t.Fatal(err)
	}
	defer mysqldriver.DeregisterTLSConfig(name)
	cfg := mysqldriver.NewConfig()
	cfg.User, cfg.Passwd, cfg.Net, cfg.TLSConfig = s.Admin.Username, s.Admin.Password, "tcp", name
	for _, peer := range s.Peers {
		if peer.Name == s.Member {
			cfg.Addr = galeraAddress(peer.Host, peer.Port)
		}
	}
	cfg.Timeout, cfg.ReadTimeout, cfg.WriteTimeout = 3*time.Second, 3*time.Second, 3*time.Second
	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err = db.PingContext(ctx); err != nil {
		t.Fatalf("first post-rotation verified encrypted SQL: %v", err)
	}
}

// Authenticate the actual replication listener with the authorized dedicated
// traffic credential and verify the presented leaf, not just option-file paths.
func nativeGaleraTrafficTLSProof(t *testing.T, ctx context.Context, node *groupRunnerAgent) {
	t.Helper()
	s := node.spec
	expected, _ := pem.Decode([]byte(s.Galera.TrafficCert.CertPEM))
	root, _ := pem.Decode([]byte(s.Galera.TrafficCAPEM))
	if expected == nil || root == nil {
		t.Fatal("CP traffic certificate or trust anchor missing")
	}
	policy := nativeTrafficPeerPolicy(t, s, *s.Galera.TrafficCert)
	var presented []*x509.Certificate
	policy.VerifyConnection = func(state tls.ConnectionState) error {
		presented = state.PeerCertificates
		if len(presented) == 0 || !bytes.Equal(presented[0].Raw, expected.Bytes) {
			return errors.New("replication listener did not present the CP traffic leaf")
		}
		return nil
	}
	local := galeraLocal(s)
	probe, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	raw, err := (&net.Dialer{}).DialContext(probe, "tcp", galeraAddress(local.Host, s.Galera.GaleraPort))
	if err == nil {
		connection := tls.Client(raw, policy)
		_ = connection.SetDeadline(time.Now().Add(5 * time.Second))
		err = connection.HandshakeContext(probe)
		_ = connection.Close()
	}
	if err != nil {
		var verification *tls.CertificateVerificationError
		if errors.As(err, &verification) {
			presented = verification.UnverifiedCertificates
		}
		fingerprints := make([]string, 0, len(presented))
		for _, cert := range presented {
			fingerprints = append(fingerprints, fmt.Sprintf("%x", sha256.Sum256(cert.Raw)))
		}
		t.Logf("%s traffic TLS public fingerprints: expected-leaf=%x expected-root=%x presented=%v", s.Member, sha256.Sum256(expected.Bytes), sha256.Sum256(root.Bytes), fingerprints)
		observation, stopObservation := context.WithTimeout(ctx, 3*time.Second)
		defer stopObservation()
		if db, dbErr := node.runner.LocalAccessDB(observation); dbErr == nil {
			var auto bool
			var options string
			if dbErr = db.QueryRowContext(observation, "SELECT @@global.pxc_encrypt_cluster_traffic,@@global.wsrep_provider_options").Scan(&auto, &options); dbErr == nil {
				var effective []string
				for _, option := range strings.Split(options, ";") {
					if strings.HasPrefix(strings.TrimSpace(option), "socket.ssl") {
						effective = append(effective, strings.TrimSpace(option))
					}
				}
				t.Logf("%s effective traffic TLS: automatic=%v options=%v", s.Member, auto, effective)
			}
		}
		t.Fatalf("%s authorized replication TLS handshake: %v", s.Member, err)
	}
	t.Logf("%s authorized replication TLS verified exact CP-issued traffic leaf sha256=%x", s.Member, sha256.Sum256(expected.Bytes))
}
