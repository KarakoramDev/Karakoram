// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package openbao

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agent"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine/openbao/baospec"
)

func testConfig() (baospec.MemberSpec, agentapi.MemberConfig) {
	s := baospec.MemberSpec{NodeID: "member", Version: "2.7.1", BinaryPath: "/opt/bao", ListenAddress: "127.0.0.1", APIAddr: "https://member:8200", ClusterAddr: "https://member:8201", Storage: baospec.Storage{Type: "raft", RetryJoin: []baospec.RetryJoin{{LeaderAPIAddr: "https://peer:8200", TLSServerName: "peer"}}}, Seal: baospec.Seal{Type: "shamir"}}
	raw, _ := json.Marshal(s)
	return s, agentapi.MemberConfig{Member: "member", Engine: "openbao", EngineSpec: raw, TLS: agentapi.TLSConfig{Mode: "requireTLS", CAPEM: "ca", CertPEM: "cert", KeyPEM: "key"}}
}
func TestRenderEscapesEveryString(t *testing.T) {
	s, c := testConfig()
	s.Storage.RetryJoin[0].TLSServerName = "quote\" newline\n"
	files, e := render(t.TempDir(), s, c)
	if e != nil {
		t.Fatal(e)
	}
	var cfg map[string]any
	if e = json.Unmarshal(files["openbao/server.json"], &cfg); e != nil {
		t.Fatal(e)
	}
	storage := cfg["storage"].(map[string]any)["raft"].(map[string]any)
	join := storage["retry_join"].([]any)[0].(map[string]any)
	if join["leader_tls_servername"] != s.Storage.RetryJoin[0].TLSServerName {
		t.Fatal("string changed by rendering")
	}
	if len(cfg["listener"].([]any)) != 2 {
		t.Fatal("missing dedicated metrics listener")
	}
}

func TestRenderSeparatesMutualTLSShareRecovery(t *testing.T) {
	s, c := testConfig()
	s.RecoveryCAPEM = "dedicated-recovery-ca"
	s.RecoveryPort = 18202
	files, err := render(t.TempDir(), s, c)
	if err != nil {
		t.Fatal(err)
	}
	var cfg map[string]any
	if err = json.Unmarshal(files["openbao/server.json"], &cfg); err != nil {
		t.Fatal(err)
	}
	listeners := cfg["listener"].([]any)
	if len(listeners) != 3 {
		t.Fatalf("expected API, metrics and recovery listeners: %d", len(listeners))
	}
	for i, entry := range listeners {
		listener := entry.(map[string]any)["tcp"].(map[string]any)
		disabled, ok := listener["disable_unauthed_generate_root_endpoints"].(bool)
		if !ok || disabled != (i != 2) {
			t.Fatalf("legacy root generation enabled on wrong listener %d", i)
		}
		if value, exists := listener["disable_unauthed_rekey_endpoints"]; exists && value != true {
			t.Fatal("legacy rekey was enabled")
		}
		if i == 2 {
			if listener["address"] != "127.0.0.1:18202" || listener["tls_require_and_verify_client_cert"] != true {
				t.Fatalf("recovery listener is not explicit mutual TLS: %v", listener)
			}
			caPath, ok := listener["tls_client_ca_file"].(string)
			if !ok || !strings.HasSuffix(caPath, "/openbao/recovery-ca.pem") || string(files["openbao/recovery-ca.pem"]) != s.RecoveryCAPEM {
				t.Fatal("recovery listener does not use independent client trust")
			}
			if listener["tls_cert_file"] == nil || listener["tls_key_file"] == nil || listener["tls_min_version"] != "tls12" {
				t.Fatal("recovery listener lost normal server TLS verification")
			}
		}
	}
}

func TestSpecRejectsUnsafeConfiguration(t *testing.T) {
	for _, change := range []func(*baospec.MemberSpec, *agentapi.MemberConfig){func(s *baospec.MemberSpec, c *agentapi.MemberConfig) { s.NodeID = "other" }, func(s *baospec.MemberSpec, c *agentapi.MemberConfig) { s.BinaryPath = "bao" }, func(s *baospec.MemberSpec, c *agentapi.MemberConfig) { c.TLS.Mode = "disabled" }, func(s *baospec.MemberSpec, c *agentapi.MemberConfig) { s.APIPort = 8210 }, func(s *baospec.MemberSpec, c *agentapi.MemberConfig) {
		s.Plugins = []baospec.Plugin{{Type: "database", Name: "x", Command: "../escape"}}
	}} {
		s, c := testConfig()
		change(&s, &c)
		c.EngineSpec, _ = json.Marshal(s)
		if _, e := spec(c); e == nil {
			t.Fatal("unsafe specification accepted")
		}
	}
}
func TestReadinessAcceptsSealedListeningServer(t *testing.T) {
	l, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	defer l.Close()
	s, c := testConfig()
	s.APIPort = l.Addr().(*net.TCPAddr).Port
	c.EngineSpec, _ = json.Marshal(s)
	dir := t.TempDir()
	if e = os.MkdirAll(filepath.Join(dir, "karakoram"), 0700); e != nil {
		t.Fatal(e)
	}
	b, _ := json.Marshal(c)
	if e = os.WriteFile(filepath.Join(dir, "karakoram/config.json"), b, 0600); e != nil {
		t.Fatal(e)
	}
	if e = New().Readiness(context.Background(), agent.Options{DataDir: dir}, nil); e != nil {
		t.Fatal(e)
	}
}
func TestInitValidation(t *testing.T) {
	for _, v := range []baospec.InitRequest{{}, {ReceiptID: "abcdefghijklmnop", RecipientPublicKey: make([]byte, 32), SecretShares: 3, SecretThreshold: 2}, {ReceiptID: "abcdefghijklmnop", RecipientPublicKey: []byte(strings.Repeat("a", 32)), SecretShares: 1, SecretThreshold: 2}} {
		if !invalidInit(v) {
			t.Fatal("invalid initialization request accepted")
		}
	}
}
func TestRenderStorageAndSeals(t *testing.T) {
	s, c := testConfig()
	s.Storage = baospec.Storage{Type: "postgresql", PostgreSQL: &baospec.PostgreSQLStorage{ConnectionURL: "postgresql://user:password@postgres/database?sslmode=verify-full", Table: "vault_kv_store", HATable: "vault_ha_locks", CAPEM: "postgres-ca"}}
	s.Seal = baospec.Seal{Type: "transit", Transit: &baospec.TransitSeal{Address: "https://transit", MountPath: "transit/", KeyName: "key", Token: "write-only", CAPEM: "transit-ca"}}
	files, e := render(t.TempDir(), s, c)
	if e != nil {
		t.Fatal(e)
	}
	if len(files["openbao/postgres-ca.pem"]) == 0 || len(files["openbao/transit-ca.pem"]) == 0 {
		t.Fatal("missing CA files")
	}
	var cfg map[string]any
	if e = json.Unmarshal(files["openbao/server.json"], &cfg); e != nil {
		t.Fatal(e)
	}
	st := cfg["storage"].(map[string]any)["postgresql"].(map[string]any)
	if st["ha_enabled"] != true || !strings.Contains(st["connection_url"].(string), "sslrootcert=") {
		t.Fatal("missing postgresql TLS/HA")
	}
}

func TestStartedGenerationSurvivesFailedStop(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "karakoram/openbao"), 0700); err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(started{Generation: 2, Binary: "/new/bao", PreviousPID: 1234, PreviousGeneration: 1, PreviousBinary: "/old/bao"})
	if err := os.WriteFile(filepath.Join(dir, "karakoram/openbao/started.json"), b, 0600); err != nil {
		t.Fatal(err)
	}
	old, err := readStarted(dir, 1234)
	if err != nil || old.Generation != 1 || old.Binary != "/old/bao" {
		t.Fatalf("previous child lost its generation: %+v %v", old, err)
	}
	next, err := readStarted(dir, 5678)
	if err != nil || next.Generation != 2 || next.Binary != "/new/bao" {
		t.Fatalf("new child lost its generation: %+v %v", next, err)
	}
}
