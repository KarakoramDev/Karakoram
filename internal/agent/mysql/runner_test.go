// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

func coreTestSpec() myspec.Spec {
	c := func(n string) myspec.Credentials {
		return myspec.Credentials{Username: n, Password: "test-password-123"}
	}
	return myspec.Spec{Member: "mysql1", Role: myspec.RoleData, Topology: myspec.TopologySingle, Version: "8.4.11-11", ServerDir: "/opt/karakoram/percona-server-8.4.11-11/server", XtraBackupDir: "/opt/karakoram/percona-xtrabackup-8.4.0-7/backup", BufferPoolBytes: 128 << 20, MaxConnections: 30, AllowInitialize: true, CAPEM: "-----BEGIN CERTIFICATE-----\nYQ==\n-----END CERTIFICATE-----", ServerCert: &myspec.Certificate{CertPEM: "-----BEGIN CERTIFICATE-----\nYQ==\n-----END CERTIFICATE-----", KeyPEM: "-----BEGIN PRIVATE KEY-----\nYQ==\n-----END PRIVATE KEY-----"}, Superuser: c("root"), Agent: c("kk_agent"), Replication: c("kk_repl"), Monitor: c("kk_monitor"), BackupUser: c("kk_backup"), Admin: c("admin")}
}
func TestCoreStrictDecode(t *testing.T) {
	s := coreTestSpec()
	raw, _ := json.Marshal(s)
	c := agentapi.MemberConfig{Engine: "mysql", Member: s.Member, Role: s.Role, EngineSpec: raw}
	if _, err := decodeSpec(c); err != nil {
		t.Fatal(err)
	}
	for _, raw := range []string{strings.TrimSuffix(string(raw), "}") + `,"unexpected":true}`, string(raw) + ` {}`} {
		c.EngineSpec = []byte(raw)
		if _, err := decodeSpec(c); err == nil {
			t.Fatal("accepted non-strict payload")
		}
	}
}
func TestCoreInitializationFailClosed(t *testing.T) {
	s := coreTestSpec()
	p := dataPaths(t.TempDir())
	for _, d := range []string{p.root, p.data} {
		if err := privateDir(d); err != nil {
			t.Fatal(err)
		}
	}
	fresh, err := guardInitialization(s, p)
	if err != nil || !fresh {
		t.Fatalf("fresh=%v err=%v", fresh, err)
	}
	if _, err = guardInitialization(s, p); err == nil {
		t.Fatal("reused initialization authorization")
	}
	if err = durableJSON(filepath.Join(p.root, "initialized.json"), myspec.DataIdentity{Initialized: true}); err != nil {
		t.Fatal(err)
	}
	if _, err = guardInitialization(s, p); err == nil {
		t.Fatal("reinitialized missing known data")
	}
}
func TestCoreForeignDataRefused(t *testing.T) {
	s := coreTestSpec()
	p := dataPaths(t.TempDir())
	for _, d := range []string{p.root, p.data} {
		if err := privateDir(d); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(p.data, "foreign"), []byte("keep"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := guardInitialization(s, p); err == nil {
		t.Fatal("accepted foreign data")
	}
	if err := os.Mkdir(filepath.Join(p.data, "mysql"), 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := guardInitialization(s, p); err == nil {
		t.Fatal("adopted foreign initialized data")
	}
}
func TestCoreRendering(t *testing.T) {
	s := coreTestSpec()
	p := dataPaths("/data")
	conf := renderConfig(s, p)
	for _, want := range []string{"require_secure_transport=\"ON\"", "gtid_mode=\"ON\"", "mysqlx=\"OFF\"", "persisted_globals_load=\"OFF\""} {
		if !strings.Contains(conf, want) {
			t.Errorf("missing %s", want)
		}
	}
	if strings.Contains(conf, s.Agent.Password) {
		t.Fatal("password in option file")
	}
	sql := renderAccounts(s)
	for _, want := range []string{"SET SESSION sql_log_bin=0", "'admin'@'%'", "REVOKE SYSTEM_USER", "GROUP_REPLICATION_STREAM", "SYSTEM_USER ON *.* TO 'kk_agent'"} {
		if !strings.Contains(sql, want) {
			t.Errorf("missing %s", want)
		}
	}
	s.Admin.Password = "changed-password-123"
	if credentialHash(s) == credentialHash(coreTestSpec()) {
		t.Fatal("admin password omitted from rekey digest")
	}
}

func TestCoreSlowShutdownRendering(t *testing.T) {
	s := coreTestSpec()
	for _, slow := range []bool{false, true, false} {
		s.SlowShutdown = slow
		want := `innodb_fast_shutdown="1"`
		if slow {
			want = `innodb_fast_shutdown="0"`
		}
		if conf := renderConfig(s, dataPaths("/data")); !strings.Contains(conf, want) {
			t.Fatalf("SlowShutdown=%t: missing %s", slow, want)
		}
	}
}

func TestCoreInterruptedCredentialRekey(t *testing.T) {
	s := coreTestSpec()
	p := dataPaths(t.TempDir())
	if err := privateDir(p.root); err != nil {
		t.Fatal(err)
	}
	if err := recordCredentials(s, p); err != nil {
		t.Fatal(err)
	}
	if !credentialsRecorded(s, p) {
		t.Fatal("completed credential receipt ignored")
	}
	if err := durableJSON(filepath.Join(p.root, "credentials-pending.json"), true); err != nil {
		t.Fatal(err)
	}
	if credentialsRecorded(s, p) {
		t.Fatal("interrupted re-key trusted stale credential receipt")
	}
	if err := recordCredentials(s, p); err != nil {
		t.Fatal(err)
	}
	if !credentialsRecorded(s, p) {
		t.Fatal("completed re-key did not consume pending receipt")
	}
}
