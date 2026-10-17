// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"gopkg.in/yaml.v3"
)

func TestPatroniLocalReplicationUsesPrivateSocket(t *testing.T) {
	for _, listener := range []string{"0.0.0.0", "::"} {
		t.Run(listener, func(t *testing.T) {
			s := testSpec()
			s.Topology = pgspec.TopologyHA
			s.ListenHost = listener
			s.Peers = []pgspec.Peer{{Name: s.Member, Host: "member.example.test"}}
			text, err := renderPatroni(s, dataPaths(t.TempDir()))
			if err != nil {
				t.Fatal(err)
			}
			var config struct {
				Postgresql struct {
					LocalSocket       bool                         `yaml:"use_unix_socket"`
					ReplicationSocket bool                         `yaml:"use_unix_socket_repl"`
					ConnectAddress    string                       `yaml:"connect_address"`
					Authentication    map[string]map[string]string `yaml:"authentication"`
				} `yaml:"postgresql"`
			}
			if err := yaml.Unmarshal([]byte(text), &config); err != nil {
				t.Fatal(err)
			}
			pg := config.Postgresql
			if !pg.LocalSocket || !pg.ReplicationSocket {
				t.Fatal("local probes must not connect to wildcard TCP certificate identities")
			}
			if pg.ConnectAddress != net.JoinHostPort("member.example.test", "5432") || pg.Authentication["replication"]["sslmode"] != "verify-full" {
				t.Fatal("remote replication must retain the member identity and verify-full TLS")
			}
		})
	}
}

func TestRestoreBootstrapGenerationUsesFileFsync(t *testing.T) {
	s := testSpec()
	s.Bootstrap = &pgspec.Bootstrap{Method: "restore", Generation: 7, Restore: &pgspec.Restore{Source: pgspec.Repository{Stanza: "kk", Path: "/source"}}}
	p := dataPaths(filepath.Join(t.TempDir(), "volume with ' quote"))
	if err := os.MkdirAll(p.root, 0700); err != nil {
		t.Fatal(err)
	}
	files, err := render(s, p, nil)
	if err != nil {
		t.Fatal(err)
	}
	var script string
	for _, file := range files {
		if file.name == "bootstrap-restore.sh" {
			script = file.content
		}
	}
	marker := filepath.Join(p.root, "bootstrap-generation.json")
	want := "\nsync " + shellQuote(marker+".tmp") + "\nmv " + shellQuote(marker+".tmp") + " " + shellQuote(marker) + "\nsync " + shellQuote(p.root) + "\n"
	if !strings.Contains(script, want) || strings.Contains(script, "sync -f") {
		t.Fatal("generation marker requires ordered file fsync, rename and directory fsync without filesystem-wide syncfs")
	}
	start := strings.Index(script, "\nprintf '%s\\n'")
	if start < 0 {
		t.Fatal("missing generation marker write")
	}
	if out, err := exec.Command("/bin/sh", "-ec", script[start:]).CombinedOutput(); err != nil {
		t.Fatalf("generation marker script: %v: %s", err, out)
	}
	data, err := os.ReadFile(marker)
	if err != nil || string(data) != "7\n" {
		t.Fatalf("generation marker = %q, err = %v", data, err)
	}
	if _, err := os.Stat(marker + ".tmp"); !os.IsNotExist(err) {
		t.Fatalf("temporary generation marker remains: %v", err)
	}
}

func TestRestoreRoleBootstrapOnlyRequiresLocalWAL(t *testing.T) {
	s := testSpec()
	statements, err := roleStatements(s)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(strings.Join(statements, ";\n"), "synchronous_commit") {
		t.Fatal("ordinary role bootstrap must retain the configured synchronous commit policy")
	}
	s.Bootstrap = &pgspec.Bootstrap{Method: "restore", Restore: &pgspec.Restore{Source: pgspec.Repository{Stanza: "kk", Path: "/source"}}}
	statements, err = roleStatements(s)
	if err != nil {
		t.Fatal(err)
	}
	sql := strings.Join(statements, ";\n")
	if strings.Contains(sql, "synchronous_commit") {
		t.Fatal("cached system identities must not retain restore-only commit settings")
	}
	files, err := render(s, dataPaths(t.TempDir()), statements)
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		if file.name == "roles.sql" {
			if !strings.HasPrefix(file.content, "BEGIN;\n") || !strings.HasSuffix(file.content, "COMMIT;\n") || !strings.Contains(file.content, "SET LOCAL synchronous_commit = local") {
				t.Fatal("Patroni recovery hook must run SET LOCAL and identity writes in one transaction")
			}
			return
		}
	}
	t.Fatal("missing recovery roles script")
}
