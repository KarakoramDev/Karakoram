// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
)

func TestRecoveryCredentialsRemovedBeforeClientRulesReturn(t *testing.T) {
	s := testSpec()
	s.CAPEM, s.ServerCert = testTLS(t)
	s.Backup = &pgspec.Backup{Repository: pgspec.Repository{Stanza: "kk", Path: "/target", S3: pgspec.S3{Endpoint: "https://objects.example", Bucket: "backup", AccessKey: "target-key", SecretKey: "target-secret"}}}
	source := s.Backup.Repository
	source.Path = "/source"
	source.S3.AccessKey = "source-key"
	source.S3.SecretKey = "source-secret"
	s.Bootstrap = &pgspec.Bootstrap{Method: "restore", Restore: &pgspec.Restore{Source: source}}
	rules := strings.Join(hba(s), "\n")
	if strings.Contains(rules, " all all 127.0.0.0/8 ") {
		t.Fatal("remote clients were allowed while source credentials were present")
	}
	if !strings.Contains(rules, "local all all scram-sha-256") || !strings.Contains(rules, "hostssl replication kk_replicator 127.0.0.0/8 scram-sha-256") {
		t.Fatal("recovery fence blocked local management or replica rebuild")
	}
	p := dataPaths(t.TempDir())
	if err := os.MkdirAll(p.root, 0700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"pgbackrest-restore.conf", "restore-s3-ca.pem", "bootstrap-restore.sh"} {
		if err := os.WriteFile(filepath.Join(p.root, name), []byte(source.S3.SecretKey), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(p.root, "pgbackrest-action.conf"), []byte(s.Backup.Repository.S3.SecretKey), 0600); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := cleanupRestoreFiles(p); err != nil {
			t.Fatal(err)
		}
	}
	entries, err := os.ReadDir(p.root)
	if err != nil || len(entries) != 1 || entries[0].Name() != "pgbackrest-action.conf" {
		t.Fatal("source cleanup removed active target backup configuration or retained source files", err)
	}
	s.Bootstrap = nil
	s.Postgres.RecoveryClientsHeld = true
	if strings.Contains(strings.Join(hba(s), "\n"), " all all 127.0.0.0/8 ") {
		t.Fatal("clearing bootstrap reopened clients before durable source cleanup")
	}
	s.Postgres.RecoveryClientsHeld = false
	rules = strings.Join(hba(s), "\n")
	if !strings.Contains(rules, "host all all 127.0.0.0/8 scram-sha-256") {
		t.Fatal("completed recovery did not release client access")
	}
	files, err := render(s, p, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		if file.name == "pgbackrest-restore.conf" || file.name == "restore-s3-ca.pem" || file.name == "bootstrap-restore.sh" || strings.Contains(file.content, source.S3.SecretKey) || strings.Contains(file.content, source.S3.AccessKey) {
			t.Fatal("completed configuration re-rendered source credentials")
		}
	}
	s.Topology = pgspec.TopologyHA
	s.Postgres.RecoveryClientsHeld = true
	s.Peers = []pgspec.Peer{{Name: s.Member, Host: "localhost", Etcd: "voter"}}
	patroni, err := renderPatroni(s, p)
	if err != nil || !strings.Contains(patroni, "use_unix_socket: true") {
		t.Fatal("Patroni management must remain on its local socket while clients are fenced", err)
	}
}
