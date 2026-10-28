// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"database/sql"
	"encoding/pem"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	mysqldriver "github.com/go-sql-driver/mysql"
)

func TestGroupNativeDiagnosticReachesClient(t *testing.T) {
	for _, action := range []string{myspec.ActionBootstrap, myspec.ActionStartGroup} {
		t.Run(action, func(t *testing.T) {
			s := testGroupSpec()
			s.GroupReplication.Bootstrap = true
			s.Replication.Password = "recovery-credential-do-not-publish"
			connector := &bootstrapFailureConnector{groupTestConnector: groupTestConnector{t: t}, exec: func(query string) error {
				if strings.HasPrefix(query, "START GROUP_REPLICATION") {
					return &mysqldriver.MySQLError{Number: 3092, Message: "server not configured; " + s.Replication.Password}
				}
				return nil
			}}
			if action == myspec.ActionStartGroup {
				connector.queries = []groupQuery{{contains: "FROM performance_schema.replication_group_members"}}
			}
			d := &cloneTestDeps{rejectedGroupDeps: rejectedGroupDeps{s: s, dir: t.TempDir(), identity: myspec.DataIdentity{Initialized: true}}, db: sql.OpenDB(connector)}
			t.Cleanup(func() { _ = d.db.Close() })
			log := "2026-10-28T18:28:01Z [ERROR] [MY-011735] [Repl] Plugin group_replication reported: '[GCS] local endpoint unavailable'\n"
			if err := os.WriteFile(filepath.Join(d.dir, "mysql-error.log"), []byte(log), 0600); err != nil {
				t.Fatal(err)
			}
			mux := http.NewServeMux()
			RegisterGroupRoutes(mux, d)
			server := httptest.NewTLSServer(mux)
			defer server.Close()
			ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
			client, err := agentapi.NewClient(agentapi.ClientOptions{BaseURL: server.URL, Token: "diagnostic-agent-token", CAPEM: ca})
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			err = client.Engine(context.Background(), "mysql", action, myspec.GroupRequest{GroupName: s.GroupReplication.GroupName, FormationEpoch: 1, Bootstrap: true}, nil)
			var remote *agentapi.HTTPError
			if !errors.As(err, &remote) || remote.StatusCode != http.StatusServiceUnavailable {
				t.Fatal("missing structured action error")
			}
			for _, want := range []string{"MySQL 3092", "server not configured", "MY-011735", "local endpoint unavailable", "may predate request"} {
				if !strings.Contains(remote.Message, want) {
					t.Errorf("native diagnosis lost: %s", want)
				}
			}
			if strings.Contains(remote.Message, s.Replication.Password) || strings.ContainsAny(remote.Message, "\n\r\x00") || len(remote.Message) > groupDiagnosticBytes {
				t.Fatal("unsafe native diagnosis")
			}
		})
	}
}

func TestGroupDiagnosticRedactsBeforeBounding(t *testing.T) {
	s := testGroupSpec()
	s.Replication.Password = "quote'and\\escape-recovery-credential"
	s.Agent.Password = "agent-administration-credential"
	s.ServerCert = &myspec.Certificate{KeyPEM: "-----BEGIN PRIVATE KEY-----\nprivate-key-body-do-not-publish\n-----END PRIVATE KEY-----"}
	s.Backup = &myspec.Backup{Repository: myspec.Repository{S3: myspec.S3{SecretKey: "backup-credential-do-not-publish"}}}
	s.Restore = &myspec.Restore{RecoveryUsers: []myspec.RecoveryUser{{Current: "current-recovery-user-credential", Secondary: "secondary-recovery-user-credential"}}}
	d := &rejectedGroupDeps{s: s, dir: t.TempDir()}
	for _, message := range []string{
		"denied " + groupQuote(s.Replication.Password),
		"denied " + s.Agent.Password,
		"password='unknown-credential' endpoint refused",
		"START GROUP_REPLICATION USER='kk_repl', PASSWORD='unknown-credential'",
		"private key: " + s.ServerCert.KeyPEM,
		"backup " + s.Backup.Repository.S3.SecretKey,
		"denied " + s.Restore.RecoveryUsers[0].Current + " " + s.Restore.RecoveryUsers[0].Secondary,
		strings.Repeat("界", 600),
	} {
		got := groupActionDiagnostic(d, myspec.ActionBootstrap, &mysqldriver.MySQLError{Number: 3092, Message: message})
		if !strings.Contains(got, "MySQL 3092") || len(got) > groupDiagnosticBytes || !utf8.ValidString(got) || strings.ContainsAny(got, "\n\r\x00") {
			t.Fatal("invalid bounded diagnostic")
		}
		for _, secret := range []string{s.Replication.Password, s.Agent.Password, "unknown-credential", "private-key-body-do-not-publish", s.Backup.Repository.S3.SecretKey, s.Restore.RecoveryUsers[0].Current, s.Restore.RecoveryUsers[0].Secondary} {
			if strings.Contains(got, secret) {
				t.Fatal("credential escaped diagnostic boundary")
			}
		}
	}
}

func TestGroupDiagnosticLogTailIsBoundedAndOptional(t *testing.T) {
	d := &rejectedGroupDeps{s: testGroupSpec(), dir: t.TempDir()}
	logPath := filepath.Join(d.dir, "mysql-error.log")
	for _, mode := range []string{"missing", "directory", "symlink", "large", "unrelated"} {
		t.Run(mode, func(t *testing.T) {
			if err := os.Remove(logPath); err != nil && !errors.Is(err, os.ErrNotExist) {
				t.Fatal(err)
			}
			switch mode {
			case "directory":
				if err := os.Mkdir(logPath, 0700); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				if err := os.Symlink(filepath.Join(d.dir, "absent"), logPath); err != nil {
					t.Fatal(err)
				}
			case "large", "unrelated":
				text := strings.Repeat("unrelated native startup line\n", 2000)
				if mode == "large" {
					text += "[Repl] group_replication [MY-011735] old failure\n[Repl] group_replication [MY-011735] final failure\n"
				}
				if err := os.WriteFile(logPath, []byte(text), 0600); err != nil {
					t.Fatal(err)
				}
			}
			got := groupActionDiagnostic(d, myspec.ActionStartGroup, context.DeadlineExceeded)
			if !strings.Contains(got, "context deadline exceeded") || len(got) > groupDiagnosticBytes {
				t.Fatal("optional log altered primary error")
			}
			if strings.Contains(got, "final failure") != (mode == "large") || strings.Contains(got, "unrelated native startup") {
				t.Fatal("incorrect recent GR log selection")
			}
		})
	}
}
