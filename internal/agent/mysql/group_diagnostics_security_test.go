// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	mysqldriver "github.com/go-sql-driver/mysql"
)

func TestGroupDiagnosticDropsHistoricalRecoveryCredentials(t *testing.T) {
	d := &rejectedGroupDeps{s: testGroupSpec(), dir: t.TempDir()}
	d.s.Replication.Password = "unrelated-current-credential"
	for _, statement := range []string{
		"CHANGE REPLICATION SOURCE TO SOURCE_USER = 'historical-user', SOURCE_PASSWORD = 'historical-value'",
		"CHANGE MASTER TO MASTER_USER = 'historical-user', MASTER_PASSWORD = 'historical-value'",
		"cHaNgE\n\tRePlIcAtIoN SOURCE TO `sOuRcE_pAsSwOrD` = \"historical-value\"",
		"'CHANGE'\t'MASTER' TO \"master_password\"   = 'historical-value'",
		"source_user = 'historical-value'",
		"MASTER_PASSWORD\t=\t'historical-value'",
		"PASSWORD 'historical-value'",
		"IDENTIFIED\nBY 'historical-value'",
		"certificate data:\nhistorical-value group_replication",
		"-----BEGIN PRIVATE KEY-----\nhistorical-value xcom\n-----END PRIVATE KEY-----",
	} {
		t.Run(statement[:min(len(statement), 24)], func(t *testing.T) {
			message := "group_replication rejected statement: " + statement
			got := groupActionDiagnostic(d, myspec.ActionBootstrap, &mysqldriver.MySQLError{Number: 3092, Message: message})
			if !strings.Contains(got, "MySQL 3092") || !strings.Contains(got, "sensitive diagnostic omitted") || strings.Contains(got, "historical-") || strings.Contains(got, "SOURCE_USER") {
				t.Fatal("historical credential message was not omitted atomically")
			}
			log := "2026-10-28T18:28:01Z [ERROR] [MY-011735] [Repl] " + message + "\n"
			if err := os.WriteFile(filepath.Join(d.dir, "mysql-error.log"), []byte(log), 0600); err != nil {
				t.Fatal(err)
			}
			if recentGroupErrors(d) != "" {
				t.Fatal("credential-bearing native log record was retained")
			}
			if err := os.Remove(filepath.Join(d.dir, "mysql-error.log")); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestGroupDiagnosticLogRecordsAreAtomic(t *testing.T) {
	d := &rejectedGroupDeps{s: testGroupSpec(), dir: t.TempDir()}
	d.s.Replication.Password = "unrelated-current-credential"
	const safe = "2026-10-28T18:28:05Z [ERROR] [MY-011735] [Repl] Plugin group_replication reported: '[GCS] safe endpoint failure'\n"
	for _, test := range []struct {
		name string
		log  string
	}{
		{
			name: "multiline historical credential continuations",
			log: "2026-10-28T18:28:01Z [ERROR] [MY-011735] [Repl] group_replication CHANGE REPLICATION SOURCE TO SOURCE_PASSWORD = '\n" +
				"historical-value group_replication\nmore-historical-value [GCS] xcom recovery\n'\n" + safe,
		},
		{
			name: "multiline certificate continuations",
			log: "2026-10-28T18:28:01Z [ERROR] [MY-011735] [Repl] group_replication certificate material\n" +
				"historical-value group_replication\nmore-historical-value xcom\n" + safe,
		},
		{
			name: "bounded window begins in sensitive record",
			log: "2026-10-28T18:28:01Z [ERROR] [MY-011735] [Repl] group_replication MASTER_PASSWORD = '\n" +
				strings.Repeat("historical-continuation\n", 2000) +
				"historical-value [MY-011735] [Repl] group_replication recovery\n'\n" + safe,
		},
		{
			name: "leading continuation even at file start",
			log:  "historical-value [MY-011735] [Repl] group_replication recovery\n" + safe,
		},
		{
			name: "uncoded or unrelated records",
			log: "2026-10-28T18:28:01Z [ERROR] [Repl] group_replication historical-value without code\n" +
				"2026-10-28T18:28:02Z [ERROR] [MY-011735] [Server] group_replication historical-value unrelated subsystem\n" + safe,
		},
		{
			name: "unterminated final record",
			log:  safe + "2026-10-28T18:28:06Z [ERROR] [MY-011735] [Repl] group_replication historical-value incomplete",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := os.WriteFile(filepath.Join(d.dir, "mysql-error.log"), []byte(test.log), 0600); err != nil {
				t.Fatal(err)
			}
			got := recentGroupErrors(d)
			if !strings.Contains(got, "MY-011735") || !strings.Contains(got, "safe endpoint failure") || strings.Contains(got, "historical-") {
				t.Fatal("partial or sensitive native record escaped selection")
			}
		})
	}
}

func TestGroupDiagnosticRecoveryPasswordLength(t *testing.T) {
	d := &rejectedGroupDeps{s: testGroupSpec(), dir: t.TempDir()}
	for _, message := range []string{
		"The START GROUP_REPLICATION command failed since the password provided for the recovery channel exceeds the maximum length of 32 characters.",
		"PASSWORD='historical-value'\n-----BEGIN PRIVATE KEY-----\nhistorical-private-material",
	} {
		got := groupActionDiagnostic(d, myspec.ActionBootstrap, &mysqldriver.MySQLError{Number: 3972, Message: message})
		if !strings.Contains(got, "MySQL 3972: Group Replication recovery credential exceeds the maximum length of 32 characters") || strings.Contains(got, "omitted") || strings.Contains(got, "historical-") {
			t.Fatal("native error 3972 did not retain only trusted credential-length semantics")
		}
	}
}
