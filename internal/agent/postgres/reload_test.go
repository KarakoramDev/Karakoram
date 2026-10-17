// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

func TestSingleReloadClassification(t *testing.T) {
	for _, name := range []string{"postgresql.conf", "pg_hba.conf", "server.pem", "server.key", "ca.pem"} {
		if !singleReloadChanged([]string{name}) {
			t.Errorf("%s change was not reloaded", name)
		}
	}
	for _, names := range [][]string{nil, {"roles.sql"}, {"pgbackrest.conf", "s3-ca.pem"}} {
		if singleReloadChanged(names) {
			t.Fatalf("unrelated files caused PostgreSQL reload: %v", names)
		}
	}
}
func TestPostgresFastShutdownConfiguration(t *testing.T) {
	s := testSpec()
	p := dataPaths("/volume")
	process := postgresProcess(s, p)
	if process.StopSignal != syscall.SIGINT {
		t.Fatal("PostgreSQL stop must not wait for idle SQL pool connections")
	}
	if process.Binary != filepath.Join(s.BinDir, "postgres") || process.Dir != p.pgdata || len(process.Args) != 4 {
		t.Fatal("foreground PostgreSQL process specification changed")
	}
}
func TestPostmasterDetectionUsesCurrentFiles(t *testing.T) {
	for _, part := range []string{"pg_catalog.pg_file_settings", "JOIN pg_catalog.pg_settings", "s.context='postmaster'", "NOT f.applied", "f.error IS NOT NULL", "s.pending_restart", "s.boot_val"} {
		if !strings.Contains(singleRestartRequiredSQL, part) {
			t.Errorf("missing restart detection guard %s", part)
		}
	}
}
