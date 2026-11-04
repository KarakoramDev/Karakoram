// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestStartupDiagnosticIncludesNativeErrorsAndOmitsSensitiveRecords(t *testing.T) {
	d := &rejectedGroupDeps{s: testGroupSpec(), dir: t.TempDir()}
	log := "2026-10-28T18:28:01Z [ERROR] [MY-000000] [WSREP] failed to open gcomm backend: Invalid argument\n" +
		"2026-10-28T18:28:02Z [ERROR] [MY-000001] [Server] unknown variable\n" +
		"2026-10-28T18:28:03Z [ERROR] [MY-000002] [WSREP] PASSWORD historical\nsecret-continuation\n" +
		"2026-10-28T18:28:04Z [ERROR] [MY-000003] [WSREP] incomplete"
	if err := os.WriteFile(filepath.Join(d.dir, "mysql-error.log"), []byte(log), 0600); err != nil {
		t.Fatal(err)
	}
	got := startupDiagnostic(d, errors.New("local socket unavailable"))
	for _, want := range []string{"local socket unavailable", "Invalid argument", "unknown variable", "may predate request"} {
		if !strings.Contains(got, want) {
			t.Fatalf("missing %q: %s", want, got)
		}
	}
	for _, secret := range []string{"historical", "secret-continuation", "incomplete"} {
		if strings.Contains(got, secret) {
			t.Fatalf("unsafe diagnostic: %s", got)
		}
	}
	if len(got) > groupDiagnosticBytes || strings.ContainsAny(got, "\n\r") {
		t.Fatal("unbounded diagnostic")
	}
	// Existing GR diagnostics must not start selecting generic server/Galera errors.
	if got := recentGroupErrors(d); got != "" {
		t.Fatalf("GR selection changed: %s", got)
	}
}
