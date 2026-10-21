// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRecoveredPosition(t *testing.T) {
	line := "2026-10-21 [Note] [WSREP] WSREP: Recovered position: 826914cc-cd20-11f1-b4c4-6ac2787ad4af:18\n"
	s, err := parseRecoveredPosition([]byte(line))
	if err != nil || s.Seqno != 18 || !s.Stopped {
		t.Fatalf("%+v %v", s, err)
	}
	modern := "2026-10-21T07:28:06.737632Z 0 [Note] [MY-000000] [WSREP] Recovered position: 826914cc-cd20-11f1-b4c4-6ac2787ad4af:18\n"
	if got, err := parseRecoveredPosition([]byte(modern)); err != nil || got != s {
		t.Fatalf("modern PXC log: %+v %v", got, err)
	}
	for _, raw := range []string{"", line + line, strings.Replace(line, ":18", ":-1", 1), strings.Replace(line, "826914cc-cd20-11f1-b4c4-6ac2787ad4af", "00000000-0000-0000-0000-000000000000", 1)} {
		if _, err := parseRecoveredPosition([]byte(raw)); err == nil {
			t.Fatalf("accepted %q", raw)
		}
	}
}
func TestSetSafeToBootstrapOnlyChangesFlag(t *testing.T) {
	p := dataPaths(t.TempDir())
	if err := privateDir(p.data); err != nil {
		t.Fatal(err)
	}
	name := filepath.Join(p.data, "grastate.dat")
	original := "# GALERA saved state\nversion: 2.1\nuuid: 826914cc-cd20-11f1-b4c4-6ac2787ad4af\nseqno: -1\nsafe_to_bootstrap: 0\n"
	if err := os.WriteFile(name, []byte(original), 0600); err != nil {
		t.Fatal(err)
	}
	if err := setSafeToBootstrap(p); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != strings.Replace(original, "safe_to_bootstrap: 0", "safe_to_bootstrap: 1", 1) {
		t.Fatal(string(got))
	}
	if err := os.WriteFile(name, []byte("safe_to_bootstrap: 0\nsafe_to_bootstrap: 1\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if setSafeToBootstrap(p) == nil {
		t.Fatal("ambiguous state accepted")
	}
}
