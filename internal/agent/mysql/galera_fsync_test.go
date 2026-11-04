// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestGaleraLauncherUsesOrderedFsyncWithoutSyncfs(t *testing.T) {
	realSync, err := exec.LookPath("sync")
	if err != nil {
		t.Fatal(err)
	}
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "durable", true: "fsync-failure"}[fail], func(t *testing.T) {
			withTransferHoldoff(t, 0)
			s := testGaleraSpec()
			s.Galera.Bootstrap = true
			s.Galera.FormationEpoch = 7
			s.ServerDir = t.TempDir()
			p := dataPaths(filepath.Join(t.TempDir(), "volume with ' quote"))
			bin := filepath.Join(s.ServerDir, "bin")
			for _, dir := range []string{bin, p.root} {
				if err := os.MkdirAll(dir, 0700); err != nil {
					t.Fatal(err)
				}
			}
			if err := os.WriteFile(filepath.Join(bin, "mysqld"), []byte("#!/bin/sh\nprintf 'mysqld %s\\n' \"$*\"\n"), 0700); err != nil {
				t.Fatal(err)
			}
			syncScript := "#!/bin/sh\nfor arg do case \"$arg\" in -f|--file-system) echo 'syncfs forbidden' >&2; exit 1;; esac; done\nprintf 'fsync %s\\n' \"$*\"\n"
			if fail {
				syncScript += "exit 1\n"
			} else {
				syncScript += "exec " + shellQuote(realSync) + " \"$@\"\n"
			}
			if err := os.WriteFile(filepath.Join(bin, "sync"), []byte(syncScript), 0700); err != nil {
				t.Fatal(err)
			}
			script := galeraLauncher(s, p)
			if strings.Contains(script, "sync -f") {
				t.Fatal("filesystem-wide syncfs remains")
			}
			command := exec.Command("/bin/sh", "-ec", script)
			command.Env = append(os.Environ(), "PATH="+bin+":/usr/bin:/bin")
			raw, err := command.CombinedOutput()
			if fail {
				if err == nil || strings.Contains(string(raw), "mysqld ") {
					t.Fatalf("fsync failure did not fence startup: %s %v", raw, err)
				}
				return
			}
			receipt := filepath.Join(p.root, "galera-bootstrap-7")
			want := "fsync -- " + receipt + "\nfsync -- " + p.root + "\nmysqld "
			if err != nil || !strings.Contains(string(raw), want) || !strings.Contains(string(raw), "--wsrep-new-cluster") {
				t.Fatalf("unordered bootstrap persistence: %s %v", raw, err)
			}
			// Replay is fenced to normal joining, with marker inode and parent persisted.
			command = exec.Command("/bin/sh", "-ec", script)
			command.Env = append(os.Environ(), "PATH="+bin+":/usr/bin:/bin")
			raw, err = command.CombinedOutput()
			want = "fsync -- " + filepath.Join(p.root, galeraUnsyncedMarker) + "\nfsync -- " + p.root + "\nmysqld "
			if err != nil || !strings.Contains(string(raw), want) || strings.Contains(string(raw), "--wsrep-new-cluster") {
				t.Fatalf("unsafe replay persistence: %s %v", raw, err)
			}
		})
	}
}
