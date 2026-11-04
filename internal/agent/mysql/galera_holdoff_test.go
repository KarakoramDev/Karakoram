// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func withTransferHoldoff(t *testing.T, d time.Duration) {
	t.Helper()
	previous := galeraTransferHoldoff
	galeraTransferHoldoff = d
	t.Cleanup(func() { galeraTransferHoldoff = previous })
}

func TestGaleraLauncherHoldsOffAfterUnsyncedLaunch(t *testing.T) {
	withTransferHoldoff(t, 2*time.Second)
	for _, recovered := range []bool{false, true} {
		t.Run(map[bool]string{false: "fresh-joiner", true: "recovered-member"}[recovered], func(t *testing.T) {
			s := testGaleraSpec()
			s.Galera.Bootstrap = false
			s.ServerDir = t.TempDir()
			p := dataPaths(t.TempDir())
			if err := privateDir(p.root); err != nil {
				t.Fatal(err)
			}
			if err := privateDir(filepath.Join(s.ServerDir, "bin")); err != nil {
				t.Fatal(err)
			}
			uuid := "826914cc-cd20-11f1-b4c4-6ac2787ad4af"
			if recovered {
				if err := durableText(filepath.Join(p.root, "galera-state-uuid"), uuid+"\n"); err != nil {
					t.Fatal(err)
				}
			}
			fake := "#!/bin/sh\nfor arg do\n case \"$arg\" in --log-error=*) log=${arg#*=} ;; esac\ndone\ncase \" $* \" in\n *' --wsrep-recover '*) printf 'WSREP: Recovered position: " + uuid + ":7\\n' > \"$log\"; exit 0 ;;\nesac\nprintf 'launched %s\\n' \"$*\"\n"
			if err := os.WriteFile(filepath.Join(s.ServerDir, "bin", "mysqld"), []byte(fake), 0700); err != nil {
				t.Fatal(err)
			}
			launcher := filepath.Join(p.root, "launch")
			if err := os.WriteFile(launcher, []byte(galeraLauncher(s, p)), 0700); err != nil {
				t.Fatal(err)
			}
			marker := filepath.Join(p.root, galeraUnsyncedMarker)
			began := time.Now()
			raw, err := exec.Command(launcher).CombinedOutput()
			if err != nil || !strings.Contains(string(raw), "launched") || time.Since(began) >= galeraTransferHoldoff {
				t.Fatalf("first launch held off or failed after %s: %s %v", time.Since(began), raw, err)
			}
			if info, err := os.Lstat(marker); err != nil || !info.Mode().IsRegular() {
				t.Fatal("launch did not record an unfinished-join marker", err)
			}
			// The marker survived: that launch never reached Synced, so the
			// relaunch must outlast upstream's stale donor reconnect window.
			began = time.Now()
			raw, err = exec.Command(launcher).CombinedOutput()
			if err != nil || !strings.Contains(string(raw), "launched") || time.Since(began) < galeraTransferHoldoff {
				t.Fatalf("relaunch after unsynced launch did not hold off (%s): %s %v", time.Since(began), raw, err)
			}
			// A Synced refresh removes the marker; ordinary crash recovery is immediate.
			if err := os.Remove(marker); err != nil {
				t.Fatal(err)
			}
			began = time.Now()
			if raw, err = exec.Command(launcher).CombinedOutput(); err != nil || time.Since(began) >= galeraTransferHoldoff {
				t.Fatalf("synced member relaunch held off (%s): %s %v", time.Since(began), raw, err)
			}
		})
	}
}

func TestGaleraLauncherRefusesResetOfFinishedMember(t *testing.T) {
	withTransferHoldoff(t, 0)
	s := testGaleraSpec()
	s.Galera.Bootstrap = false
	s.ServerDir = t.TempDir()
	p := dataPaths(t.TempDir())
	for _, dir := range []string{p.root, p.data, filepath.Join(s.ServerDir, "bin")} {
		if err := privateDir(dir); err != nil {
			t.Fatal(err)
		}
	}
	uuid := "826914cc-cd20-11f1-b4c4-6ac2787ad4af"
	if err := durableText(filepath.Join(p.root, "galera-state-uuid"), uuid+"\n"); err != nil {
		t.Fatal(err)
	}
	launched := filepath.Join(p.root, "launched")
	fake := "#!/bin/sh\nfor arg do\n case \"$arg\" in --log-error=*) log=${arg#*=} ;; esac\ndone\ncase \" $* \" in\n *' --wsrep-recover '*) printf 'WSREP: Recovered position: " + uuid + ":7\\n' > \"$log\"; exit 0 ;;\nesac\n: > " + shellQuote(launched) + "\n"
	if err := os.WriteFile(filepath.Join(s.ServerDir, "bin", "mysqld"), []byte(fake), 0700); err != nil {
		t.Fatal(err)
	}
	// Galera's inconsistency eviction writes upstream's zero/-1 tuple.
	if err := os.WriteFile(filepath.Join(p.data, "grastate.dat"), []byte("# GALERA saved state\nversion: 2.1\nuuid:    "+galeraTransferRequiredUUID+"\nseqno:   -1\nsafe_to_bootstrap: 0\n"), 0600); err != nil {
		t.Fatal(err)
	}
	launcher := filepath.Join(p.root, "launch")
	if err := os.WriteFile(launcher, []byte(galeraLauncher(s, p)), 0700); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(p.root, galeraUnsyncedMarker)
	if raw, err := exec.Command(launcher).CombinedOutput(); err == nil || !strings.Contains(string(raw), "explicit recovery required") {
		t.Fatalf("finished member's reset relaunched: %s %v", raw, err)
	}
	if _, err := os.Lstat(launched); !os.IsNotExist(err) {
		t.Fatal("evicted member exec'd mysqld", err)
	}
	if _, err := os.Lstat(marker); !os.IsNotExist(err) {
		t.Fatal("refused launch manufactured an unfinished-join marker", err)
	}
	// The same tuple after an unfinished join is upstream's transfer request.
	if err := os.WriteFile(marker, nil, 0600); err != nil {
		t.Fatal(err)
	}
	if raw, err := exec.Command(launcher).CombinedOutput(); err != nil {
		t.Fatalf("interrupted join reset refused: %s %v", raw, err)
	}
	if _, err := os.Lstat(launched); err != nil {
		t.Fatal("interrupted join did not relaunch", err)
	}
}

func TestGaleraLauncherHoldoffCancellation(t *testing.T) {
	withTransferHoldoff(t, 30*time.Second)
	s := testGaleraSpec()
	s.Galera.Bootstrap = false
	s.ServerDir = t.TempDir()
	p := dataPaths(t.TempDir())
	if err := privateDir(p.root); err != nil {
		t.Fatal(err)
	}
	if err := privateDir(filepath.Join(s.ServerDir, "bin")); err != nil {
		t.Fatal(err)
	}
	launched := filepath.Join(p.root, "launched")
	if err := os.WriteFile(filepath.Join(s.ServerDir, "bin", "mysqld"), []byte("#!/bin/sh\n: > "+shellQuote(launched)+"\n"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(p.root, galeraUnsyncedMarker), nil, 0600); err != nil {
		t.Fatal(err)
	}
	launcher := filepath.Join(p.root, "launch")
	if err := os.WriteFile(launcher, []byte(galeraLauncher(s, p)), 0700); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(launcher)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	t.Cleanup(func() { _ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) })
	time.Sleep(300 * time.Millisecond)
	if err := syscall.Kill(cmd.Process.Pid, syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		var exit *exec.ExitError
		if err == nil || !errors.As(err, &exit) || exit.ExitCode() != 143 {
			t.Fatalf("held-off launcher did not exit on TERM: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("TERM did not interrupt the transfer hold-off")
	}
	if _, err := os.Stat(launched); !os.IsNotExist(err) {
		t.Fatal("canceled hold-off launched mysqld", err)
	}
}
