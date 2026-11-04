// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestGaleraSupervisorLauncherRecoversWithoutNodeHook(t *testing.T) {
	withTransferHoldoff(t, 0) // repeated launches here model Synced restarts
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
	if err := durableText(filepath.Join(p.root, "galera-state-uuid"), uuid+"\n"); err != nil {
		t.Fatal(err)
	}
	position := filepath.Join(s.ServerDir, "position")
	fake := "#!/bin/sh\nrecover=no\nlog=''\nfor arg do\n case \"$arg\" in --wsrep-recover) recover=yes ;; --log-error=*) log=${arg#*=} ;; esac\ndone\nif [ \"$recover\" = yes ]; then\n read -r position < " + shellQuote(position) + "\n printf 'WSREP: Recovered position: %s\\n' \"$position\" > \"$log\"\n exit 0\nfi\nprintf '%s\\n' \"$@\"\n"
	if err := os.WriteFile(filepath.Join(s.ServerDir, "bin", "mysqld"), []byte(fake), 0700); err != nil {
		t.Fatal(err)
	}
	launcher := filepath.Join(p.root, "launch")
	if err := os.WriteFile(launcher, []byte(galeraLauncher(s, p)), 0700); err != nil {
		t.Fatal(err)
	}
	for _, seq := range []string{"18", "24"} {
		if err := durableText(position, uuid+":"+seq+"\n"); err != nil {
			t.Fatal(err)
		}
		raw, err := exec.Command(launcher).CombinedOutput()
		if err != nil || !strings.Contains(string(raw), "--wsrep-start-position="+uuid+":"+seq) || strings.Contains(string(raw), "--wsrep-new-cluster") {
			t.Fatalf("supervisor recovery output=%s err=%v", raw, err)
		}
	}
	s.Galera.Bootstrap = true
	s.Galera.FormationEpoch = 2
	s.Galera.StartPosition = uuid + ":24"
	if err := os.WriteFile(launcher, []byte(galeraLauncher(s, p)), 0700); err != nil {
		t.Fatal(err)
	}
	if raw, err := exec.Command(launcher).CombinedOutput(); err != nil || !strings.Contains(string(raw), "--wsrep-new-cluster") {
		t.Fatalf("recovered bootstrap output=%s err=%v", raw, err)
	}
	if raw, err := exec.Command(launcher).CombinedOutput(); err != nil || strings.Contains(string(raw), "--wsrep-new-cluster") {
		t.Fatalf("repeated bootstrap output=%s err=%v", raw, err)
	}
	if err := durableText(position, "11111111-1111-1111-1111-111111111111:25\n"); err != nil {
		t.Fatal(err)
	}
	if raw, err := exec.Command(launcher).CombinedOutput(); err == nil {
		t.Fatalf("foreign recovered state accepted: %s", raw)
	}
}

func TestGaleraLauncherCancellationDuringRecovery(t *testing.T) {
	for _, group := range []bool{false, true} {
		t.Run(map[bool]string{false: "launcher", true: "owned-group"}[group], func(t *testing.T) {
			s := testGaleraSpec()
			s.Galera.Bootstrap = false
			s.ServerDir = t.TempDir()
			p := dataPaths(t.TempDir())
			if err := os.MkdirAll(filepath.Join(s.ServerDir, "bin"), 0700); err != nil {
				t.Fatal(err)
			}
			if err := privateDir(p.root); err != nil {
				t.Fatal(err)
			}
			uuid := "826914cc-cd20-11f1-b4c4-6ac2787ad4af"
			if err := durableText(filepath.Join(p.root, "galera-state-uuid"), uuid+"\n"); err != nil {
				t.Fatal(err)
			}
			ready, launched := filepath.Join(p.root, "ready"), filepath.Join(p.root, "launched")
			// Even a recovery child exiting successfully after TERM must not
			// allow the canceled launcher to start a new live mysqld.
			fake := "#!/bin/sh\nfor arg do\n case \"$arg\" in --log-error=*) log=${arg#*=} ;; esac\ndone\ncase \" $* \" in\n *' --wsrep-recover '*)\n printf 'WSREP: Recovered position: " + uuid + ":24\\n' > \"$log\"\n trap 'exit 0' TERM\n : > " + shellQuote(ready) + "\n while :; do sleep 0.05; done ;;\nesac\n: > " + shellQuote(launched) + "\n"
			if err := os.WriteFile(filepath.Join(s.ServerDir, "bin", "mysqld"), []byte(fake), 0700); err != nil {
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
			t.Cleanup(func() {
				_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
				// The receive below is safe whether Wait has been observed
				// already: closing the channel makes cleanup idempotent.
				<-done
			})
			deadline := time.Now().Add(3 * time.Second)
			for {
				if _, err := os.Stat(ready); err == nil {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("recovery did not start")
				}
				time.Sleep(time.Millisecond)
			}
			pid := cmd.Process.Pid
			if group {
				pid = -pid
			}
			if err := syscall.Kill(pid, syscall.SIGTERM); err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-done:
				close(done)
				if err == nil {
					t.Fatal("canceled launcher succeeded")
				}
			case <-time.After(3 * time.Second):
				t.Fatal("canceled recovery did not exit")
			}
			if _, err := os.Stat(launched); !os.IsNotExist(err) {
				t.Fatal("canceled recovery launched a live mysqld", err)
			}
		})
	}
}
