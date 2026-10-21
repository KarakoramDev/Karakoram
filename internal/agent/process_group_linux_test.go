//go:build linux

// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package agent

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestSupervisorShutdownWaitsForOwnedDescendants(t *testing.T) {
	s := newSupervisor(io.Discard)
	pidFile := filepath.Join(t.TempDir(), "descendant")
	s.configure("worker", processSpec{binary: "/bin/sh", args: []string{"-c", `
trap '' TERM
sleep 30 &
trap 'exit 0' TERM
printf '%s\n' "$!" > "$1"
wait
`, "owned-group", pidFile}})
	t.Cleanup(func() {
		if err := s.shutdown(context.Background()); err != nil {
			t.Error(err)
		}
	})
	if err := s.action(context.Background(), "worker", "start"); err != nil {
		t.Fatal(err)
	}
	var descendant int
	deadline := time.Now().Add(3 * time.Second)
	for descendant == 0 {
		raw, err := os.ReadFile(pidFile)
		if err == nil {
			descendant, _ = strconv.Atoi(strings.TrimSpace(string(raw)))
		}
		if time.Now().After(deadline) {
			t.Fatal("descendant did not start")
		}
		time.Sleep(time.Millisecond)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := s.shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	_, state, err := processIdentity(descendant)
	if err == nil && state != "Z" && state != "X" && state != "x" {
		t.Fatal("shutdown returned with live descendant", descendant, state)
	}
	if err != nil && !os.IsNotExist(err) {
		t.Fatal("could not confirm descendant death", err)
	}
}

func TestSupervisorNeverSignalsReapingGeneration(t *testing.T) {
	s, _, _ := shutdownFixture(t, "")
	s.mu.Lock()
	p := s.children["forwarder"]
	pid := p.cmd.Process.Pid
	// Model a generation whose reaping decision won the race with shutdown.
	// Its old numeric group must no longer be considered an ownership proof.
	p.reaping = true
	err := s.signalChildLocked(p, syscall.SIGKILL)
	p.reaping = false
	s.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if err := syscall.Kill(pid, 0); err != nil {
		t.Fatal("reaping generation was signalled", err)
	}
}
