// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package agent

import (
	"context"
	"io"
	"syscall"
	"testing"
	"time"
)

func TestSupervisorLaunchIdentityChangesOnlyAfterReapedReplacement(t *testing.T) {
	s := newSupervisor(io.Discard)
	s.configure("pbm-agent", processSpec{binary: "/bin/sleep", args: []string{"60"}})
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	defer s.shutdown(ctx)
	if err := s.action(ctx, "pbm-agent", "start"); err != nil {
		t.Fatal(err)
	}
	states, _ := s.snapshot()
	first := states["pbm-agent"]
	if first.SupervisorSession == "" || first.Launch != 1 {
		t.Fatal(first)
	}
	if err := syscall.Kill(first.PID, syscall.SIGSTOP); err != nil {
		t.Fatal(err)
	}
	states, _ = s.snapshot()
	paused := states["pbm-agent"]
	if paused.SupervisorSession != first.SupervisorSession || paused.Launch != first.Launch {
		t.Fatal("paused live process acquired a replacement identity", first, paused)
	}
	if err := syscall.Kill(first.PID, syscall.SIGCONT); err != nil {
		t.Fatal(err)
	}
	if err := s.action(ctx, "pbm-agent", "restart"); err != nil {
		t.Fatal(err)
	}
	states, _ = s.snapshot()
	replacement := states["pbm-agent"]
	if replacement.SupervisorSession != first.SupervisorSession || replacement.Launch != first.Launch+1 {
		t.Fatal("reaped replacement did not advance launch identity", first, replacement)
	}
}
