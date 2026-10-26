// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package agent

import (
	"context"
	"errors"
	"syscall"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
)

func pbmFenceAgent(t *testing.T) *nodeAgent {
	t.Helper()
	a, _ := recoveryAgent(t)
	a.sup.configure("pbm-agent", processSpec{binary: "/bin/sleep", args: []string{"30"}})
	if err := a.sup.action(t.Context(), "pbm-agent", "start"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		states, _ := a.sup.snapshot()
		if pid := states["pbm-agent"].PID; pid > 0 {
			_ = syscall.Kill(pid, syscall.SIGCONT)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := a.sup.shutdown(ctx); err != nil {
			t.Error(err)
		}
	})
	return a
}

func TestPBMRestoreFenceConfirmsWaitAndReplaysReceipt(t *testing.T) {
	a := pbmFenceAgent(t)
	states, _ := a.sup.snapshot()
	old := states["pbm-agent"].PID
	request := agentapi.ProcessAction{Process: "pbm-agent", Action: "restart", RequestID: "restore-fence/m0", RestoreCommandTS: time.Now().Add(-2 * time.Minute).Unix()}
	if err := a.processActionRequest(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	out, err := a.processActionResult(request)
	if err != nil || out.PBMFence == nil || !out.PBMFence.OldExited || out.PBMFence.PID == old {
		t.Fatal(out, err)
	}
	if err := syscall.Kill(old, 0); !errors.Is(err, syscall.ESRCH) {
		t.Fatal("old process did not exit", err)
	}
	next := out.PBMFence.PID
	if err := a.processActionRequest(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	again, err := a.processActionResult(request)
	if err != nil || again.PBMFence.PID != next {
		t.Fatal("receipt replay restarted twice", again, err)
	}
	a.sup.session = "different-supervisor"
	if err := a.processActionRequest(t.Context(), request); err == nil {
		t.Fatal("historical restart receipt survived unknown supervisor interruption")
	}
}

func TestPBMRestoreFenceCannotConfirmExit(t *testing.T) {
	a := pbmFenceAgent(t)
	states, _ := a.sup.snapshot()
	old := states["pbm-agent"].PID
	if err := syscall.Kill(old, syscall.SIGSTOP); err != nil {
		t.Fatal(err)
	}
	for range 100 {
		states, _ = a.sup.snapshot()
		if states["pbm-agent"].State == "paused" {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if states["pbm-agent"].State != "paused" {
		t.Fatal("fixture failed to pause original PBM process")
	}
	request := agentapi.ProcessAction{Process: "pbm-agent", Action: "restart", RequestID: "blocked-exit", RestoreCommandTS: time.Now().Add(-2 * time.Minute).Unix()}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if err := a.processActionRequest(ctx, request); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("stop did not retain unknown wait", err)
	}
	if _, err := a.processActionResult(request); err == nil {
		t.Fatal("timed-out wait fabricated exit confirmation")
	}
	states, _ = a.sup.snapshot()
	if states["pbm-agent"].PID != old {
		t.Fatal("replacement started before old process exited")
	}
}

func TestPBMRestoreFenceMemberClockBehindKeepsStopped(t *testing.T) {
	a := pbmFenceAgent(t)
	request := agentapi.ProcessAction{Process: "pbm-agent", Action: "restart", RequestID: "behind-clock", RestoreCommandTS: time.Now().Unix()}
	if err := a.processActionRequest(t.Context(), request); err == nil {
		t.Fatal("member clock behind command margin was accepted")
	}
	if _, err := a.processActionResult(request); err == nil {
		t.Fatal("clock rejection fabricated restart proof")
	}
	states, _ := a.sup.snapshot()
	if states["pbm-agent"].PID != 0 {
		t.Fatal("clock-unsafe replacement started", states)
	}
	if !a.controls.Stopped["pbm-agent"] {
		t.Fatal("unknown clock fence would be undone by a future config pull")
	}
	// A retry must not replay stop or autonomously launch without passing guard.
	if err := a.processActionRequest(t.Context(), request); err == nil {
		t.Fatal("clock-unsafe retry accepted")
	}
	states, _ = a.sup.snapshot()
	if states["pbm-agent"].PID != 0 {
		t.Fatal("retry bypassed clock guard")
	}
}
