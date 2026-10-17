// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package agent

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
)

func TestSupervisorInheritedPipes(t *testing.T) {
	for _, mode := range []string{"clean-exit", "failed-exit", "restart", "stop"} {
		t.Run(mode, func(t *testing.T) {
			s := newSupervisor(io.Discard)
			s.backoff = 10 * time.Millisecond
			if mode != "restart" {
				s.setMaintenance(true)
			}
			pidFile := filepath.Join(t.TempDir(), "descendants")
			// Ignoring TERM survives exec in the descendant. Stop must still
			// signal the existing process group, but cannot rely on pipe EOF.
			s.configure("mongod", processSpec{binary: "/bin/sh", args: []string{"-c", `
trap '' TERM
sleep 30 &
descendant=$!
trap 'exit 0' TERM
printf '%s\n' "$descendant" >> "$1"
printf 'ready\n'
case "$2" in
  failed-exit|restart) exit 7 ;;
  clean-exit) exit 0 ;;
  stop) wait "$descendant" ;;
esac
`, "supervisor-pipes", pidFile, mode}})
			t.Cleanup(func() {
				s.setMaintenance(true)
				// Exit observation must not kill descendants. Terminate the
				// remaining sleepers explicitly, including earlier generations.
				data, _ := os.ReadFile(pidFile)
				for _, raw := range strings.Fields(string(data)) {
					if pid, err := strconv.Atoi(raw); err == nil && pid > 0 {
						_ = syscall.Kill(pid, syscall.SIGKILL)
					}
				}
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()
				if err := s.shutdown(ctx); err != nil {
					t.Error(err)
				}
			})
			if err := s.action(context.Background(), "mongod", "start"); err != nil {
				t.Fatal(err)
			}
			deadline := time.Now().Add(3 * time.Second)
			for {
				if lines := s.logTail("mongod", 1); len(lines) == 1 && lines[0].Message == "ready" {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("shell did not start its background descendant")
				}
				time.Sleep(5 * time.Millisecond)
			}
			if mode == "restart" {
				awaitState(t, s, func(p agentapi.ProcessStatus) bool { return p.Restarts > 0 })
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()
				if err := s.action(ctx, "mongod", "stop"); err != nil {
					t.Fatal("stop after restart waited for descendant pipes:", err)
				}
			} else if mode == "stop" {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()
				if err := s.action(ctx, "mongod", "stop"); err != nil {
					t.Fatal("stop waited for descendant pipes:", err)
				}
			} else {
				awaitState(t, s, func(p agentapi.ProcessStatus) bool { return p.PID == 0 })
			}
			states, _ := s.snapshot()
			status := states["mongod"]
			if status.PID != 0 {
				t.Fatalf("direct child exit was not observed: %+v", status)
			}
			wantCode, wantState := 0, "stopped"
			if mode == "failed-exit" {
				wantCode, wantState = 7, "failed"
			}
			if status.ExitCode == nil {
				t.Fatalf("missing direct child exit code: %+v", status)
			}
			if mode != "restart" && (*status.ExitCode != wantCode || status.State != wantState) {
				t.Fatalf("exit status = %+v, want code %d and state %s", status, wantCode, wantState)
			}
			data, err := os.ReadFile(pidFile)
			if err != nil {
				t.Fatal(err)
			}
			pids := strings.Fields(string(data))
			if len(pids) == 0 {
				t.Fatal("missing descendant PID")
			}
			pid, err := strconv.Atoi(pids[0])
			if err != nil {
				t.Fatal(err)
			}
			if err := syscall.Kill(pid, 0); err != nil {
				t.Fatal("exit observation unexpectedly killed the descendant:", err)
			}
		})
	}
}

func TestSupervisorCompleteFinalLogs(t *testing.T) {
	s := newSupervisor(io.Discard)
	s.setMaintenance(true)
	// More than a pipe buffer on both streams, plus unterminated final lines.
	// All output must be captured before the supervisor reports completion.
	padding := strings.Repeat("x", 256)
	s.configure("mongod", processSpec{binary: "/bin/sh", args: []string{"-c", `
i=0
while [ "$i" -lt 300 ]; do
  printf 'out-%s-%s\n' "$i" "$1"
  printf 'err-%s-%s\n' "$i" "$1" >&2
  i=$((i + 1))
done
printf 'stdout-final'
printf 'stderr-final' >&2
`, "supervisor-logs", padding}})
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := s.shutdown(ctx); err != nil {
			t.Error(err)
		}
	})
	if err := s.action(context.Background(), "mongod", "start"); err != nil {
		t.Fatal(err)
	}
	awaitState(t, s, func(p agentapi.ProcessStatus) bool { return p.PID == 0 })
	lines := s.logTail("mongod", 1000)
	if len(lines) != 602 {
		t.Fatalf("captured %d lines, want 602", len(lines))
	}
	for _, stream := range []string{"stdout", "stderr"} {
		var messages []string
		for _, line := range lines {
			if line.Stream == stream {
				messages = append(messages, line.Message)
			}
		}
		if len(messages) != 301 {
			t.Fatalf("captured %d %s lines, want 301", len(messages), stream)
		}
		prefix := "out"
		if stream == "stderr" {
			prefix = "err"
		}
		for i := range 300 {
			if want := fmt.Sprintf("%s-%d-%s", prefix, i, padding); messages[i] != want {
				t.Fatalf("%s line %d = %q, want %q", stream, i, messages[i], want)
			}
		}
		if messages[300] != stream+"-final" {
			t.Fatalf("lost final unterminated %s line: %q", stream, messages[300])
		}
	}
}
