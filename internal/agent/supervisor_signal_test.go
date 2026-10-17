// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package agent

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"testing"
	"time"
)

func TestSupervisorStopSignalHelper(t *testing.T) {
	if os.Getenv("KARAKORAM_STOP_SIGNAL_HELPER") != "1" {
		return
	}
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT)
	fmt.Println("ready")
	for received := range signals {
		if os.Getenv("KARAKORAM_STOP_SIGNAL_HELPER_INT") == "1" && received != syscall.SIGINT {
			continue
		}
		fmt.Printf("stopped:%d\n", received.(syscall.Signal))
		os.Exit(0)
	}
}

func TestNodeConfiguredStopSignal(t *testing.T) {
	a, _, _ := fakeNode(t, "/bin/true")
	node := &Node{a: a}
	if err := node.Configure("worker", ProcessSpec{Binary: "/bin/true", StopSignal: syscall.SIGINT}); err != nil {
		t.Fatal(err)
	}
	a.sup.mu.Lock()
	configured := a.sup.children["worker"].spec.stopSignal
	a.sup.mu.Unlock()
	if configured != syscall.SIGINT {
		t.Fatal("Node.Configure lost the engine shutdown mode")
	}
}

func TestSupervisorConfiguredStopSignal(t *testing.T) {
	for _, tc := range []struct {
		name     string
		signal   syscall.Signal
		expected syscall.Signal
		intOnly  bool
	}{
		{"default-term", 0, syscall.SIGTERM, false}, {"postgres-fast-int", syscall.SIGINT, syscall.SIGINT, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			supervisor := newSupervisor(io.Discard)
			environment := []string{"KARAKORAM_STOP_SIGNAL_HELPER=1"}
			if tc.intOnly {
				environment = append(environment, "KARAKORAM_STOP_SIGNAL_HELPER_INT=1")
			}
			supervisor.configure("worker", processSpec{binary: os.Args[0], args: []string{"-test.run=^TestSupervisorStopSignalHelper$"}, env: environment, cleanEnv: true, stopSignal: tc.signal})
			t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()
				if err := supervisor.shutdown(ctx); err != nil {
					states, _ := supervisor.snapshot()
					if pid := states["worker"].PID; pid != 0 {
						_ = syscall.Kill(-pid, syscall.SIGKILL)
					}
					t.Error(err)
				}
			})
			waitReady := func(count int) {
				t.Helper()
				deadline := time.Now().Add(2 * time.Second)
				for time.Now().Before(deadline) {
					found := 0
					for _, line := range supervisor.logTail("worker", 100) {
						if line.Message == "ready" {
							found++
						}
					}
					if found >= count {
						return
					}
					time.Sleep(5 * time.Millisecond)
				}
				t.Fatal("helper did not install its signal handlers")
			}
			if err := supervisor.action(context.Background(), "worker", "start"); err != nil {
				t.Fatal(err)
			}
			waitReady(1)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			if err := supervisor.action(ctx, "worker", "restart"); err != nil {
				cancel()
				t.Fatal("restart did not use configured shutdown mode:", err)
			}
			cancel()
			waitReady(2)
			ctx, cancel = context.WithTimeout(context.Background(), time.Second)
			if err := supervisor.action(ctx, "worker", "stop"); err != nil {
				cancel()
				t.Fatal("stop did not use configured shutdown mode:", err)
			}
			cancel()
			found := 0
			for _, line := range supervisor.logTail("worker", 100) {
				if line.Message == fmt.Sprintf("stopped:%d", tc.expected) {
					found++
				}
			}
			if found != 2 {
				t.Fatalf("expected both restart and stop to deliver signal%d, observed%d", tc.expected, found)
			}
		})
	}
}
