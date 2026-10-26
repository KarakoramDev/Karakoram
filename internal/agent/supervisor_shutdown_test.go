// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package agent

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestSupervisorShutdownProcessHelper(t *testing.T) {
	if os.Getenv("KARAKORAM_SHUTDOWN_HELPER") != "1" {
		return
	}
	name := os.Getenv("KARAKORAM_SHUTDOWN_NAME")
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM)
	if err := os.WriteFile(os.Getenv("KARAKORAM_SHUTDOWN_READY"), []byte(strconv.Itoa(os.Getpid())), 0600); err != nil {
		os.Exit(2)
	}
	for range signals {
		log, err := os.OpenFile(os.Getenv("KARAKORAM_SHUTDOWN_LOG"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
		if err != nil {
			os.Exit(2)
		}
		_, err = fmt.Fprintln(log, name)
		log.Close()
		if err != nil {
			os.Exit(2)
		}
		if os.Getenv("KARAKORAM_SHUTDOWN_HUNG") != "1" {
			os.Exit(0)
		}
	}
}

func shutdownFixture(t *testing.T, hung string) (*supervisor, string, func(string)) {
	t.Helper()
	dir := t.TempDir()
	log := filepath.Join(dir, "shutdown.log")
	s := newSupervisor(io.Discard)
	s.backoff = 5 * time.Millisecond
	// Registration order is deliberately not the launch dependency order.
	for _, name := range []string{"forwarder", "patroni", "etcd"} {
		// Race-instrumented helpers otherwise sleep one second at exit, beyond the shutdown deadlines under test.
		env := []string{"GORACE=atexit_sleep_ms=0", "KARAKORAM_SHUTDOWN_HELPER=1", "KARAKORAM_SHUTDOWN_NAME=" + name, "KARAKORAM_SHUTDOWN_LOG=" + log, "KARAKORAM_SHUTDOWN_READY=" + filepath.Join(dir, name+".ready")}
		if name == hung {
			env = append(env, "KARAKORAM_SHUTDOWN_HUNG=1")
		}
		s.configure(name, processSpec{binary: os.Args[0], args: []string{"-test.run=^TestSupervisorShutdownProcessHelper$"}, env: env})
	}
	t.Cleanup(func() {
		s.mu.Lock()
		s.closed = true
		var waits []<-chan struct{}
		for _, p := range s.children {
			p.desired = false
			if p.cmd != nil {
				syscall.Kill(-p.cmd.Process.Pid, syscall.SIGKILL)
				waits = append(waits, p.done)
			}
		}
		s.mu.Unlock()
		for _, done := range waits {
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Error("helper cleanup stalled")
			}
		}
	})
	ready := func(name string) {
		t.Helper()
		deadline := time.Now().Add(3 * time.Second)
		for {
			states, _ := s.snapshot()
			data, err := os.ReadFile(filepath.Join(dir, name+".ready"))
			if err == nil && string(data) == strconv.Itoa(states[name].PID) && states[name].PID > 0 {
				return
			}
			if time.Now().After(deadline) {
				t.Fatal("helper did not become ready", name, states[name])
			}
			time.Sleep(time.Millisecond)
		}
	}
	for _, name := range []string{"etcd", "patroni", "forwarder"} {
		if err := s.action(context.Background(), name, "start"); err != nil {
			t.Fatal(err)
		}
		ready(name)
	}
	return s, log, ready
}

func TestSupervisorDependencyShutdownOrder(t *testing.T) {
	for _, scenario := range []string{"normal", "dependency-restarted", "hung-consumer"} {
		t.Run(scenario, func(t *testing.T) {
			hung := ""
			if scenario == "hung-consumer" {
				hung = "forwarder"
			}
			s, log, ready := shutdownFixture(t, hung)
			if scenario == "dependency-restarted" {
				if err := s.action(context.Background(), "etcd", "restart"); err != nil {
					t.Fatal(err)
				}
				ready("etcd")
				if err := os.WriteFile(log, nil, 0600); err != nil {
					t.Fatal(err)
				}
			}
			ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
			defer cancel()
			err := s.shutdown(ctx)
			if hung != "" && !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal("hung child timeout lost", err)
			}
			if hung == "" && err != nil {
				t.Fatal(err)
			}
			data, readErr := os.ReadFile(log)
			if readErr != nil {
				t.Fatal(readErr)
			}
			actual := strings.Fields(string(data))
			if !slices.Equal(actual, []string{"forwarder", "patroni", "etcd"}) {
				t.Fatal("dependency shutdown order", actual)
			}
			states, _ := s.snapshot()
			if states["patroni"].PID != 0 || states["etcd"].PID != 0 {
				t.Fatal("hung consumer prevented dependency stops", states)
			}
		})
	}
}

func TestSupervisorCanceledShutdownStillSignalsEveryChild(t *testing.T) {
	s, _, _ := shutdownFixture(t, "")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_ = s.shutdown(ctx)
	deadline := time.Now().Add(3 * time.Second)
	for {
		states, _ := s.snapshot()
		stopped := true
		for _, state := range states {
			stopped = stopped && state.PID == 0
		}
		if stopped {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("expired shutdown omitted child stop signals", states)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestSupervisorMaintenanceResumeUsesStartDependencyOrder(t *testing.T) {
	s, _, ready := shutdownFixture(t, "")
	s.setMaintenance(true)
	states, _ := s.snapshot()
	for _, state := range states {
		if err := syscall.Kill(-state.PID, syscall.SIGKILL); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		states, _ = s.snapshot()
		stopped := true
		for _, state := range states {
			stopped = stopped && state.PID == 0
		}
		if stopped {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("maintenance did not retain stopped children", states)
		}
		time.Sleep(time.Millisecond)
	}
	var beforeLaunch [][]string
	s.mu.Lock()
	s.beforeStart = func() error {
		// The callback already holds supervisor.mu.
		var running []string
		for _, name := range []string{"etcd", "patroni", "forwarder"} {
			if s.children[name].cmd != nil {
				running = append(running, name)
			}
		}
		beforeLaunch = append(beforeLaunch, running)
		return nil
	}
	s.mu.Unlock()
	s.setMaintenance(false)
	want := [][]string{nil, {"etcd"}, {"etcd", "patroni"}}
	if len(beforeLaunch) != len(want) {
		t.Fatal("maintenance resume did not launch all children", beforeLaunch)
	}
	for i := range want {
		if !slices.Equal(beforeLaunch[i], want[i]) {
			t.Fatal("maintenance resume violated dependency order", beforeLaunch)
		}
	}
	for _, name := range []string{"etcd", "patroni", "forwarder"} {
		ready(name)
	}
}
