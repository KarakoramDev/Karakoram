// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package agent

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func initTestEnv(role, mode, dir, parent string) []string {
	env := []string{}
	for _, value := range os.Environ() {
		if !strings.HasPrefix(value, "KARAKORAM_INIT_TEST_") {
			env = append(env, value)
		}
	}
	return append(env, "KARAKORAM_INIT_TEST_ROLE="+role, "KARAKORAM_INIT_TEST_MODE="+mode, "KARAKORAM_INIT_TEST_DIR="+dir, "KARAKORAM_INIT_TEST_PARENT="+parent)
}
func initTestFailure(err error) {
	fmt.Fprintln(os.Stderr, "init fixture:", err)
	os.Exit(111)
}
func waitInitFixture(condition func() bool) bool {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return false
}

// A private subreaper process gives these tests the same orphan-adoption
// semantics as namespace PID 1, without changing the test runner's wait ownership.
func TestInitProcessHelper(t *testing.T) {
	role := os.Getenv("KARAKORAM_INIT_TEST_ROLE")
	if role == "" {
		return
	}
	mode := os.Getenv("KARAKORAM_INIT_TEST_MODE")
	dir := os.Getenv("KARAKORAM_INIT_TEST_DIR")
	parent := os.Getenv("KARAKORAM_INIT_TEST_PARENT")
	args := []string{os.Args[0], "-test.run=^TestInitProcessHelper$"}
	switch role {
	case "init":
		if err := unix.Prctl(unix.PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0); err != nil {
			initTestFailure(err)
		}
		code, err := runInitChild(os.Args[0], args, initTestEnv("agent", mode, dir, strconv.Itoa(os.Getpid())))
		if err != nil {
			initTestFailure(err)
		}
		os.Exit(code)
	case "agent":
		if mode == "signals" {
			signals := make(chan os.Signal, 1)
			signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)
			if err := os.WriteFile(filepath.Join(dir, "ready"), nil, 0600); err != nil {
				initTestFailure(err)
			}
			received := <-signals
			if err := os.WriteFile(filepath.Join(dir, "signal"), []byte(strconv.Itoa(int(received.(syscall.Signal)))), 0600); err != nil {
				initTestFailure(err)
			}
			os.Exit(9)
		}
		// This direct command must retain its own os/exec Wait result while the
		// separate init process is concurrently reaping adopted descendants.
		command := exec.Command(os.Args[0], args[1:]...)
		command.Env = initTestEnv("intermediate", mode, dir, parent)
		command.Stdout = os.Stdout
		command.Stderr = os.Stderr
		if err := command.Run(); err != nil {
			initTestFailure(fmt.Errorf("direct child wait: %w", err))
		}
		data, err := os.ReadFile(filepath.Join(dir, "orphan-pid"))
		if err != nil {
			initTestFailure(err)
		}
		pid, err := strconv.Atoi(string(data))
		if err != nil {
			initTestFailure(err)
		}
		expected, err := strconv.Atoi(parent)
		if err != nil {
			initTestFailure(err)
		}
		adopted := waitInitFixture(func() bool {
			stat, ok := readProcessTreeStat("/proc", pid, false)
			return ok && stat.parent == expected
		})
		if !adopted {
			initTestFailure(errors.New("grandchild was not adopted by the init process"))
		}
		if err = os.WriteFile(filepath.Join(dir, "release-orphan"), nil, 0600); err != nil {
			initTestFailure(err)
		}
		reaped := waitInitFixture(func() bool { _, err := os.Stat(filepath.Join("/proc", strconv.Itoa(pid))); return os.IsNotExist(err) })
		if !reaped {
			initTestFailure(errors.New("adopted grandchild remained a zombie"))
		}
		if err = os.WriteFile(filepath.Join(dir, "orphan-reaped"), nil, 0600); err != nil {
			initTestFailure(err)
		}
		os.Exit(7)
	case "intermediate":
		child, err := os.StartProcess(os.Args[0], args, &os.ProcAttr{Env: initTestEnv("orphan", mode, dir, parent), Files: []*os.File{os.Stdin, os.Stdout, os.Stderr}})
		if err != nil {
			initTestFailure(err)
		}
		if err = os.WriteFile(filepath.Join(dir, "orphan-pid"), []byte(strconv.Itoa(child.Pid)), 0600); err != nil {
			initTestFailure(err)
		}
		_ = child.Release()
		os.Exit(0)
	case "orphan":
		if !waitInitFixture(func() bool { _, err := os.Stat(filepath.Join(dir, "release-orphan")); return err == nil }) {
			initTestFailure(errors.New("orphan was not released"))
		}
		os.Exit(0)
	default:
		initTestFailure(errors.New("unknown init fixture role"))
	}
}
func initTestCommand(ctx context.Context, mode, dir string) *exec.Cmd {
	command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestInitProcessHelper$")
	command.Env = initTestEnv("init", mode, dir, "")
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	command.Cancel = func() error {
		err := syscall.Kill(-command.Process.Pid, syscall.SIGKILL)
		if errors.Is(err, syscall.ESRCH) {
			return os.ErrProcessDone
		}
		return err
	}
	return command
}
func TestInitReapsOrphansWithoutStealingCommandWaits(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	dir := t.TempDir()
	command := initTestCommand(ctx, "orphans", dir)
	output, err := command.CombinedOutput()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 7 {
		t.Fatalf("init lost the agent exit status or command wait: %v: %s", err, output)
	}
	if _, err = os.Stat(filepath.Join(dir, "orphan-reaped")); err != nil {
		t.Fatal("orphan adoption/reaping was not observed")
	}
}
func TestInitForwardsTerminationAndHangup(t *testing.T) {
	for _, forward := range []syscall.Signal{syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP} {
		t.Run(strconv.Itoa(int(forward)), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			dir := t.TempDir()
			command := initTestCommand(ctx, "signals", dir)
			var output bytes.Buffer
			command.Stdout = &output
			command.Stderr = &output
			if err := command.Start(); err != nil {
				cancel()
				t.Fatal(err)
			}
			waited := false
			defer func() {
				cancel()
				if !waited {
					_ = command.Wait()
				}
			}()
			if !waitInitFixture(func() bool { _, err := os.Stat(filepath.Join(dir, "ready")); return err == nil }) {
				t.Fatal("child did not install signal handlers")
			}
			if err := command.Process.Signal(forward); err != nil {
				t.Fatal(err)
			}
			err := command.Wait()
			waited = true
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != 9 {
				t.Fatalf("signal was not forwarded with child exit status: %v: %s", err, output.String())
			}
			data, err := os.ReadFile(filepath.Join(dir, "signal"))
			if err != nil || string(data) != strconv.Itoa(int(forward)) {
				t.Fatal("child did not receive the original signal")
			}
		})
	}
}
func TestInitDoesNothingOutsidePID1(t *testing.T) {
	if os.Getpid() == 1 {
		t.Skip("no-op case requires an ordinary process")
	}
	handled, _, err := ReexecIfPID1()
	if handled || err != nil {
		t.Fatalf("non-init member attempted to reexec: handled=%t err=%v", handled, err)
	}
}
func TestInitExitCode(t *testing.T) {
	if initExitCode(syscall.WaitStatus(7<<8)) != 7 {
		t.Fatal("normal exit status changed")
	}
	if initExitCode(syscall.WaitStatus(syscall.SIGTERM)) != 143 {
		t.Fatal("signal exit status changed")
	}
}
