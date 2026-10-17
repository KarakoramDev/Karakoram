// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package agent

import (
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
)

// ReexecIfPID1 isolates init duties from the real member process. The child owns
// its os/exec waits; this parent alone reaps daemonized descendants adopted by
// PID 1. A broad reaper in the real member would race its command waiters.
// Outside a PID namespace's init position this function does nothing.
func ReexecIfPID1() (handled bool, code int, err error) {
	if os.Getpid() != 1 {
		return false, 0, nil
	}
	executable, err := os.Executable()
	if err != nil {
		return true, 1, err
	}
	code, err = runInitChild(executable, os.Args, os.Environ())
	return true, code, err
}

func runInitChild(executable string, args, environment []string) (int, error) {
	signals := make(chan os.Signal, 16)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)
	defer signal.Stop(signals)

	child, err := os.StartProcess(executable, args, &os.ProcAttr{Env: environment, Files: []*os.File{os.Stdin, os.Stdout, os.Stderr}})
	if err != nil {
		return 1, fmt.Errorf("start member process: %w", err)
	}
	defer child.Release()
	stopped := make(chan struct{})
	forwardingDone := make(chan struct{})
	go func() {
		defer close(forwardingDone)
		for {
			select {
			case received := <-signals:
				_ = child.Signal(received)
			case <-stopped:
				return
			}
		}
	}()
	defer func() { close(stopped); <-forwardingDone }()

	for {
		var status syscall.WaitStatus
		pid, err := syscall.Wait4(-1, &status, 0, nil)
		if errors.Is(err, syscall.EINTR) {
			continue
		}
		if err != nil {
			return 1, fmt.Errorf("wait for member process: %w", err)
		}
		if pid != child.Pid {
			continue
		}
		code := initExitCode(status)
		// Drain adopted zombies already available. A live descendant cannot keep
		// the namespace alive after its init exits, so do not wait on it here.
		for {
			var adopted syscall.WaitStatus
			pid, err = syscall.Wait4(-1, &adopted, syscall.WNOHANG, nil)
			if errors.Is(err, syscall.EINTR) {
				continue
			}
			if err != nil || pid <= 0 {
				break
			}
		}
		return code, nil
	}
}

func initExitCode(status syscall.WaitStatus) int {
	if status.Exited() {
		return status.ExitStatus()
	}
	if status.Signaled() {
		return 128 + int(status.Signal())
	}
	return 1
}
