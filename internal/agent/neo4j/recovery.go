// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package neo4j

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agent"
	"github.com/KarakoramDev/Karakoram/internal/engine/neo4j/neo4jspec"
)

// recoverAdmin is only called under the node action lock while the supervised
// server is stopped. Authentication is disabled solely in this loopback child.
func (e *Engine) recoverAdmin(ctx context.Context, n *agent.Node, s neo4jspec.Spec, password string) error {
	if password == "" {
		return errors.New("Neo4j recovery password is required")
	}
	states, _ := n.Snapshot()
	if states[neo4jspec.Process].State == "running" {
		return errors.New("Neo4j must be stopped for credential recovery")
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	stock, err := os.ReadFile(filepath.Join(s.Install.Neo4jHome, "conf", "neo4j.conf"))
	if err != nil {
		return errors.New("Neo4j stock configuration unavailable")
	}
	b, h := e.ports(true)
	files, err := render(s, baseDir(n), string(stock), true, b, h)
	if err != nil {
		return err
	}
	if _, err := writeFiles(n, files); err != nil {
		return err
	}
	conf := filepath.Join(baseDir(n), "recovery-conf")
	proc, err := serverCommand(ctx, s, conf)
	if err != nil {
		return err
	}
	cmd := exec.Command(proc.Binary, proc.Args...)
	cmd.Env = proc.Env
	cmd.Dir = proc.Dir
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL}
	// The server can log request diagnostics. Never route recovery output into
	// agent logs or return native output as an error.
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	if err := cmd.Start(); err != nil {
		return errors.New("Neo4j recovery start failed")
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	exited := false
	defer func() {
		if !exited {
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
			<-done
		}
	}()
	deadline := time.NewTimer(80 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(300 * time.Millisecond)
	defer tick.Stop()
	ready := false
	for !ready {
		select {
		case <-ctx.Done():
			return errors.New("Neo4j credential recovery interrupted")
		case <-deadline.C:
			return errors.New("Neo4j credential recovery readiness timed out")
		case <-done:
			exited = true
			return errors.New("Neo4j credential recovery server exited")
		case <-tick.C:
			probe, stop := context.WithTimeout(ctx, 2*time.Second)
			_, err := query(probe, s, h, "", "system", "SHOW USERS", nil)
			stop()
			ready = err == nil
		}
	}
	if _, err := query(ctx, s, h, "", "system", "ALTER USER neo4j SET PASSWORD $p CHANGE NOT REQUIRED", map[string]any{"p": password}); err != nil {
		return errors.New("Neo4j credential recovery failed")
	}
	_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM)
	stop := time.NewTimer(45 * time.Second)
	defer stop.Stop()
	select {
	case err := <-done:
		exited = true
		if err != nil {
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != 128+int(syscall.SIGTERM) {
				return errors.New("Neo4j recovery shutdown failed")
			}
		}
	case <-stop.C:
		return errors.New("Neo4j recovery shutdown timed out")
	case <-ctx.Done():
		return errors.New("Neo4j recovery shutdown interrupted")
	}
	return nil
}
