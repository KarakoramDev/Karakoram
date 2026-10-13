// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package agent

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
)

type processSpec struct {
	binary    string
	args, env []string
}
type child struct {
	spec       processSpec
	cmd        *exec.Cmd
	desired    bool
	status     agentapi.ProcessStatus
	done       chan struct{}
	crashes    int
	generation uint64
}
type supervisor struct {
	mu                  sync.Mutex
	children            map[string]*child
	maintenance, closed bool
	logs                map[string][]agentapi.LogLine
	output              io.Writer
	backoff             time.Duration
}

func newSupervisor(output io.Writer) *supervisor {
	return &supervisor{children: map[string]*child{}, logs: map[string][]agentapi.LogLine{}, output: output, backoff: time.Second}
}
func (s *supervisor) configure(name string, spec processSpec) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p := s.children[name]
	if p == nil {
		p = &child{status: agentapi.ProcessStatus{State: "stopped"}}
		s.children[name] = p
	}
	p.spec = spec
}
func (s *supervisor) startLocked(name string, p *child) error {
	if s.closed {
		return errors.New("supervisor shut down")
	}
	if p.cmd != nil {
		return nil
	}
	cmd := exec.Command(p.spec.binary, p.spec.args...)
	cmd.Env = append(os.Environ(), p.spec.env...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return err
	}
	if err = cmd.Start(); err != nil {
		p.status.State = "failed"
		p.status.Error = err.Error()
		s.scheduleLocked(name, p)
		return err
	}
	now := time.Now().UTC()
	p.cmd = cmd
	p.done = make(chan struct{})
	p.status.State = "running"
	p.status.PID = cmd.Process.Pid
	p.status.StartedAt = &now
	p.status.Error = ""
	p.status.ExitCode = nil
	done := p.done
	var logs sync.WaitGroup
	logs.Add(2)
	go func() { defer logs.Done(); s.capture(name, "stdout", stdout) }()
	go func() { defer logs.Done(); s.capture(name, "stderr", stderr) }()
	go func() {
		logs.Wait()
		err := cmd.Wait()
		s.mu.Lock()
		defer s.mu.Unlock()
		p.cmd = nil
		p.status.PID = 0
		code := cmd.ProcessState.ExitCode()
		p.status.ExitCode = &code
		p.status.State = "stopped"
		if err != nil {
			p.status.Error = err.Error()
			p.status.State = "failed"
		}
		if time.Since(now) > time.Minute {
			p.crashes = 0
		}
		close(done)
		s.scheduleLocked(name, p)
	}()
	return nil
}
func (s *supervisor) scheduleLocked(name string, p *child) {
	if !p.desired || s.maintenance || s.closed {
		return
	}
	p.generation++
	generation := p.generation
	delay := s.backoff * time.Duration(1<<min(p.crashes, 6))
	p.crashes++
	time.AfterFunc(delay, func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		if p.generation != generation || !p.desired || s.maintenance || s.closed || p.cmd != nil {
			return
		}
		p.status.Restarts++
		_ = s.startLocked(name, p)
	})
}
func (s *supervisor) action(ctx context.Context, name, action string) error {
	s.mu.Lock()
	p := s.children[name]
	if p == nil {
		s.mu.Unlock()
		return fmt.Errorf("process %s is not configured", name)
	}
	switch action {
	case "start":
		p.desired = true
		p.generation++
		err := s.startLocked(name, p)
		s.mu.Unlock()
		return err
	case "stop", "restart":
		p.desired = false
		p.generation++
		cmd, done := p.cmd, p.done
		s.mu.Unlock()
		if cmd != nil {
			if err := syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM); err != nil && err != syscall.ESRCH {
				return err
			}
			select {
			case <-done:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		if action == "restart" {
			return s.action(ctx, name, "start")
		}
		return nil
	default:
		s.mu.Unlock()
		return errors.New("invalid process action")
	}
}
func (s *supervisor) setMaintenance(enabled bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.maintenance = enabled
	for name, p := range s.children {
		p.generation++
		if !enabled && p.desired && p.cmd == nil {
			_ = s.startLocked(name, p)
		}
	}
}
func (s *supervisor) snapshot() (map[string]agentapi.ProcessStatus, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string]agentapi.ProcessStatus{}
	for name, p := range s.children {
		out[name] = p.status
	}
	return out, s.maintenance
}
func (s *supervisor) shutdown(ctx context.Context) error {
	s.mu.Lock()
	s.closed = true
	names := make([]string, 0, len(s.children))
	for name := range s.children {
		names = append(names, name)
	}
	s.mu.Unlock()
	var errs []error
	for _, name := range names {
		if err := s.action(ctx, name, "stop"); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
func (s *supervisor) capture(name, stream string, r io.Reader) {
	scan := bufio.NewScanner(r)
	scan.Buffer(make([]byte, 4096), 1024*1024)
	for scan.Scan() {
		line := agentapi.LogLine{Timestamp: time.Now().UTC(), Process: name, Stream: stream, Message: scan.Text()}
		s.mu.Lock()
		ring := s.logs[name]
		if len(ring) == 1000 {
			copy(ring, ring[1:])
			ring = ring[:999]
		}
		s.logs[name] = append(ring, line)
		fmt.Fprintf(s.output, "[%s %s] %s\n", name, stream, line.Message)
		s.mu.Unlock()
	}
}
func (s *supervisor) logTail(name string, n int) []agentapi.LogLine {
	s.mu.Lock()
	defer s.mu.Unlock()
	lines := s.logs[name]
	n = min(max(n, 0), len(lines))
	return append([]agentapi.LogLine{}, lines[len(lines)-n:]...)
}
