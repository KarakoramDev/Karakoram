// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package agent

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sort"
	"sync"
	"syscall"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/redact"
)

// Give log readers time to consume buffered output after the direct child exits.
// Descendants may keep the write ends open indefinitely.
const supervisorLogDrainGrace = 250 * time.Millisecond

type processSpec struct {
	binary      string
	args, env   []string
	dir         string
	stopSignal  syscall.Signal
	stopTimeout time.Duration
	// cleanEnv starts the child from a minimal environment instead of the
	// agent's, so member credentials such as the agent token are not inherited.
	cleanEnv           bool
	goMemoryLimitBytes uint64
}

// minimalEnv carries only non-secret process basics into engine children.
func minimalEnv() []string {
	env := []string{}
	for _, key := range []string{"PATH", "LANG", "LC_ALL", "TZ"} {
		if v, ok := os.LookupEnv(key); ok {
			env = append(env, key+"="+v)
		}
	}
	if _, ok := os.LookupEnv("PATH"); !ok {
		env = append(env, "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin")
	}
	return env
}

type child struct {
	spec       processSpec
	cmd        *exec.Cmd
	desired    bool
	status     agentapi.ProcessStatus
	done       chan struct{}
	crashes    int
	startError error
	generation uint64
	// Reaping authorization is serialized with every process-group signal.
	// Until then Linux waitid retains the leader as the PGID identity anchor.
	reaping bool
	// First launch intent is stable across failed starts and later restarts.
	startOrder int
}
type supervisor struct {
	mu                  sync.Mutex
	children            map[string]*child
	maintenance, closed bool
	logs                map[string][]agentapi.LogLine
	output              io.Writer
	backoff             time.Duration
	pipe                func() (*os.File, *os.File, error)
	outputOnce          sync.Once
	outputStop          sync.Once
	outputQueue         chan agentapi.LogLine
	outputDone          chan struct{}
	ownershipDir        string
	beforeStart         func() error
	starts              int
	session             string
}

func newSupervisor(output io.Writer) *supervisor {
	return &supervisor{children: map[string]*child{}, logs: map[string][]agentapi.LogLine{}, output: output, outputDone: make(chan struct{}), backoff: time.Second, session: rand.Text()}
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
func (s *supervisor) startLocked(name string, p *child) (err error) {
	defer func() {
		if err != nil {
			p.status.State, p.status.Error = "failed", err.Error()
			p.startError = err
			s.scheduleLocked(name, p)
		}
	}()
	if s.closed {
		return errors.New("supervisor shut down")
	}
	if p.cmd != nil {
		return nil
	}
	if p.startOrder == 0 {
		s.starts++
		p.startOrder = s.starts
	}
	if s.beforeStart != nil {
		if err := s.beforeStart(); err != nil {
			return err
		}
	}
	release, err := s.acquireProcess(name)
	if err != nil {
		return err
	}
	defer release()
	cmd := exec.Command(p.spec.binary, p.spec.args...)
	if p.spec.cleanEnv {
		cmd.Env = append(minimalEnv(), p.spec.env...)
	} else {
		cmd.Env = append(os.Environ(), p.spec.env...)
	}
	cmd.Dir = p.spec.dir
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	pipe := s.pipe
	if pipe == nil {
		pipe = os.Pipe
	}
	// Own the pipes so cmd.Wait can reap the child without closing readers
	// before its final output has been captured.
	stdout, stdoutWriter, err := pipe()
	if err != nil {
		return err
	}
	stderr, stderrWriter, err := pipe()
	if err != nil {
		_ = stdout.Close()
		_ = stdoutWriter.Close()
		return err
	}
	cmd.Stdout = stdoutWriter
	cmd.Stderr = stderrWriter
	err = s.recordProcessIntent(name)
	if err == nil {
		err = cmd.Start()
		if err != nil {
			err = errors.Join(err, s.writeProcessOwner(name, processOwner{}))
		}
	}
	_ = stdoutWriter.Close()
	_ = stderrWriter.Close()
	if err != nil {
		_ = stdout.Close()
		_ = stderr.Close()
		return err
	}
	if err := s.recordProcess(name, cmd.Process.Pid); err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		_ = stdout.Close()
		_ = stderr.Close()
		return fmt.Errorf("persist process ownership; child stopped: %w", err)
	}
	now := time.Now().UTC()
	p.cmd = cmd
	p.reaping = false
	p.done = make(chan struct{})
	p.status.State = "running"
	p.status.PID = cmd.Process.Pid
	p.status.StartedAt = &now
	p.status.SupervisorSession = s.session
	p.status.Launch++
	p.status.GoMemoryLimitBytes = p.spec.goMemoryLimitBytes
	p.status.Error = ""
	p.startError = nil
	p.status.ExitCode = nil
	done := p.done
	var logs sync.WaitGroup
	logs.Add(2)
	go func() { defer logs.Done(); s.capture(name, "stdout", stdout) }()
	go func() { defer logs.Done(); s.capture(name, "stderr", stderr) }()
	go func() {
		if ownedGroupAnchors {
			// Do not release the leader's PID/PGID until shutdown finishes
			// signalling and observing every remaining group member.
			for {
				if err := observeChildExit(cmd.Process.Pid); err == nil {
					break
				} else {
					s.mu.Lock()
					p.status.Error = "observe owned child exit: " + err.Error()
					s.mu.Unlock()
					time.Sleep(100 * time.Millisecond)
				}
			}
			s.mu.Lock()
			cleanup := s.closed
			if !cleanup {
				p.reaping = true
			}
			s.mu.Unlock()
			if cleanup {
				for {
					s.mu.Lock()
					err := s.signalChildLocked(p, syscall.SIGKILL)
					s.mu.Unlock()
					if err == nil {
						err = awaitOwnedGroupExit(cmd.Process.Pid)
					}
					if err == nil {
						break
					}
					// Incomplete observation cannot authorize data removal.
					// Keep the anchor and expose the failure until the
					// kernel can positively confirm group death.
					s.mu.Lock()
					p.status.Error = "finish owned process group: " + err.Error()
					s.mu.Unlock()
					time.Sleep(100 * time.Millisecond)
				}
				s.mu.Lock()
				p.reaping = true
				s.mu.Unlock()
			}
		}
		err := cmd.Wait()
		// Reaping must not depend on EOF: an orphaned descendant can still
		// hold stdout/stderr. Preserve normal final logs, then close readers
		// after a bounded grace without signalling any descendants.
		drain := time.AfterFunc(supervisorLogDrainGrace, func() {
			_ = stdout.Close()
			_ = stderr.Close()
		})
		logs.Wait()
		drain.Stop()
		_ = stdout.Close()
		_ = stderr.Close()
		s.mu.Lock()
		defer s.mu.Unlock()
		p.cmd = nil
		p.status.PID = 0
		code := cmd.ProcessState.ExitCode()
		p.status.ExitCode = &code
		// Qualify retained output only after this exact launch has been reaped.
		// Readers never infer failure from log text or a later child's state.
		lines := s.logs[name]
		for i := range lines {
			if lines[i].SupervisorSession == p.status.SupervisorSession && lines[i].Launch == p.status.Launch {
				lines[i].ExitCode = &code
			}
		}
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
	return s.actionWithAdmission(ctx, name, action, nil, nil)
}

// Admission runs under the process lock immediately before changing intent.
// Identified restarts use it to durably bind their stop to the owned generation.
func (s *supervisor) actionWithAdmission(ctx context.Context, name, action string, admit func(*child) error, beforeStart func() error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	p := s.children[name]
	if p == nil {
		s.mu.Unlock()
		return fmt.Errorf("process %s is not configured", name)
	}
	if admit != nil {
		if err := admit(p); err != nil {
			s.mu.Unlock()
			return err
		}
	}
	switch action {
	case "start":
		p.desired = true
		p.generation++
		err := s.startLocked(name, p)
		s.mu.Unlock()
		return err
	case "stop", "restart":
		p.desired = action == "restart" && beforeStart == nil
		p.generation++
		s.mu.Unlock()
		if err := s.stopChild(ctx, p); err != nil {
			return err
		}
		if action == "restart" {
			if beforeStart != nil {
				if err := beforeStart(); err != nil {
					return err
				}
			}
			return s.action(ctx, name, "start")
		}
		return nil
	default:
		s.mu.Unlock()
		return errors.New("invalid process action")
	}
}

// stopChild sends the graceful signal even when the shutdown wait budget expired.
// Ordinary actions check cancellation before changing intent; shutdown must still
// signal every remaining child rather than abandon dependencies after one timeout.
func (s *supervisor) stopChild(ctx context.Context, p *child) error {
	s.mu.Lock()
	cmd, done := p.cmd, p.done
	err := s.signalChildLocked(p, p.spec.stopSignal)
	s.mu.Unlock()
	if err != nil || cmd == nil {
		return err
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// mu serializes the decision to reap with every process-group signal. Once
// reaping is authorized, an old numeric PGID is no longer an ownership proof.
func (s *supervisor) signalChildLocked(p *child, signal syscall.Signal) error {
	if p.cmd == nil || p.reaping {
		return nil
	}
	if signal == 0 {
		signal = syscall.SIGTERM
	}
	if err := syscall.Kill(-p.cmd.Process.Pid, signal); err != nil && err != syscall.ESRCH {
		return err
	}
	return nil
}

func (s *supervisor) orderedChildrenLocked() []string {
	names := make([]string, 0, len(s.children))
	for name := range s.children {
		names = append(names, name)
	}
	sort.Slice(names, func(i, j int) bool {
		a, b := s.children[names[i]].startOrder, s.children[names[j]].startOrder
		if a == b {
			return names[i] < names[j]
		}
		return a < b
	})
	return names
}
func (s *supervisor) setMaintenance(enabled bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.maintenance = enabled
	for _, name := range s.orderedChildrenLocked() {
		p := s.children[name]
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
		status := p.status
		if status.PID > 0 && processStopped(status.PID) {
			status.State = "paused"
			status.Error = "process is stopped by a signal; resume it with SIGCONT after checking the cause"
		}
		out[name] = status
	}
	return out, s.maintenance
}

// closeLaunches fences automatic retries and concurrent starts before engine
// handoff begins, without signalling the processes needed for that handoff.
func (s *supervisor) closeLaunches() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	for _, p := range s.children {
		p.desired = false
		p.generation++
	}
}

func (s *supervisor) shutdown(ctx context.Context) error {
	s.closeLaunches()
	s.mu.Lock()
	names := s.orderedChildrenLocked()
	s.mu.Unlock()
	var errs []error
	// Each child gets a bounded share, so a stuck consumer cannot consume the
	// entire shutdown deadline before dependencies receive their graceful stop.
	for i := len(names) - 1; i >= 0; i-- {
		name := names[i]
		s.mu.Lock()
		p := s.children[name]
		cmd, done, budget := p.cmd, p.done, p.spec.stopTimeout
		s.mu.Unlock()
		timeout := shutdownChildTimeout(ctx, budget, i+1)
		childCtx, cancel := context.WithTimeout(ctx, timeout)
		err := s.stopChild(childCtx, p)
		cancel()
		if cmd != nil {
			if err != nil {
				s.mu.Lock()
				var killErr error
				if ownedGroupAnchors {
					killErr = s.signalChildLocked(p, syscall.SIGKILL)
				} else {
					// No identity-safe group anchor is available here.
					// Preserve non-Linux launch support, but never add a
					// recycled-PGID kill as a fallback.
					killErr = cmd.Process.Kill()
					if errors.Is(killErr, os.ErrProcessDone) {
						killErr = nil
					}
				}
				s.mu.Unlock()
				err = errors.Join(err, killErr)
			}
			// Linux waiters retain the leader until group death is confirmed,
			// then reap it. Expired grace cannot authorize data removal.
			<-done
		}
		if err != nil {
			errs = append(errs, fmt.Errorf("stop %s: %w", name, err))
		}
	}
	s.outputStop.Do(func() { close(s.outputDone) })
	return errors.Join(errs...)
}

func shutdownChildTimeout(ctx context.Context, budget time.Duration, remaining int) time.Duration {
	timeout := 10 * time.Second
	if budget > timeout {
		timeout = min(budget, 45*time.Second)
	}
	if deadline, ok := ctx.Deadline(); ok {
		timeout = min(timeout, max(time.Until(deadline), 0)/time.Duration(remaining))
	}
	return timeout
}

const supervisorLogLineBytes = 16 << 10
const supervisorLogBytes = 1 << 20

func (s *supervisor) capture(name, stream string, r io.Reader) {
	reader := bufio.NewReaderSize(r, supervisorLogLineBytes)
	var message []byte
	var sanitizer redact.Stream
	var truncated bool
	for {
		part, err := reader.ReadSlice('\n')
		remaining := supervisorLogLineBytes - len(message)
		retained := min(len(part), remaining)
		sanitizer.Observe(part[:retained], false)
		sanitizer.Observe(part[retained:], true)
		truncated = truncated || retained < len(part)
		message = append(message, part[:retained]...)
		if err == bufio.ErrBufferFull {
			continue // Discard excess bytes, but always keep draining the pipe.
		}
		if len(message) > 0 {
			safe, gap, keep := sanitizer.Line(string(bytes.TrimSuffix(message, []byte{'\n'})))
			sanitizer.EndLine(truncated)
			if gap != "" {
				s.captureLine(name, stream, gap)
			}
			if keep {
				s.captureLine(name, stream, safe)
			}
			message = message[:0]
			truncated = false
		}
		if err != nil {
			if gap := sanitizer.Omission(); gap != "" {
				s.captureLine(name, stream, gap)
			}
			return
		}
	}
}

func (s *supervisor) captureLine(name, stream, message string) {
	line := agentapi.LogLine{Timestamp: time.Now().UTC(), Process: name, Stream: stream, Message: message}
	s.mu.Lock()
	if p := s.children[name]; p != nil {
		line.SupervisorSession = p.status.SupervisorSession
		line.Launch = p.status.Launch
	}
	ring := append(s.logs[name], line)
	total := 0
	first := len(ring)
	for first > 0 && len(ring)-first < 1000 && total+len(ring[first-1].Message) <= supervisorLogBytes {
		first--
		total += len(ring[first].Message)
	}
	for i := range first {
		ring[i] = agentapi.LogLine{}
	}
	s.logs[name] = ring[first:]
	s.mu.Unlock()
	s.emit(line)
}

// A stalled external collector may lose output, never process control or pipe draining.
// There is at most one blocked writer and a bounded queue per supervisor.
func (s *supervisor) emit(line agentapi.LogLine) {
	if s.output == nil || s.output == io.Discard {
		return
	}
	s.outputOnce.Do(func() {
		s.outputQueue = make(chan agentapi.LogLine, 32)
		go func() {
			for {
				select {
				case line := <-s.outputQueue:
					fmt.Fprintf(s.output, "[%s %s] %s\n", line.Process, line.Stream, line.Message)
				case <-s.outputDone:
					return
				}
			}
		}()
	})
	select {
	case <-s.outputDone:
		return
	case s.outputQueue <- line:
	default:
	}
}
func (s *supervisor) logTail(name string, n int) []agentapi.LogLine {
	s.mu.Lock()
	defer s.mu.Unlock()
	lines := s.logs[name]
	n = min(max(n, 0), len(lines))
	return append([]agentapi.LogLine{}, lines[len(lines)-n:]...)
}
