// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
)

func recoveryAgent(t *testing.T) (*nodeAgent, agentapi.MemberConfig) {
	t.Helper()
	c := memberConfig(t)
	c.ConfigVersion = 2
	a := &nodeAgent{o: Options{Cluster: c.Cluster, Member: c.Member, DataDir: t.TempDir(), Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}, sup: newSupervisor(io.Discard)}
	return a, c
}

func TestRecoveryRejectsStaleConfig(t *testing.T) {
	a, c := recoveryAgent(t)
	if _, err := a.apply(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	c.ConfigVersion--
	c.Keyfile = "stale-key"
	if _, err := a.apply(context.Background(), c); err == nil {
		t.Fatal("accepted stale generation")
	}
	if a.config().ConfigVersion != 2 {
		t.Fatal("regressed configuration")
	}
}

func TestRecoveryRepairsMongoFileDrift(t *testing.T) {
	a, c := recoveryAgent(t)
	c.TLS = agentapi.TLSConfig{Mode: "requireTLS", CAPEM: "ca", CertPEM: "cert", KeyPEM: "key"}
	if _, err := a.apply(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"keyfile", "mongod.conf", "mongodb.pem", "ca.pem"} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(a.o.DataDir, "karakoram", name)
			expected := append([]byte(nil), a.files[name]...)
			if err := os.Remove(path); err != nil {
				t.Fatal(err)
			}
			if _, err := a.apply(context.Background(), c); err != nil {
				t.Fatal(err)
			}
			actual, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(actual, expected) {
				t.Fatalf("file not repaired: %v", err)
			}
		})
	}
}

func TestRecoveryMissingControlsFailsClosed(t *testing.T) {
	a, c := recoveryAgent(t)
	if _, err := a.apply(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	if err := a.setProcessStopped("mongod", true); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(a.o.DataDir, "karakoram", "controls.json")); err != nil {
		t.Fatal(err)
	}
	if _, err := loadControls(a.o.DataDir); err == nil {
		t.Fatal("lost safety state accepted")
	}
	if _, err := loadControls(t.TempDir()); err != nil {
		t.Fatal("fresh member rejected", err)
	}
}

func TestRecoveryCanceledControlDoesNotMutate(t *testing.T) {
	a, _ := recoveryAgent(t)
	a.sup.configure("mongod", processSpec{binary: "/bin/sleep", args: []string{"30"}})
	a.applyMu.Lock()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.processAction(ctx, "mongod", "start") }()
	cancel()
	a.applyMu.Unlock()
	err := <-done
	defer a.sup.shutdown(context.Background())
	if err == nil {
		t.Fatal("canceled action succeeded")
	}
	states, _ := a.sup.snapshot()
	if states["mongod"].PID != 0 {
		t.Fatal("canceled action started child")
	}
	if _, err := os.Stat(filepath.Join(a.o.DataDir, "karakoram", "controls.json")); !os.IsNotExist(err) {
		t.Fatal("canceled action persisted intent", err)
	}
}

type blockedLogWriter struct {
	entered chan struct{}
	release chan struct{}
}

func (w *blockedLogWriter) Write(p []byte) (int, error) {
	select {
	case w.entered <- struct{}{}:
	default:
	}
	<-w.release
	return len(p), nil
}

func TestRecoveryBlockedLogSinkDoesNotBlockSupervisor(t *testing.T) {
	w := &blockedLogWriter{make(chan struct{}, 1), make(chan struct{})}
	s := newSupervisor(w)
	defer close(w.release)
	captured := make(chan struct{})
	go func() { s.capture("mongod", "stdout", strings.NewReader("line\n")); close(captured) }()
	select {
	case <-w.entered:
	case <-time.After(time.Second):
		t.Fatal("sink not called")
	}
	done := make(chan struct{})
	go func() { s.snapshot(); s.setMaintenance(true); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("log sink blocked controls")
	}
	select {
	case <-captured:
	case <-time.After(time.Second):
		t.Fatal("sink blocked draining")
	}
}

func TestRecoveryOversizedLogsKeepDraining(t *testing.T) {
	s := newSupervisor(io.Discard)
	s.capture("mongod", "stdout", strings.NewReader(strings.Repeat("x", 2<<20)+"\nprogress\n"))
	lines := s.logTail("mongod", 1000)
	if len(lines) == 0 || lines[len(lines)-1].Message != "progress" {
		t.Fatal("stream stopped after oversized line")
	}
}

func TestRecoveryLogRetentionByteBound(t *testing.T) {
	s := newSupervisor(io.Discard)
	for range 100 {
		s.capture("mongod", "stderr", strings.NewReader(strings.Repeat("x", 64<<10)+"\n"))
	}
	total := 0
	for _, line := range s.logTail("mongod", 1000) {
		total += len(line.Message)
	}
	if total > 2<<20 {
		t.Fatalf("retained %d bytes", total)
	}
}

func TestRecoveryRestartTimeoutRetainsDesire(t *testing.T) {
	s := newSupervisor(io.Discard)
	s.backoff = 10 * time.Millisecond
	s.configure("mongod", processSpec{binary: "/bin/sleep", args: []string{"30"}})
	if err := s.action(context.Background(), "mongod", "start"); err != nil {
		t.Fatal(err)
	}
	states, _ := s.snapshot()
	pid := states["mongod"].PID
	defer func() {
		_ = syscall.Kill(pid, syscall.SIGCONT)
		_ = s.shutdown(context.Background())
	}()
	if err := syscall.Kill(pid, syscall.SIGSTOP); err != nil {
		t.Fatal(err)
	}
	awaitState(t, s, func(p agentapi.ProcessStatus) bool { return p.State == "paused" })
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := s.action(ctx, "mongod", "restart"); err == nil {
		t.Fatal("restart did not time out")
	}
	if err := syscall.Kill(pid, syscall.SIGCONT); err != nil {
		t.Fatal(err)
	}
	awaitState(t, s, func(p agentapi.ProcessStatus) bool { return p.PID != 0 && p.PID != pid })
}

func TestRecoveryStoppedProcessIsNotRunning(t *testing.T) {
	s := newSupervisor(io.Discard)
	s.configure("mongod", processSpec{binary: "/bin/sleep", args: []string{"30"}})
	if err := s.action(context.Background(), "mongod", "start"); err != nil {
		t.Fatal(err)
	}
	states, _ := s.snapshot()
	pid := states["mongod"].PID
	defer func() { syscall.Kill(pid, syscall.SIGCONT); s.shutdown(context.Background()) }()
	if err := syscall.Kill(pid, syscall.SIGSTOP); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	states, _ = s.snapshot()
	if states["mongod"].State == "running" {
		t.Fatal("stopped process reported running")
	}
}

func TestRecoveryRestartReceipt(t *testing.T) {
	a, _ := recoveryAgent(t)
	a.o.Token = "token"
	a.sup.configure("mongod", processSpec{binary: "/bin/sleep", args: []string{"30"}})
	if err := a.sup.action(context.Background(), "mongod", "start"); err != nil {
		t.Fatal(err)
	}
	defer a.sup.shutdown(context.Background())
	body := `{"process":"mongod","action":"restart","requestID":"restart-operation/member/step"}`
	first := requestNode(a, "POST", "/v1/processes", body, "token")
	if first.Code != 200 {
		t.Fatal(first.Code, first.Body.String())
	}
	states, _ := a.sup.snapshot()
	pid := states["mongod"].PID
	second := requestNode(a, "POST", "/v1/processes", body, "token")
	if second.Code != 200 {
		t.Fatal(second.Code, second.Body.String())
	}
	states, _ = a.sup.snapshot()
	if states["mongod"].PID != pid {
		t.Fatal("replayed restart created another generation")
	}
}

func TestRecoveryNodeSignal(t *testing.T) {
	a, _ := recoveryAgent(t)
	signaler, ok := any(&Node{a: a}).(interface {
		Signal(string, syscall.Signal) error
	})
	if !ok {
		t.Fatal("node has no owned-process signal operation")
	}
	if err := signaler.Signal("mongod", syscall.SIGHUP); err == nil {
		t.Fatal("unconfigured process accepted")
	}
	a.sup.configure("mongod", processSpec{binary: "/bin/sleep", args: []string{"30"}})
	if err := a.sup.action(context.Background(), "mongod", "start"); err != nil {
		t.Fatal(err)
	}
	defer a.sup.shutdown(context.Background())
	if err := signaler.Signal("mongod", syscall.SIGSTOP); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	states, _ := a.sup.snapshot()
	if states["mongod"].State != "paused" {
		t.Fatal("signal not delivered")
	}
	if err := signaler.Signal("mongod", syscall.SIGCONT); err != nil {
		t.Fatal(err)
	}
}

func TestRecoveryPausedRouterStatusRemainsResponsive(t *testing.T) {
	a, c := recoveryAgent(t)
	c.Role = agentapi.RoleRouter
	c.SystemUsers["clusterMonitor"] = agentapi.Credentials{Username: "clusterMonitor", Password: "test-only"}
	a.c = c
	a.started = time.Now()
	a.sup.configure("mongos", processSpec{binary: "/bin/sleep", args: []string{"30"}})
	if err := a.sup.action(t.Context(), "mongos", "start"); err != nil {
		t.Fatal(err)
	}
	states, _ := a.sup.snapshot()
	pid := states["mongos"].PID
	defer func() {
		_ = syscall.Kill(pid, syscall.SIGCONT)
		_ = a.sup.shutdown(context.Background())
	}()
	if err := syscall.Kill(pid, syscall.SIGSTOP); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for !processStopped(pid) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !processStopped(pid) {
		t.Fatal("router fixture did not stop")
	}
	request := httptest.NewRequest("GET", "/v1/status", nil)
	request.Header.Set("Authorization", "Bearer test-token")
	a.o.Token = "test-token"
	response := httptest.NewRecorder()
	started := time.Now()
	a.handler().ServeHTTP(response, request)
	if elapsed := time.Since(started); elapsed >= time.Second {
		t.Fatalf("paused child blocked status for %s", elapsed)
	}
	var status agentapi.Status
	if err := json.Unmarshal(response.Body.Bytes(), &status); err != nil {
		t.Fatal(err)
	}
	if response.Code != 200 || status.Processes["mongos"].State != "paused" || status.MongoReady == nil || *status.MongoReady || !strings.Contains(status.MongoError, "SIGCONT") {
		t.Fatalf("paused router status lost diagnostics: %+v (HTTP %d)", status, response.Code)
	}
}
