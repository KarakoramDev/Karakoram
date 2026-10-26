// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package agent

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
)

func TestStartupReadinessDoesNotRequireConfigurationOrPeers(t *testing.T) {
	dir := t.TempDir()
	opts := Options{DataDir: dir, Engine: "fake"}
	unavailable := &fakeEngine{readinessErr: context.DeadlineExceeded}
	args := []string{"--startup", "--data-dir=" + dir}
	if err := Readiness(t.Context(), opts, args, unavailable); err == nil {
		t.Fatal("absent agent reported started")
	}
	listener, err := serveStartup(dir, func() error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	if err := Readiness(t.Context(), opts, args, unavailable); err != nil {
		t.Fatal("initialising agent cannot free deployment slot", err)
	}
	if err := Readiness(t.Context(), opts, nil, unavailable); err != context.DeadlineExceeded {
		t.Fatal("native readiness was weakened", err)
	}
	if err := Readiness(t.Context(), opts, []string{"--startup", "--unexpected"}, unavailable); err == nil {
		t.Fatal("invalid startup option accepted")
	}
	if err := Readiness(t.Context(), Options{}, []string{"--startup"}); err == nil {
		t.Fatal("missing data directory accepted")
	}
	if err := Readiness(t.Context(), opts, []string{"--startup", "--data-dir=relative"}); err == nil {
		t.Fatal("relative data directory accepted")
	}
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	if err := Readiness(t.Context(), opts, args, unavailable); err == nil {
		t.Fatal("terminated agent retained startup readiness")
	}
	replacement, err := serveStartup(dir, func() error { return nil })
	if err != nil {
		t.Fatal("terminated agent retained readiness socket", err)
	}
	defer replacement.Close()
	if err := Readiness(t.Context(), opts, args, unavailable); err != nil {
		t.Fatal("replacement not started", err)
	}
}

func TestRunPublishesStartupBeforeFirstConfiguration(t *testing.T) {
	requested := make(chan struct{}, 1)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case requested <- struct{}{}:
		default:
		}
		http.Error(w, "configuration pending", http.StatusServiceUnavailable)
	}))
	defer server.Close()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	dir := t.TempDir()
	opts := Options{ControlPlaneURL: server.URL, InternalCAPEM: string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})), Token: "test-token", Cluster: "test", Member: "member", DataDir: dir, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	done := make(chan error, 1)
	go func() { done <- Run(ctx, opts) }()
	select {
	case <-requested:
	case err := <-done:
		t.Fatal("agent exited before configuration", err)
	case <-time.After(5 * time.Second):
		t.Fatal("agent did not request configuration")
	}
	if err := Readiness(t.Context(), Options{DataDir: dir}, []string{"--startup"}); err != nil {
		t.Fatal("pending configuration blocked startup", err)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("agent did not stop")
	}
	if err := Readiness(t.Context(), Options{DataDir: dir}, []string{"--startup"}); err == nil {
		t.Fatal("stopped agent still started")
	}
}

func TestStartupRejectsKnownLocalInitializationFailures(t *testing.T) {
	for _, failure := range []string{"identity", "agent-tls", "controls", "storage"} {
		t.Run(failure, func(t *testing.T) {
			a, c, _ := fakeNode(t, "/bin/true")
			switch failure {
			case "identity":
				c.Member = "wrong-member"
			case "agent-tls":
				c.AgentTLS.KeyPEM = "invalid-key"
			case "controls":
				if err := os.MkdirAll(filepath.Join(a.o.DataDir, "karakoram"), 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(a.o.DataDir, "karakoram", "controls.json"), []byte("{"), 0600); err != nil {
					t.Fatal(err)
				}
				a.controlsErr = errors.New("invalid local safety controls")
			case "storage":
				blocked := filepath.Join(a.o.DataDir, "blocked")
				if err := os.WriteFile(blocked, nil, 0600); err != nil {
					t.Fatal(err)
				}
				a.o.DataDir = blocked
			}
			if _, err := a.apply(t.Context(), c); err == nil {
				t.Fatal("invalid initialization unexpectedly succeeded")
			}
			listener, err := serveStartup(a.o.DataDir, a.startupHealth)
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			if err := Readiness(t.Context(), a.o, []string{"--startup"}); err == nil {
				t.Fatal("known local initialization failure passed platform readiness")
			}
		})
	}
}

func TestStartupRejectsInvalidAuthoritativeConfiguration(t *testing.T) {
	a, c, _ := fakeNode(t, "/bin/true")
	c.Member = "wrong-member"
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _ = json.NewEncoder(w).Encode(c) }))
	defer server.Close()
	client, err := agentapi.NewClient(agentapi.ClientOptions{BaseURL: server.URL, Token: "test-token", CAPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	a.client = client
	if _, err := a.reload(t.Context()); err == nil {
		t.Fatal("invalid authoritative configuration accepted")
	}
	listener, err := serveStartup(a.o.DataDir, a.startupHealth)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	if err := Readiness(t.Context(), a.o, []string{"--startup"}); err == nil {
		t.Fatal("invalid authoritative configuration passed startup")
	}
}

func TestStartupAcceptsEngineApplyErrorsAndReportsStatus(t *testing.T) {
	migration := exec.Command("/bin/sh", "-c", "exit 1").Run()
	var exit *exec.ExitError
	if !errors.As(migration, &exit) {
		t.Fatal("migration fixture did not fail with an exit error", migration)
	}
	for _, pending := range []error{errors.New("invalid engine configuration"), migration, context.DeadlineExceeded, &net.DNSError{Name: "queued-peer", Err: "no such host", IsNotFound: true}, &net.OpError{Op: "listen", Net: "tcp", Err: syscall.EADDRINUSE}} {
		t.Run(pending.Error(), func(t *testing.T) {
			a, c, e := fakeNode(t, "/bin/true")
			e.onApply = func(*Node, agentapi.MemberConfig) error { return pending }
			if _, err := a.apply(t.Context(), c); !errors.Is(err, pending) {
				t.Fatal("dependency wait not reached", err)
			}
			listener, err := serveStartup(a.o.DataDir, a.startupHealth)
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			if err := Readiness(t.Context(), a.o, []string{"--startup"}); err != nil {
				t.Fatal("peer/quorum wait retained deployment slot", err)
			}
			response := requestNode(a, "GET", "/v1/status", "", "token")
			var status agentapi.Status
			if response.Code != http.StatusOK {
				t.Fatal("member status unavailable", response.Code)
			}
			if err := json.Unmarshal(response.Body.Bytes(), &status); err != nil {
				t.Fatal(err)
			}
			if status.Error != pending.Error() {
				t.Fatalf("apply diagnostic lost from member status: got %q want %q", status.Error, pending.Error())
			}
		})
	}
}

func TestStartupRejectsFatalSupervisorStartFailure(t *testing.T) {
	a, _, _ := fakeNode(t, "/bin/true")
	a.sup.configure("worker", processSpec{binary: "/missing-local-engine-binary"})
	if err := a.sup.action(t.Context(), "worker", "start"); err == nil {
		t.Fatal("missing executable started")
	}
	listener, err := serveStartup(a.o.DataDir, a.startupHealth)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	if err := Readiness(t.Context(), a.o, []string{"--startup"}); err == nil {
		t.Fatal("fatal supervisor start failure passed startup")
	}
}

func TestStartupChildExitIsReportedByMemberStatus(t *testing.T) {
	a, _, _ := fakeNode(t, "/bin/true")
	a.sup.configure("worker", processSpec{binary: "/bin/sh", args: []string{"-c", "exit 1"}})
	if err := a.sup.action(t.Context(), "worker", "start"); err != nil {
		t.Fatal(err)
	}
	a.sup.mu.Lock()
	done := a.sup.children["worker"].done
	a.sup.mu.Unlock()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("child did not exit")
	}
	listener, err := serveStartup(a.o.DataDir, a.startupHealth)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	if err := Readiness(t.Context(), a.o, []string{"--startup"}); err != nil {
		t.Fatal("child exit incorrectly became platform readiness failure", err)
	}
	response := requestNode(a, "GET", "/v1/status", "", "token")
	var status agentapi.Status
	if err := json.Unmarshal(response.Body.Bytes(), &status); err != nil {
		t.Fatal(err)
	}
	if process := status.Processes["worker"]; process.State != "failed" || process.ExitCode == nil || *process.ExitCode != 1 {
		t.Fatalf("child failure not visible to engine status: %+v", process)
	}
}

func TestStartupStaleConfigurationIsNotAgentFatal(t *testing.T) {
	a, c, _ := fakeNode(t, "/bin/true")
	a.acceptedVersion = c.ConfigVersion + 1
	_, applyErr := a.apply(t.Context(), c)
	if applyErr == nil {
		t.Fatal("stale authoritative configuration was accepted")
	}
	listener, err := serveStartup(a.o.DataDir, a.startupHealth)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	if err := Readiness(t.Context(), a.o, []string{"--startup"}); err != nil {
		t.Fatal("stale CP reply became an agent fatal initialization error", err)
	}
	response := requestNode(a, "GET", "/v1/status", "", "token")
	var status agentapi.Status
	if err := json.Unmarshal(response.Body.Bytes(), &status); err != nil {
		t.Fatal(err)
	}
	if status.Error != "" {
		t.Fatal("superseded reply polluted active apply failure status", status.Error)
	}
}

func TestSupersededReplyPreservesActiveApplyOutcome(t *testing.T) {
	a, current, engine := fakeNode(t, buildFake(t))
	current.ConfigVersion = 5
	observe := func() agentapi.Status {
		t.Helper()
		response := requestNode(a, "GET", "/v1/status", "", "token")
		if response.Code != http.StatusOK {
			t.Fatal(response.Code)
		}
		var status agentapi.Status
		if err := json.Unmarshal(response.Body.Bytes(), &status); err != nil {
			t.Fatal(err)
		}
		return status
	}
	if _, err := a.apply(t.Context(), current); err != nil {
		t.Fatal(err)
	}
	older := current
	older.ConfigVersion = 4
	if _, err := a.apply(t.Context(), older); err == nil {
		t.Fatal("superseded configuration accepted")
	}
	status := observe()
	if status.ConfigVersion != 5 || status.Error != "" || status.Processes["worker"].State != "running" {
		t.Fatal("late reply invalidated newer healthy apply", status)
	}

	failure := errors.New("current migration failed")
	engine.onApply = func(*Node, agentapi.MemberConfig) error { return failure }
	current.ConfigVersion = 6
	if _, err := a.apply(t.Context(), current); !errors.Is(err, failure) {
		t.Fatal("current failure not reached", err)
	}
	if status := observe(); status.ConfigVersion != 5 || status.Error != failure.Error() {
		t.Fatal("genuine current failure not active", status)
	}
	if _, err := a.apply(t.Context(), older); err == nil {
		t.Fatal("superseded reply accepted after current failure")
	}
	if status := observe(); status.Error != failure.Error() {
		t.Fatal("late reply overwrote genuine current failure", status)
	}
	engine.onApply = nil
	if _, err := a.apply(t.Context(), current); err != nil {
		t.Fatal(err)
	}
	if status := observe(); status.ConfigVersion != 6 || status.Error != "" || status.Processes["worker"].State != "running" {
		t.Fatal("successful current apply did not repair active failure", status)
	}
}
