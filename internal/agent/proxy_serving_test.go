// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package agent

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
)

type servingProxyEngine struct {
	fakeEngine
	name string
}

func (e *servingProxyEngine) ProxyServingReadiness(context.Context) error { return e.readinessErr }
func (e *servingProxyEngine) Name() string                                { return e.name }

func TestProxyServingReadinessFailsClosed(t *testing.T) {
	for _, engine := range []string{"mysql", "postgres"} {
		for _, condition := range []string{"serving", "no-revision", "unapplied-revision", "latest-apply-failed", "not-proxy", "maintenance", "local-maintenance", "stopped", "not-desired", "not-running", "apply-failed", "writer-offline"} {
			t.Run(engine+"/"+condition, func(t *testing.T) {
				e := &servingProxyEngine{name: engine}
				a := &nodeAgent{o: Options{Engine: engine, DataDir: t.TempDir()}, engine: e, sup: newSupervisor(io.Discard), c: agentapi.MemberConfig{Role: "proxy", ConfigVersion: 4, DesiredProcesses: []string{"proxysql"}}}
				t.Cleanup(func() { _ = os.RemoveAll(filepath.Dir(proxyAdmissionPath(a.o.DataDir))) })
				a.acceptedVersion = 4
				a.sup.children["proxysql"] = &child{status: agentapi.ProcessStatus{State: "running", PID: 123}}
				switch condition {
				case "no-revision":
					a.c.ConfigVersion = 0
				case "unapplied-revision":
					a.acceptedVersion = 5
				case "latest-apply-failed":
					a.lastError = "native config apply failed"
				case "not-proxy":
					a.c.Role = "data"
				case "maintenance":
					a.c.Maintenance = true
				case "local-maintenance":
					a.controls.Maintenance = true
				case "stopped":
					a.controls.Stopped = map[string]bool{"proxysql": true}
				case "not-desired":
					a.c.DesiredProcesses = []string{}
				case "not-running":
					a.sup.children["proxysql"].status.PID = 0
				case "apply-failed":
					a.startupErr = errors.New("configuration apply failed")
				case "writer-offline":
					e.readinessErr = errors.New("no online writer")
				}
				ln, err := a.serveProxyServing()
				if err != nil {
					t.Fatal(err)
				}
				defer ln.Close()
				err = Readiness(context.Background(), Options{}, []string{"--serving-proxy", "--data-dir=" + a.o.DataDir})
				if (err == nil) != (condition == "serving") {
					t.Fatalf("serving check = %v", err)
				}
			})
		}
	}
}

func TestProxyServingReadinessOldAgentAndArgumentsFailClosed(t *testing.T) {
	for _, args := range [][]string{
		{"--serving-proxy", "--data-dir=" + t.TempDir()},
		{"--serving-proxy"},
		{"--serving-proxy", "--data-dir=relative"},
		{"--serving-proxy", "--data-dir=/tmp", "extra"},
	} {
		if err := Readiness(t.Context(), Options{}, args); err == nil {
			t.Fatalf("unsafe serving probe accepted %v", args)
		}
	}
}

func proxyServingLatchFixture(t *testing.T, engine, dir string) (*nodeAgent, *servingProxyEngine) {
	t.Helper()
	t.Cleanup(func() { _ = os.RemoveAll(filepath.Dir(proxyAdmissionPath(dir))) })
	e := &servingProxyEngine{name: engine}
	a := &nodeAgent{o: Options{Engine: engine, DataDir: dir}, engine: e, sup: newSupervisor(io.Discard),
		c: agentapi.MemberConfig{Role: "proxy", ConfigVersion: 4, DesiredProcesses: []string{"proxysql"}}, acceptedVersion: 4}
	a.sup.children["proxysql"] = &child{status: agentapi.ProcessStatus{State: "running", PID: 123}}
	return a, e
}

func TestProxyServingAdmissionLatch(t *testing.T) {
	for _, engine := range []string{"mysql", "postgres"} {
		for _, admitted := range []bool{false, true} {
			phase := "before-admission"
			if admitted {
				phase = "after-admission"
			}
			for _, condition := range []string{"writer-loss", "maintenance", "local-maintenance", "stop-hold", "not-desired", "config-apply-failed", "unapplied-config"} {
				t.Run(engine+"/"+phase+"/"+condition, func(t *testing.T) {
					a, e := proxyServingLatchFixture(t, engine, t.TempDir())
					listener, err := a.serveProxyServing()
					if err != nil {
						t.Fatal(err)
					}
					defer listener.Close()
					probe := func() error { return proxyServingReadiness(t.Context(), a.o.DataDir) }
					if admitted {
						if err := probe(); err != nil {
							t.Fatal("initial admission failed:", err)
						}
					}
					if err := a.applyMu.LockContext(t.Context()); err != nil {
						t.Fatal(err)
					}
					a.mu.Lock()
					switch condition {
					case "writer-loss":
						e.readinessErr = errors.New("no online writer")
					case "maintenance":
						a.c.Maintenance = true
					case "local-maintenance":
						a.controls.Maintenance = true
					case "stop-hold":
						a.controls.Stopped = map[string]bool{"proxysql": true}
					case "not-desired":
						a.c.DesiredProcesses = []string{}
					case "config-apply-failed":
						a.lastError = "config apply failed"
					case "unapplied-config":
						a.acceptedVersion++
					}
					a.mu.Unlock()
					a.applyMu.Unlock()
					if err := probe(); (err == nil) != admitted {
						t.Fatalf("admitted=%v, dependency/control change affected admission incorrectly: %v", admitted, err)
					}
				})
			}
		}
	}
}

func TestProxyServingAdmissionSurvivesAgentOnlyRestart(t *testing.T) {
	dir := t.TempDir()
	a, _ := proxyServingLatchFixture(t, "mysql", dir)
	listener, err := a.serveProxyServing()
	if err != nil {
		t.Fatal(err)
	}
	if err := proxyServingReadiness(t.Context(), dir); err != nil {
		t.Fatal(err)
	}
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	if err := proxyServingReadiness(t.Context(), dir); err == nil {
		t.Fatal("dead agent retained healthy admission")
	}

	// Restart/re-exec the agent inside the same container during writer loss.
	// Its already-admitted Swarm task must not lose established admission.
	restarted, e := proxyServingLatchFixture(t, "mysql", dir)
	e.readinessErr = errors.New("writer is temporarily unavailable")
	listener, err = restarted.serveProxyServing()
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	if err := proxyServingReadiness(t.Context(), dir); err != nil {
		t.Fatal("agent-only restart lost this container's admission:", err)
	}
}

func TestAdmittedProxyChecksSupervisorLivenessWithoutApplyLock(t *testing.T) {
	a, _ := proxyServingLatchFixture(t, "mysql", t.TempDir())
	listener, err := a.serveProxyServing()
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	if err := proxyServingReadiness(t.Context(), a.o.DataDir); err != nil {
		t.Fatal(err)
	}
	if err := a.applyMu.LockContext(t.Context()); err != nil {
		t.Fatal(err)
	}
	defer a.applyMu.Unlock()
	if err := proxyServingReadiness(t.Context(), a.o.DataDir); err != nil {
		t.Fatal("admitted proxy depended on in-flight config application:", err)
	}
	a.sup.mu.Lock()
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	err = proxyServingReadiness(ctx, a.o.DataDir)
	cancel()
	a.sup.mu.Unlock()
	if err != nil {
		t.Fatal("admitted proxy health blocked on process management:", err)
	}
	a.sup.closeLaunches()
	if err := proxyServingReadiness(t.Context(), a.o.DataDir); err == nil {
		t.Fatal("closed supervisor remained healthy")
	}
}

func TestProxyServingNewTaskCannotInheritContainerAdmission(t *testing.T) {
	dir := t.TempDir()
	a, _ := proxyServingLatchFixture(t, "mysql", dir)
	listener, err := a.serveProxyServing()
	if err != nil {
		t.Fatal(err)
	}
	if err := proxyServingReadiness(t.Context(), dir); err != nil {
		t.Fatal(err)
	}
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}

	admission, err := proxyAdmissionForContainer(dir)
	if err != nil {
		t.Fatal(err)
	}
	marker, err := os.ReadFile(admission.path)
	if err != nil {
		t.Fatal(err)
	}
	// Model a fresh task's changed PID namespace/PID1-start provenance, even
	// when an old writable-layer marker was copied to the new task unchanged.
	admission.identity = "different-container-task"
	replacement, e := proxyServingLatchFixture(t, "mysql", dir)
	e.readinessErr = errors.New("replacement has not admitted a writer")
	listener, err = replacement.serveProxyServingAdmission(admission)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	if err := proxyServingReadiness(t.Context(), dir); err == nil {
		t.Fatal("replacement task inherited another container's admission")
	}
	current, err := os.ReadFile(admission.path)
	if err != nil || string(current) != string(marker) {
		t.Fatal("failed native admission overwrote the provenance record")
	}
}

func TestProxyAdmissionUsesStableContainerProvenanceOutsideDataDir(t *testing.T) {
	dir := t.TempDir()
	first, err := proxyAdmissionForContainer(dir)
	if err != nil {
		t.Fatal(err)
	}
	second, err := proxyAdmissionForContainer(dir)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatal("agent-only re-exec would change container provenance")
	}
	if filepath.Dir(filepath.Dir(first.path)) != "/tmp" || first.path == filepath.Join(dir, filepath.Base(first.path)) {
		t.Fatal("admission marker can live in an engine data volume")
	}
}

func TestNonProxyStartupDoesNotInitializeAdmission(t *testing.T) {
	a := &nodeAgent{o: Options{DataDir: t.TempDir()}}
	if listener := a.startProxyServing(); listener != nil {
		listener.Close()
		t.Fatal("non-proxy agent started a serving listener")
	}
	if _, err := os.Lstat(filepath.Dir(proxyAdmissionPath(a.o.DataDir))); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("non-proxy agent created admission state: %v", err)
	}
	if err := proxyServingReadiness(t.Context(), a.o.DataDir); err == nil {
		t.Fatal("non-proxy agent exposed serving readiness")
	}
}

func TestProxyAdmissionSetupFailureHoldsReadinessNotStartup(t *testing.T) {
	a, _ := proxyServingLatchFixture(t, "mysql", t.TempDir())
	a.o.ProxyServingProbe = true
	var logs strings.Builder
	a.o.Logger = slog.New(slog.NewTextHandler(&logs, nil))
	// A pre-created, non-private directory is rejected without weakening any
	// inode, ownership, or no-follow checks, even for an otherwise healthy proxy.
	private := filepath.Dir(proxyAdmissionPath(a.o.DataDir))
	if err := os.Mkdir(private, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(private, 0755); err != nil {
		t.Fatal(err)
	}
	listener := a.startProxyServing()
	if listener == nil {
		t.Fatal("admission setup failure prevented not-ready listener startup")
	}
	defer listener.Close()
	for range 2 {
		err := proxyServingReadiness(t.Context(), a.o.DataDir)
		if err == nil || !strings.Contains(err.Error(), "initialize proxy admission") || !strings.Contains(err.Error(), "mode 0700") {
			t.Fatalf("admission setup failure missing from probe diagnostic: %v", err)
		}
	}
	if !strings.Contains(logs.String(), "initialize proxy admission") {
		t.Fatal("admission setup failure was not logged")
	}
	if _, err := os.Lstat(proxyAdmissionPath(a.o.DataDir)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed setup published admission: %v", err)
	}
}

func TestProxyServingBindFailureDoesNotAbortStartup(t *testing.T) {
	a, _ := proxyServingLatchFixture(t, "mysql", t.TempDir())
	a.o.ProxyServingProbe = true
	var logs strings.Builder
	a.o.Logger = slog.New(slog.NewTextHandler(&logs, nil))
	blocker, err := net.Listen("unix", proxyServingAddress(a.o.DataDir))
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Close()
	if listener := a.startProxyServing(); listener != nil {
		listener.Close()
		t.Fatal("agent bound an occupied serving socket")
	}
	if !strings.Contains(logs.String(), "proxy serving readiness unavailable") {
		t.Fatal("bind failure was not diagnosed")
	}
	// The process holding the address exits without a serving response.
	go func() {
		conn, err := blocker.Accept()
		if err == nil {
			conn.Close()
		}
	}()
	if err := proxyServingReadiness(t.Context(), a.o.DataDir); err == nil {
		t.Fatal("bind failure admitted the proxy")
	}
}
