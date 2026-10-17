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
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
)

type fakeEngine struct {
	binary, version string
	args            []string
	readinessErr    error
	metricWait      bool
	onApply         func(*Node, agentapi.MemberConfig) error
}

func (*fakeEngine) Name() string        { return "fake" }
func (*fakeEngine) Processes() []string { return []string{"worker"} }
func (e *fakeEngine) Apply(ctx context.Context, n *Node, c agentapi.MemberConfig) ([]string, error) {
	if e.onApply != nil {
		if err := e.onApply(n, c); err != nil {
			return nil, err
		}
	}
	if err := n.WriteFile("engine/config.json", c.EngineSpec, 0600); err != nil {
		return nil, err
	}
	if err := n.WriteFile("engine/key", []byte("secret"), 0400); err != nil {
		return nil, err
	}
	if err := n.Configure("worker", ProcessSpec{Binary: e.binary, Dir: n.DataDir()}); err != nil {
		return nil, err
	}
	states, _ := n.Snapshot()
	action := "start"
	if states["worker"].PID != 0 {
		action = "restart"
	}
	return []string{"engine/config.json"}, n.Action(ctx, "worker", action)
}
func (e *fakeEngine) Readiness(_ context.Context, _ Options, args []string) error {
	e.args = slices.Clone(args)
	return e.readinessErr
}
func (e *fakeEngine) Version(context.Context) string { return e.version }
func (e *fakeEngine) Metrics(ctx context.Context, _ *Node) []Metric {
	if e.metricWait {
		<-ctx.Done()
		return nil
	}
	return []Metric{{Name: "fake_work_total", Help: "Completed work.", Labels: map[string]string{"queue": "primary", "kind": "jobs"}, Value: 3, Counter: true}}
}
func (*fakeEngine) Routes(mux *http.ServeMux, _ *Node) {
	mux.HandleFunc("POST /v1/fake/ping", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(`{"ok":true}`)) })
	// Even an engine registering outside its namespace cannot replace generic routes.
	mux.HandleFunc("GET /v1/outside", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
}
func fakeNode(t *testing.T, binary string) (*nodeAgent, agentapi.MemberConfig, *fakeEngine) {
	t.Helper()
	c := memberConfig(t)
	c.Engine, c.Maintenance, c.DesiredProcesses = "fake", false, []string{"worker"}
	c.EngineSpec = json.RawMessage(`{"port":1234}`)
	e := &fakeEngine{binary: binary, version: "1.0"}
	a := &nodeAgent{o: Options{Engine: "fake", Cluster: c.Cluster, Member: c.Member, Token: "token", DataDir: t.TempDir(), Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}, engine: e, sup: newSupervisor(io.Discard), started: time.Now()}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := a.sup.shutdown(ctx); err != nil {
			t.Error(err)
		}
	})
	return a, c, e
}
func workerState(a *nodeAgent) agentapi.ProcessStatus {
	states, _ := a.sup.snapshot()
	return states["worker"]
}
func requestNode(a *nodeAgent, method, path, body, token string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	w := httptest.NewRecorder()
	a.handler().ServeHTTP(w, r)
	return w
}
func TestEngineApplyAndDurableControls(t *testing.T) {
	a, c, e := fakeNode(t, buildFake(t))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := a.apply(ctx, c); err != nil {
		t.Fatal(err)
	}
	for name, mode := range map[string]os.FileMode{"engine/config.json": 0600, "engine/key": 0400, "config.json": 0600} {
		path := filepath.Join(a.o.DataDir, "karakoram", name)
		info, err := os.Stat(path)
		if err != nil || info.Mode().Perm() != mode {
			t.Fatal(name, info, err)
		}
	}
	data, err := os.ReadFile(filepath.Join(a.o.DataDir, "karakoram", "engine/config.json"))
	if err != nil || string(data) != string(c.EngineSpec) {
		t.Fatal(string(data), err)
	}
	first := workerState(a)
	if first.PID == 0 {
		t.Fatal(first)
	}
	// Give the compiled fake time to install its SIGTERM handler.
	time.Sleep(50 * time.Millisecond)
	e.version = "1.1"
	c.ConfigVersion++
	if _, err := a.apply(ctx, c); err != nil {
		t.Fatal(err)
	}
	if workerState(a).PID == first.PID || a.version != "1.1" {
		t.Fatal(workerState(a), a.version)
	}
	time.Sleep(50 * time.Millisecond)
	if err := a.maintenance(true); err != nil {
		t.Fatal(err)
	}
	before := workerState(a).PID
	if _, err := a.apply(ctx, c); err != nil {
		t.Fatal(err)
	}
	if workerState(a).PID != before {
		t.Fatal("maintenance pull restarted process")
	}
	if err := a.processAction(ctx, "worker", "stop"); err != nil {
		t.Fatal(err)
	}
	if _, err := a.apply(ctx, c); err != nil {
		t.Fatal(err)
	}
	if workerState(a).PID != 0 {
		t.Fatal("pull released stop hold")
	}
	controls, err := loadControls(a.o.DataDir)
	if err != nil || !controls.Maintenance || !controls.Stopped["worker"] {
		t.Fatal(controls, err)
	}
	info, err := os.Stat(filepath.Join(a.o.DataDir, "karakoram", "controls.json"))
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatal(info, err)
	}
	restarted, _, _ := fakeNode(t, e.binary)
	restarted.o.DataDir, restarted.controls = a.o.DataDir, controls
	if _, err := restarted.apply(ctx, c); err != nil {
		t.Fatal(err)
	}
	if workerState(restarted).PID != 0 || !(&Node{a: restarted}).Maintenance() {
		t.Fatal("restart released durable holds")
	}
	if err := restarted.maintenance(false); err != nil {
		t.Fatal(err)
	}
	if _, err := restarted.apply(ctx, c); err != nil {
		t.Fatal(err)
	}
	if workerState(restarted).PID != 0 {
		t.Fatal("maintenance-off released stop hold")
	}
	if err := restarted.processAction(ctx, "worker", "restart"); err != nil {
		t.Fatal(err)
	}
	if workerState(restarted).PID == 0 || (&Node{a: restarted}).Stopped("worker") {
		t.Fatal("explicit restart did not release stop hold")
	}
	time.Sleep(50 * time.Millisecond)
	c.DesiredProcesses = []string{}
	if _, err := restarted.apply(ctx, c); err != nil {
		t.Fatal(err)
	}
	if workerState(restarted).PID != 0 {
		t.Fatal("explicit empty intent did not stop process")
	}
}
func TestEngineRoutesMetricsAndWhitelist(t *testing.T) {
	a, c, _ := fakeNode(t, buildFake(t))
	if _, err := a.apply(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	for _, token := range []string{"", "wrong"} {
		if w := requestNode(a, "POST", "/v1/fake/ping", `{}`, token); w.Code != http.StatusUnauthorized {
			t.Fatal(w.Code)
		}
	}
	if w := requestNode(a, "POST", "/v1/fake/ping", `{}`, "token"); w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"ok":true`) {
		t.Fatal(w.Code, w.Body.String())
	}
	for _, path := range []string{"/v1/bootstrap", "/v1/pbm", "/v1/outside"} {
		if w := requestNode(a, "POST", path, `{}`, "token"); w.Code != http.StatusNotFound {
			t.Fatal(path, w.Code)
		}
	}
	for _, path := range []string{"/v1/processes/mongod/start", "/v1/processes/worker/exec"} {
		if w := requestNode(a, "POST", path, "", "token"); w.Code != http.StatusBadRequest {
			t.Fatal(path, w.Code)
		}
	}
	if w := requestNode(a, "POST", "/v1/processes", `{"process":"shell","action":"start"}`, "token"); w.Code != http.StatusBadRequest {
		t.Fatal(w.Code)
	}
	if err := (&Node{a: a}).Configure("shell", ProcessSpec{}); err == nil {
		t.Fatal("unlisted process configured")
	}
	if err := (&Node{a: a}).Action(context.Background(), "shell", "start"); err == nil {
		t.Fatal("unlisted process started")
	}
	w := requestNode(a, "GET", "/v1/metrics", "", "token")
	if w.Code != http.StatusOK {
		t.Fatal(w.Code, w.Body.String())
	}
	for _, text := range []string{"karakoram_process_cpu_seconds_total{process=\"worker\"}", "karakoram_process_resident_memory_bytes", "karakoram_ram_used_bytes", "karakoram_ram_total_bytes", "karakoram_disk_used_bytes", "karakoram_disk_total_bytes", "# HELP fake_work_total Completed work.", "# TYPE fake_work_total counter", `fake_work_total{kind="jobs",queue="primary"} 3`} {
		if !strings.Contains(w.Body.String(), text) {
			t.Fatal(text, w.Body.String())
		}
	}
	w = requestNode(a, "GET", "/v1/status", "", "token")
	var status agentapi.Status
	if err := json.Unmarshal(w.Body.Bytes(), &status); err != nil {
		t.Fatal(err)
	}
	if status.EngineVersion != "1.0" || status.Processes["worker"].PID == 0 || status.Disk.TotalBytes == 0 {
		t.Fatal(status)
	}
	time.Sleep(50 * time.Millisecond)
	if w := requestNode(a, "POST", "/v1/processes", `{"process":"worker","action":"stop"}`, "token"); w.Code != http.StatusOK {
		t.Fatal(w.Code, w.Body.String())
	}
}
func TestEngineConfigIdentityAndMismatch(t *testing.T) {
	a, c, _ := fakeNode(t, "")
	for _, change := range []func(*agentapi.MemberConfig){func(c *agentapi.MemberConfig) { c.Cluster = "other" }, func(c *agentapi.MemberConfig) { c.Member = "other" }, func(c *agentapi.MemberConfig) { c.Engine = "" }, func(c *agentapi.MemberConfig) { c.Engine = "psmdb" }} {
		bad := c
		change(&bad)
		if _, err := a.apply(context.Background(), bad); err == nil {
			t.Fatal("invalid config accepted", bad)
		}
	}
	psmdb := &nodeAgent{o: Options{Cluster: c.Cluster, Member: c.Member}, sup: newSupervisor(io.Discard)}
	if _, err := psmdb.apply(context.Background(), c); err == nil || !strings.Contains(err.Error(), "engine") {
		t.Fatal(err)
	}
}
func TestNodeFileConfinement(t *testing.T) {
	a, _, _ := fakeNode(t, "")
	n := &Node{a: a}
	for _, name := range []string{"../escape", "/escape", ""} {
		if err := n.WriteFile(name, []byte("x"), 0600); err == nil {
			t.Fatal(name)
		}
	}
	for _, mode := range []os.FileMode{0644, 0660, os.ModeSetuid | 0600} {
		if err := n.WriteFile("bad", nil, mode); err == nil {
			t.Fatal(mode)
		}
	}
	outside := t.TempDir()
	if err := os.MkdirAll(filepath.Join(a.o.DataDir, "karakoram"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(a.o.DataDir, "karakoram", "escape")); err != nil {
		t.Fatal(err)
	}
	if err := n.WriteFile("escape/key", []byte("secret"), 0600); err == nil {
		t.Fatal("symlink escaped root")
	}
}
func TestEngineReadinessAndSelection(t *testing.T) {
	e := &fakeEngine{readinessErr: errors.New("not ready")}
	args := []string{"component", "1234"}
	if err := Readiness(context.Background(), Options{Engine: "fake"}, args, e); !errors.Is(err, e.readinessErr) || !slices.Equal(e.args, args) {
		t.Fatal(err, e.args)
	}
	for _, name := range []string{"", "psmdb"} {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := Readiness(ctx, Options{Engine: name}, args, e)
		var networkError *net.OpError
		if !errors.As(err, &networkError) {
			t.Fatal(name, err)
		}
	}
	if err := Run(context.Background(), Options{Engine: "unknown"}, e); err == nil || !strings.Contains(err.Error(), "unknown KARAKORAM_ENGINE") {
		t.Fatal(err)
	}
	if err := Readiness(context.Background(), Options{Engine: "unknown"}, nil, e); err == nil {
		t.Fatal("unknown readiness engine")
	}
}
func TestEngineMetricsDeadline(t *testing.T) {
	a, _, e := fakeNode(t, "")
	e.metricWait = true
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	values := a.collectEngineMetrics(ctx)
	if len(values) == 0 || ctx.Err() == nil {
		t.Fatal(values, ctx.Err())
	}
}

func TestEngineRunTLSReloadAndCachedStartup(t *testing.T) {
	binary := buildFake(t)
	for _, cached := range []bool{false, true} {
		name := "first-pull"
		if cached {
			name = "offline-cache"
		}
		t.Run(name, func(t *testing.T) {
			c := memberConfig(t)
			c.Engine, c.Maintenance, c.DesiredProcesses = "fake", false, []string{"worker"}
			c.EngineSpec = json.RawMessage(`{"port":1234}`)
			c.ConfigVersion = 1
			var mu sync.Mutex
			control := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if !agentapi.AuthenticateBearer(r, "token") || r.URL.Path != "/internal/agent/v1/config" {
					w.WriteHeader(http.StatusUnauthorized)
					return
				}
				if cached {
					w.WriteHeader(http.StatusServiceUnavailable)
					return
				}
				mu.Lock()
				defer mu.Unlock()
				_ = json.NewEncoder(w).Encode(c)
			}))
			defer control.Close()
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			addr := listener.Addr().String()
			_, port, _ := net.SplitHostPort(addr)
			if err := listener.Close(); err != nil {
				t.Fatal(err)
			}
			o := Options{Engine: "fake", Cluster: c.Cluster, Member: c.Member, Token: "token", DataDir: t.TempDir(), Listen: addr, ControlPlaneURL: control.URL, InternalCAPEM: string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: control.Certificate().Raw})), Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
			if cached {
				if err := cacheConfig(c, o.DataDir); err != nil {
					t.Fatal(err)
				}
			}
			client, err := agentapi.NewClient(agentapi.ClientOptions{BaseURL: "https://member:" + port, Token: o.Token, CAPEM: []byte(c.AgentTLS.CertPEM), Timeout: time.Second, DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, network, addr)
			}})
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- Run(ctx, o, &fakeEngine{binary: binary, version: "1.0"}) }()
			t.Cleanup(func() {
				cancel()
				select {
				case err := <-done:
					if err != nil {
						t.Error(err)
					}
				case <-time.After(5 * time.Second):
					t.Error("agent did not shut down")
				}
			})
			var status agentapi.Status
			deadline := time.Now().Add(5 * time.Second)
			for {
				status, err = client.Status(context.Background())
				if err == nil {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("TLS status unavailable", err)
				}
				time.Sleep(10 * time.Millisecond)
			}
			if status.EngineVersion != "1.0" || status.Processes["worker"].PID == 0 {
				t.Fatal(status)
			}
			if err := client.Engine(context.Background(), "fake", "ping", struct{}{}, &struct {
				OK bool `json:"ok"`
			}{}); err != nil {
				t.Fatal(err)
			}
			if !cached {
				mu.Lock()
				c.ConfigVersion = 2
				mu.Unlock()
				time.Sleep(50 * time.Millisecond)
				if err := client.Reload(context.Background()); err != nil {
					t.Fatal(err)
				}
				status, err = client.Status(context.Background())
				if err != nil || status.ConfigVersion != 2 {
					t.Fatal(status, err)
				}
			}
			if err := client.Maintenance(context.Background(), agentapi.Maintenance{Enabled: true}); err != nil {
				t.Fatal(err)
			}
			status, err = client.Status(context.Background())
			if err != nil || !status.Maintenance {
				t.Fatal(status, err)
			}
		})
	}
}

func TestNodeActionSerializesSafetyHolds(t *testing.T) {
	binary := buildFake(t)
	for _, hold := range []string{"stop", "maintenance"} {
		t.Run(hold, func(t *testing.T) {
			a, c, _ := fakeNode(t, binary)
			a.c = c
			n := &Node{a: a}
			if err := n.Configure("worker", ProcessSpec{Binary: binary}); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			// Pause a generic control transition while a route action arrives.
			a.applyMu.Lock()
			entered := make(chan struct{})
			done := make(chan error, 1)
			go func() { close(entered); done <- n.Action(ctx, "worker", "start") }()
			<-entered
			select {
			case err := <-done:
				a.applyMu.Unlock()
				t.Fatalf("route action bypassed generic control serialization: %v", err)
			case <-time.After(30 * time.Millisecond):
			}
			controls := localControls{Stopped: map[string]bool{}}
			if hold == "stop" {
				controls.Stopped["worker"] = true
			} else {
				controls.Maintenance = true
			}
			err := a.saveControls(controls)
			if err == nil {
				a.sup.setMaintenance(controls.Maintenance)
				err = a.sup.action(ctx, "worker", "stop")
			}
			a.applyMu.Unlock()
			if err != nil {
				t.Fatal(err)
			}
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			if workerState(a).PID != 0 {
				t.Fatal("route start overrode control hold")
			}
			// Exercise the actual generic control and route paths concurrently.
			errs := make(chan error, 16)
			for range 8 {
				go func() { errs <- n.Action(ctx, "worker", "restart") }()
				go func() {
					if hold == "stop" {
						errs <- a.processAction(ctx, "worker", "stop")
					} else {
						errs <- a.maintenance(true)
					}
				}()
			}
			for range 16 {
				if err := <-errs; err != nil {
					t.Fatal(err)
				}
			}
			if workerState(a).PID != 0 {
				t.Fatal("concurrent route action overrode hold")
			}
		})
	}
}

func TestEngineCacheFailureDoesNotPublishConfig(t *testing.T) {
	a, c, e := fakeNode(t, "")
	c.Maintenance = true
	c.ConfigVersion = 1
	if _, err := a.apply(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	previousCert := a.cert
	e.version = "2.0"
	c.ConfigVersion = 2
	e.onApply = func(n *Node, desired agentapi.MemberConfig) error {
		if n.Config().ConfigVersion != desired.ConfigVersion {
			t.Error("Apply toolkit lacks pending configuration")
		}
		var status agentapi.Status
		response := requestNode(a, "GET", "/v1/status", "", "token")
		if err := json.Unmarshal(response.Body.Bytes(), &status); err != nil {
			return err
		}
		if status.ConfigVersion != 1 || status.EngineVersion != "1.0" {
			t.Error("pending configuration became visible", status)
		}
		return nil
	}
	path := filepath.Join(a.o.DataDir, "karakoram", "config.json")
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := a.apply(context.Background(), c); err == nil {
		t.Fatal("cache failure accepted")
	}
	if a.config().ConfigVersion != 1 || a.cert != previousCert || a.version != "1.0" {
		t.Fatal("cache failure published applied state", a.config(), a.version)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if _, err := a.apply(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	if a.config().ConfigVersion != 2 || a.version != "2.0" {
		t.Fatal("successful cache did not publish new state")
	}
}

func TestNodeWithLockSerializesWholeCommand(t *testing.T) {
	a, c, _ := fakeNode(t, "")
	c.Maintenance = true
	c.ConfigVersion = 1
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := a.apply(ctx, c); err != nil {
		t.Fatal(err)
	}
	n := &Node{a: a}
	entered, release := make(chan struct{}), make(chan struct{})
	command := make(chan error, 1)
	go func() {
		command <- n.WithLock(ctx, func(locked *Node) error {
			if locked.Config().ConfigVersion != 1 {
				return errors.New("wrong locked configuration")
			}
			if err := locked.Action(ctx, "worker", "stop"); err != nil {
				return err
			}
			if err := locked.WithLock(ctx, func(nested *Node) error {
				return nested.Action(ctx, "worker", "start")
			}); err != nil {
				return err
			}
			close(entered)
			<-release
			return nil
		})
	}()
	select {
	case <-entered:
	case err := <-command:
		t.Fatal("nested locked action failed", err)
	case <-ctx.Done():
		t.Fatal("nested locked action deadlocked")
	}
	applied, controlled := make(chan error, 1), make(chan error, 1)
	c.ConfigVersion = 2
	go func() { _, err := a.apply(ctx, c); applied <- err }()
	go func() { controlled <- a.maintenance(true) }()
	select {
	case err := <-applied:
		t.Errorf("Apply bypassed command lock: %v", err)
		applied <- err
	case <-time.After(30 * time.Millisecond):
	}
	select {
	case err := <-controlled:
		t.Errorf("generic control bypassed command lock: %v", err)
		controlled <- err
	case <-time.After(30 * time.Millisecond):
	}
	close(release)
	for _, done := range []chan error{command, applied, controlled} {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	if a.config().ConfigVersion != 2 {
		t.Fatal("Apply did not resume")
	}
	sentinel := errors.New("command failed")
	if err := n.WithLock(ctx, func(*Node) error { return sentinel }); !errors.Is(err, sentinel) {
		t.Fatal("callback error not propagated", err)
	}
	if err := n.WithLock(ctx, func(*Node) error { return nil }); err != nil {
		t.Fatal("callback error retained lock", err)
	}
}

func TestNodeWithLockCancelsWaiting(t *testing.T) {
	a, _, _ := fakeNode(t, "")
	n := &Node{a: a}
	a.applyMu.Lock()
	defer a.applyMu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	called := false
	err := n.WithLock(ctx, func(*Node) error { called = true; return nil })
	if !errors.Is(err, context.DeadlineExceeded) || called {
		t.Fatal(err, called)
	}
	if err := n.Action(ctx, "worker", "start"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("cancelled action waited for control lock", err)
	}
}

func TestEngineProcessesDoNotInheritAgentEnvironment(t *testing.T) {
	t.Setenv("KARAKORAM_AGENT_TOKEN", "member-secret")
	s := newSupervisor(io.Discard)
	s.configure("env", processSpec{binary: "/usr/bin/env", env: []string{"ENGINE_VAR=1"}, cleanEnv: true})
	if err := s.action(context.Background(), "env", "start"); err != nil {
		t.Fatal(err)
	}
	var out string
	eventuallyTrue(t, func() bool {
		lines := s.logTail("env", 100)
		out = ""
		for _, l := range lines {
			out += l.Message + "\n"
		}
		return strings.Contains(out, "ENGINE_VAR=1")
	})
	if strings.Contains(out, "member-secret") || strings.Contains(out, "KARAKORAM_AGENT_TOKEN") || !strings.Contains(out, "PATH=") {
		t.Fatalf("engine child environment: %q", out)
	}
	_ = s.shutdown(context.Background())
}

func eventuallyTrue(t *testing.T, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatal("condition not reached")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestNodeStopDurableHold(t *testing.T) {
	a, c, e := fakeNode(t, buildFake(t))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := a.apply(ctx, c); err != nil {
		t.Fatal(err)
	}
	n := &Node{a: a}
	if err := n.WithLock(ctx, func(locked *Node) error { return locked.Stop(ctx, "worker") }); err != nil {
		t.Fatal(err)
	}
	if !n.Stopped("worker") || workerState(a).PID != 0 {
		t.Fatal("engine stop did not hold and stop process")
	}
	if _, err := a.apply(ctx, c); err != nil {
		t.Fatal(err)
	}
	if workerState(a).PID != 0 {
		t.Fatal("configuration pull released hold")
	}
	controls, err := loadControls(a.o.DataDir)
	if err != nil || !controls.Stopped["worker"] {
		t.Fatal("stop hold was not persisted", err)
	}
	restarted, _, _ := fakeNode(t, e.binary)
	restarted.o.DataDir, restarted.controls = a.o.DataDir, controls
	if _, err := restarted.apply(ctx, c); err != nil {
		t.Fatal(err)
	}
	if workerState(restarted).PID != 0 {
		t.Fatal("cached configuration restart released hold")
	}
	if err := restarted.processAction(ctx, "worker", "start"); err != nil {
		t.Fatal(err)
	}
	controls, err = loadControls(a.o.DataDir)
	if err != nil || controls.Stopped["worker"] || workerState(restarted).PID == 0 {
		t.Fatal("restore's explicit start did not release durable hold", err)
	}
}
