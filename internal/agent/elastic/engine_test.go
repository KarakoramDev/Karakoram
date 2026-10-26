// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agent"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine/elastic/agentspec"
)

type adapterResult struct {
	changes []string
	states  map[string]agentapi.ProcessStatus
	err     error
	node    *agent.Node
}

type observedEngine struct {
	*Engine
	results chan adapterResult
}

func (e *observedEngine) Apply(ctx context.Context, n *agent.Node, c agentapi.MemberConfig) ([]string, error) {
	changes, err := e.Engine.Apply(ctx, n, c)
	states, _ := n.Snapshot()
	e.results <- adapterResult{changes: changes, states: states, err: err, node: n}
	return changes, err
}

func runAdapter(t *testing.T, e *Engine, dir string, spec agentspec.Spec, desired []string) adapterResult {
	t.Helper()
	_, cert := testCertificates(t, "localhost")
	payload, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	c := agentapi.MemberConfig{Engine: agentspec.Engine, EngineSpec: payload, Cluster: "test", Member: "testn0", DesiredProcesses: desired, AgentTLS: agentapi.Certificate{CertPEM: cert.CertPEM, KeyPEM: cert.KeyPEM}}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/internal/agent/v1/config" || r.Header.Get("Authorization") != "Bearer token" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(c); err != nil {
			t.Error(err)
		}
	}))
	t.Cleanup(server.Close)
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	o := agent.Options{Engine: agentspec.Engine, ControlPlaneURL: server.URL, InternalCAPEM: string(ca), Cluster: c.Cluster, Member: c.Member, Token: "token", DataDir: dir, Listen: "127.0.0.1:0", Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	observed := &observedEngine{Engine: e, results: make(chan adapterResult, 1)}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	done := make(chan error, 1)
	go func() { done <- agent.Run(ctx, o, observed) }()
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
	select {
	case result := <-observed.results:
		return result
	case err := <-done:
		done <- err
		t.Fatalf("agent exited before apply: %v", err)
	case <-ctx.Done():
		t.Fatal("agent did not apply configuration")
	}
	return adapterResult{}
}

func TestAdapterApplyRealNode(t *testing.T) {
	t.Setenv("KARAKORAM_AGENT_TOKEN", "must-not-reach-product")
	t.Setenv("JAVA_TOOL_OPTIONS", "must-not-reach-java")
	t.Setenv("NODE_OPTIONS", "must-not-reach-node")
	for _, component := range NewEngine().Processes() {
		t.Run(component, func(t *testing.T) {
			r, _ := fixture(t)
			e := NewEngine()
			e.runner = r
			s := esSpec()
			s.Component = component
			s.Kibana = &agentspec.KibanaSpec{ESHosts: []string{"https://testn0:9200"}, ServiceAccountToken: "token", SecurityKey: "security-key", SavedObjectsKey: "saved-objects-key", ReportingKey: "reporting-key"}
			s.Logstash = &agentspec.LogstashSpec{ESHosts: []string{"https://testn0:9200"}, Writer: agentspec.Credentials{Username: "writer", Password: "password"}}
			s.APM = &agentspec.APMSpec{ESHosts: []string{"https://testn0:9200"}, SecretToken: "token"}
			dir := t.TempDir()
			capture := filepath.Join(dir, "process.txt")
			binary := filepath.Join(r.home(s), "bin", component)
			if component == agentspec.ComponentAPM {
				binary = filepath.Join(r.home(s), component)
			}
			if err := os.MkdirAll(filepath.Dir(binary), 0700); err != nil {
				t.Fatal(err)
			}
			script := "#!/bin/sh\nprintf '%s\\n' \"$HOME\" \"$TMPDIR\" \"$ES_PATH_CONF\" \"$KBN_PATH_CONF\" \"$KARAKORAM_AGENT_TOKEN\" \"$JAVA_TOOL_OPTIONS\" \"$NODE_OPTIONS\" \"$@\" > '" + capture + "'\nexec sleep 30\n"
			if err := os.WriteFile(binary, []byte(script), 0700); err != nil {
				t.Fatal(err)
			}
			result := runAdapter(t, e, dir, s, []string{component})
			if result.err != nil {
				t.Fatal(result.err)
			}
			if result.states[component].State != "running" || result.states[component].PID == 0 {
				t.Fatal(result.states)
			}
			if len(result.changes) == 0 || e.Version(context.Background()) != s.Version {
				t.Fatal(result.changes, e.Version(context.Background()))
			}
			conf := filepath.Join(dir, "karakoram", "config")
			name := map[string]string{"elasticsearch": "elasticsearch.yml", "kibana": "kibana.yml", "logstash": "logstash.yml", "apm-server": "apm-server.yml"}[component]
			for _, path := range []string{filepath.Join(conf, name), filepath.Join(conf, "certs", "ca.crt"), filepath.Join(dir, "karakoram", "elastic-applied.json")} {
				info, err := os.Stat(path)
				if err != nil {
					t.Fatal(err)
				}
				if info.Mode().Perm() != 0600 {
					t.Fatalf("file mode %o", info.Mode().Perm())
				}
			}
			for _, name := range []string{"data", "logs", "tmp", "home"} {
				info, err := os.Stat(filepath.Join(dir, "karakoram", name))
				if err != nil {
					t.Fatal(err)
				}
				if info.Mode().Perm() != 0700 {
					t.Fatalf("directory mode %o", info.Mode().Perm())
				}
			}
			if _, err := os.Stat(filepath.Join(dir, "config")); !os.IsNotExist(err) {
				t.Fatal("configuration outside protected root", err)
			}
			deadline := time.Now().Add(3 * time.Second)
			for {
				data, err := os.ReadFile(capture)
				if err == nil && strings.Contains(string(data), conf) {
					text := string(data)
					if !strings.Contains(text, filepath.Join(dir, "karakoram", "home")) || !strings.Contains(text, filepath.Join(dir, "karakoram", "tmp")) {
						t.Fatal("missing managed home or temporary directory", text)
					}
					if strings.Contains(text, "must-not-reach-") {
						t.Fatal("product inherited agent environment")
					}
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("process configuration path not rooted: %s: %v", data, err)
				}
				time.Sleep(10 * time.Millisecond)
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			metrics := e.Metrics(ctx, nil)
			if len(metrics) != 1 || metrics[0].Name != metricPrefix(component)+"up" || metrics[0].Value != 0 || metrics[0].Help == "" || metrics[0].Counter {
				t.Fatal("metrics lost applied component or sample fields", metrics)
			}
		})
	}
}

func TestAdapterRejectsInvalidPayload(t *testing.T) {
	e := NewEngine()
	if e.Name() != agentspec.Engine || !slices.Contains(e.Processes(), "apm-server") {
		t.Fatal(e.Name(), e.Processes())
	}
	for _, c := range []agentapi.MemberConfig{
		{Engine: "psmdb"}, {Engine: ""}, {Engine: agentspec.Engine},
		{Engine: agentspec.Engine, EngineSpec: json.RawMessage(`null`)},
		{Engine: agentspec.Engine, EngineSpec: json.RawMessage(`{"component":`)},
		{Engine: agentspec.Engine, EngineSpec: json.RawMessage(`{"component":"elasticsearch","version":"../../other","caPEM":"ca","restartToken":"one"}`)},
		{Engine: agentspec.Engine, EngineSpec: json.RawMessage(`{"component":"elasticsearch","version":"9.5.5","caPEM":"ca","restartToken":"one"}`)},
		{Engine: agentspec.Engine, EngineSpec: json.RawMessage(`{"component":"other","version":"9.5.5","caPEM":"ca","restartToken":"one"}`)},
	} {
		if _, err := e.Apply(context.Background(), nil, c); err == nil {
			t.Fatal("accepted invalid payload", c.Engine)
		}
	}
	if e.Version(context.Background()) != "" || e.Metrics(context.Background(), nil) != nil {
		t.Fatal("unapplied engine reports observations")
	}
}

func TestAdapterExplicitEmptyProcessIntent(t *testing.T) {
	r, _ := fixture(t)
	e := NewEngine()
	e.runner = r
	result := runAdapter(t, e, t.TempDir(), esSpec(), []string{})
	if result.err != nil {
		t.Fatal(result.err)
	}
	state := result.states[agentspec.ComponentElasticsearch]
	if state.State != "stopped" || state.PID != 0 {
		t.Fatal("empty intent started configured process", state)
	}
}

func TestAdapterCachedVersionFetchesInstalledVersion(t *testing.T) {
	r, _ := fixture(t)
	installed := r.Home
	missing := filepath.Join(t.TempDir(), "elasticsearch-9.5.4")
	r.Home = func(component, version string) string {
		if version == "9.5.4" {
			return missing
		}
		return installed(component, version)
	}
	e := NewEngine()
	e.runner = r
	dir := t.TempDir()
	old := esSpec()
	old.Version, old.RestartToken = "9.5.4", "old"
	payload, err := json.Marshal(old)
	if err != nil {
		t.Fatal(err)
	}
	_, cert := testCertificates(t, "localhost")
	cached := agentapi.MemberConfig{Engine: agentspec.Engine, EngineSpec: payload, Cluster: "test", Member: "testn0", DesiredProcesses: []string{old.Component}, AgentTLS: agentapi.Certificate{CertPEM: cert.CertPEM, KeyPEM: cert.KeyPEM}}
	data, err := json.Marshal(cached)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := atomicWrite(filepath.Join(dir, "karakoram", "config.json"), data, 0600); err != nil {
		t.Fatal(err)
	}
	fresh := esSpec()
	binary := filepath.Join(r.home(fresh), "bin", fresh.Component)
	if err := os.MkdirAll(filepath.Dir(binary), 0700); err != nil {
		t.Fatal(err)
	}
	capture := filepath.Join(dir, "started")
	if err := os.WriteFile(binary, []byte("#!/bin/sh\ntouch '"+capture+"'\nexec sleep 30\n"), 0700); err != nil {
		t.Fatal(err)
	}
	result := runAdapter(t, e, dir, fresh, []string{fresh.Component})
	if result.err != nil || len(result.changes) != 0 || len(result.states) != 0 {
		t.Fatal("cached missing version was not deferred", result)
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		_, err := os.Stat(capture)
		if err == nil && e.Version(context.Background()) == fresh.Version {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("agent did not fetch and start installed version", err, e.Version(context.Background()))
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := os.Stat(missing); !os.IsNotExist(err) {
		t.Fatal("fixture unexpectedly installed cached version", err)
	}
}

func TestAdapterProtectedFilesRejectSymlinkEscape(t *testing.T) {
	r, _ := fixture(t)
	e := NewEngine()
	e.runner = r
	dir, outside := t.TempDir(), t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "karakoram"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(dir, "karakoram", "config")); err != nil {
		t.Fatal(err)
	}
	result := runAdapter(t, e, dir, esSpec(), []string{})
	if result.err == nil {
		t.Fatal("accepted config symlink escape")
	}
	entries, err := os.ReadDir(outside)
	if err != nil || len(entries) != 0 {
		t.Fatal("wrote outside protected root", entries, err)
	}
}

func TestAdapterReadinessDispatch(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/status":
			w.WriteHeader(http.StatusOK)
		case "/_node/pipelines":
			_, _ = io.WriteString(w, `{"pipelines":{"main":{}}}`)
		case "/_node/stats/pipelines":
			_, _ = io.WriteString(w, `{"pipelines":{"main":{"events":{}}}}`)
		default:
			t.Errorf("unexpected readiness path %s", r.URL.Path)
		}
	}))
	defer server.Close()
	port := strconv.Itoa(server.Listener.Addr().(*net.TCPAddr).Port)
	dir := t.TempDir()
	if _, err := atomicWrite(filepath.Join(dir, "karakoram", "config", "pipelines.yml"), []byte("- pipeline.id: main\n"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, component := range NewEngine().Processes() {
		if err := agent.Readiness(context.Background(), agent.Options{Engine: agentspec.Engine, DataDir: dir}, []string{component, port}, NewEngine()); err != nil {
			t.Fatal(component, err)
		}
	}
	for _, args := range [][]string{nil, {"other"}, {"kibana", "invalid"}, {"logstash", "0"}, {"apm-server", port, "extra"}} {
		if err := agent.Readiness(context.Background(), agent.Options{Engine: agentspec.Engine}, args, NewEngine()); err == nil {
			t.Fatal("accepted readiness args", args)
		}
	}
}

func TestReadinessRouteDoesNotTreatStoppedProcessAsReady(t *testing.T) {
	r, _ := fixture(t)
	e := NewEngine()
	e.runner = r
	spec := esSpec()
	spec.Component = agentspec.ComponentLogstash
	spec.Logstash = &agentspec.LogstashSpec{ESHosts: []string{"https://testn0:9200"}}
	binary := filepath.Join(r.home(spec), "bin", spec.Component)
	if err := os.MkdirAll(filepath.Dir(binary), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(binary, []byte("#!/bin/sh\nexec sleep 30\n"), 0700); err != nil {
		t.Fatal(err)
	}
	result := runAdapter(t, e, t.TempDir(), spec, []string{})
	if result.err != nil {
		t.Fatal(result.err)
	}
	mux := http.NewServeMux()
	e.Routes(mux, result.node)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/v1/elasticsearch/readiness", nil))
	var observation agentspec.Readiness
	if err := json.Unmarshal(w.Body.Bytes(), &observation); err != nil {
		t.Fatal(err)
	}
	if w.Code != 200 || observation.Ready || observation.Component != spec.Component || observation.Message != "Component process is not running" {
		t.Fatalf("unexpected readiness: %d %+v", w.Code, observation)
	}
}

func TestConfigurationLogCutoffFollowsRunnerApply(t *testing.T) {
	r, _ := fixture(t)
	keystore := r.KeystoreCommand
	var runnerReachedAt time.Time
	r.KeystoreCommand = func(ctx context.Context, binary string, args, env []string, stdin io.Reader) error {
		runnerReachedAt = time.Now()
		return keystore(ctx, binary, args, env, stdin)
	}
	e := NewEngine()
	e.runner = r
	result := runAdapter(t, e, t.TempDir(), esSpec(), []string{})
	if result.err != nil {
		t.Fatal(result.err)
	}
	e.mu.RLock()
	cutoff := e.configAppliedAt
	e.mu.RUnlock()
	if runnerReachedAt.IsZero() || cutoff.Before(runnerReachedAt) {
		t.Fatalf("log cutoff %s preceded runner apply %s", cutoff, runnerReachedAt)
	}
}

func TestKibanaRefusedListenerSamplesFreshPressureWithinRequest(t *testing.T) {
	r, _ := fixture(t)
	e := NewEngine()
	e.runner = r
	spec := esSpec()
	spec.Component = agentspec.ComponentKibana
	spec.Kibana = &agentspec.KibanaSpec{ESHosts: []string{"https://testn0:9200"}, ServiceAccountToken: "token", SecurityKey: "security-key", SavedObjectsKey: "saved-objects-key", ReportingKey: "reporting-key"}
	binary := filepath.Join(r.home(spec), "bin", spec.Component)
	if err := os.MkdirAll(filepath.Dir(binary), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(binary, []byte("#!/bin/sh\nexec sleep 30\n"), 0700); err != nil {
		t.Fatal(err)
	}
	result := runAdapter(t, e, t.TempDir(), spec, []string{spec.Component})
	if result.err != nil {
		t.Fatal(result.err)
	}
	events := 30117
	growing := true
	eventReads := 0
	e.memoryPressure.readFile = func(path string) ([]byte, error) {
		switch path {
		case "/proc/self/cgroup":
			return []byte("0::/nomad.slice/share.slice/alloc.member.scope\n"), nil
		case "/proc/self/mountinfo":
			return []byte("29 23 0:26 / /sys/fs/cgroup rw - cgroup2 cgroup rw\n"), nil
		}
		switch filepath.Base(path) {
		case "memory.events":
			eventReads++
			if growing {
				events++
			}
			return []byte("max " + strconv.Itoa(events) + "\noom 0\noom_kill 0\n"), nil
		case "memory.pressure":
			return []byte("some avg10=0.00 avg60=0.00 total=1693514\n"), nil
		case "memory.current":
			return []byte("316526592"), nil
		case "memory.max":
			return []byte("1073741824"), nil
		}
		return nil, os.ErrNotExist
	}
	mux := http.NewServeMux()
	e.Routes(mux, result.node)
	probe := func() agentspec.Readiness {
		t.Helper()
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/v1/elasticsearch/readiness", nil))
		var observation agentspec.Readiness
		if err := json.Unmarshal(w.Body.Bytes(), &observation); err != nil {
			t.Fatal(err)
		}
		return observation
	}
	for range 2 {
		// A first RPC, or an RPC following a long gap, has no fresh baseline.
		e.memoryPressure.mu.Lock()
		e.memoryPressure.sampleAt = time.Now().Add(-time.Minute)
		e.memoryPressure.eventAt = time.Time{}
		e.memoryPressure.mu.Unlock()
		observation := probe()
		if observation.Ready || !observation.MemoryPressure ||
			!strings.Contains(observation.Message, "listener unavailable") ||
			!strings.Contains(observation.Message, "max increased") ||
			!strings.Contains(observation.Message, "minRam and maxRam") {
			t.Fatalf("refused listener with fresh max growth lost actionable pressure: %+v", observation)
		}
	}
	growing = false
	e.memoryPressure.mu.Lock()
	e.memoryPressure.sampleAt = time.Now().Add(-time.Minute)
	e.memoryPressure.eventAt = time.Time{}
	e.memoryPressure.mu.Unlock()
	if observation := probe(); observation.Ready || observation.MemoryPressure {
		t.Fatalf("static historical max events asserted readiness or pressure: %+v", observation)
	}
	eventReads = 0
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	request := httptest.NewRequest(http.MethodPost, "/v1/elasticsearch/readiness", nil).WithContext(ctx)
	mux.ServeHTTP(httptest.NewRecorder(), request)
	if eventReads != 1 {
		t.Fatalf("cancelled request took a second sample: %d reads", eventReads)
	}
	scopedRead := e.memoryPressure.readFile
	e.memoryPressure.readFile = func(path string) ([]byte, error) {
		if path == "/proc/self/mountinfo" {
			// Nomad exec exposes /sys as sysfs, but no cgroup2 submount.
			return []byte("520 509 0:72 / /sys ro,nosuid,nodev,noexec - sysfs sysfs ro\n"), nil
		}
		return scopedRead(path)
	}
	if observation := probe(); observation.Ready || observation.MemoryPressure ||
		!strings.Contains(observation.Message, "memory pressure evidence unavailable in this runtime") {
		t.Fatalf("Nomad exec missing cgroup2 mount silently lost evidence: %+v", observation)
	}
}
