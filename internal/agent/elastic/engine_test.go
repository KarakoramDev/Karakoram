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
}

type observedEngine struct {
	*Engine
	results chan adapterResult
}

func (e *observedEngine) Apply(ctx context.Context, n *agent.Node, c agentapi.MemberConfig) ([]string, error) {
	changes, err := e.Engine.Apply(ctx, n, c)
	states, _ := n.Snapshot()
	e.results <- adapterResult{changes: changes, states: states, err: err}
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
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
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
		if r.URL.Path != "/api/status" {
			t.Errorf("unexpected readiness path %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	port := strconv.Itoa(server.Listener.Addr().(*net.TCPAddr).Port)
	for _, component := range NewEngine().Processes() {
		if err := agent.Readiness(context.Background(), agent.Options{Engine: agentspec.Engine}, []string{component, port}, NewEngine()); err != nil {
			t.Fatal(component, err)
		}
	}
	for _, args := range [][]string{nil, {"other"}, {"kibana", "invalid"}, {"logstash", "0"}, {"apm-server", port, "extra"}} {
		if err := agent.Readiness(context.Background(), agent.Options{Engine: agentspec.Engine}, args, NewEngine()); err == nil {
			t.Fatal("accepted readiness args", args)
		}
	}
}
