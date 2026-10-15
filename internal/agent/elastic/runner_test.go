// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine/elastic/agentspec"
	"gopkg.in/yaml.v3"
)

type fakeHost struct {
	dir                           string
	running, stopped, maintenance bool
	actions                       []string
	processes                     map[string]Process
	actionErr                     error
	desired                       bool
	writes                        int
	configures                    int
	logger                        *slog.Logger
}

func (h *fakeHost) DataDir() string { return h.dir }
func (h *fakeHost) WriteFile(rel string, data []byte, mode os.FileMode) error {
	h.writes++
	_, err := atomicWrite(filepath.Join(h.dir, "karakoram", rel), data, mode)
	return err
}
func (h *fakeHost) Configure(name string, p Process) error {
	h.configures++
	if h.processes == nil {
		h.processes = map[string]Process{}
	}
	h.processes[name] = p
	return nil
}
func (h *fakeHost) Action(_ context.Context, name, action string) error {
	if h.actionErr != nil {
		return h.actionErr
	}
	h.actions = append(h.actions, name+":"+action)
	h.running, h.desired = action != "stop", action != "stop"
	return nil
}
func (h *fakeHost) Running(string) bool { return h.running }
func (h *fakeHost) Stopped(string) bool { return h.stopped }
func (h *fakeHost) Maintenance() bool   { return h.maintenance }
func (h *fakeHost) Logger() *slog.Logger {
	if h.logger != nil {
		return h.logger
	}
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func fixture(t *testing.T) (*Runner, *fakeHost) {
	t.Helper()
	home := t.TempDir()
	for _, name := range []string{"jvm.options", "log4j2.properties"} {
		if _, err := atomicWrite(filepath.Join(home, "config", name), []byte("baseline "+name+"\n"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	r := New()
	r.Home = func(string, string) string { return home }
	r.KeystoreCommand = func(_ context.Context, _ string, args, env []string, stdin io.Reader) error {
		conf := strings.TrimPrefix(env[0], "ES_PATH_CONF=")
		for _, name := range []string{"HOME", "ES_TMPDIR", "TMPDIR"} {
			value := ""
			for _, entry := range env {
				if strings.HasPrefix(entry, name+"=") {
					value = strings.TrimPrefix(entry, name+"=")
				}
			}
			info, err := os.Stat(value)
			if err != nil || !info.IsDir() || info.Mode().Perm() != 0700 {
				t.Fatalf("keystore lacks private %s directory", name)
			}
		}
		if args[0] == "create" {
			return os.WriteFile(filepath.Join(conf, "elasticsearch.keystore"), []byte("keystore"), 0600)
		}
		if stdin != nil {
			_, err := io.ReadAll(stdin)
			return err
		}
		return nil
	}
	return r, &fakeHost{dir: t.TempDir()}
}
func esSpec() agentspec.Spec {
	return agentspec.Spec{Component: "elasticsearch", Version: "9.5.5", HeapMB: 768, CAPEM: "ca", RestartToken: "one", Elasticsearch: &agentspec.ElasticsearchSpec{
		ClusterName: "test", NodeName: "testn0", Roles: []string{"master", "data", "ingest"}, SeedHosts: []string{"testn0:9300", "testn1:9300"}, InitialMasterNodes: []string{"testn0", "testn1"},
		TransportCert: agentspec.Certificate{CertPEM: "transport-cert", KeyPEM: "transport-key"}, HTTPCert: agentspec.Certificate{CertPEM: "http-cert", KeyPEM: "http-key"},
		FileUsers: "monitor:hash\n", FileUsersRoles: "karakoram_monitor:monitor\n", FileRoles: "karakoram_monitor:\n  cluster: [monitor]\n",
		Settings: map[string]any{"s3.client.backup.endpoint": "https://example.com", "indices.recovery.max_bytes_per_sec": "50mb"},
	}}
}
func readText(t *testing.T, h *fakeHost, name string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(h.dir, "karakoram", name))
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}
func readYAML(t *testing.T, h *fakeHost, name string) map[string]any {
	t.Helper()
	var result map[string]any
	if err := yaml.Unmarshal([]byte(readText(t, h, "config/"+name)), &result); err != nil {
		t.Fatal(err)
	}
	return result
}
func apply(t *testing.T, r *Runner, h *fakeHost, s agentspec.Spec) []string {
	t.Helper()
	changes, err := r.Apply(context.Background(), h, s, "test", "testn0")
	if err != nil {
		t.Fatal(err)
	}
	return changes
}

func TestMissingProductHomeDefersApply(t *testing.T) {
	for _, component := range New().Processes() {
		for _, status := range []string{"unconfigured", "stopped", "running", "held", "maintenance"} {
			t.Run(component+"/"+status, func(t *testing.T) {
				r, h := fixture(t)
				installed := r.Home
				missing := filepath.Join(t.TempDir(), "old-version")
				r.Home = func(component, version string) string {
					if version == "9.5.4" {
						return missing
					}
					return installed(component, version)
				}
				s := esSpec()
				s.Component, s.Version = component, "9.5.4"
				s.Kibana = &agentspec.KibanaSpec{}
				s.Logstash = &agentspec.LogstashSpec{}
				s.APM = &agentspec.APMSpec{}
				if status != "unconfigured" {
					h.processes = map[string]Process{component: {Binary: filepath.Join(missing, "bin", component), Dir: missing}}
					h.desired = true
				}
				h.running = status == "running"
				h.stopped = status == "held"
				h.maintenance = status == "maintenance"
				old := `{"version":"9.5.4","restartToken":"old","applied":true}`
				if err := h.WriteFile("elastic-applied.json", []byte(old), 0600); err != nil {
					t.Fatal(err)
				}
				writes, configures := h.writes, h.configures
				var logs bytes.Buffer
				h.logger = slog.New(slog.NewTextHandler(&logs, nil))
				keystore := r.KeystoreCommand
				r.KeystoreCommand = func(context.Context, string, []string, []string, io.Reader) error {
					t.Fatal("missing home invoked keystore")
					return nil
				}
				if changes := apply(t, r, h, s); len(changes) != 0 {
					t.Fatal("missing home rendered configuration", changes)
				}
				if h.writes != writes || readText(t, h, "elastic-applied.json") != old || h.configures != configures {
					t.Fatal("missing home changed files or configured an unavailable binary")
				}
				if h.running || h.desired || !reflect.DeepEqual(h.actions, []string{component + ":stop"}) {
					t.Fatal("missing home left process desired", h.actions, h.running, h.desired)
				}
				if !strings.Contains(logs.String(), "level=WARN") || !strings.Contains(logs.String(), missing) {
					t.Fatal("missing home warning absent", logs.String())
				}
				r.KeystoreCommand = keystore
				h.stopped, h.maintenance = false, false
				s.Version, s.RestartToken = "9.5.5", "new"
				apply(t, r, h, s)
				if !h.running || !h.desired || h.actions[1] != component+":start" || h.processes[component].Dir != installed(component, s.Version) {
					t.Fatal("fresh configuration did not start installed version", h.actions, h.processes)
				}
			})
		}
	}
}

func TestElasticsearchConfig(t *testing.T) {
	r, h := fixture(t)
	s := esSpec()
	changes := apply(t, r, h, s)
	if len(changes) < 12 {
		t.Fatalf("changed files: %v", changes)
	}
	config := readYAML(t, h, "elasticsearch.yml")
	for key, want := range map[string]any{"cluster.name": "test", "node.name": "testn0", "network.host": "::", "network.publish_host": "testn0", "http.port": 9200, "transport.port": 9300, "discovery.seed_providers": "file", "xpack.security.enabled": true, "xpack.security.autoconfiguration.enabled": false, "xpack.security.transport.ssl.verification_mode": "certificate", "xpack.security.transport.ssl.client_authentication": "required", "xpack.security.http.ssl.enabled": true, "xpack.security.authc.realms.file.file1.order": 0, "xpack.security.authc.realms.native.native1.order": 1, "s3.client.backup.endpoint": "https://example.com"} {
		if !reflect.DeepEqual(config[key], want) {
			t.Errorf("%s=%v want %v", key, config[key], want)
		}
	}
	if !reflect.DeepEqual(config["cluster.initial_master_nodes"], []any{"testn0", "testn1"}) {
		t.Fatal(config)
	}
	if !reflect.DeepEqual(config["discovery.seed_hosts"], []any{}) {
		t.Fatal(config)
	}
	if got := readText(t, h, "config/unicast_hosts.txt"); got != "testn0:9300\ntestn1:9300\n" {
		t.Fatal(got)
	}
	if got := readText(t, h, "config/jvm.options.d/karakoram-heap.options"); got != "-Xms768m\n-Xmx768m\n" {
		t.Fatal(got)
	}
	if err := filepath.WalkDir(h.dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if path == h.dir {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		want := os.FileMode(0600)
		if d.IsDir() {
			want = 0700
		}
		if info.Mode().Perm() != want {
			t.Errorf("%s mode %o want %o", d.Name(), info.Mode().Perm(), want)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	p := h.processes["elasticsearch"]
	if p.Binary != filepath.Join(r.home(s), "bin", "elasticsearch") || !contains(p.Env, "ES_PATH_CONF="+filepath.Join(h.dir, "karakoram", "config")) || !contains(p.Env, "ES_TMPDIR="+filepath.Join(h.dir, "karakoram", "tmp")) {
		t.Fatal(p)
	}
	s.Elasticsearch.InitialMasterNodes = nil
	s.Elasticsearch.SeedHosts = nil
	apply(t, r, h, s)
	if _, ok := readYAML(t, h, "elasticsearch.yml")["cluster.initial_master_nodes"]; ok {
		t.Fatal("bootstrap key retained")
	}
	if readText(t, h, "config/unicast_hosts.txt") != "" {
		t.Fatal("seed list retained")
	}
	if len(h.actions) != 1 {
		t.Fatalf("hot updates restarted process: %v", h.actions)
	}
	if got := apply(t, r, h, s); len(got) != 0 {
		t.Fatalf("non-idempotent Apply: %v", got)
	}
}
func contains(values []string, want string) bool {
	for _, v := range values {
		if v == want {
			return true
		}
	}
	return false
}

func TestManagedOverrideRejected(t *testing.T) {
	for _, settings := range []map[string]any{
		{"node.name": "other"}, {"path.data": "other"}, {"network.bind_host": "other"}, {"cluster.initial_master_nodes": []string{"other"}}, {"xpack.security.enabled": false}, {"bootstrap.memory_lock": true}, {"discovery.seed_hosts": []string{"other"}},
		{"xpack": map[string]any{"security": map[string]any{"enabled": false}}}, {"network": map[string]any{"host": "other"}}, {"node": "other"},
	} {
		r, h := fixture(t)
		s := esSpec()
		s.Elasticsearch.Settings = settings
		if _, err := r.Apply(context.Background(), h, s, "test", "testn0"); err == nil {
			t.Fatalf("accepted %v", settings)
		}
		if len(h.actions) != 0 {
			t.Fatal("process action before validation")
		}
	}
}

func TestRestartTokensAndHolds(t *testing.T) {
	r, h := fixture(t)
	s := esSpec()
	apply(t, r, h, s)
	apply(t, r, h, s)
	s.Elasticsearch.FileUsers = "changed:hash"
	apply(t, r, h, s)
	if !reflect.DeepEqual(h.actions, []string{"elasticsearch:start"}) {
		t.Fatal(h.actions)
	}
	s.RestartToken = "two"
	apply(t, r, h, s)
	apply(t, r, h, s)
	if !reflect.DeepEqual(h.actions, []string{"elasticsearch:start", "elasticsearch:restart"}) {
		t.Fatal(h.actions)
	}
	// A new runner reads the durable applied token rather than restarting.
	next := New()
	next.Home = r.Home
	next.KeystoreCommand = r.KeystoreCommand
	apply(t, next, h, s)
	if len(h.actions) != 2 {
		t.Fatal(h.actions)
	}
	h.stopped = true
	s.RestartToken = "three"
	apply(t, next, h, s)
	if len(h.actions) != 2 {
		t.Fatal("stop hold bypassed")
	}
	h.stopped = false
	h.maintenance = true
	apply(t, next, h, s)
	if len(h.actions) != 2 {
		t.Fatal("maintenance hold bypassed")
	}
	h.maintenance = false
	apply(t, next, h, s)
	if h.actions[2] != "elasticsearch:restart" {
		t.Fatal(h.actions)
	}
	h.running = false
	apply(t, next, h, s)
	if h.actions[3] != "elasticsearch:start" {
		t.Fatal(h.actions)
	}
	h.actionErr = errors.New("action failed")
	s.RestartToken = "four"
	if _, err := next.Apply(context.Background(), h, s, "test", "testn0"); err == nil {
		t.Fatal("action failure swallowed")
	}
	h.actionErr = nil
	apply(t, next, h, s)
	if h.actions[4] != "elasticsearch:restart" {
		t.Fatal("failed action advanced applied token")
	}
}

func TestKeystoreDigestAndBaselines(t *testing.T) {
	r, h := fixture(t)
	s := esSpec()
	s.Elasticsearch.SecureSettings = map[string]string{"s3.client.one.secret_key": "secret", "s3.client.one.access_key": "access"}
	calls := 0
	original := r.KeystoreCommand
	r.KeystoreCommand = func(ctx context.Context, bin string, args, env []string, in io.Reader) error {
		calls++
		if !strings.HasSuffix(bin, "/bin/elasticsearch-keystore") {
			t.Fatal(bin)
		}
		if args[0] == "add" {
			if !reflect.DeepEqual(args[:3], []string{"add", "-x", "-f"}) {
				t.Fatal(args)
			}
			data, err := io.ReadAll(in)
			if err != nil {
				t.Fatal(err)
			}
			if string(data) != s.Elasticsearch.SecureSettings[args[3]]+"\n" {
				t.Fatal("incorrect secure input")
			}
		}
		return original(ctx, bin, args, env, nil)
	}
	apply(t, r, h, s)
	if calls != 3 {
		t.Fatal(calls)
	}
	apply(t, r, h, s)
	if calls != 3 {
		t.Fatal("unnecessary rebuild")
	}
	s.RestartToken = "two"
	apply(t, r, h, s)
	if calls != 3 {
		t.Fatal("token rebuilt keystore")
	}
	s.Elasticsearch.SecureSettings["s3.client.one.secret_key"] = "rotated"
	apply(t, r, h, s)
	if calls != 6 {
		t.Fatal("digest change not rebuilt")
	}
	if len(h.actions) != 2 {
		t.Fatal("secure setting restarted process")
	}
	delete(s.Elasticsearch.SecureSettings, "s3.client.one.access_key")
	apply(t, r, h, s)
	if calls != 8 {
		t.Fatal(calls)
	}
	if err := os.Remove(filepath.Join(h.dir, "karakoram", "config/elasticsearch.keystore")); err != nil {
		t.Fatal(err)
	}
	apply(t, r, h, s)
	if calls != 10 {
		t.Fatal("missing keystore not rebuilt")
	}
	baseline := filepath.Join(r.home(s), "config/jvm.options")
	if err := os.WriteFile(baseline, []byte("new baseline"), 0600); err != nil {
		t.Fatal(err)
	}
	apply(t, r, h, s)
	if readText(t, h, "config/jvm.options") == "new baseline" {
		t.Fatal("baseline copied without version change")
	}
	s.Version = "9.5.4"
	apply(t, r, h, s)
	if readText(t, h, "config/jvm.options") != "new baseline" {
		t.Fatal("version baseline not copied")
	}
	if err := os.Remove(filepath.Join(h.dir, "karakoram", "config/log4j2.properties")); err != nil {
		t.Fatal(err)
	}
	apply(t, r, h, s)
	if r.Version(context.Background(), s) != s.Version {
		t.Fatal("version missing")
	}
}

func TestComponentConfigs(t *testing.T) {
	tests := []struct {
		s    agentspec.Spec
		file string
		want map[string]any
	}{
		{agentspec.Spec{Component: "kibana", Version: "9.5.5", HeapMB: 900, CAPEM: "ca", Kibana: &agentspec.KibanaSpec{ESHosts: []string{"https://one:9200"}, ServiceAccountToken: "token", SecurityKey: "security", SavedObjectsKey: "saved", ReportingKey: "report", PublicBaseURL: "https://example.com", Settings: map[string]any{"server.basePath": "/kibana"}}}, "kibana.yml", map[string]any{"server.host": "::", "server.port": 5601, "server.publicBaseUrl": "https://example.com", "xpack.security.secureCookies": true, "elasticsearch.serviceAccountToken": "token", "elasticsearch.ssl.verificationMode": "full", "telemetry.optIn": false, "telemetry.allowChangingOptInStatus": false, "xpack.security.encryptionKey": "security", "xpack.encryptedSavedObjects.encryptionKey": "saved", "xpack.reporting.encryptionKey": "report", "server.basePath": "/kibana"}},
		{agentspec.Spec{Component: "logstash", Version: "9.5.5", HeapMB: 512, CAPEM: "ca", Logstash: &agentspec.LogstashSpec{ESHosts: []string{"https://one:9200", "https://two:9200"}, Writer: agentspec.Credentials{Username: "writer", Password: "password"}, PersistentQueue: true}}, "logstash.yml", map[string]any{"api.http.host": "127.0.0.1", "api.http.port": 9600, "queue.type": "persisted", "pipeline.ecs_compatibility": "v8"}},
		{agentspec.Spec{Component: "apm-server", Version: "9.5.5", CAPEM: "ca", APM: &agentspec.APMSpec{ESHosts: []string{"https://one:9200"}, Writer: agentspec.Credentials{Username: "apm", Password: "password"}, SecretToken: "secret", APIKeys: true}}, "apm-server.yml", map[string]any{"apm-server.host": "[::]:8200", "apm-server.auth.secret_token": "secret", "apm-server.auth.api_key.enabled": true, "output.elasticsearch.username": "apm", "output.elasticsearch.password": "password", "http.enabled": true, "http.host": "127.0.0.1", "http.port": 5066, "logging.to_stderr": true}},
	}
	for _, tt := range tests {
		t.Run(tt.s.Component, func(t *testing.T) {
			r, h := fixture(t)
			apply(t, r, h, tt.s)
			config := readYAML(t, h, tt.file)
			for key, want := range tt.want {
				if !reflect.DeepEqual(config[key], want) {
					t.Errorf("%s=%v want %v", key, config[key], want)
				}
			}
			p := h.processes[tt.s.Component]
			switch tt.s.Component {
			case "kibana":
				if readText(t, h, "config/node.options") != "--max-old-space-size=900\n" || !contains(p.Env, "KBN_PATH_CONF="+filepath.Join(h.dir, "karakoram", "config")) {
					t.Fatal(p)
				}
				tt.s.Kibana.PublicBaseURL = ""
				apply(t, r, h, tt.s)
				if _, ok := readYAML(t, h, tt.file)["server.publicBaseUrl"]; ok {
					t.Fatal("public URL retained")
				}
				tt.s.Kibana.Settings = map[string]any{"elasticsearch.hosts": []string{"http://bad"}}
				if _, err := r.Apply(context.Background(), h, tt.s, "test", "testn0"); err == nil {
					t.Fatal("Kibana managed override accepted")
				}
				tt.s.Kibana.Settings = nil
			case "logstash":
				pipeline := readText(t, h, "config/pipelines/main.conf")
				for _, text := range []string{`beats { host => "::" port => 5044 }`, `http { host => "::" port => 8080 }`, `hosts => ["https://one:9200", "https://two:9200"]`, `password => "${KARAKORAM_ES_PASSWORD}"`, `ssl_enabled => true`, `data_stream => true`} {
					if !strings.Contains(pipeline, text) {
						t.Errorf("missing %s", text)
					}
				}
				if strings.Contains(pipeline, "password => \"password\"") {
					t.Fatal("password embedded")
				}
				if !contains(p.Env, "KARAKORAM_ES_HOSTS=https://one:9200 https://two:9200") || !contains(p.Env, "LS_JAVA_OPTS=-Xms512m -Xmx512m") {
					t.Fatal(p)
				}
				tt.s.Logstash.Pipelines = []agentspec.Pipeline{{ID: "custom", Config: "input {} output {}"}}
				tt.s.Logstash.PersistentQueue = false
				apply(t, r, h, tt.s)
				if readText(t, h, "config/pipelines/custom.conf") != "input {} output {}" {
					t.Fatal("custom pipeline missing")
				}
				var entries []map[string]any
				if err := yaml.Unmarshal([]byte(readText(t, h, "config/pipelines.yml")), &entries); err != nil {
					t.Fatal(err)
				}
				if len(entries) != 1 || entries[0]["pipeline.id"] != "custom" || entries[0]["path.config"] != filepath.Join(h.dir, "karakoram", "config/pipelines/custom.conf") {
					t.Fatal(entries)
				}
				if readYAML(t, h, tt.file)["queue.type"] != "memory" {
					t.Fatal("queue not updated")
				}
			case "apm-server":
				if !reflect.DeepEqual(p.Args, []string{"-e", "-c", filepath.Join(h.dir, "karakoram", "config/apm-server.yml"), "--path.home", r.home(tt.s), "--path.data", filepath.Join(h.dir, "karakoram", "data")}) {
					t.Fatal(p)
				}
				info, err := os.Stat(filepath.Join(h.dir, "karakoram", "config/apm-server.yml"))
				if err != nil || info.Mode().Perm() != 0600 {
					t.Fatal("unsafe APM config mode", err)
				}
			}
			tt.s.RestartToken = "new"
			apply(t, r, h, tt.s)
			if h.actions[len(h.actions)-1] != tt.s.Component+":restart" {
				t.Fatal(h.actions)
			}
		})
	}
}

func TestReadiness(t *testing.T) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	port := strconv.Itoa(listener.Addr().(*net.TCPAddr).Port)
	for _, component := range New().Processes() {
		if component == "kibana" || component == "logstash" {
			continue // Application readiness requires HTTP health, not just TCP.
		}
		if err := New().Readiness(context.Background(), []string{component, port}); err != nil {
			t.Fatal(err)
		}
	}
	for _, args := range [][]string{nil, {"unknown"}, {"kibana", "bad"}, {"logstash", "0"}, {"apm-server", "65536"}, {"kibana", "1", "2"}} {
		if err := New().Readiness(context.Background(), args); err == nil {
			t.Fatal("invalid args accepted", args)
		}
	}
	listener.Close()
	if err := New().Readiness(context.Background(), []string{"elasticsearch", port}); err == nil {
		t.Fatal("closed listener ready")
	}
	v6, err := net.Listen("tcp6", "[::1]:0")
	if err != nil {
		t.Log("IPv6 unavailable")
		return
	}
	defer v6.Close()
	if err := New().Readiness(context.Background(), []string{"elasticsearch", strconv.Itoa(v6.Addr().(*net.TCPAddr).Port)}); err != nil {
		t.Fatal("IPv6 fallback failed", err)
	}
}

func TestKibanaReadinessRequiresSuccessfulStatus(t *testing.T) {
	for _, status := range []int{http.StatusOK, http.StatusServiceUnavailable, http.StatusFound} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
				if request.URL.Path != "/api/status" {
					t.Errorf("unexpected readiness path %s", request.URL.Path)
				}
				w.Header().Set("Location", "/ready")
				w.WriteHeader(status)
			}))
			defer server.Close()
			port := strconv.Itoa(server.Listener.Addr().(*net.TCPAddr).Port)
			err := New().Readiness(t.Context(), []string{"kibana", port})
			if (err == nil) != (status == http.StatusOK) {
				t.Fatalf("HTTP %d readiness error=%v", status, err)
			}
		})
	}
}

func atomicWrite(path string, data []byte, mode os.FileMode) (bool, error) {
	old, err := os.ReadFile(path)
	if err == nil && bytes.Equal(old, data) {
		info, err := os.Stat(path)
		if err != nil {
			return false, err
		}
		if info.Mode().Perm() == mode {
			return false, nil
		}
		return true, os.Chmod(path, mode)
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	if err := privateDir(filepath.Dir(path)); err != nil {
		return false, err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".elastic-*")
	if err != nil {
		return false, err
	}
	defer os.Remove(f.Name())
	if err := f.Chmod(mode); err != nil {
		f.Close()
		return false, err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return false, err
	}
	if err := f.Close(); err != nil {
		return false, err
	}
	return true, os.Rename(f.Name(), path)
}

func TestKeystoreCommandUsesMinimalEnvironment(t *testing.T) {
	for _, key := range []string{"KARAKORAM_AGENT_TOKEN", "KARAKORAM_INTERNAL_CA", "JAVA_TOOL_OPTIONS", "_JAVA_OPTIONS", "ES_JAVA_HOME", "JAVA_HOME", "CLI_JAVA_OPTS"} {
		t.Setenv(key, "must-not-reach-keystore")
	}
	dir := t.TempDir()
	capture := filepath.Join(dir, "environment")
	env := []string{"HOME=" + dir, "ES_PATH_CONF=" + dir, "ES_TMPDIR=" + dir, "TMPDIR=" + dir}
	if err := keystoreCommand(context.Background(), "/bin/sh", []string{"-c", `env > "$1"`, "keystore-test", capture}, env, nil); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(capture)
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	if strings.Contains(text, "must-not-reach-keystore") {
		t.Fatal("keystore inherited agent environment")
	}
	for _, entry := range env {
		if !strings.Contains(text, entry+"\n") {
			t.Fatal("missing keystore environment", entry)
		}
	}
	if !strings.Contains(text, "PATH=") || !strings.Contains(text, "LANG=C.UTF-8\n") {
		t.Fatal("missing process basics")
	}
}
