// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package rabbitmq

import (
	"context"
	"crypto/tls"
	"crypto/x509"
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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agent"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine/rabbitmq/rmqspec"
	"github.com/KarakoramDev/Karakoram/internal/pki"
)

func testSpec() rmqspec.MemberSpec {
	return rmqspec.MemberSpec{NodeName: "rabbit@rmqone", Peers: []string{"rabbit@rmqone"}, Cookie: strings.Repeat("a", 32), Version: "4.3.6", Plugins: []string{"rabbitmq_management", "rabbitmq_prometheus"}, AMQPPort: 5672, ManagementPort: 15672, PrometheusPort: 15692, MemoryHighWatermarkBytes: 512 << 20, TotalMemoryBytes: 1 << 30, DiskFreeLimitBytes: 1 << 30, DefaultQueueType: "quorum"}
}
func memberConfig(s rmqspec.MemberSpec) agentapi.MemberConfig {
	raw, _ := json.Marshal(s)
	return agentapi.MemberConfig{Cluster: "test", Member: "rmqone", Engine: "rabbitmq", EngineSpec: raw, TLS: agentapi.TLSConfig{Mode: "disabled"}, DesiredProcesses: []string{process}}
}
func TestRendering(t *testing.T) {
	for _, mode := range []string{"disabled", "preferTLS", "requireTLS"} {
		t.Run(mode, func(t *testing.T) {
			s := testSpec()
			c := memberConfig(s)
			c.TLS = agentapi.TLSConfig{Mode: mode, CAPEM: "CA", CertPEM: "CERT", KeyPEM: "KEY"}
			if mode != "disabled" {
				s.AMQPSPort = 5671
				s.ManagementTLSPort = 15671
			}
			if mode == "requireTLS" {
				s.AMQPPort = 0
				s.ManagementPort = 0
				s.InterNodeTLS = true
			}
			s.ConfigOverrides = "heartbeat = 42"
			files, env := render("/data", s, c)
			config := files[0].content
			golden, err := os.ReadFile(filepath.Join("testdata", mode+".conf"))
			if err != nil {
				t.Fatal(err)
			}
			if config != strings.TrimPrefix(string(golden), "# SPDX-License-Identifier: Apache-2.0\n# Copyright 2026 The Karakoram Authors\n") {
				t.Fatalf("rendered config differs from golden:\n%s", config)
			}
			for _, line := range []string{"log.console = true\n", "log.file = false\n", "cluster_formation.classic_config.nodes.1 = rabbit@rmqone\n", "quorum_queue.initial_cluster_size = 1\n", "default_queue_type = quorum\n", "vm_memory_high_watermark.absolute = 536870912\n", "total_memory_available_override_value = 1073741824\n", "disk_free_limit.absolute = 1073741824\n"} {
				if !strings.Contains(config, line) {
					t.Fatalf("missing %q in %s", line, config)
				}
			}
			if !strings.HasSuffix(config, "heartbeat = 42\n") {
				t.Fatal("overrides not appended last")
			}
			if strings.Contains(config, "loopback_users") {
				t.Fatal("guest restriction disabled")
			}
			tcp := "listeners.tcp.default = 5672\n"
			management := "management.tcp.port = 15672\n"
			if mode == "requireTLS" {
				tcp = "listeners.tcp = none\n"
				management = "management.ssl.port = 15671\n"
				if strings.Contains(config, "management.tcp.") {
					t.Fatal("required TLS exposed management HTTP", config)
				}
			}
			if !strings.Contains(config, tcp) || !strings.Contains(config, management) {
				t.Fatal(config)
			}
			if mode == "disabled" && strings.Contains(config, "ssl_options") {
				t.Fatal(config)
			}
			if mode != "disabled" {
				for _, line := range []string{"listeners.ssl.default = 5671\n", "management.ssl.port = 15671\n", "ssl_options.keyfile = /data/karakoram/tls/server.key\n", "management.ssl.cacertfile = /data/karakoram/tls/ca.pem\n"} {
					if !strings.Contains(config, line) {
						t.Fatal(line, config)
					}
				}
			}
			for _, value := range []string{"HOME=/data/karakoram/home", "RABBITMQ_MNESIA_BASE=/data/rabbitmq", "RABBITMQ_CONFIG_FILE=/data/karakoram/rabbitmq.conf", "RABBITMQ_ENABLED_PLUGINS_FILE=/data/karakoram/enabled_plugins"} {
				if !slices.Contains(env, value) {
					t.Fatal(env)
				}
			}
			if files[1].content != "[rabbitmq_management,rabbitmq_prometheus].\n" || files[2].mode != 0400 {
				t.Fatal(files)
			}
			if mode == "requireTLS" {
				dist := slices.IndexFunc(files, func(f renderedFile) bool { return f.name == "inter_node_tls.config" })
				if dist < 0 || !strings.Contains(files[dist].content, "{verify,verify_peer}") {
					t.Fatal(files)
				}
			}
		})
	}
}
func TestRestartDecision(t *testing.T) {
	s := testSpec()
	c := memberConfig(s)
	first := restartFingerprint("/data", s, c)
	s.Peers = append(s.Peers, "rabbit@rmqtwo", "rabbit@rmqthree")
	if restartFingerprint("/data", s, c) != first {
		t.Fatal("peer-list change restarts node")
	}
	s.ConfigOverrides = "heartbeat = 20"
	if restartFingerprint("/data", s, c) == first {
		t.Fatal("config change does not restart")
	}
	s.ConfigOverrides = ""
	s.Cookie = strings.Repeat("b", 32)
	if restartFingerprint("/data", s, c) == first {
		t.Fatal("cookie change does not restart")
	}
	s = testSpec()
	s.Version = "4.3.5"
	if restartFingerprint("/data", s, c) == first {
		t.Fatal("version change does not restart")
	}
	s = testSpec()
	c.TLS = agentapi.TLSConfig{Mode: "preferTLS", CAPEM: "ca", CertPEM: "cert", KeyPEM: "key"}
	s.AMQPSPort = 5671
	second := restartFingerprint("/data", s, c)
	c.TLS.KeyPEM = "new key"
	if restartFingerprint("/data", s, c) == second {
		t.Fatal("TLS rotation does not restart")
	}
}
func TestValidation(t *testing.T) {
	for _, node := range []string{"rabbit@ab", "rabbit@cluster1"} {
		if err := validateRequest(&rmqspec.NodeRequest{Node: node}); err != nil {
			t.Fatal(err)
		}
	}
	for _, node := range []string{"rabbit@a", "rabbit@CAPS", "rabbit@ab;halt().", "-n", "rabbit@" + strings.Repeat("x", 26), "rabbit@a-b"} {
		if validateRequest(&rmqspec.NodeRequest{Node: node}) == nil {
			t.Fatal(node)
		}
	}
	for _, r := range []any{&rmqspec.AwaitQuorumRequest{TimeoutSeconds: 121}, &rmqspec.SetPasswordRequest{User: "guest"}, &rmqspec.StreamReplicasRequest{Action: "eval", VHost: "/", Queue: "q"}, &rmqspec.StreamReplicasRequest{Action: "add", VHost: "/", Queue: "q", Node: "-n"}, &rmqspec.StreamReplicasRequest{Action: "list", VHost: "/", Queue: strings.Repeat("q", 256)}} {
		if validateRequest(r) == nil {
			t.Fatalf("accepted %#v", r)
		}
	}
	for _, body := range []string{`{"node":"rabbit@ab","args":["eval"]}`, `{} {}`, `null`, strings.Repeat(" ", 16385) + `{}`} {
		r := httptest.NewRequest("POST", "/", strings.NewReader(body))
		if decode(r, &rmqspec.NodeRequest{}) == nil {
			t.Fatal(body)
		}
	}
}
func TestMetricsTranslation(t *testing.T) {
	sample := `# TYPE rabbitmq_connections gauge
rabbitmq_connections 3
# TYPE rabbitmq_erlang_uptime_seconds gauge
rabbitmq_erlang_uptime_seconds 15
# TYPE rabbitmq_queue_messages_ready gauge
rabbitmq_queue_messages_ready 12
# TYPE rabbitmq_channel_messages_published_total counter
rabbitmq_channel_messages_published_total 19
# TYPE rabbitmq_channel_messages_delivered_total counter
rabbitmq_channel_messages_delivered_total 2
# TYPE rabbitmq_channel_messages_delivered_ack_total counter
rabbitmq_channel_messages_delivered_ack_total 4
# TYPE rabbitmq_alarms_memory_used_watermark gauge
rabbitmq_alarms_memory_used_watermark 1
# TYPE erlang_vm_dist_node_state gauge
erlang_vm_dist_node_state{node="rabbit@rmqtwo"} 3
erlang_vm_dist_node_state{node="rabbitmqcli-1@rmqone"} 3
# TYPE rabbitmq_secret_info gauge
rabbitmq_secret_info{password="never-export"} 1
`
	metrics, err := translateMetrics(strings.NewReader(sample), []string{"rabbit@rmqone", "rabbit@rmqtwo"})
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]float64{}
	for _, m := range metrics {
		if !slices.Contains(rmqspec.MetricNames, m.Name) || len(m.Labels) != 0 {
			t.Fatal(m)
		}
		got[m.Name] = m.Value
	}
	for name, want := range map[string]float64{"up": 1, "uptime_seconds": 15, "connections": 3, "messages_ready": 12, "published_total": 19, "delivered_total": 6, "memory_alarm": 1, "cluster_members_running": 2} {
		if got["karakoram_rabbitmq_"+name] != want {
			t.Fatal(name, got)
		}
	}
}
func TestHeldReadiness(t *testing.T) {
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "karakoram"), 0700); err != nil {
		t.Fatal(err)
	}
	for _, body := range []string{`{"maintenance":true}`, `{"stopped":{"rabbitmq-server":true}}`} {
		if err := os.WriteFile(filepath.Join(dir, "karakoram", "controls.json"), []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
		if err := New().Readiness(context.Background(), agent.Options{DataDir: dir}, nil); err != nil {
			t.Fatal(err)
		}
	}
	// Zerops readiness runs without the service environment.
	t.Setenv("KARAKORAM_DATA_DIR", "")
	if err := New().Readiness(context.Background(), agent.Options{}, []string{"--data-dir=" + dir}); err != nil {
		t.Fatal(err)
	}
	for _, bad := range [][]string{{"--data-dir=relative"}, {"--port=5672"}} {
		if err := New().Readiness(context.Background(), agent.Options{}, bad); err == nil {
			t.Fatalf("readiness accepted %q", bad)
		}
	}
}
func TestRoutesRequireBearer(t *testing.T) {
	dir := t.TempDir()
	bin := t.TempDir()
	t.Setenv("KARAKORAM_RABBITMQ_BIN", bin)
	// The executable fake supplies the real cluster_status field encodings.
	fake := "#!/bin/sh\nif [ \"$1\" = version ]; then echo 4.3.6; exit; fi\nif [ \"$1\" = cluster_status ]; then echo '{\"running_nodes\":[\"rabbit@rmqone\"],\"disk_nodes\":[\"rabbit@rmqone\"],\"alarms\":[],\"partitions\":{},\"maintenance_status\":{\"rabbit@rmqone\":\"marked for maintenance\"},\"versions\":{\"rabbit@rmqone\":{\"rabbitmq_version\":\"4.3.6\"}},\"feature_flags\":[{\"name\":\"test\",\"state\":\"enabled\"}]}'; exit; fi\nexec sleep 30\n"
	fake = strings.Replace(fake, "exec sleep 30\n", "if [ \"$1\" = stop_app ] || [ \"$1\" = start_app ]; then printf '%s\\n' \"$1\" >> \"$HOME/app-actions\"; echo '{\"result\":\"ok\"}'; exit; fi\nexec sleep 30\n", 1)
	for _, name := range []string{process, "rabbitmqctl"} {
		if err := os.WriteFile(filepath.Join(bin, name), []byte(fake), 0700); err != nil {
			t.Fatal(err)
		}
	}
	ca, err := pki.NewCA("test", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	cert, key, err := ca.Issue(pki.CertRequest{CommonName: "localhost", DNSNames: []string{"localhost"}, IPs: []net.IP{net.ParseIP("127.0.0.1")}, Server: true})
	if err != nil {
		t.Fatal(err)
	}
	c := memberConfig(testSpec())
	c.AgentTLS = agentapi.Certificate{CertPEM: string(cert), KeyPEM: string(key)}
	var configMu sync.RWMutex
	cp := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		configMu.RLock()
		defer configMu.RUnlock()
		_ = json.NewEncoder(w).Encode(c)
	}))
	defer cp.Close()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	listener.Close()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- agent.Run(ctx, agent.Options{Engine: "rabbitmq", Cluster: c.Cluster, Member: c.Member, Token: "token", ControlPlaneURL: cp.URL, InternalCAPEM: string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cp.Certificate().Raw})), DataDir: dir, Listen: address, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}, New())
	}()
	defer func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()
	client, err := agentapi.NewClient(agentapi.ClientOptions{BaseURL: "https://" + address, Token: "token", CAPEM: ca.CertPEM, Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err = client.Status(context.Background()); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal(err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	var broker rmqspec.StatusResponse
	if err := client.Engine(context.Background(), "rabbitmq", "status", rmqspec.EmptyRequest{}, &broker); err != nil {
		t.Fatal(err)
	}
	if broker.Version != "4.3.6" || !broker.Maintenance || len(broker.RunningNodes) != 1 || len(broker.FeatureFlags) != 1 {
		t.Fatal(broker)
	}
	before, err := client.Status(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Maintenance(context.Background(), agentapi.Maintenance{Enabled: true}); err != nil {
		t.Fatal(err)
	}
	s := testSpec()
	s.ConfigOverrides = "heartbeat = 30"
	raw, _ := json.Marshal(s)
	configMu.Lock()
	c.EngineSpec = raw
	configMu.Unlock()
	if err := client.Reload(context.Background()); err != nil {
		t.Fatal(err)
	}
	held, err := client.Status(context.Background())
	if err != nil || held.Processes[process].PID != before.Processes[process].PID {
		t.Fatal("maintenance failed to hold restart", held, err)
	}
	if err := client.Maintenance(context.Background(), agentapi.Maintenance{Enabled: false}); err != nil {
		t.Fatal(err)
	}
	if err := client.Reload(context.Background()); err != nil {
		t.Fatal(err)
	}
	after, err := client.Status(context.Background())
	if err != nil || after.Processes[process].PID == before.Processes[process].PID {
		t.Fatal("released maintenance lost pending restart", after, err)
	}
	fakeCLI(t, "rabbitmq-upgrade", "echo '{\"result\":\"ok\"}'\n")
	if err := client.Engine(context.Background(), "rabbitmq", "stop", rmqspec.EmptyRequest{}, nil); err != nil {
		t.Fatal(err)
	}
	if err := client.Reload(context.Background()); err != nil {
		t.Fatal(err)
	}
	stopped, err := client.Status(context.Background())
	if err != nil || stopped.Processes[process].PID != 0 {
		t.Fatal("reload undid RabbitMQ stop", stopped, err)
	}
	if err := client.Process(context.Background(), agentapi.ProcessAction{Process: process, Action: "start"}); err != nil {
		t.Fatal(err)
	}
	resumed, err := client.Status(context.Background())
	if err != nil || resumed.Processes[process].PID == 0 {
		t.Fatal(resumed, err)
	}
	s.ConfigOverrides = "heartbeat = 31"
	raw, _ = json.Marshal(s)
	configMu.Lock()
	c.EngineSpec = raw
	configMu.Unlock()
	if err := client.Reload(context.Background()); err != nil {
		t.Fatal(err)
	}
	restarted, err := client.Status(context.Background())
	if err != nil || restarted.Processes[process].PID == resumed.Processes[process].PID {
		t.Fatal("generic start left transient hold active", restarted, err)
	}
	if err := client.Engine(context.Background(), "rabbitmq", "stop-app", rmqspec.EmptyRequest{}, nil); err != nil {
		t.Fatal(err)
	}
	appStopped, err := client.Status(context.Background())
	if err != nil || appStopped.Processes[process].PID != restarted.Processes[process].PID {
		t.Fatal("stop-app replaced Erlang runtime", appStopped, err)
	}
	if err := client.Maintenance(context.Background(), agentapi.Maintenance{Enabled: true}); err != nil {
		t.Fatal(err)
	}
	if err := client.Engine(context.Background(), "rabbitmq", "start", rmqspec.EmptyRequest{}, nil); err != nil {
		t.Fatal(err)
	}
	actions, err := os.ReadFile(filepath.Join(dir, "karakoram", "home", "app-actions"))
	if err != nil || string(actions) != "stop_app\n" {
		t.Fatal("start_app bypassed maintenance", string(actions), err)
	}
	if err := client.Maintenance(context.Background(), agentapi.Maintenance{Enabled: false}); err != nil {
		t.Fatal(err)
	}
	if err := client.Engine(context.Background(), "rabbitmq", "start", rmqspec.EmptyRequest{}, nil); err != nil {
		t.Fatal(err)
	}
	actions, err = os.ReadFile(filepath.Join(dir, "karakoram", "home", "app-actions"))
	if err != nil || string(actions) != "stop_app\nstart_app\n" {
		t.Fatal("start did not resume application", string(actions), err)
	}
	if err := client.Engine(context.Background(), "rabbitmq", "stop-app", rmqspec.EmptyRequest{}, nil); err != nil {
		t.Fatal(err)
	}
	fakeCLI(t, "rabbitmq-upgrade", "exit 1\n")
	if err := client.Engine(context.Background(), "rabbitmq", "stop", rmqspec.EmptyRequest{}, nil); err != nil {
		t.Fatal("full stop unnecessarily drained a stopped application", err)
	}
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(ca.CertPEM)
	bad := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}}, Timeout: time.Second}
	defer bad.CloseIdleConnections()
	for _, token := range []string{"", "wrong"} {
		for _, action := range []string{"status", "bootstrap", "join", "forget", "await-quorum", "quorum-critical", "drain", "revive", "stop", "stop-app", "start", "grow", "shrink", "rebalance", "enable-feature-flags", "clear-pem-cache", "set-password", "stream-replicas"} {
			req, _ := http.NewRequest(http.MethodPost, "https://"+address+"/v1/rabbitmq/"+action, strings.NewReader("{}"))
			if token != "" {
				req.Header.Set("Authorization", "Bearer "+token)
			}
			res, err := bad.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			res.Body.Close()
			if res.StatusCode != 401 {
				t.Fatal(action, res.StatusCode)
			}
		}
	}
}

// Reloads run every minute; identical files must not be rewritten or reported as changed.
func TestUnchangedFileSkipsIdenticalContent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "rabbitmq.conf")
	if err := os.WriteFile(path, []byte("a = 1\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if !unchangedFile(path, "a = 1\n", 0600) {
		t.Fatal("identical file reported as changed")
	}
	if unchangedFile(path, "a = 2\n", 0600) || unchangedFile(path, "a = 1\n", 0400) || unchangedFile(filepath.Join(dir, "missing"), "", 0600) {
		t.Fatal("changed, mode-mismatched or missing file reported as unchanged")
	}
	link := filepath.Join(dir, "link")
	if err := os.Symlink(path, link); err != nil {
		t.Fatal(err)
	}
	if unchangedFile(link, "a = 1\n", 0600) {
		t.Fatal("symlink reported as an unchanged regular file")
	}
}
