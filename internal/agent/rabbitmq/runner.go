// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

// Package rabbitmq implements the node-local RabbitMQ engine.
package rabbitmq

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agent"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine/rabbitmq/rmqspec"
)

const process = "rabbitmq-server"

var nodePattern = regexp.MustCompile(`^rabbit@[a-z][a-z0-9]{1,24}$`)
var plugins = []string{"rabbitmq_management", "rabbitmq_prometheus", "rabbitmq_shovel", "rabbitmq_shovel_management", "rabbitmq_federation", "rabbitmq_federation_management", "rabbitmq_stream", "rabbitmq_stream_management"}

type Runner struct {
	mu            sync.Mutex
	fingerprint   [32]byte
	applied       bool
	stopped       bool
	appStoppedPID int
	env           []string
}

func New() *Runner                  { return &Runner{} }
func (*Runner) Name() string        { return "rabbitmq" }
func (*Runner) Processes() []string { return []string{process} }
func binary(name string) string {
	dir := os.Getenv("KARAKORAM_RABBITMQ_BIN")
	if dir == "" {
		dir = "/usr/lib/rabbitmq/bin"
	}
	return filepath.Join(dir, name)
}
func cpuSchedulers() int {
	data, err := os.ReadFile("/sys/fs/cgroup/cpu.max")
	if err != nil {
		return schedulersFromQuota("", runtime.NumCPU())
	}
	return schedulersFromQuota(string(data), runtime.NumCPU())
}
func schedulersFromQuota(cpuMax string, fallback int) int {
	fields := strings.Fields(cpuMax)
	if len(fields) == 2 && fields[0] != "max" {
		quota, qerr := strconv.ParseInt(fields[0], 10, 64)
		period, perr := strconv.ParseInt(fields[1], 10, 64)
		if qerr == nil && perr == nil && quota > 0 && period > 0 {
			n := quota / period
			if quota%period != 0 {
				n++
			}
			return int(min(int64(4), max(int64(1), n)))
		}
	}
	return min(4, max(1, fallback))
}
func spec(c agentapi.MemberConfig) (rmqspec.MemberSpec, error) {
	var s rmqspec.MemberSpec
	if err := json.Unmarshal(c.EngineSpec, &s); err != nil {
		return s, err
	}
	if !nodePattern.MatchString(s.NodeName) {
		return s, errors.New("invalid node name")
	}
	if len(s.Peers) > 8 || len(s.Peers) == 0 {
		return s, errors.New("invalid peer count")
	}
	for _, peer := range s.Peers {
		if !nodePattern.MatchString(peer) {
			return s, errors.New("invalid peer node")
		}
	}
	if !slices.Contains(s.Peers, s.NodeName) {
		return s, errors.New("local node missing from peers")
	}
	if len(s.Cookie) < 16 || len(s.Cookie) > 255 || strings.ContainsAny(s.Cookie, "\r\n\x00") {
		return s, errors.New("invalid cookie")
	}
	for _, p := range []int{s.AMQPPort, s.AMQPSPort, s.ManagementPort, s.ManagementTLSPort, s.PrometheusPort, s.StreamPort, s.StreamTLSPort} {
		if p < 0 || p > 65535 {
			return s, errors.New("invalid listener port")
		}
	}
	if s.AMQPPort == 0 && s.AMQPSPort == 0 {
		return s, errors.New("client listener required")
	}
	if s.ManagementPort == 0 && s.ManagementTLSPort == 0 {
		return s, errors.New("management listener required")
	}
	if s.PrometheusPort == 0 {
		return s, errors.New("Prometheus listener required")
	}
	if s.MemoryHighWatermarkBytes <= 0 || s.TotalMemoryBytes <= 0 || s.DiskFreeLimitBytes <= 0 || s.DefaultQueueType != "quorum" {
		return s, errors.New("invalid resource limits or queue type")
	}
	for _, p := range s.Plugins {
		if !slices.Contains(plugins, p) {
			return s, errors.New("unsupported plugin")
		}
	}
	if !slices.Contains(s.Plugins, "rabbitmq_management") || !slices.Contains(s.Plugins, "rabbitmq_prometheus") {
		return s, errors.New("required plugin missing")
	}
	if c.TLS.Mode != "disabled" && c.TLS.Mode != "preferTLS" && c.TLS.Mode != "requireTLS" {
		return s, errors.New("invalid TLS mode")
	}
	if slices.Contains(s.Plugins, "rabbitmq_stream") {
		switch c.TLS.Mode {
		case "disabled":
			if s.StreamPort == 0 || s.StreamTLSPort != 0 {
				return s, errors.New("invalid disabled-TLS stream listeners")
			}
		case "preferTLS":
			if s.StreamPort == 0 || s.StreamTLSPort == 0 {
				return s, errors.New("both stream listeners required for preferTLS")
			}
		case "requireTLS":
			if s.StreamPort != 0 || s.StreamTLSPort == 0 {
				return s, errors.New("only TLS stream listener permitted for requireTLS")
			}
		}
	} else if s.StreamPort != 0 || s.StreamTLSPort != 0 {
		return s, errors.New("stream listener requires stream plugin")
	}
	if (s.AMQPSPort > 0 || s.ManagementTLSPort > 0 || s.StreamTLSPort > 0 || s.InterNodeTLS) && (c.TLS.CAPEM == "" || c.TLS.CertPEM == "" || c.TLS.KeyPEM == "") {
		return s, errors.New("TLS material required")
	}
	return s, nil
}

type renderedFile struct {
	name, content string
	mode          os.FileMode
}

func render(dir string, s rmqspec.MemberSpec, c agentapi.MemberConfig) ([]renderedFile, []string) {
	root := filepath.Join(dir, "karakoram")
	path := func(p string) string { return filepath.Join(root, p) }
	var b strings.Builder
	line := func(k string, v any) { fmt.Fprintf(&b, "%s = %v\n", k, v) }
	if s.AMQPPort == 0 {
		line("listeners.tcp", "none")
	} else {
		line("listeners.tcp.default", s.AMQPPort)
	}
	if s.AMQPSPort > 0 {
		line("listeners.ssl.default", s.AMQPSPort)
	}
	if s.AMQPSPort > 0 || s.ManagementTLSPort > 0 || s.StreamTLSPort > 0 {
		for _, pair := range [][2]string{{"cacertfile", "tls/ca.pem"}, {"certfile", "tls/server.pem"}, {"keyfile", "tls/server.key"}} {
			if s.AMQPSPort > 0 || s.StreamTLSPort > 0 {
				line("ssl_options."+pair[0], path(pair[1]))
			}
			if s.ManagementTLSPort > 0 {
				line("management.ssl."+pair[0], path(pair[1]))
			}
		}
		if s.AMQPSPort > 0 || s.StreamTLSPort > 0 {
			line("ssl_options.verify", "verify_none")
			line("ssl_options.fail_if_no_peer_cert", "false")
		}
	}
	if s.ManagementPort > 0 {
		line("management.tcp.port", s.ManagementPort)
	}
	if s.ManagementTLSPort > 0 {
		line("management.ssl.port", s.ManagementTLSPort)
	}
	if s.PrometheusPort > 0 {
		line("prometheus.tcp.port", s.PrometheusPort)
		line("prometheus.tcp.ip", "127.0.0.1")
	}
	if slices.Contains(s.Plugins, "rabbitmq_stream") {
		if s.StreamPort == 0 {
			line("stream.listeners.tcp", "none")
		} else {
			line("stream.listeners.tcp.default", s.StreamPort)
		}
		if s.StreamTLSPort > 0 {
			// Stream TLS consumes the shared rabbit.ssl_options, not a plugin-specific key.
			line("stream.listeners.ssl.default", s.StreamTLSPort)
		}
	}
	line("cluster_formation.peer_discovery_backend", "classic_config")
	for i, p := range s.Peers {
		line(fmt.Sprintf("cluster_formation.classic_config.nodes.%d", i+1), p)
	}
	line("cluster_formation.discovery_retry_limit", 30)
	line("cluster_formation.discovery_retry_interval", 1000)
	line("log.console", true)
	line("log.file", false)
	line("vm_memory_high_watermark.absolute", s.MemoryHighWatermarkBytes)
	line("total_memory_available_override_value", s.TotalMemoryBytes)
	line("disk_free_limit.absolute", s.DiskFreeLimitBytes)
	line("default_queue_type", s.DefaultQueueType)
	line("quorum_queue.initial_cluster_size", min(3, len(s.Peers)))
	if s.ConfigOverrides != "" {
		b.WriteString("\n# Customer overrides\n")
		b.WriteString(s.ConfigOverrides)
		b.WriteByte('\n')
	}
	files := []renderedFile{{"rabbitmq.conf", b.String(), 0600}, {"enabled_plugins", "[" + strings.Join(s.Plugins, ",") + "].\n", 0600}, {"home/.erlang.cookie", s.Cookie, 0400}}
	advanced := "[].\n"
	dist := "-proto_dist inet6_tcp"
	if s.AMQPSPort > 0 || s.ManagementTLSPort > 0 || s.StreamTLSPort > 0 || s.InterNodeTLS {
		files = append(files, renderedFile{"tls/ca.pem", c.TLS.CAPEM, 0600}, renderedFile{"tls/server.pem", c.TLS.CertPEM, 0600}, renderedFile{"tls/server.key", c.TLS.KeyPEM, 0600})
	}
	if s.InterNodeTLS {
		// Erlang terms use quoted strings; JSON escaping is compatible for managed paths.
		quote := func(p string) string { v, _ := json.Marshal(path(p)); return string(v) }
		opts := "[{server, [{cacertfile," + quote("tls/ca.pem") + "},{certfile," + quote("tls/server.pem") + "},{keyfile," + quote("tls/server.key") + "},{verify,verify_peer},{fail_if_no_peer_cert,true}]},\n{client, [{cacertfile," + quote("tls/ca.pem") + "},{certfile," + quote("tls/server.pem") + "},{keyfile," + quote("tls/server.key") + "},{verify,verify_peer}]}].\n"
		files = append(files, renderedFile{"inter_node_tls.config", opts, 0600})
		serverOptions := "[{cacertfile," + quote("tls/ca.pem") + "},{certfile," + quote("tls/server.pem") + "},{keyfile," + quote("tls/server.key") + "},{verify,verify_peer},{fail_if_no_peer_cert,true}]"
		clientOptions := "[{cacertfile," + quote("tls/ca.pem") + "},{certfile," + quote("tls/server.pem") + "},{keyfile," + quote("tls/server.key") + "},{verify,verify_peer}]"
		advanced = "[{osiris,[{replication_transport,ssl},{replication_server_ssl_options," + serverOptions + "},{replication_client_ssl_options," + clientOptions + "}]}].\n"
		dist = "-proto_dist inet6_tls -ssl_dist_optfile " + path("inter_node_tls.config")
	}
	files = append(files, renderedFile{"advanced.config", advanced, 0600})
	schedulers := cpuSchedulers()
	flags := fmt.Sprintf("+S %d:%d +SDcpu %d:%d +SDio 2 +A 4", schedulers, schedulers, schedulers, schedulers)
	// Discovery peers inherit scheduler caps, but RabbitMQ explicitly replays
	// distribution arguments into their argv. Keep dist options out of inherited
	// ERL_FLAGS to avoid a duplicate ssl_dist_optfile, which aborts ssl_dist_sup.
	// The CLI wrapper replaces ERL_FLAGS, so its dedicated flags include both.
	env := []string{"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin", "LANG=C.UTF-8", "LC_ALL=C.UTF-8", "HOME=" + path("home"), "RABBITMQ_NODENAME=" + s.NodeName, "RABBITMQ_MNESIA_BASE=" + filepath.Join(dir, "rabbitmq"), "RABBITMQ_CONFIG_FILE=" + path("rabbitmq.conf"), "RABBITMQ_ADVANCED_CONFIG_FILE=" + path("advanced.config"), "RABBITMQ_ENABLED_PLUGINS_FILE=" + path("enabled_plugins"), "RABBITMQ_LOG_BASE=" + path("logs"), "ERL_FLAGS=" + flags, "RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS=" + dist, "RABBITMQ_CTL_ERL_ARGS=" + flags + " " + dist}
	return files, env
}
func restartFingerprint(dir string, s rmqspec.MemberSpec, c agentapi.MemberConfig) [32]byte {
	// Discovery membership can change while running. Initial queue replicas are
	// creation-time defaults, so neither planned membership nor its size restarts.
	s.Peers = []string{s.NodeName}
	files, env := render(dir, s, c)
	h := sha256.New()
	fmt.Fprint(h, s.Version, binary(process), env)
	for _, f := range files {
		fmt.Fprint(h, f.name, f.content)
	}
	var result [32]byte
	copy(result[:], h.Sum(nil))
	return result
}
func (e *Runner) Apply(ctx context.Context, n *agent.Node, c agentapi.MemberConfig) ([]string, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	s, err := spec(c)
	if err != nil {
		return nil, err
	}
	files, env := render(n.DataDir(), s, c)
	names := make([]string, 0, len(files))
	for _, f := range files {
		if err := n.WriteFile(f.name, []byte(f.content), f.mode); err != nil {
			return nil, err
		}
		names = append(names, f.name)
	}
	if err := os.MkdirAll(filepath.Join(n.DataDir(), "karakoram", "logs"), 0700); err != nil {
		return nil, err
	}
	if err := n.Configure(process, agent.ProcessSpec{Binary: binary(process), Env: env, Dir: n.DataDir()}); err != nil {
		return nil, err
	}
	e.env = env
	fingerprint := restartFingerprint(n.DataDir(), s, c)
	states, maintenance := n.Snapshot()
	if e.appStoppedPID != states[process].PID {
		e.appStoppedPID = 0
	}
	// An explicit generic supervisor start also releases a transient stop.
	if states[process].PID != 0 {
		e.stopped = false
	}
	action := "start"
	if states[process].PID != 0 && e.applied && e.fingerprint != fingerprint {
		action = "restart"
	}
	if !e.stopped {
		if err := n.Action(ctx, process, action); err != nil {
			return nil, err
		}
	}
	// A running process under a safety hold has not consumed the new files.
	// Preserve its fingerprint so releasing the hold still applies the restart.
	if states[process].PID == 0 || (!maintenance && !n.Stopped(process)) {
		e.fingerprint = fingerprint
		e.applied = true
	}
	return names, nil
}
func (*Runner) Readiness(ctx context.Context, o agent.Options, args []string) error {
	if len(args) != 0 {
		return errors.New("rabbitmq readiness takes no arguments")
	}
	dir := o.DataDir
	if dir == "" {
		dir = os.Getenv("KARAKORAM_DATA_DIR")
	}
	if dir == "" {
		return errors.New("data directory required")
	}
	data, err := os.ReadFile(filepath.Join(dir, "karakoram", "controls.json"))
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	var controls struct {
		Maintenance bool            `json:"maintenance"`
		Stopped     map[string]bool `json:"stopped"`
	}
	if len(data) > 0 {
		if err = json.Unmarshal(data, &controls); err != nil {
			return err
		}
	}
	if controls.Maintenance || controls.Stopped[process] {
		return nil
	}
	data, err = os.ReadFile(filepath.Join(dir, "karakoram", "config.json"))
	if err != nil {
		return err
	}
	var c agentapi.MemberConfig
	if err = json.Unmarshal(data, &c); err != nil {
		return err
	}
	if c.Maintenance {
		return nil
	}
	s, err := spec(c)
	if err != nil {
		return err
	}
	port := s.AMQPSPort
	if port == 0 {
		port = s.AMQPPort
	}
	d := net.Dialer{Timeout: 3 * time.Second}
	conn, err := d.DialContext(ctx, "tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err == nil {
		conn.Close()
	}
	return err
}
func (e *Runner) Version(ctx context.Context) string {
	e.mu.Lock()
	env := slices.Clone(e.env)
	e.mu.Unlock()
	out, err := command(ctx, env, "rabbitmqctl", nil, 10*time.Second, "version")
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}
