// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

//go:build integration

package integration

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine/rabbitmq/rmqspec"
	"github.com/KarakoramDev/Karakoram/internal/pki"
)

const rabbitMQImage = "karakoram-rabbitmq-agent:4.3.6-otp27.3.4.17"
const rabbitMQDockerfile = `FROM ubuntu:24.04
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates gnupg && printf '#!/bin/sh\nexit 101\n' > /usr/sbin/policy-rc.d && chmod +x /usr/sbin/policy-rc.d && curl -fsSL https://keys.openpgp.org/vks/v1/by-fingerprint/0A9AF2115F4687BD29803A206B73A36E6026DFCA | gpg --dearmor -o /usr/share/keyrings/com.rabbitmq.team.gpg && printf 'deb [arch=amd64 signed-by=/usr/share/keyrings/com.rabbitmq.team.gpg] https://deb1.rabbitmq.com/rabbitmq-erlang/ubuntu/noble noble main\ndeb [arch=amd64 signed-by=/usr/share/keyrings/com.rabbitmq.team.gpg] https://deb2.rabbitmq.com/rabbitmq-erlang/ubuntu/noble noble main\ndeb [arch=amd64 signed-by=/usr/share/keyrings/com.rabbitmq.team.gpg] https://deb1.rabbitmq.com/rabbitmq-server/ubuntu/noble noble main\ndeb [arch=amd64 signed-by=/usr/share/keyrings/com.rabbitmq.team.gpg] https://deb2.rabbitmq.com/rabbitmq-server/ubuntu/noble noble main\n' > /etc/apt/sources.list.d/rabbitmq.list && apt-get update && apt-get install -y --no-install-recommends rabbitmq-server=4.3.6-1 erlang-base=1:27.3.4.17-1 erlang-asn1=1:27.3.4.17-1 erlang-crypto=1:27.3.4.17-1 erlang-eldap=1:27.3.4.17-1 erlang-inets=1:27.3.4.17-1 erlang-mnesia=1:27.3.4.17-1 erlang-os-mon=1:27.3.4.17-1 erlang-parsetools=1:27.3.4.17-1 erlang-public-key=1:27.3.4.17-1 erlang-runtime-tools=1:27.3.4.17-1 erlang-ssl=1:27.3.4.17-1 erlang-syntax-tools=1:27.3.4.17-1 erlang-tools=1:27.3.4.17-1 erlang-xmerl=1:27.3.4.17-1 && rm -rf /var/lib/apt/lists/*
`

func TestRabbitMQAgent(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("Docker integration requires Linux")
	}
	started := time.Now()
	defer func() { t.Logf("RabbitMQ integration runtime: %s", time.Since(started)) }()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	docker := func(args ...string) string {
		t.Helper()
		output, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
		if err != nil {
			t.Fatalf("docker %s: %v\n%s", strings.Join(args, " "), err, output)
		}
		return strings.TrimSpace(string(output))
	}
	docker("info")
	if err := exec.CommandContext(ctx, "docker", "image", "inspect", rabbitMQImage).Run(); err != nil {
		cmd := exec.CommandContext(ctx, "docker", "build", "-t", rabbitMQImage, "-")
		cmd.Stdin = strings.NewReader(rabbitMQDockerfile)
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("build pinned RabbitMQ image: %v\n%s", err, output)
		}
	}
	_, source, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(source), "../.."))
	binary := filepath.Join(t.TempDir(), "karakoram")
	build := exec.CommandContext(ctx, "go", "build", "-o", binary, "./cmd/karakoram")
	build.Dir = root
	build.Env = append(os.Environ(), "CGO_ENABLED=0")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("agent build: %v\n%s", err, out)
	}
	ca, err := pki.NewCA("integration", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	cpCert, cpKey, err := ca.Issue(pki.CertRequest{CommonName: "controlplane", DNSNames: []string{"controlplane"}, Server: true})
	if err != nil {
		t.Fatal(err)
	}
	pair, err := tls.X509KeyPair(cpCert, cpKey)
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.RWMutex
	configs := map[string]agentapi.MemberConfig{}
	cp := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.RLock()
		c, ok := configs[strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer integration-")]
		mu.RUnlock()
		if r.URL.Path != "/internal/agent/v1/config" || !strings.HasPrefix(r.Header.Get("Authorization"), "Bearer integration-") || !ok {
			http.Error(w, "unauthorized", 401)
			return
		}
		_ = json.NewEncoder(w).Encode(c)
	}))
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal(err)
	}
	cp.Listener.Close()
	cp.Listener = listener
	cp.TLS = &tls.Config{Certificates: []tls.Certificate{pair}, MinVersion: tls.VersionTLS12}
	cp.StartTLS()
	defer cp.Close()
	cpURL := "https://controlplane:" + fmt.Sprint(listener.Addr().(*net.TCPAddr).Port)
	network := fmt.Sprintf("karakoram-rmq-%d", time.Now().UnixNano())
	docker("network", "create", "--ipv6", network)
	t.Cleanup(func() { _ = exec.Command("docker", "network", "rm", network).Run() })
	peers := []string{"rabbit@rmqone", "rabbit@rmqtwo", "rabbit@rmqthree"}
	type node struct {
		name, container, management string
		client                      *agentapi.Client
	}
	nodes := make([]node, 0, 3)
	wait := func(description string, fn func() error) {
		t.Helper()
		deadline := time.Now().Add(90 * time.Second)
		var err error
		for {
			err = fn()
			if err == nil {
				return
			}
			if time.Now().After(deadline) || ctx.Err() != nil {
				t.Fatalf("%s: %v", description, err)
			}
			time.Sleep(300 * time.Millisecond)
		}
	}
	for _, member := range []string{"rmqone", "rmqtwo", "rmqthree"} {
		cert, key, err := ca.Issue(pki.CertRequest{CommonName: member, DNSNames: []string{member, "localhost"}, IPs: []net.IP{net.ParseIP("127.0.0.1")}, Server: true, Client: true})
		if err != nil {
			t.Fatal(err)
		}
		s := rmqspec.MemberSpec{NodeName: "rabbit@" + member, Peers: []string{"rabbit@" + member}, Cookie: strings.Repeat("integration-cookie", 3), Version: "4.3.6", Plugins: []string{"rabbitmq_management", "rabbitmq_prometheus", "rabbitmq_stream"}, AMQPSPort: 5671, ManagementTLSPort: 15671, PrometheusPort: 15692, MemoryHighWatermarkBytes: 512 << 20, TotalMemoryBytes: 1 << 30, DiskFreeLimitBytes: 50 << 20, DefaultQueueType: "quorum", InterNodeTLS: true}
		raw, _ := json.Marshal(s)
		c := agentapi.MemberConfig{Engine: "rabbitmq", Cluster: "integration", Member: member, EngineSpec: raw, AgentTLS: agentapi.Certificate{CertPEM: string(cert), KeyPEM: string(key)}, TLS: agentapi.TLSConfig{Mode: "requireTLS", CAPEM: string(ca.CertPEM), CertPEM: string(cert), KeyPEM: string(key)}, SystemUsers: map[string]agentapi.Credentials{"karakoram": {Username: "karakoram", Password: "integration-control"}, "admin": {Username: "admin", Password: "integration-admin"}}, DesiredProcesses: []string{"rabbitmq-server"}, ConfigVersion: 1}
		mu.Lock()
		configs[member] = c
		mu.Unlock()
		name := network + "-" + member
		docker("run", "-d", "--name", name, "--hostname", member, "--network", network, "--network-alias", member, "--add-host", "controlplane:host-gateway", "--user", "1000:1000", "--tmpfs", "/data:uid=1000,gid=1000,mode=0700", "--memory", "1g", "-p", "127.0.0.1::9443", "-p", "127.0.0.1::5671", "-p", "127.0.0.1::15671", "-v", binary+":/karakoram:ro", "-e", "ERL_AFLAGS=+S 2:2", "-e", "KARAKORAM_ENGINE=rabbitmq", "-e", "KARAKORAM_URL="+cpURL, "-e", "KARAKORAM_INTERNAL_CA="+string(ca.CertPEM), "-e", "KARAKORAM_AGENT_TOKEN=integration-"+member, "-e", "KARAKORAM_CLUSTER=integration", "-e", "KARAKORAM_MEMBER="+member, "-e", "KARAKORAM_DATA_DIR=/data", rabbitMQImage, "/karakoram", "agent")
		t.Cleanup(func() {
			if t.Failed() {
				out, _ := exec.Command("docker", "logs", name).CombinedOutput()
				t.Log(string(out))
			}
			_ = exec.Command("docker", "rm", "-f", name).Run()
		})
		address := docker("port", name, "9443/tcp")
		client, err := agentapi.NewClient(agentapi.ClientOptions{BaseURL: "https://" + address, Token: "integration-" + member, CAPEM: ca.CertPEM, Timeout: 3 * time.Minute})
		if err != nil {
			t.Fatal(err)
		}
		defer client.Close()
		nodes = append(nodes, node{name: member, container: name, client: client, management: "https://" + docker("port", name, "15671/tcp")})
		wait("broker startup "+member, func() error {
			out, err := exec.CommandContext(ctx, "docker", "exec", name, "/karakoram", "agent", "readiness").CombinedOutput()
			if err != nil {
				return fmt.Errorf("%w: %s", err, out)
			}
			return nil
		})
		roots := x509.NewCertPool()
		roots.AppendCertsFromPEM(ca.CertPEM)
		conn, err := tls.DialWithDialer(&net.Dialer{Timeout: 3 * time.Second}, "tcp", docker("port", name, "5671/tcp"), &tls.Config{RootCAs: roots, ServerName: "localhost", MinVersion: tls.VersionTLS12})
		if err != nil {
			t.Fatal("TLS AMQP listener", err)
		}
		conn.Close()
	}
	first := nodes[0].client
	action := func(client *agentapi.Client, name string, req any) {
		t.Helper()
		if err := client.Engine(ctx, "rabbitmq", name, req, &rmqspec.ActionResponse{}); err != nil {
			for _, n := range nodes {
				if n.client == client {
					diagnosticCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					out, _ := exec.CommandContext(diagnosticCtx, "docker", "exec", "-e", "HOME=/data/karakoram/home", "-e", "RABBITMQ_NODENAME=rabbit@"+n.name, "-e", "RABBITMQ_CTL_ERL_ARGS=-proto_dist inet6_tls -ssl_dist_optfile /data/karakoram/inter_node_tls.config", n.container, "/usr/lib/rabbitmq/bin/rabbitmqctl", "-t", "5", "list_users", "--quiet", "--formatter", "json").CombinedOutput()
					cancel()
					t.Logf("RabbitMQ CLI diagnostics: %s", out)
				}
			}
			t.Fatalf("%s: %v", name, err)
		}
	}
	action(first, "bootstrap", rmqspec.EmptyRequest{})
	action(first, "bootstrap", rmqspec.EmptyRequest{})
	for _, n := range nodes[1:] {
		action(n.client, "join", rmqspec.JoinRequest{Seed: peers[0]})
		action(n.client, "join", rmqspec.JoinRequest{Seed: peers[0]})
	}
	for _, n := range nodes {
		wait("joined cluster "+n.name, func() error {
			var status rmqspec.StatusResponse
			if err := n.client.Engine(ctx, "rabbitmq", "status", rmqspec.EmptyRequest{}, &status); err != nil {
				return err
			}
			if len(status.RunningNodes) != 3 {
				return fmt.Errorf("running nodes: %v", status.RunningNodes)
			}
			if status.Version != "4.3.6" {
				return fmt.Errorf("version: %s", status.Version)
			}
			if string(status.Alarms) != "[]" || string(status.Partitions) != "{}" {
				return fmt.Errorf("alarms %s, partitions %s", status.Alarms, status.Partitions)
			}
			return nil
		})
	}
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(ca.CertPEM)
	httpClient := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}}, Timeout: 10 * time.Second}
	defer httpClient.CloseIdleConnections()
	management := func(method, path, body string) []byte {
		t.Helper()
		req, _ := http.NewRequestWithContext(ctx, method, nodes[0].management+"/api/"+path, strings.NewReader(body))
		req.SetBasicAuth("karakoram", "integration-control")
		req.Header.Set("Content-Type", "application/json")
		res, err := httpClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		data, _ := io.ReadAll(res.Body)
		if res.StatusCode >= 300 {
			t.Fatalf("%s %s: %d %s", method, path, res.StatusCode, data)
		}
		return data
	}
	users := management("GET", "users", "")
	if bytes.Contains(users, []byte(`"guest"`)) || !bytes.Contains(users, []byte(`"karakoram"`)) || !bytes.Contains(users, []byte(`"admin"`)) {
		t.Fatal("bootstrap users", string(users))
	}
	management("PUT", "queues/%2F/integration-quorum", `{"durable":true,"arguments":{"x-queue-type":"quorum","x-quorum-initial-group-size":1}}`)
	for _, peer := range peers[1:] {
		action(first, "grow", rmqspec.NodeRequest{Node: peer})
	}
	management("PUT", "queues/%2F/integration-stream", `{"durable":true,"arguments":{"x-queue-type":"stream","x-initial-cluster-size":1}}`)
	var stream rmqspec.StreamReplicasResponse
	for _, action := range []string{"list", "add", "list", "delete"} {
		req := rmqspec.StreamReplicasRequest{Action: action, VHost: "/", Queue: "integration-stream"}
		if action != "list" {
			req.Node = peers[1]
		}
		if err := first.Engine(ctx, "rabbitmq", "stream-replicas", req, &stream); err != nil {
			t.Fatal("stream replicas", action, err)
		}
		if !json.Valid(stream.Replicas) {
			t.Fatal("invalid stream JSON", string(stream.Replicas))
		}
	}
	var streamCritical rmqspec.QuorumCriticalResponse
	if err := first.Engine(ctx, "rabbitmq", "quorum-critical", rmqspec.EmptyRequest{}, &streamCritical); err != nil || !streamCritical.Critical {
		t.Fatal("single-replica stream quorum check", streamCritical, err)
	}
	// Leave only the three-replica quorum queue for shutdown safety checks.
	management("DELETE", "queues/%2F/integration-stream", "")
	// Refresh classic discovery peers without restarting existing broker PIDs.
	for _, n := range nodes {
		before, err := n.client.Status(ctx)
		if err != nil {
			t.Fatal(err)
		}
		mu.Lock()
		c := configs[n.name]
		var s rmqspec.MemberSpec
		_ = json.Unmarshal(c.EngineSpec, &s)
		s.Peers = slices.Clone(peers)
		c.EngineSpec, _ = json.Marshal(s)
		c.ConfigVersion++
		configs[n.name] = c
		mu.Unlock()
		if err = n.client.Reload(ctx); err != nil {
			t.Fatal(err)
		}
		after, err := n.client.Status(ctx)
		if err != nil || after.Processes["rabbitmq-server"].PID != before.Processes["rabbitmq-server"].PID {
			t.Fatal("peer-only reload restarted broker", before, after, err)
		}
	}
	wait("metrics", func() error {
		metrics, err := first.Metrics(ctx)
		if err != nil {
			return err
		}
		for _, name := range []string{"karakoram_rabbitmq_up 1", "karakoram_rabbitmq_connections", "karakoram_rabbitmq_resident_memory_bytes", "karakoram_rabbitmq_cluster_members_running 3"} {
			if !bytes.Contains(metrics, []byte(name)) {
				return fmt.Errorf("missing %s in %s", name, metrics)
			}
		}
		return nil
	})
	var critical rmqspec.QuorumCriticalResponse
	if err = first.Engine(ctx, "rabbitmq", "quorum-critical", rmqspec.EmptyRequest{}, &critical); err != nil || critical.Critical {
		t.Fatal("quorum check", critical, err)
	}
	action(first, "await-quorum", rmqspec.AwaitQuorumRequest{TimeoutSeconds: 30})
	action(first, "rebalance", rmqspec.EmptyRequest{})
	action(first, "enable-feature-flags", rmqspec.EmptyRequest{})
	action(first, "clear-pem-cache", rmqspec.EmptyRequest{})
	action(first, "set-password", rmqspec.SetPasswordRequest{User: "admin"})
	leaving := nodes[2]
	action(first, "shrink", rmqspec.NodeRequest{Node: peers[2]})
	if err := leaving.client.Maintenance(ctx, agentapi.Maintenance{Enabled: true}); err != nil {
		t.Fatal(err)
	}
	action(leaving.client, "drain", rmqspec.EmptyRequest{})
	action(leaving.client, "stop", rmqspec.EmptyRequest{})
	state, err := leaving.client.Status(ctx)
	if err != nil || state.Processes["rabbitmq-server"].PID != 0 {
		t.Fatal("stop", state, err)
	}
	docker("exec", leaving.container, "/karakoram", "agent", "readiness")
	if err = leaving.client.Reload(ctx); err != nil {
		t.Fatal(err)
	}
	state, err = leaving.client.Status(ctx)
	if err != nil || state.Processes["rabbitmq-server"].PID != 0 {
		t.Fatal("reload undid transient stop", state, err)
	}
	if err := leaving.client.Maintenance(ctx, agentapi.Maintenance{Enabled: false}); err != nil {
		t.Fatal(err)
	}
	action(leaving.client, "start", rmqspec.EmptyRequest{})
	wait("restart listener", func() error {
		_, err := exec.CommandContext(ctx, "docker", "exec", leaving.container, "/karakoram", "agent", "readiness").CombinedOutput()
		return err
	})
	action(leaving.client, "revive", rmqspec.EmptyRequest{})
	action(leaving.client, "stop", rmqspec.EmptyRequest{})
	action(first, "forget", rmqspec.NodeRequest{Node: peers[2]})
	var status rmqspec.StatusResponse
	if err = first.Engine(ctx, "rabbitmq", "status", rmqspec.EmptyRequest{}, &status); err != nil || len(status.DiskNodes) != 2 {
		t.Fatal("forget", status, err)
	}
}
