// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

//go:build integration

package integration

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func TestAgent(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("integration uses Docker host networking on Linux")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
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
	for _, port := range []string{":27017", ":9443"} {
		listener, err := net.Listen("tcp", port)
		if err != nil {
			t.Fatalf("integration requires unused port %s: %v", port, err)
		}
		listener.Close()
	}
	_, source, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(source), "../.."))
	binary := filepath.Join(t.TempDir(), "karakoram-agent")
	build := exec.CommandContext(ctx, "go", "build", "-o", binary, "./cmd/karakoram")
	build.Dir = root
	build.Env = append(os.Environ(), "CGO_ENABLED=0")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("static agent build: %v\n%s", err, output)
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "localhost"}, DNSNames: []string{"localhost"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}}
	der, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: raw})
	config := agentapi.MemberConfig{Cluster: "integration", Member: "localhost", Role: agentapi.RoleReplicaSet, ReplSetName: "rs0", Port: 27017, Keyfile: strings.Repeat("a", 64), TLS: agentapi.TLSConfig{Mode: "disabled"}, AgentTLS: agentapi.Certificate{CertPEM: string(certPEM), KeyPEM: string(keyPEM)}, WiredTigerCacheSizeGB: 0.25, SystemUsers: map[string]agentapi.Credentials{"clusterMonitor": {Username: "clusterMonitor", Password: "integration-monitor", Database: "admin"}}, DesiredProcesses: []string{"mongod"}, ConfigVersion: 7}
	config.TLS = agentapi.TLSConfig{Mode: "requireTLS", CAPEM: string(certPEM), CertPEM: string(certPEM), KeyPEM: string(keyPEM)}
	cp := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/internal/agent/v1/config" || r.Header.Get("Authorization") != "Bearer integration-token" {
			http.Error(w, "unauthorized", 401)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(config)
	}))
	defer cp.Close()
	cpCA := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cp.Certificate().Raw})
	name := fmt.Sprintf("karakoram-agent-%d", time.Now().UnixNano())
	docker("run", "-d", "--name", name, "--network", "host", "--user", "0", "--entrypoint", "/karakoram-agent", "-v", binary+":/karakoram-agent:ro", "-e", "KARAKORAM_URL="+cp.URL, "-e", "KARAKORAM_INTERNAL_CA="+string(cpCA), "-e", "KARAKORAM_AGENT_TOKEN=integration-token", "-e", "KARAKORAM_CLUSTER=integration", "-e", "KARAKORAM_MEMBER=localhost", "-e", "KARAKORAM_DATA_DIR=/data", "percona/percona-server-mongodb:8.0", "agent")
	t.Cleanup(func() {
		if t.Failed() {
			output, _ := exec.Command("docker", "logs", name).CombinedOutput()
			t.Log(string(output))
		}
		output, err := exec.Command("docker", "rm", "-f", name).CombinedOutput()
		if err != nil {
			t.Log("docker cleanup", err, string(output))
		}
	})
	client, err := agentapi.NewClient(agentapi.ClientOptions{BaseURL: "https://localhost:9443", Token: "integration-token", CAPEM: certPEM, Timeout: 2 * time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	var status agentapi.Status
	for {
		status, err = client.Status(ctx)
		if err == nil && status.Processes["mongod"].State == "running" {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("agent startup", err)
		case <-time.After(250 * time.Millisecond):
		}
	}
	if status.ConfigVersion != 7 || status.EngineVersion == "" || status.Disk.TotalBytes == 0 {
		t.Fatal(status)
	}
	for {
		output, err := exec.CommandContext(ctx, "docker", "exec", name, "/karakoram-agent", "agent", "readiness").CombinedOutput()
		if err == nil {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("mongod readiness", err, string(output))
		case <-time.After(250 * time.Millisecond):
		}
	}
	rendered := docker("exec", name, "cat", "/data/karakoram/mongod.conf")
	if !strings.Contains(rendered, "dbPath: /data/db") || !strings.Contains(rendered, "replSetName: rs0") {
		t.Fatal(rendered)
	}
	request := agentapi.BootstrapRequest{ReplSetName: "rs0", Members: []agentapi.BootstrapMember{{ID: 0, Host: "localhost:27017"}}, UserAdmin: agentapi.Credentials{Username: "userAdmin", Password: "integration-admin", Database: "admin"}}
	result, err := client.Bootstrap(ctx, request)
	if err != nil || !result.Initialized || !result.UserCreated {
		t.Fatal(result, err)
	}
	result, err = client.Bootstrap(ctx, request)
	if err != nil || !result.Initialized || result.UserCreated {
		t.Fatal("idempotent bootstrap", result, err)
	}
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(certPEM)
	mongoClient, err := mongo.Connect(options.Client().SetHosts([]string{"localhost:27017"}).SetDirect(true).SetTLSConfig(&tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}).SetAuth(options.Credential{Username: "userAdmin", Password: "integration-admin", AuthSource: "admin"}))
	if err != nil {
		t.Fatal(err)
	}
	defer mongoClient.Disconnect(context.Background())
	if err = mongoClient.Database("admin").RunCommand(ctx, bson.D{{Key: "createUser", Value: "clusterMonitor"}, {Key: "pwd", Value: "integration-monitor"}, {Key: "roles", Value: bson.A{bson.M{"role": "clusterMonitor", "db": "admin"}}}}).Err(); err != nil {
		t.Fatal(err)
	}
	metrics, err := client.Metrics(ctx)
	if err != nil || !strings.Contains(string(metrics), "karakoram_mongodb_up 1") || !strings.Contains(string(metrics), "karakoram_connections_current") || !strings.Contains(string(metrics), "karakoram_cache_used_bytes") {
		t.Fatal(err, string(metrics))
	}
	before := status.Processes["mongod"].PID
	if err = client.Reload(ctx); err != nil {
		t.Fatal(err)
	}
	status, err = client.Status(ctx)
	if err != nil || status.Processes["mongod"].PID != before {
		t.Fatal("unchanged configuration restarted mongod", status, err)
	}
	if err = client.Maintenance(ctx, agentapi.Maintenance{Enabled: true}); err != nil {
		t.Fatal(err)
	}
	if err = client.Reload(ctx); err != nil {
		t.Fatal(err)
	}
	status, err = client.Status(ctx)
	if err != nil || !status.Maintenance || status.Processes["mongod"].PID != before {
		t.Fatal("config pull released maintenance", status, err)
	}
	if err = client.Maintenance(ctx, agentapi.Maintenance{Enabled: false}); err != nil {
		t.Fatal(err)
	}
	docker("stop", "--time", "60", name)
	state := docker("inspect", "--format", "{{.State.ExitCode}}", name)
	if state != "0" {
		t.Fatal("agent shutdown exit", state)
	}
}
