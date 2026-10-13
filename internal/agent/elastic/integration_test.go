// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

//go:build integration

package elastic

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine/elastic/agentspec"
	"golang.org/x/crypto/bcrypt"
)

// TestSecuredElasticsearchDocker uses only an already-cached official image.
// It exercises rendered YAML/PEM/file-realm files, the real keystore CLI, product
// startup, CA and hostname verification, and monitor-authorized cluster health.
func TestSecuredElasticsearchDocker(t *testing.T) {
	const image = "docker.elastic.co/elasticsearch/elasticsearch:9.5.5"
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("Docker not installed")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	if err := exec.CommandContext(ctx, "docker", "info").Run(); err != nil {
		t.Skip("Docker unavailable")
	}
	if err := exec.CommandContext(ctx, "docker", "image", "inspect", image).Run(); err != nil {
		t.Skip("Elasticsearch 9.5.5 image not cached; test never downloads images")
	}
	docker := func(args ...string) string {
		t.Helper()
		output, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
		if err != nil {
			t.Fatalf("Docker %s failed: %v: %s", args[0], err, output)
		}
		return strings.TrimSpace(string(output))
	}
	home := t.TempDir()
	seed := docker("create", "--pull=never", image)
	t.Cleanup(func() { exec.Command("docker", "rm", "-f", seed).Run() })
	if err := os.MkdirAll(filepath.Join(home, "config"), 0700); err != nil {
		t.Fatal(err)
	}
	docker("cp", seed+":/usr/share/elasticsearch/config/.", filepath.Join(home, "config"))
	h := &fakeHost{dir: t.TempDir()}
	uid, gid := os.Getuid(), os.Getgid()
	if uid == 0 {
		uid, gid = 1000, 0
	}
	user := strconv.Itoa(uid) + ":" + strconv.Itoa(gid)
	owner := func(root string) {
		t.Helper()
		if os.Getuid() != 0 {
			return
		}
		if err := filepath.WalkDir(root, func(path string, _ os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			return os.Chown(path, uid, gid)
		}); err != nil {
			t.Fatal(err)
		}
	}
	// Use the same absolute data path inside the container: the rendered paths
	// are therefore tested unchanged rather than patched for Docker.
	mount := h.dir + ":" + h.dir
	ca, cert := testCertificates(t, "elastic-node")
	password := "integration-monitor-password"
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		t.Fatal(err)
	}
	s := esSpec()
	s.HeapMB = 512
	s.CAPEM = ca
	s.Elasticsearch.ClusterName = "secured-test"
	s.Elasticsearch.NodeName = "elastic-node"
	s.Elasticsearch.Roles = []string{"master", "data", "ingest"}
	s.Elasticsearch.SeedHosts = []string{"elastic-node:9300"}
	s.Elasticsearch.InitialMasterNodes = []string{"elastic-node"}
	s.Elasticsearch.TransportCert = cert
	s.Elasticsearch.HTTPCert = cert
	s.Elasticsearch.Settings = nil
	s.Elasticsearch.FileUsers = "karakoram-monitor:" + string(hash) + "\n"
	s.Elasticsearch.FileUsersRoles = "karakoram_monitor:karakoram-monitor\n"
	s.Elasticsearch.FileRoles = "karakoram_monitor:\n  cluster: [monitor]\n  indices:\n    - names: ['*']\n      privileges: [monitor]\n"
	s.Elasticsearch.Monitor = agentspec.Credentials{Username: "karakoram-monitor", Password: password}
	s.Elasticsearch.SecureSettings = map[string]string{"s3.client.default.access_key": "integration-access", "s3.client.default.secret_key": "integration-secret"}
	r := New()
	r.Home = func(string, string) string { return home }
	keystoreCalls := 0
	r.KeystoreCommand = func(cmdCtx context.Context, _ string, args, env []string, stdin io.Reader) error {
		keystoreCalls++
		owner(h.dir)
		dockerArgs := []string{"run", "--rm", "-i", "--pull=never", "--network", "none", "--user", user, "-v", mount, "-e", env[0], "--entrypoint", "/usr/share/elasticsearch/bin/elasticsearch-keystore", image}
		dockerArgs = append(dockerArgs, args...)
		cmd := exec.CommandContext(cmdCtx, "docker", dockerArgs...)
		cmd.Stdin = stdin
		// Keystore output may contain secrets, so never include it in failures.
		cmd.Stdout, cmd.Stderr = io.Discard, io.Discard
		return cmd.Run()
	}
	apply(t, r, h, s)
	if keystoreCalls != 3 {
		t.Fatalf("real keystore calls=%d", keystoreCalls)
	}
	apply(t, r, h, s)
	if keystoreCalls != 3 {
		t.Fatal("unchanged secure settings rebuilt real keystore")
	}
	owner(h.dir)
	container := docker("run", "-d", "--pull=never", "--hostname", "elastic-node", "--user", user, "--memory", "2g", "--ulimit", "nofile=65535:65535", "-p", "127.0.0.1::9200", "-v", mount, "-e", "ES_PATH_CONF="+filepath.Join(h.dir, "config"), "-e", "ES_TMPDIR="+filepath.Join(h.dir, "tmp"), image)
	t.Cleanup(func() { exec.Command("docker", "rm", "-f", container).Run() })
	binding := docker("port", container, "9200/tcp")
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM([]byte(ca)) {
		t.Fatal("invalid test CA")
	}
	transport := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, ServerName: "elastic-node", MinVersion: tls.VersionTLS12}}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 5 * time.Second}
	healthURL := "https://" + binding + "/_cluster/health?local=true"
	var health esHealth
	var lastError error
	for ctx.Err() == nil {
		lastError = getJSON(ctx, client, healthURL, s.Elasticsearch.Monitor, false, &health)
		if lastError == nil && health.Nodes == 1 && (health.Status == "green" || health.Status == "yellow") {
			break
		}
		select {
		case <-ctx.Done():
		case <-time.After(time.Second):
		}
	}
	if lastError != nil || health.Nodes != 1 || (health.Status != "green" && health.Status != "yellow") {
		logs, _ := exec.Command("docker", "logs", "--tail", "100", container).CombinedOutput()
		t.Fatalf("secured single-master formation failed: %v health=%+v logs=%s", lastError, health, logs)
	}
	// The monitoring account is file-realm authenticated, not a built-in user.
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://"+binding+"/_security/_authenticate", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.SetBasicAuth(s.Elasticsearch.Monitor.Username, password)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var identity struct {
		Username string `json:"username"`
		Realm    struct {
			Type string `json:"type"`
		} `json:"authentication_realm"`
	}
	if resp.StatusCode != 200 {
		t.Fatalf("authenticate status=%d", resp.StatusCode)
	}
	if err := json.NewDecoder(resp.Body).Decode(&identity); err != nil {
		t.Fatal(err)
	}
	if identity.Username != "karakoram-monitor" || identity.Realm.Type != "file" {
		t.Fatalf("wrong identity: %+v", identity)
	}
	// Missing credentials must not be able to read the secured API.
	unauth, err := http.NewRequestWithContext(ctx, http.MethodGet, healthURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp2, err := client.Do(unauth)
	if err != nil {
		t.Fatal(err)
	}
	resp2.Body.Close()
	if resp2.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthenticated health status=%d", resp2.StatusCode)
	}
	t.Logf("Elasticsearch %s: rendered config, TLS, bcrypt file realm, real keystore create/add; cluster=%s nodes=%s monitor realm=file unauthorized health=401", s.Version, health.Status, fmt.Sprint(health.Nodes))
}
