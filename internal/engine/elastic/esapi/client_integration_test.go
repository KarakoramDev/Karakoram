// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

//go:build integration

package esapi

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func integrationCA(t *testing.T, dir string) []byte {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "esapi test CA"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
	der, err := x509.CreateCertificate(rand.Reader, ca, ca, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	leaf := &x509.Certificate{SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "localhost"}, NotBefore: ca.NotBefore, NotAfter: ca.NotAfter, DNSNames: []string{"localhost", "esapi-node"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")}, KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}}
	leafDER, err := x509.CreateCertificate(rand.Reader, leaf, ca, &leafKey.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(leafKey)
	if err != nil {
		t.Fatal(err)
	}
	// Docker's unprivileged test process must be able to read the mounted files.
	if err = os.Chmod(dir, 0755); err != nil {
		t.Fatal(err)
	}
	for name, data := range map[string][]byte{"ca.crt": caPEM, "http.crt": pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leafDER}), "http.key": pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})} {
		if err = os.WriteFile(filepath.Join(dir, name), data, 0644); err != nil {
			t.Fatal(err)
		}
	}
	return caPEM
}

func TestDockerElasticsearch(t *testing.T) {
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("Docker is unavailable")
	}
	probeCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := exec.CommandContext(probeCtx, "docker", "info").Run(); err != nil {
		t.Skip("Docker daemon is unavailable")
	}
	dir := t.TempDir()
	ca := integrationCA(t, dir)
	random := make([]byte, 8)
	if _, err := rand.Read(random); err != nil {
		t.Fatal(err)
	}
	name := fmt.Sprintf("karakoram-esapi-%x", random)
	password := fmt.Sprintf("esapi-%x-password", random)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if out, err := exec.CommandContext(ctx, "docker", "rm", "-f", name).CombinedOutput(); err != nil {
			t.Errorf("removing test container: %v: %s", err, out)
		}
	})
	args := []string{"run", "-d", "--name", name, "--publish", "127.0.0.1::9200", "--mount", "type=bind,src=" + dir + ",dst=/usr/share/elasticsearch/config/test-certs,readonly", "--ulimit", "nofile=65535:65535"}
	settings := []string{"discovery.type=single-node", "node.name=esapi-node", "xpack.security.enabled=true", "xpack.security.autoconfiguration.enabled=false", "ELASTIC_PASSWORD=" + password, "ES_JAVA_OPTS=-Xms512m -Xmx512m", "path.repo=/usr/share/elasticsearch/data/snapshots", "xpack.security.http.ssl.enabled=true", "xpack.security.http.ssl.key=test-certs/http.key", "xpack.security.http.ssl.certificate=test-certs/http.crt", "xpack.security.http.ssl.certificate_authorities=test-certs/ca.crt", "xpack.security.transport.ssl.enabled=true", "xpack.security.transport.ssl.verification_mode=certificate", "xpack.security.transport.ssl.key=test-certs/http.key", "xpack.security.transport.ssl.certificate=test-certs/http.crt", "xpack.security.transport.ssl.certificate_authorities=test-certs/ca.crt"}
	for _, s := range settings {
		args = append(args, "--env", s)
	}
	args = append(args, "docker.elastic.co/elasticsearch/elasticsearch:9.5.5")
	startCtx, startCancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer startCancel()
	if out, err := exec.CommandContext(startCtx, "docker", args...).CombinedOutput(); err != nil {
		t.Fatalf("starting Elasticsearch: %v: %s", err, strings.ReplaceAll(string(out), password, "[redacted]"))
	}
	out, err := exec.CommandContext(startCtx, "docker", "port", name, "9200/tcp").Output()
	if err != nil {
		t.Fatal(err)
	}
	endpoint := "https://" + strings.TrimSpace(string(out))
	cfg := Config{Endpoints: []string{endpoint}, CAPEM: ca, Username: "elastic", Password: password, Timeout: 15 * time.Second}
	c, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	for {
		if _, err = c.Info(ctx); err == nil {
			break
		}
		select {
		case <-ctx.Done():
			logs, _ := exec.Command("docker", "logs", "--tail", "40", name).CombinedOutput()
			t.Fatalf("Elasticsearch readiness failed: %v\n%s", err, strings.ReplaceAll(string(logs), password, "[redacted]"))
		case <-time.After(time.Second):
		}
	}
	require := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	info, err := c.Info(ctx)
	require(err)
	if info.Version.Number != "9.5.5" || info.ClusterUUID == "" || info.ClusterUUID == "_na_" {
		t.Fatalf("unexpected identity: %+v", info)
	}
	health, err := c.Health(ctx, HealthOptions{WaitForStatus: "yellow", Timeout: 10 * time.Second})
	require(err)
	if health.NumberOfNodes != 1 || health.TimedOut {
		t.Fatalf("unexpected health: %+v", health)
	}
	nodes, err := c.Nodes(ctx)
	require(err)
	if len(nodes) != 1 || nodes[0].Name != "esapi-node" {
		t.Fatalf("unexpected nodes: %+v", nodes)
	}
	master, err := c.MasterNode(ctx)
	require(err)
	if master != "esapi-node" {
		t.Fatal(master)
	}
	require(c.PutRole(ctx, "esapi-test", Role{Cluster: []string{"monitor"}, Indices: []IndexPrivileges{{Names: []string{"esapi-docs"}, Privileges: []string{"read"}}}}))
	roles, err := c.GetRoles(ctx, "esapi-test")
	require(err)
	if len(roles) != 1 || len(roles["esapi-test"].Cluster) != 1 {
		t.Fatal("role was not persisted")
	}
	enabled := true
	require(c.PutUser(ctx, "esapi-user", User{Password: "esapi-user-password", Roles: []string{"esapi-test"}, FullName: "Test User", Enabled: &enabled}))
	users, err := c.GetUsers(ctx, "esapi-user")
	require(err)
	if len(users) != 1 || users["esapi-user"].FullName != "Test User" {
		t.Fatal("user was not persisted")
	}
	require(c.ChangePassword(ctx, "esapi-user", "changed-user-password"))
	require(c.DeleteUser(ctx, "esapi-user"))
	require(c.DeleteRole(ctx, "esapi-test"))
	token, err := c.CreateServiceToken(ctx, "elastic", "kibana", "esapi-test")
	require(err)
	bearerCfg := cfg
	bearerCfg.Username = ""
	bearerCfg.Password = ""
	bearerCfg.BearerToken = token
	bearer, err := New(bearerCfg)
	require(err)
	defer bearer.Close()
	auth, err := bearer.Authenticate(ctx)
	require(err)
	if auth.Username != "elastic/kibana" || auth.AuthenticationType != "token" {
		t.Fatalf("unexpected service authentication: %+v", auth)
	}
	require(c.DeleteServiceToken(ctx, "elastic", "kibana", "esapi-test"))
	key, err := c.CreateAPIKey(ctx, APIKeyRequest{Name: "esapi-test", Expiration: "1h", RoleDescriptors: map[string]Role{"monitor": {Cluster: []string{"monitor"}, Indices: []IndexPrivileges{}}}})
	require(err)
	if key.ID == "" || key.Encoded == "" || key.APIKey == "" {
		t.Fatal("incomplete key response")
	}
	keys, err := c.GetAPIKeys(ctx, true, "esapi-test")
	require(err)
	if len(keys) != 1 || keys[0].ID != key.ID {
		t.Fatal("key was not listed")
	}
	require(c.InvalidateAPIKeys(ctx, []string{key.ID}))
	keys, err = c.GetAPIKeys(ctx, true, "esapi-test")
	require(err)
	if len(keys) != 1 || !keys[0].Invalidated {
		t.Fatal("key was not invalidated")
	}
	allocation := "primaries"
	require(c.SetAllocationEnable(ctx, &allocation))
	clusterSettings, err := c.ClusterSettings(ctx)
	require(err)
	if clusterSettings.Persistent["cluster.routing.allocation.enable"] != "primaries" {
		t.Fatal("cluster settings did not round trip")
	}
	require(c.SetAllocationEnable(ctx, nil))
	require(c.SetAllocationExcludeNames(ctx, []string{"absent-node"}))
	excluded, err := c.AllocationExcludeNames(ctx)
	require(err)
	if len(excluded) != 1 || excluded[0] != "absent-node" {
		t.Fatal(excluded)
	}
	require(c.SetAllocationExcludeNames(ctx, nil))
	require(c.ReloadSecureSettings(ctx))
	require(c.Index(ctx, "esapi-docs", "1", map[string]any{"message": "snapshot document"}, true))
	total, hits, err := c.Search(ctx, "esapi-docs", map[string]any{"query": map[string]any{"match_all": map[string]any{}}})
	require(err)
	if total != 1 || len(hits) != 1 {
		t.Fatal("document was not searchable")
	}
	counts, err := c.ShardCounts(ctx)
	require(err)
	if counts["esapi-node"] == 0 {
		t.Fatal("no shards reported")
	}
	require(c.Flush(ctx))
	require(c.PutRepository(ctx, "esapi-repo", Repository{Type: "fs", Settings: map[string]any{"location": "esapi-test"}}, true))
	repo, err := c.GetRepository(ctx, "esapi-repo")
	require(err)
	if repo.Type != "fs" {
		t.Fatal("wrong repository type")
	}
	verified, err := c.VerifyRepository(ctx, "esapi-repo")
	require(err)
	if len(verified) != 1 {
		t.Fatal("repository not verified on node")
	}
	require(c.CreateSnapshot(ctx, "esapi-repo", "snapshot", SnapshotRequest{Indices: "esapi-docs", FeatureStates: []string{"none"}}, true))
	snap, err := c.GetSnapshot(ctx, "esapi-repo", "snapshot")
	require(err)
	if snap.State != "SUCCESS" || snap.Shards.Failed != 0 || snap.StartTime.IsZero() {
		t.Fatalf("snapshot failed: %+v", snap)
	}
	snapshots, err := c.ListSnapshots(ctx, "esapi-repo")
	require(err)
	if len(snapshots) != 1 {
		t.Fatal("snapshot missing from list")
	}
	size, err := c.SnapshotSize(ctx, "esapi-repo", "snapshot")
	require(err)
	if size <= 0 {
		t.Fatal("snapshot size missing")
	}
	indices, err := c.ListIndices(ctx, false)
	require(err)
	found := false
	for _, index := range indices {
		found = found || index == "esapi-docs"
	}
	if !found {
		t.Fatal("test index not listed")
	}
	require(c.CloseIndices(ctx, []string{"esapi-docs"}))
	require(c.DeleteIndices(ctx, []string{"esapi-docs"}))
	require(c.RestoreSnapshot(ctx, "esapi-repo", "snapshot", RestoreRequest{Indices: "esapi-docs", FeatureStates: []string{"none"}, IncludeAliases: true}))
	for {
		count, countErr := c.Count(ctx, "esapi-docs")
		active, recoveryErr := c.RecoveryInProgress(ctx)
		if countErr == nil && recoveryErr == nil && count == 1 && !active {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("restore did not finish")
		case <-time.After(time.Second):
		}
	}
	require(c.DeleteSnapshot(ctx, "esapi-repo", "snapshot"))
	require(c.DeleteRepository(ctx, "esapi-repo"))
	require(c.DeleteIndices(ctx, []string{"esapi-docs"}))
	t.Log("Elasticsearch 9.5.5 HTTPS: identity, health, nodes, users/roles, service tokens, API keys, settings, fs snapshot/restore/delete passed")
}
