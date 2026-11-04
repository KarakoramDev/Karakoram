// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

//go:build integration

package weaviate

import (
	"bytes"
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
	"io"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agent"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine/weaviate/wvspec"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type nativeAgent struct {
	url, dir string
	client   *http.Client
	mu       sync.Mutex
	config   agentapi.MemberConfig
	s        wvspec.MemberSpec
}

func nativeTLS(t *testing.T, node string) (string, agentapi.Certificate) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test-ca"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
	caDER, err := x509.CreateCertificate(rand.Reader, ca, ca, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	leaf := &x509.Certificate{SerialNumber: big.NewInt(2), DNSNames: []string{"localhost", node}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, NotBefore: ca.NotBefore, NotAfter: ca.NotAfter, KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, leaf, ca, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	pk, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})), agentapi.Certificate{CertPEM: string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})), KeyPEM: string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: pk}))}
}
func startNativeAgent(t *testing.T, s wvspec.MemberSpec) *nativeAgent {
	t.Helper()
	ca, cert := nativeTLS(t, s.NodeName)
	pair, err := tls.X509KeyPair([]byte(cert.CertPEM), []byte(cert.KeyPEM))
	if err != nil {
		t.Fatal(err)
	}
	c := config(s)
	c.Cluster = "integration"
	c.Member = s.NodeName
	c.AgentTLS = cert
	c.TLS = agentapi.TLSConfig{Mode: "requireTLS", CAPEM: ca, CertPEM: cert.CertPEM, KeyPEM: cert.KeyPEM}
	h := &nativeAgent{dir: t.TempDir(), config: c, s: s}
	cp := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/internal/agent/v1/config" || r.Header.Get("Authorization") != "Bearer integration-token" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		h.mu.Lock()
		defer h.mu.Unlock()
		_ = json.NewEncoder(w).Encode(h.config)
	}))
	cp.TLS = &tls.Config{Certificates: []tls.Certificate{pair}, MinVersion: tls.VersionTLS12}
	cp.StartTLS()
	t.Cleanup(cp.Close)
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	l.Close()
	h.url = "https://" + addr
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM([]byte(ca))
	h.client = &http.Client{Transport: &http.Transport{Proxy: nil, TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}}, Timeout: 10 * time.Second}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- agent.Run(ctx, agent.Options{ControlPlaneURL: cp.URL, InternalCAPEM: ca, Token: "integration-token", Cluster: "integration", Member: s.NodeName, DataDir: h.dir, Listen: addr, Engine: wvspec.Engine, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}, New())
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("shutdown: %v", err)
			}
		case <-time.After(65 * time.Second):
			t.Error("shutdown timed out")
		}
		h.client.CloseIdleConnections()
	})
	return h
}
func (h *nativeAgent) request(method, path string, value any) (int, []byte, error) {
	var body io.Reader
	if value != nil {
		data, _ := json.Marshal(value)
		body = bytes.NewReader(data)
	}
	req, _ := http.NewRequest(method, h.url+path, body)
	req.Header.Set("Authorization", "Bearer integration-token")
	req.Header.Set("Content-Type", "application/json")
	resp, err := h.client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	return resp.StatusCode, data, err
}
func (h *nativeAgent) native(t *testing.T, method, path string, value any, key string) (int, []byte) {
	t.Helper()
	scheme := "http"
	if h.s.TLS {
		scheme = "https"
	}
	var body io.Reader
	if value != nil {
		b, _ := json.Marshal(value)
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, fmt.Sprintf("%s://127.0.0.1:%d%s", scheme, h.s.Ports.HTTP, path), body)
	if err != nil {
		t.Fatal(err)
	}
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := h.client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, data
}
func (h *nativeAgent) must(t *testing.T, method, path string, value any) []byte {
	t.Helper()
	status, data := h.native(t, method, path, value, h.s.AllowedKeys[0])
	if status < 200 || status >= 300 {
		t.Fatalf("%s %s HTTP %d: %s", method, path, status, data)
	}
	return data
}
func (h *nativeAgent) ready(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		status, data, err := h.request("POST", "/v1/weaviate/readiness", struct{}{})
		var r wvspec.ReadinessResponse
		if err == nil && status == 200 && json.Unmarshal(data, &r) == nil && r.Ready {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	_, data, _ := h.request("GET", "/v1/status", nil)
	t.Fatalf("native readiness deadline: %s", data)
}
func (h *nativeAgent) update(t *testing.T, s wvspec.MemberSpec) {
	t.Helper()
	h.mu.Lock()
	raw, _ := json.Marshal(s)
	h.config.EngineSpec = raw
	h.config.EngineVersion = s.Version
	h.config.ConfigVersion++
	h.mu.Unlock()
	status, data, err := h.request("POST", "/v1/reload", struct{}{})
	if err != nil || status != 200 {
		t.Fatalf("reload %d %s %v", status, data, err)
	}
	h.s = s
	h.ready(t)
}
func nativeGate(t *testing.T) {
	t.Helper()
	if os.Getenv("KARAKORAM_WEAVIATE_INSTALL_ROOT") == "" {
		t.Skip("set KARAKORAM_WEAVIATE_INSTALL_ROOT for native qualification")
	}
	if os.Geteuid() == 0 {
		t.Fatal("run native tests as an unprivileged user")
	}
}
func collection(t *testing.T, h *nativeAgent, rf int) {
	t.Helper()
	h.must(t, "POST", "/v1/schema", map[string]any{"class": "Probe", "vectorizer": "none", "replicationConfig": map[string]any{"factor": rf}, "properties": []any{map[string]any{"name": "text", "dataType": []string{"text"}}}})
}

const objectID = "9bf0df42-c2f0-4cad-91ab-b0a033ec1433"

func writeObject(t *testing.T, h *nativeAgent) {
	t.Helper()
	h.must(t, "POST", "/v1/objects", map[string]any{"class": "Probe", "id": objectID, "properties": map[string]any{"text": "native persisted vector"}, "vector": []float64{1, 0, 0}})
}
func readObject(t *testing.T, h *nativeAgent) {
	t.Helper()
	b := h.must(t, "GET", "/v1/objects/Probe/"+objectID, nil)
	if !bytes.Contains(b, []byte("native persisted vector")) {
		t.Fatal(string(b))
	}
}
func TestNativeWeaviateSingleTLSAuthRestart(t *testing.T) {
	nativeGate(t)
	s := testSpec()
	s.TLS = true
	h := startNativeAgent(t, s)
	h.ready(t)
	status, _ := h.native(t, "GET", "/v1/schema", nil, "")
	if status != http.StatusUnauthorized {
		t.Fatalf("anonymous HTTP %d", status)
	}
	h.must(t, "GET", "/v1/meta", nil)
	b := h.must(t, "POST", "/v1/users/db/application", struct{}{})
	var user struct {
		APIKey string `json:"apikey"`
	}
	if json.Unmarshal(b, &user) != nil || user.APIKey == "" {
		t.Fatal("DB user key missing")
	}
	h.must(t, "POST", "/v1/authz/users/application/assign", map[string]any{"roles": []string{"viewer"}})
	status, _ = h.native(t, "GET", "/v1/schema", nil, user.APIKey)
	if status != 200 {
		t.Fatalf("DB user key HTTP %d", status)
	}
	collection(t, h, 1)
	writeObject(t, h)
	b = h.must(t, "POST", "/v1/graphql", map[string]any{"query": "{Get{Probe(nearVector:{vector:[1,0,0]},limit:1){text}}}"})
	if bytes.Contains(b, []byte(`"errors"`)) || !bytes.Contains(b, []byte("native persisted vector")) {
		t.Fatal(string(b))
	}
	// Capture real exposition after native data/query; normalize only the
	// disposable data-directory prefix for a portable, public-safe fixture.
	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/metrics", s.Ports.Metrics))
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	resp.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if target := os.Getenv("KARAKORAM_WEAVIATE_CAPTURE_METRICS"); target != "" {
		if err = os.WriteFile(target, []byte(strings.ReplaceAll(string(data), h.dir, "/data")), 0600); err != nil {
			t.Fatal(err)
		}
	}
	s.RestartToken = "restart-test"
	h.update(t, s)
	readObject(t, h)
}
func TestNativeWeaviateThreeMembers(t *testing.T) {
	nativeGate(t)
	t.Setenv("KARAKORAM_WEAVIATE_LOCAL_CLUSTER", "true")
	// Internal Raft/data/monitoring listeners bind wildcard upstream, so separate
	// ports are required even though alternate 127/8 addresses are unprivileged.
	members := startThreeNativeMembers(t, "1.40.1")
	deadline := time.Now().Add(60 * time.Second)
	synchronized := false
	for time.Now().Before(deadline) {
		b := members[0].must(t, "GET", "/v1/cluster/statistics", nil)
		var v struct {
			Synchronized bool `json:"synchronized"`
			Statistics   []struct {
				Ready bool `json:"ready"`
			} `json:"statistics"`
		}
		if json.Unmarshal(b, &v) == nil && v.Synchronized && len(v.Statistics) == 3 {
			synchronized = true
			for _, n := range v.Statistics {
				synchronized = synchronized && n.Ready
			}
			if synchronized {
				break
			}
		}
		time.Sleep(time.Second)
	}
	if !synchronized {
		t.Fatal("three-member Raft statistics did not synchronize")
	}
	collection(t, members[0], 3)
	writeObject(t, members[0])
	readObject(t, members[1])
}
func TestNativeWeaviateBackupRestore(t *testing.T) {
	nativeGate(t)
	get := func(s string) string { return os.Getenv("KARAKORAM_WEAVIATE_TEST_S3_" + s) }
	for _, name := range []string{"ENDPOINT", "BUCKET", "ACCESS_KEY", "SECRET_KEY", "USE_SSL"} {
		if get(name) == "" {
			t.Skip("set all KARAKORAM_WEAVIATE_TEST_S3_* variables for backup qualification")
		}
	}
	b := &wvspec.BackupS3{Endpoint: get("ENDPOINT"), Bucket: get("BUCKET"), AccessKey: get("ACCESS_KEY"), SecretKey: get("SECRET_KEY"), UseSSL: get("USE_SSL") == "true", Region: "us-east-1", Path: fmt.Sprintf("qualification-%d", time.Now().UnixNano())}
	client, err := minio.New(b.Endpoint, &minio.Options{Creds: credentials.NewStaticV4(b.AccessKey, b.SecretKey, ""), Secure: b.UseSSL})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	exists, err := client.BucketExists(ctx, b.Bucket)
	if err != nil {
		t.Fatal(err)
	}
	if !exists {
		if err = client.MakeBucket(ctx, b.Bucket, minio.MakeBucketOptions{Region: b.Region}); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		for object := range client.ListObjects(ctx, b.Bucket, minio.ListObjectsOptions{Prefix: b.Path + "/", Recursive: true}) {
			if object.Err != nil {
				t.Error(object.Err)
				break
			}
			if err := client.RemoveObject(ctx, b.Bucket, object.Key, minio.RemoveObjectOptions{}); err != nil {
				t.Error(err)
			}
		}
	})
	s := testSpec()
	s.Backup = b
	h := startNativeAgent(t, s)
	h.ready(t)
	collection(t, h, 1)
	writeObject(t, h)
	id := "native-backup"
	h.must(t, "POST", "/v1/backups/s3", map[string]any{"id": id, "include": []string{"Probe"}})
	pollBackup(t, h, "/v1/backups/s3/"+id)
	h.must(t, "DELETE", "/v1/schema/Probe", nil)
	h.must(t, "POST", "/v1/backups/s3/"+id+"/restore", map[string]any{"include": []string{"Probe"}})
	pollBackup(t, h, "/v1/backups/s3/"+id+"/restore")
	readObject(t, h)
}
func pollBackup(t *testing.T, h *nativeAgent, path string) {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		b := h.must(t, "GET", path, nil)
		var v struct {
			Status string `json:"status"`
			Error  string `json:"error"`
		}
		if json.Unmarshal(b, &v) != nil {
			t.Fatal(string(b))
		}
		if v.Status == "SUCCESS" {
			return
		}
		if v.Status == "FAILED" {
			t.Fatalf("backup/restore failed: %s", v.Error)
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatal("backup/restore deadline")
}
func TestNativeWeaviateUpgrade(t *testing.T) {
	nativeGate(t)
	for _, v := range []string{"1.39.12", "1.40.1"} {
		if _, err := os.Stat(binary(v)); err != nil {
			t.Skip("both native versions required")
		}
	}
	s := testSpec()
	s.Version = "1.39.12"
	h := startNativeAgent(t, s)
	h.ready(t)
	collection(t, h, 1)
	writeObject(t, h)
	s.Version = "1.40.1"
	s.RestartToken = "upgrade"
	h.update(t, s)
	readObject(t, h)
	b := h.must(t, "GET", "/v1/meta", nil)
	if !strings.Contains(string(b), `"version":"1.40.1"`) {
		t.Fatal("native version did not upgrade", string(b))
	}
	if _, err := os.Stat(filepath.Join(h.dir, "weaviate", "raft", "raft.db")); err != nil {
		t.Fatal(err)
	}
}

func startThreeNativeMembers(t *testing.T, version string) []*nativeAgent {
	t.Helper()
	members := make([]*nativeAgent, 3)
	joins := []string{"node00:28301", "node01:28401", "node02:28501"}
	gossip := []string{"127.0.0.1:27946", "127.0.0.1:28046", "127.0.0.1:28146"}
	for i := range members {
		s := testSpec()
		s.Version = version
		s.NodeName = fmt.Sprintf("node%02d", i)
		s.Ports = wvspec.Ports{HTTP: 28080 + i*100, GRPC: 25051 + i*100, Gossip: 27946 + i*100, Data: 27947 + i*100, Raft: 28300 + i*100, RaftRPC: 28301 + i*100, Metrics: 22112 + i*100}
		s.RaftJoin = joins
		s.GossipJoin = gossip
		s.BootstrapExpect = 3
		s.ReplicationMinimumFactor = 3
		members[i] = startNativeAgent(t, s)
	}
	for _, h := range members {
		h.ready(t)
	}
	return members
}

func awaitThreeVoterSynchronization(t *testing.T, members []*nativeAgent) {
	t.Helper()
	expected := map[string]bool{}
	for _, h := range members {
		expected[h.s.NodeName] = true
	}
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		allSynchronized := true
		for _, h := range members {
			status, data := h.native(t, "GET", "/v1/cluster/statistics", nil, h.s.AllowedKeys[0])
			var observation struct {
				Synchronized bool `json:"synchronized"`
				Statistics   []struct {
					Name     string `json:"name"`
					Ready    bool   `json:"ready"`
					DBLoaded bool   `json:"dbLoaded"`
					IsVoter  bool   `json:"isVoter"`
					LeaderID string `json:"leaderId"`
					Raft     struct {
						State               string `json:"state"`
						LatestConfiguration []struct {
							ID       string `json:"id"`
							Suffrage int    `json:"suffrage"`
						} `json:"latestConfiguration"`
					} `json:"raft"`
				} `json:"statistics"`
			}
			if status != http.StatusOK || json.Unmarshal(data, &observation) != nil || !observation.Synchronized || len(observation.Statistics) != 3 {
				allSynchronized = false
				break
			}
			seen := map[string]bool{}
			leader, leaders := "", 0
			for _, node := range observation.Statistics {
				if !expected[node.Name] || seen[node.Name] || !node.Ready || !node.DBLoaded || !node.IsVoter || !expected[node.LeaderID] {
					allSynchronized = false
					break
				}
				seen[node.Name] = true
				if leader == "" {
					leader = node.LeaderID
				} else if leader != node.LeaderID {
					allSynchronized = false
					break
				}
				if node.Raft.State == "Leader" {
					leaders++
					if node.Name != leader {
						allSynchronized = false
					}
				}
				voters := map[string]bool{}
				for _, voter := range node.Raft.LatestConfiguration {
					if !expected[voter.ID] || voters[voter.ID] || voter.Suffrage != 0 {
						allSynchronized = false
						break
					}
					voters[voter.ID] = true
				}
				if len(voters) != 3 || len(node.Raft.LatestConfiguration) != 3 {
					allSynchronized = false
				}
			}
			if len(seen) != 3 || leaders != 1 {
				allSynchronized = false
			}
			if !allSynchronized {
				break
			}
		}
		if allSynchronized {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatal("three-member cluster did not synchronize with exactly three voters and one agreed leader")
}

func readReplicatedVector(t *testing.T, h *nativeAgent, consistency string) {
	t.Helper()
	data := h.must(t, "GET", "/v1/objects/Probe/"+objectID+"?include=vector&consistency_level="+consistency, nil)
	var object struct {
		Vector     []float64      `json:"vector"`
		Properties map[string]any `json:"properties"`
	}
	if err := json.Unmarshal(data, &object); err != nil || len(object.Vector) != 3 || object.Vector[0] != 1 || object.Vector[1] != 0 || object.Vector[2] != 0 || object.Properties["text"] != "native persisted vector" {
		t.Fatalf("replicated vector read with %s returned unexpected data: %s", consistency, data)
	}
}

func TestNativeWeaviateThreeMemberRollingUpgrade(t *testing.T) {
	nativeGate(t)
	for _, version := range []string{"1.39.12", "1.40.1"} {
		if _, err := os.Stat(binary(version)); err != nil {
			t.Skip("both native versions required for three-member rolling upgrade")
		}
	}
	t.Setenv("KARAKORAM_WEAVIATE_LOCAL_CLUSTER", "true")
	members := startThreeNativeMembers(t, "1.39.12")
	awaitThreeVoterSynchronization(t, members)
	collection(t, members[0], 3)
	writeObject(t, members[0])
	awaitThreeVoterSynchronization(t, members)
	for _, index := range []int{2, 1, 0} {
		h := members[index]
		s := h.s
		s.Version = "1.40.1"
		s.RestartToken = "rolling-upgrade"
		h.update(t, s)
		awaitThreeVoterSynchronization(t, members)
		survivor := members[(index+1)%len(members)]
		for _, consistency := range []string{"ONE", "QUORUM"} {
			readReplicatedVector(t, survivor, consistency)
		}
	}
	for _, h := range members {
		var meta struct {
			Version string `json:"version"`
		}
		if data := h.must(t, "GET", "/v1/meta", nil); json.Unmarshal(data, &meta) != nil || meta.Version != "1.40.1" {
			t.Fatalf("%s did not report native version 1.40.1: %s", h.s.NodeName, data)
		}
		for _, consistency := range []string{"ONE", "QUORUM"} {
			readReplicatedVector(t, h, consistency)
		}
	}
}
