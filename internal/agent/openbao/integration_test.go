// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

//go:build integration

package openbao

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agent"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine/openbao/baoapi"
	"github.com/KarakoramDev/Karakoram/internal/engine/openbao/baospec"
	"github.com/KarakoramDev/Karakoram/internal/pki"
)

type nativeAgent struct {
	dir    string
	client *agentapi.Client
	mu     sync.Mutex
	config agentapi.MemberConfig
}

func reservePorts(t *testing.T, count int) []net.Listener {
	t.Helper()
	listeners := make([]net.Listener, 0, count)
	for range count {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { l.Close() })
		listeners = append(listeners, l)
	}
	return listeners
}
func leaf(t *testing.T, ca *pki.CA, name string) agentapi.Certificate {
	t.Helper()
	c, k, e := ca.Issue(pki.CertRequest{CommonName: name, DNSNames: []string{name, "localhost"}, IPs: []net.IP{net.ParseIP("127.0.0.1")}, Server: true, Client: true, Validity: time.Hour})
	if e != nil {
		t.Fatal(e)
	}
	return agentapi.Certificate{CertPEM: string(c), KeyPEM: string(k)}
}
func eventually(t *testing.T, what string, fn func() bool) {
	t.Helper()
	end := time.Now().Add(70 * time.Second)
	for time.Now().Before(end) {
		if fn() {
			return
		}
		time.Sleep(150 * time.Millisecond)
	}
	t.Fatalf("timed out: %s", what)
}
func startNativeAgent(t *testing.T, ca *pki.CA, s baospec.MemberSpec, reserved []net.Listener) *nativeAgent {
	t.Helper()
	cert := leaf(t, ca, s.NodeID)
	raw, _ := json.Marshal(s)
	// Keep native data beneath the worktree and remove it after process shutdown.
	dir, e := os.MkdirTemp("../../../", ".openbao-integration-")
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	dir, e = filepath.Abs(dir)
	if e != nil {
		t.Fatal(e)
	}
	h := &nativeAgent{dir: dir, config: agentapi.MemberConfig{Cluster: "integration", Member: s.NodeID, Engine: baospec.Engine, EngineSpec: raw, TLS: agentapi.TLSConfig{Mode: "requireTLS", CAPEM: string(ca.CertPEM), CertPEM: cert.CertPEM, KeyPEM: cert.KeyPEM}, AgentTLS: cert, ConfigVersion: 1}}
	pair, e := tls.X509KeyPair([]byte(cert.CertPEM), []byte(cert.KeyPEM))
	if e != nil {
		t.Fatal(e)
	}
	cp := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/internal/agent/v1/config" || r.Header.Get("Authorization") != "Bearer integration-token" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		h.mu.Lock()
		defer h.mu.Unlock()
		_ = json.NewEncoder(w).Encode(h.config)
	}))
	cp.TLS = &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{pair}}
	cp.StartTLS()
	t.Cleanup(cp.Close)
	addr := reserved[3].Addr().String()
	h.client, e = agentapi.NewClient(agentapi.ClientOptions{BaseURL: "https://" + addr, Token: "integration-token", CAPEM: ca.CertPEM, Timeout: 10 * time.Second, EngineTimeout: 35 * time.Second})
	if e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	for _, listener := range reserved {
		listener.Close()
	}
	go func() {
		done <- agent.Run(ctx, agent.Options{ControlPlaneURL: cp.URL, InternalCAPEM: string(ca.CertPEM), Token: "integration-token", Cluster: "integration", Member: s.NodeID, DataDir: dir, Listen: addr, Engine: baospec.Engine, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}, New())
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case e := <-done:
			if e != nil {
				t.Error(e)
			}
		case <-time.After(65 * time.Second):
			t.Error("agent shutdown timed out")
		}
	})
	eventually(t, "agent API", func() bool { _, e := h.client.Status(context.Background()); return e == nil })
	return h
}
func (h *nativeAgent) native() (*baoapi.Client, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return localClient(h.config, false)
}
func (h *nativeAgent) status() baospec.StatusResponse {
	var s baospec.StatusResponse
	_ = h.client.Engine(context.Background(), baospec.Engine, baospec.ActionStatus, struct{}{}, &s)
	return s
}
func (h *nativeAgent) pid() int {
	s, e := h.client.Status(context.Background())
	if e != nil {
		return 0
	}
	return s.Processes[baospec.Process].PID
}
func (h *nativeAgent) update(t *testing.T, fn func(*agentapi.MemberConfig)) {
	t.Helper()
	h.mu.Lock()
	fn(&h.config)
	h.config.ConfigVersion++
	h.mu.Unlock()
	if e := h.client.Reload(context.Background()); e != nil {
		t.Fatal(e)
	}
}
func TestNativeOpenBao(t *testing.T) {
	binary := os.Getenv("KARAKORAM_OPENBAO_BIN")
	if binary == "" {
		t.Skip("set KARAKORAM_OPENBAO_BIN to an installed OpenBao binary")
	}
	ca, e := pki.NewCA("integration", time.Hour)
	if e != nil {
		t.Fatal(e)
	}
	specs := make([]baospec.MemberSpec, 3)
	reserved := reservePorts(t, 4*len(specs))
	for i := range specs {
		a := reserved[4*i].Addr().(*net.TCPAddr).Port
		c := reserved[4*i+1].Addr().(*net.TCPAddr).Port
		m := reserved[4*i+2].Addr().(*net.TCPAddr).Port
		specs[i] = baospec.MemberSpec{NodeID: fmt.Sprintf("member%d", i), Version: "2.7.1", BinaryPath: binary, ListenAddress: "127.0.0.1", APIAddr: fmt.Sprintf("https://127.0.0.1:%d", a), ClusterAddr: fmt.Sprintf("https://127.0.0.1:%d", c), APIPort: a, ClusterPort: c, MetricsPort: m, Storage: baospec.Storage{Type: baospec.StorageRaft}, Seal: baospec.Seal{Type: baospec.SealShamir}, ProcessGeneration: 1}
	}
	for i := range specs {
		for j := range specs {
			if i != j {
				specs[i].Storage.RetryJoin = append(specs[i].Storage.RetryJoin, baospec.RetryJoin{LeaderAPIAddr: specs[j].APIAddr, TLSServerName: specs[j].NodeID})
			}
		}
	}
	if plugin := os.Getenv("KARAKORAM_OPENBAO_MONGODB_PLUGIN"); plugin != "" {
		for i := range specs {
			specs[i].PluginDirectory = filepath.Dir(plugin)
			specs[i].Plugins = []baospec.Plugin{{Type: "database", Name: "mongodb-database-plugin", Command: filepath.Base(plugin), Version: "v0.0.1"}}
		}
	}
	nodes := make([]*nativeAgent, 3)
	for i := range nodes {
		nodes[i] = startNativeAgent(t, ca, specs[i], reserved[4*i:4*i+4])
		eventually(t, "uninitialized sealed status", func() bool { s := nodes[i].status(); return s.Error == "" && !s.Initialized && s.Sealed })
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	private, public, e := baospec.NewRecipientKey(rand.Reader)
	if e != nil {
		t.Fatal(e)
	}
	req := baospec.InitRequest{ReceiptID: "integration_receipt_0001", RecipientPublicKey: public, SecretShares: 3, SecretThreshold: 2}
	var init, repeat baospec.InitResponse
	if e = nodes[0].client.Engine(ctx, baospec.Engine, baospec.ActionInit, req, &init); e != nil {
		t.Fatal(e)
	}
	req.ReceiptID = "different_receipt_0002"
	if e = nodes[0].client.Engine(ctx, baospec.Engine, baospec.ActionInit, req, &repeat); e != nil {
		t.Fatal(e)
	}
	if !reflect.DeepEqual(init, repeat) {
		t.Fatal("init replay changed encrypted envelope")
	}
	plain, e := baospec.OpenReceipt(private, "integration", specs[0].NodeID, init.Envelope)
	if e != nil {
		t.Fatal(e)
	}
	var material baospec.InitMaterial
	if e = json.Unmarshal(plain, &material); e != nil {
		t.Fatal(e)
	}
	clear(plain)
	keys := material.KeysBase64
	if len(keys) == 0 {
		keys = material.Keys
	}
	if len(keys) != 3 || material.RootToken == "" {
		t.Fatal("missing initialization material")
	}
	receiptBytes, err := os.ReadFile(filepath.Join(nodes[0].dir, "karakoram/openbao/init-receipt.enc"))
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range append(append([]string{material.RootToken}, material.Keys...), material.KeysBase64...) {
		if secret != "" && bytes.Contains(receiptBytes, []byte(secret)) {
			t.Fatal("plaintext initialization material in encrypted receipt")
		}
	}
	// A sealed native server is still reachable and contributes seal state.
	metrics, err := nodes[0].client.Metrics(ctx)
	if err != nil || !bytes.Contains(metrics, []byte("karakoram_openbao_sealed 1")) ||
		!bytes.Contains(metrics, []byte("karakoram_openbao_up 1")) {
		t.Fatalf("sealed member metrics unavailable: %v", err)
	}
	err = nodes[0].client.Engine(ctx, baospec.Engine, baospec.ActionInitAck, baospec.InitAckRequest{ReceiptID: "wrong_receipt_identity"}, &agentapi.ActionResponse{})
	if response, ok := err.(*agentapi.HTTPError); !ok || response.StatusCode != http.StatusConflict {
		t.Fatalf("mismatching receipt acknowledgement did not conflict: %v", err)
	}
	unseal := func(i int) {
		t.Helper()
		eventually(t, "unseal member", func() bool {
			c, e := nodes[i].native()
			if e != nil {
				return false
			}
			s, e := c.UnsealWith(ctx, keys[:2])
			return e == nil && !s.Sealed
		})
	}
	unseal(0)
	eventually(t, "active seed", func() bool { return nodes[0].status().Active })
	for i := 1; i < 3; i++ {
		unseal(i)
	}
	client, e := nodes[0].native()
	if e != nil {
		t.Fatal(e)
	}
	client = client.WithToken(material.RootToken)
	eventually(t, "three healthy voters", func() bool { s, e := client.AutopilotState(ctx); return e == nil && s.Healthy && len(s.Voters) == 3 })
	t.Run("plugin", func(t *testing.T) {
		if len(specs[0].Plugins) == 0 {
			t.Skip("set KARAKORAM_OPENBAO_MONGODB_PLUGIN to test declarative plugin registration")
		}
		var out map[string]any
		if e := client.Do(ctx, "GET", "sys/plugins/catalog/database/mongodb-database-plugin?version=v0.0.1", nil, &out); e != nil {
			t.Fatal(e)
		}
	})
	// Retain a live exposition for inspecting the native metric source vocabulary.
	mc, _ := localClient(nodes[0].config, true)
	body, e := mc.Stream(ctx, "GET", "sys/metrics?format=prometheus", nil, "")
	if e != nil {
		t.Fatal(e)
	}
	exposition, e := io.ReadAll(body)
	body.Close()
	if e != nil {
		t.Fatal(e)
	}
	for _, line := range strings.Split(string(exposition), "\n") {
		if strings.HasPrefix(line, "# TYPE vault_") {
			t.Log(line)
		}
	}
	if e = nodes[0].client.Engine(ctx, baospec.Engine, baospec.ActionInitAck, baospec.InitAckRequest{ReceiptID: init.Envelope.ReceiptID}, &agentapi.ActionResponse{}); e != nil {
		t.Fatal(e)
	}
	if _, e = os.Stat(filepath.Join(nodes[0].dir, "karakoram/openbao/init-receipt.enc")); !os.IsNotExist(e) {
		t.Fatal("receipt remains after acknowledgement")
	}
	old := nodes[2].pid()
	if old == 0 {
		t.Fatal("missing child PID")
	}
	if e = syscall.Kill(old, syscall.SIGKILL); e != nil {
		t.Fatal(e)
	}
	eventually(t, "crashed child restarted sealed", func() bool {
		s := nodes[2].status()
		p := nodes[2].pid()
		return p != 0 && p != old && s.Error == "" && s.Sealed
	})
	unseal(2)
	old = nodes[2].pid()
	rotated := leaf(t, ca, specs[2].NodeID)
	nodes[2].update(t, func(c *agentapi.MemberConfig) { c.TLS.CertPEM = rotated.CertPEM; c.TLS.KeyPEM = rotated.KeyPEM })
	eventually(t, "rotated listener certificate", func() bool {
		pool := x509.NewCertPool()
		pool.AppendCertsFromPEM(ca.CertPEM)
		conn, e := tls.DialWithDialer(&net.Dialer{Timeout: time.Second}, "tcp", fmt.Sprintf("127.0.0.1:%d", specs[2].APIPort), &tls.Config{RootCAs: pool, ServerName: specs[2].NodeID, MinVersion: tls.VersionTLS12})
		if e != nil {
			return false
		}
		defer conn.Close()
		pair, _ := tls.X509KeyPair([]byte(rotated.CertPEM), []byte(rotated.KeyPEM))
		return bytes.Equal(conn.ConnectionState().PeerCertificates[0].Raw, pair.Certificate[0])
	})
	if nodes[2].pid() != old {
		t.Fatal("certificate reload restarted OpenBao")
	}
	nodes[2].update(t, func(c *agentapi.MemberConfig) {
		var s baospec.MemberSpec
		_ = json.Unmarshal(c.EngineSpec, &s)
		s.Storage.RetryJoin = s.Storage.RetryJoin[:1]
		c.EngineSpec, _ = json.Marshal(s)
	})
	if nodes[2].pid() != old {
		t.Fatal("retry_join update restarted OpenBao")
	}
	nodes[2].update(t, func(c *agentapi.MemberConfig) {
		var s baospec.MemberSpec
		_ = json.Unmarshal(c.EngineSpec, &s)
		s.ProcessGeneration++
		c.EngineSpec, _ = json.Marshal(s)
	})
	eventually(t, "generation restart", func() bool {
		s := nodes[2].status()
		return nodes[2].pid() != old && s.Error == "" && s.Sealed && s.ProcessGeneration == 2
	})
	unseal(2)
	secrets := append(append([]string{material.RootToken}, material.Keys...), material.KeysBase64...)
	for _, n := range nodes {
		e = filepath.WalkDir(n.dir, func(path string, d os.DirEntry, e error) error {
			if e != nil {
				return e
			}
			if d.IsDir() {
				return nil
			}
			b, e := os.ReadFile(path)
			if e != nil {
				return e
			}
			for _, secret := range secrets {
				if secret != "" && bytes.Contains(b, []byte(secret)) {
					return fmt.Errorf("plaintext initialization material in %s", filepath.Base(path))
				}
			}
			return nil
		})
		if e != nil {
			t.Fatal(e)
		}
		logs, e := n.client.Logs(ctx, baospec.Process, 1000)
		if e != nil {
			t.Fatal(e)
		}
		b, _ := json.Marshal(logs)
		for _, secret := range secrets {
			if secret != "" && bytes.Contains(b, []byte(secret)) {
				t.Fatal("plaintext initialization material in logs")
			}
		}
	}
	t.Log("three-member TLS Raft formation, encrypted init replay/ack, healthy voters, child restart/re-unseal, SIGHUP certificate rotation, generation restart, and plaintext scans passed")
}
