// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

//go:build integration

package neo4j

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
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agent"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine/neo4j/neo4jspec"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type nativeNeoAgent struct {
	url, dir string
	client   *http.Client
	engine   *Engine
	spec     neo4jspec.Spec
	mu       sync.Mutex
	config   agentapi.MemberConfig
	cancel   context.CancelFunc
	done     chan error
	stopOnce sync.Once
}

var nativePorts = struct {
	sync.Mutex
	used map[int]bool
}{used: make(map[int]bool)}

func nativePort(t *testing.T) int {
	t.Helper()
	nativePorts.Lock()
	defer nativePorts.Unlock()
	// Do not hand future JVM listeners ports from the ephemeral source-port
	// range: control-plane polling can otherwise occupy them before Java binds.
	for range 100 {
		n, err := rand.Int(rand.Reader, big.NewInt(16000))
		if err != nil {
			t.Fatal(err)
		}
		port := 12000 + int(n.Int64())
		if nativePorts.used[port] {
			continue
		}
		l, err := net.Listen("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)))
		if err != nil {
			continue
		}
		l.Close()
		nativePorts.used[port] = true
		return port
	}
	t.Fatal("no free native integration port")
	return 0
}
func nativeTLS(t *testing.T) (string, neo4jspec.Certificate) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "localhost"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, DNSNames: []string{"localhost"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	rawKey, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	cert := string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
	return cert, neo4jspec.Certificate{CertPEM: cert, KeyPEM: string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: rawKey}))}
}
func startNativeNeoAgent(t *testing.T, s neo4jspec.Spec, dir string) *nativeNeoAgent {
	t.Helper()
	if dir == "" {
		dir = t.TempDir()
	}
	ca, cert := s.CAPEM, s.Server
	if ca == "" {
		ca, cert = nativeTLS(t)
	}
	s.CAPEM, s.Server = ca, cert
	s.AdvertisedHost = "localhost"
	pair, err := tls.X509KeyPair([]byte(cert.CertPEM), []byte(cert.KeyPEM))
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(s)
	h := &nativeNeoAgent{dir: dir, spec: s, done: make(chan error, 1), engine: New(), config: agentapi.MemberConfig{Cluster: "integration", Member: "neo4j-0", Engine: neo4jspec.Engine, EngineSpec: raw, AgentTLS: agentapi.Certificate{CertPEM: cert.CertPEM, KeyPEM: cert.KeyPEM}, ConfigVersion: 1}}
	if cached, err := os.ReadFile(filepath.Join(dir, "karakoram", "config.json")); err == nil {
		var previous agentapi.MemberConfig
		if err = json.Unmarshal(cached, &previous); err != nil {
			t.Fatal(err)
		}
		h.config.ConfigVersion = previous.ConfigVersion + 1
	}
	h.engine.listenAddress = "127.0.0.1"
	h.engine.boltPort = nativePort(t)
	h.engine.httpsPort = nativePort(t)
	h.engine.recoveryBoltPort = nativePort(t)
	h.engine.recoveryHTTPSPort = nativePort(t)
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
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM([]byte(ca))
	h.client = &http.Client{Transport: &http.Transport{Proxy: nil, TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}}, Timeout: 90 * time.Second}
	address := net.JoinHostPort("127.0.0.1", strconv.Itoa(nativePort(t)))
	h.url = "https://" + address
	ctx, cancel := context.WithCancel(context.Background())
	h.cancel = cancel
	logFile, err := os.CreateTemp(t.TempDir(), "agent-integration-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = logFile.Close() })
	go func() {
		h.done <- agent.Run(ctx, agent.Options{ControlPlaneURL: cp.URL, InternalCAPEM: ca, Token: "integration-token", Cluster: "integration", Member: "neo4j-0", DataDir: dir, Listen: address, Engine: neo4jspec.Engine, Logger: slog.New(slog.NewTextHandler(logFile, nil))}, h.engine)
	}()
	t.Cleanup(func() { h.stop(t); h.client.CloseIdleConnections() })
	// The production connector must not accept a connection before recovery has
	// durably recorded successful initialization of the managed administrator.
	deadline := time.Now().Add(150 * time.Second)
	for time.Now().Before(deadline) {
		if !exists(filepath.Join(dir, "karakoram", "neo4j", "credentials-initialized.json")) {
			conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", h.engine.httpsPort), 30*time.Millisecond)
			if err == nil {
				conn.Close()
				t.Fatal("network listener existed before admin initialization")
			}
		}
		if response, err := h.request("POST", "/v1/neo4j/readiness", struct{}{}); err == nil {
			var ready neo4jspec.Readiness
			_ = json.NewDecoder(response.Body).Decode(&ready)
			response.Body.Close()
			if ready.Ready {
				return h
			}
			if ready.ConfigurationError != "" {
				t.Logf("readiness: %#v", ready)
				break
			}
		}
		select {
		case err := <-h.done:
			h.done <- err
			t.Fatalf("native agent exited: %v", err)
		default:
		}
		time.Sleep(200 * time.Millisecond)
	}
	for _, path := range []string{"/v1/status", "/v1/logs?process=neo4j&lines=100"} {
		if response, err := h.request("GET", path, nil); err == nil {
			body, _ := io.ReadAll(io.LimitReader(response.Body, 32<<10))
			response.Body.Close()
			t.Logf("startup diagnostic %s: %s", path, body)
		} else {
			t.Logf("startup diagnostic %s: %v", path, err)
		}
	}
	if b, err := os.ReadFile(logFile.Name()); err == nil {
		t.Logf("agent startup log: %s", b)
	}
	for _, name := range []string{"neo4j.log", "debug.log"} {
		if b, err := os.ReadFile(filepath.Join(dir, "karakoram", "neo4j", "logs", name)); err == nil {
			if len(b) > 32<<10 {
				b = b[len(b)-(32<<10):]
			}
			t.Logf("native startup %s: %s", name, b)
		}
	}
	t.Fatal("native Neo4j did not become ready")
	return nil
}
func (h *nativeNeoAgent) request(method, path string, value any) (*http.Response, error) {
	var body io.Reader
	if value != nil {
		b, _ := json.Marshal(value)
		body = bytes.NewReader(b)
	}
	r, _ := http.NewRequest(method, h.url+path, body)
	r.Header.Set("Authorization", "Bearer integration-token")
	r.Header.Set("Content-Type", "application/json")
	return h.client.Do(r)
}
func (h *nativeNeoAgent) stop(t *testing.T) {
	t.Helper()
	h.stopOnce.Do(func() {
		h.cancel()
		select {
		case err := <-h.done:
			if err != nil {
				t.Errorf("native agent shutdown: %v", err)
			}
		case <-time.After(70 * time.Second):
			t.Error("native agent shutdown timed out")
		}
	})
}
func (h *nativeNeoAgent) rpc(t *testing.T, action string, request, response any) {
	t.Helper()
	r, err := h.request("POST", "/v1/neo4j/"+action, request)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Body.Close()
	if r.StatusCode != 200 {
		b, _ := io.ReadAll(r.Body)
		t.Fatalf("%s HTTP %d: %s", action, r.StatusCode, b)
	}
	if response != nil {
		if err = json.NewDecoder(r.Body).Decode(response); err != nil {
			t.Fatal(err)
		}
	}
}
func (h *nativeNeoAgent) job(t *testing.T, action string, q any) neo4jspec.JobResponse {
	t.Helper()
	deadline := time.Now().Add(5 * time.Minute)
	for time.Now().Before(deadline) {
		var r neo4jspec.JobResponse
		h.rpc(t, action, q, &r)
		if r.State == neo4jspec.JobDone {
			readyDeadline := time.Now().Add(120 * time.Second)
			for time.Now().Before(readyDeadline) {
				var ready neo4jspec.Readiness
				h.rpc(t, "readiness", struct{}{}, &ready)
				if ready.Ready {
					return r
				}
				time.Sleep(200 * time.Millisecond)
			}
			t.Fatal("server did not become ready after offline job")
			return r
		}
		if r.State == neo4jspec.JobFailed {
			t.Fatalf("%s failed: %s", action, r.Message)
		}
		time.Sleep(300 * time.Millisecond)
	}
	t.Fatalf("%s timeout", action)
	return neo4jspec.JobResponse{}
}
func (h *nativeNeoAgent) cypher(t *testing.T, db, statement string, params map[string]any) queryResult {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	r, err := query(ctx, h.spec, h.engine.httpsPort, h.spec.AdminPassword, db, statement, params)
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func (h *nativeNeoAgent) reload(t *testing.T, s neo4jspec.Spec) {
	t.Helper()
	h.mu.Lock()
	raw, _ := json.Marshal(s)
	h.config.EngineSpec = raw
	h.config.ConfigVersion++
	h.mu.Unlock()
	response, err := h.request("POST", "/v1/reload", struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(io.LimitReader(response.Body, 16<<10))
	response.Body.Close()
	if response.StatusCode != 200 {
		if bytes.Contains(body, []byte("command derivation failed")) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			output, diagnosticErr := nativeCommand(ctx, s, filepath.Join(h.dir, "karakoram", "neo4j", "conf"), "neo4j", "console", "--dry-run").CombinedOutput()
			cancel()
			if len(output) > 32<<10 {
				output = output[:32<<10]
			}
			t.Logf("native command derivation diagnostic: %v: %s", diagnosticErr, output)
		}
		t.Fatalf("reload HTTP %d: %s", response.StatusCode, body)
	}
	h.spec = s
	deadline := time.Now().Add(120 * time.Second)
	for time.Now().Before(deadline) {
		var r neo4jspec.Readiness
		h.rpc(t, "readiness", struct{}{}, &r)
		if r.Ready && r.AppliedRestartToken == s.RestartToken {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatal("updated spec did not become ready")
}
func nativeSpec(t *testing.T) neo4jspec.Spec {
	t.Helper()
	home, java := os.Getenv("KARAKORAM_NEO4J_HOME"), os.Getenv("KARAKORAM_NEO4J_JAVA_HOME")
	if home == "" || java == "" {
		t.Skip("set KARAKORAM_NEO4J_HOME and KARAKORAM_NEO4J_JAVA_HOME for native Neo4j")
	}
	return neo4jspec.Spec{Version: "2026.10.0", Install: neo4jspec.Install{Neo4jHome: home, JavaHome: java}, HeapMB: 256, PageCacheMB: 64, AdminPassword: "native-managed-password", RestartToken: "initial"}
}
func TestNativeNeo4j(t *testing.T) {
	h := startNativeNeoAgent(t, nativeSpec(t), "")
	t.Run("native metrics", func(t *testing.T) {
		response, err := h.request("GET", "/v1/metrics", nil)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		body, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
		if err != nil || response.StatusCode != 200 {
			t.Fatalf("metrics HTTP %d: %v", response.StatusCode, err)
		}
		for _, metric := range []string{"neo4j_up 1", "neo4j_heap_used_bytes ", "neo4j_heap_configured_bytes ", "neo4j_pagecache_configured_bytes ", "neo4j_nodes ", "neo4j_relationships ", "neo4j_transactions_active ", "neo4j_store_size_bytes "} {
			if !strings.Contains(string(body), metric) {
				t.Errorf("missing native metric %s", metric)
			}
		}
		if t.Failed() {
			t.Logf("metrics response: %s", body)
			for _, q := range []struct{ db, statement string }{
				{"neo4j", "CALL dbms.queryJmx('java.lang:type=Memory') YIELD attributes RETURN attributes.HeapMemoryUsage.value AS heap"},
				{"neo4j", "MATCH (n) RETURN count(n) AS value"},
				{"neo4j", "MATCH ()-[r]->() RETURN count(r) AS value"},
				{"system", "SHOW TRANSACTIONS YIELD transactionId RETURN count(transactionId) AS value"},
			} {
				b, _ := json.Marshal(map[string]string{"statement": q.statement})
				request, _ := http.NewRequest("POST", fmt.Sprintf("https://127.0.0.1:%d/db/%s/query/v2", h.engine.httpsPort, q.db), bytes.NewReader(b))
				request.Header.Set("Content-Type", "application/json")
				request.SetBasicAuth(neo4jspec.AdminUser, h.spec.AdminPassword)
				reply, err := h.client.Do(request)
				if err != nil {
					t.Logf("metric query %s: %v", q.statement, err)
					continue
				}
				b, _ = io.ReadAll(io.LimitReader(reply.Body, 16<<10))
				reply.Body.Close()
				t.Logf("metric query %s HTTP%d: %s", q.statement, reply.StatusCode, b)
			}
		}
	})
	t.Run("admin password idempotency", func(t *testing.T) {
		q := neo4jspec.AdminPasswordRequest{Receipt: "rotate-admin", Current: h.spec.AdminPassword, Next: "native-rotated-admin-password"}
		h.rpc(t, neo4jspec.ActionAdminPassword, q, nil)
		h.rpc(t, neo4jspec.ActionAdminPassword, q, nil)
		h.rpc(t, neo4jspec.ActionAdminPassword, neo4jspec.AdminPasswordRequest{Receipt: "rotate-admin-back", Current: q.Next, Next: q.Current}, nil)
	})
	t.Run("TLS and users", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := query(ctx, h.spec, h.engine.httpsPort, "wrong-password", "system", "SHOW USERS", nil); err == nil {
			t.Fatal("wrong password accepted")
		}
		// A cleartext Bolt negotiation must not be accepted when TLS is REQUIRED.
		c, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", h.engine.boltPort), time.Second)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		_ = c.SetDeadline(time.Now().Add(2 * time.Second))
		_, _ = c.Write([]byte{0x60, 0x60, 0xb0, 0x17, 0, 0, 0, 5, 0, 0, 0, 4, 0, 0, 0, 3, 0, 0, 0, 0})
		var reply [4]byte
		if _, err = io.ReadFull(c, reply[:]); err == nil && reply != [4]byte{} {
			t.Fatalf("cleartext Bolt negotiated %x", reply)
		}
		h.rpc(t, "users", neo4jspec.UserRequest{Op: neo4jspec.UserCreate, Name: "application", Password: "application-first-password"}, nil)
		var users neo4jspec.UserResponse
		h.rpc(t, "users", neo4jspec.UserRequest{Op: neo4jspec.UserList}, &users)
		found := false
		for _, u := range users.Users {
			if u.Name == "application" {
				found = true
			}
		}
		if !found {
			t.Fatal("created user absent")
		}
		h.rpc(t, "users", neo4jspec.UserRequest{Op: neo4jspec.UserPassword, Name: "application", Password: "application-next-password"}, nil)
		nativeUserAuthenticates(t, h, "application", "application-next-password", true)
		nativeUserAuthenticates(t, h, "application", "application-first-password", false)
		h.rpc(t, "users", neo4jspec.UserRequest{Op: neo4jspec.UserCreate, Name: "temporary", Password: "temporary-password"}, nil)
		h.rpc(t, "users", neo4jspec.UserRequest{Op: neo4jspec.UserDelete, Name: "temporary"}, nil)
	})
	t.Run("user errors and name bounds", func(t *testing.T) {
		name := strings.Repeat("a", 64)
		password := "native-user-bounds-password"
		h.rpc(t, neo4jspec.ActionUsers, neo4jspec.UserRequest{Op: neo4jspec.UserCreate, Name: name, Password: password}, nil)
		for _, tc := range []struct {
			request    neo4jspec.UserRequest
			statement  string
			parameters map[string]any
			status     int
		}{
			{neo4jspec.UserRequest{Op: neo4jspec.UserCreate, Name: name, Password: password}, "CREATE USER `" + name + "` SET PASSWORD $p CHANGE NOT REQUIRED", map[string]any{"p": password}, http.StatusConflict},
			{neo4jspec.UserRequest{Op: neo4jspec.UserPassword, Name: "missing_application", Password: password}, "ALTER USER `missing_application` SET PASSWORD $p CHANGE NOT REQUIRED", map[string]any{"p": password}, http.StatusNotFound},
			{neo4jspec.UserRequest{Op: neo4jspec.UserDelete, Name: "missing_application"}, "DROP USER `missing_application`", nil, http.StatusNotFound},
			{neo4jspec.UserRequest{Op: neo4jspec.UserCreate, Name: "invalid_password", Password: "tiny"}, "CREATE USER `invalid_password` SET PASSWORD $p CHANGE NOT REQUIRED", map[string]any{"p": "tiny"}, http.StatusBadRequest},
		} {
			// Observe the native status without ever logging its message or
			// parameters: messages may contain user-controlled query inputs.
			body, _ := json.Marshal(map[string]any{"statement": tc.statement, "parameters": tc.parameters})
			req, _ := http.NewRequest("POST", fmt.Sprintf("https://127.0.0.1:%d/db/system/query/v2", h.engine.httpsPort), bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			req.SetBasicAuth(neo4jspec.AdminUser, h.spec.AdminPassword)
			response, err := h.client.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			var failure queryResult
			err = json.NewDecoder(io.LimitReader(response.Body, 64<<10)).Decode(&failure)
			response.Body.Close()
			if err != nil || len(failure.Errors) != 1 {
				t.Fatal("native user request did not return one query error")
			}
			t.Logf("native user %s status: %s", tc.request.Op, failure.Errors[0].Code)
			nativeUserRequestStatus(t, h, tc.request, tc.status)
		}
		nativeUserRequestStatus(t, h, neo4jspec.UserRequest{Op: neo4jspec.UserCreate, Name: strings.Repeat("a", 65), Password: password}, http.StatusBadRequest)
		h.rpc(t, neo4jspec.ActionUsers, neo4jspec.UserRequest{Op: neo4jspec.UserDelete, Name: name}, nil)
	})
	t.Run("drift repair receipt", func(t *testing.T) {
		h.cypher(t, "system", "ALTER USER neo4j SET PASSWORD $p CHANGE NOT REQUIRED", map[string]any{"p": "native-drifted-admin-password"})
		response, err := h.request("POST", "/v1/neo4j/admin-password", neo4jspec.AdminPasswordRequest{Receipt: "drift-detection", Current: h.spec.AdminPassword, Next: "native-unused-password"})
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		if response.StatusCode != http.StatusConflict {
			t.Fatalf("credential drift HTTP %d, want409", response.StatusCode)
		}
		q := neo4jspec.RecoverAdminRequest{Receipt: "repair-admin-drift", AdminPassword: h.spec.AdminPassword}
		h.job(t, neo4jspec.ActionRecoverAdmin, q)
		pid := nativePID(t, h)
		h.job(t, neo4jspec.ActionRecoverAdmin, q)
		if nativePID(t, h) != pid {
			t.Fatal("recover-admin receipt replay restarted JVM")
		}
	})
	h.cypher(t, "neo4j", "CREATE (:NativeProof {value: $value})", map[string]any{"value": "preserved"})
	t.Run("restart token", func(t *testing.T) {
		before := nativePID(t, h)
		s := h.spec
		s.RestartToken = "restart-token"
		h.reload(t, s)
		if nativePID(t, h) == before {
			t.Fatal("restart token did not replace JVM")
		}
	})
	if binary := os.Getenv("KARAKORAM_NEO4J_VERSITYGW"); binary != "" {
		t.Run("backup restore", func(t *testing.T) {
			target := startNativeS3(t, binary)
			oldFactory := newJobS3
			newJobS3 = func(s neo4jspec.S3) (*minio.Client, func(), error) {
				client, err := minio.New(strings.TrimPrefix(s.Endpoint, "http://"), &minio.Options{Creds: credentials.NewStaticV4(s.AccessKey, s.SecretKey, ""), Region: s.Region, BucketLookup: minio.BucketLookupPath})
				return client, func() {}, err
			}
			defer func() { newJobS3 = oldFactory }()
			receipt := neo4jspec.BackupRequest{Receipt: "native-backup", Target: target}
			var admitted neo4jspec.JobResponse
			h.rpc(t, neo4jspec.ActionBackup, receipt, &admitted)
			if admitted.State != neo4jspec.JobRunning {
				t.Fatalf("backup admission: %#v", admitted)
			}
			competing := receipt
			competing.Receipt = "competing-native-backup"
			competing.Target.Prefix += "-competing"
			conflict, err := h.request("POST", "/v1/neo4j/backup", competing)
			if err != nil {
				t.Fatal(err)
			}
			conflict.Body.Close()
			if conflict.StatusCode != http.StatusConflict {
				t.Fatalf("competing job HTTP %d", conflict.StatusCode)
			}
			backup := h.job(t, neo4jspec.ActionBackup, receipt)
			if backup.Manifest == nil {
				t.Fatal("backup missing manifest")
			}
			var replay neo4jspec.JobResponse
			h.rpc(t, neo4jspec.ActionBackup, receipt, &replay)
			if replay.State != neo4jspec.JobDone {
				t.Fatal("receipt not replayed")
			}
			changed := receipt
			changed.Target.Prefix += "-different"
			r, err := h.request("POST", "/v1/neo4j/backup", changed)
			if err != nil {
				t.Fatal(err)
			}
			r.Body.Close()
			if r.StatusCode != 409 {
				t.Fatalf("mismatch HTTP %d", r.StatusCode)
			}
			// The source's managed password is already present in its system dump.
			// Restoring it in place must not mistake that postcondition for failure.
			h.cypher(t, "neo4j", "MATCH (n:NativeProof) SET n.value = 'after-backup'", nil)
			h.job(t, neo4jspec.ActionRestore, neo4jspec.RestoreRequest{Receipt: "native-in-place-restore", Source: target, Manifest: *backup.Manifest, AdminPassword: h.spec.AdminPassword})
			if rows := h.cypher(t, "neo4j", "MATCH (n:NativeProof) RETURN n.value AS value", nil).rows(); len(rows) != 1 || rows[0]["value"] != "preserved" {
				t.Fatalf("in-place restore did not replace graph data: %#v", rows)
			}
			s := nativeSpec(t)
			s.AdminPassword = "target-different-admin-password"
			restored := startNativeNeoAgent(t, s, "")
			restored.job(t, neo4jspec.ActionRestore, neo4jspec.RestoreRequest{Receipt: "native-restore", Source: target, Manifest: *backup.Manifest, AdminPassword: s.AdminPassword})
			rows := restored.cypher(t, "neo4j", "MATCH (n:NativeProof) RETURN n.value AS value", nil).rows()
			if len(rows) != 1 || rows[0]["value"] != "preserved" {
				t.Fatalf("data not restored: %#v", rows)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			if _, err = query(ctx, restored.spec, restored.engine.httpsPort, h.spec.AdminPassword, "system", "SHOW USERS", nil); err == nil {
				t.Fatal("source admin password still works")
			}
			var users neo4jspec.UserResponse
			restored.rpc(t, "users", neo4jspec.UserRequest{Op: neo4jspec.UserList}, &users)
			found := false
			for _, u := range users.Users {
				if u.Name == "application" {
					found = true
				}
			}
			if !found {
				t.Fatal("application user lost")
			}
			nativeUserAuthenticates(t, restored, "application", "application-next-password", true)
			restored.stop(t)
		})
	} else {
		t.Log("S3 case skipped: KARAKORAM_NEO4J_VERSITYGW is unset")
	}
	h.stop(t)
	t.Run("interrupted receipt releases stop hold", func(t *testing.T) {
		q := neo4jspec.BackupRequest{Receipt: "interrupted-operation", Target: neo4jspec.S3{Prefix: "interrupted/backup"}}
		hash, _ := requestHash(q)
		name, err := jobReceiptPath(filepath.Join(h.dir, "karakoram", "neo4j"), neo4jspec.ActionBackup, q.Receipt)
		if err != nil {
			t.Fatal(err)
		}
		if err = durableJSON(name, &jobReceipt{RequestHash: hash, JobResponse: neo4jspec.JobResponse{State: neo4jspec.JobRunning, Phase: neo4jspec.PhaseDumping}}); err != nil {
			t.Fatal(err)
		}
		controls := filepath.Join(h.dir, "karakoram", "controls.json")
		b, err := os.ReadFile(controls)
		if err != nil {
			t.Fatal(err)
		}
		var state map[string]any
		if err = json.Unmarshal(b, &state); err != nil {
			t.Fatal(err)
		}
		state["stopped"] = map[string]bool{neo4jspec.Process: true}
		if err = durableJSON(controls, state); err != nil {
			t.Fatal(err)
		}
		restarted := startNativeNeoAgent(t, h.spec, h.dir)
		var response neo4jspec.JobResponse
		restarted.rpc(t, neo4jspec.ActionBackup, q, &response)
		if response.State != neo4jspec.JobFailed || response.Message != "interrupted; retry with a new operation" {
			t.Fatalf("unexpected interrupted receipt: %#v", response)
		}
		restarted.cypher(t, "neo4j", "RETURN 1 AS ready", nil)
		restarted.stop(t)
	})
}
func nativePID(t *testing.T, h *nativeNeoAgent) int {
	t.Helper()
	r, err := h.request("GET", "/v1/status", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Body.Close()
	var status agentapi.Status
	if err = json.NewDecoder(r.Body).Decode(&status); err != nil {
		t.Fatal(err)
	}
	return status.Processes[neo4jspec.Process].PID
}
func startNativeS3(t *testing.T, binary string) neo4jspec.S3 {
	t.Helper()
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "backups"), 0700); err != nil {
		t.Fatal(err)
	}
	address := fmt.Sprintf("127.0.0.1:%d", nativePort(t))
	cmd := exec.Command(binary, "--port", address, "posix", dir)
	cmd.Env = []string{"PATH=/usr/bin:/bin", "ROOT_ACCESS_KEY=integration-access", "ROOT_SECRET_KEY=integration-secret"}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	t.Cleanup(func() {
		_ = cmd.Process.Signal(os.Interrupt)
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			_ = cmd.Process.Kill()
			<-done
		}
	})
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", address, 100*time.Millisecond)
		if err == nil {
			c.Close()
			return neo4jspec.S3{Endpoint: "http://" + address, Region: "us-east-1", Bucket: "backups", Prefix: "native/backup", PathStyle: true, AccessKey: "integration-access", SecretKey: "integration-secret"}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("versitygw failed to listen")
	return neo4jspec.S3{}
}
func TestNativeNeo4jUpgrade(t *testing.T) {
	s := nativeSpec(t)
	previous := os.Getenv("KARAKORAM_NEO4J_HOME_PREVIOUS")
	if previous == "" {
		t.Skip("set KARAKORAM_NEO4J_HOME_PREVIOUS for native upgrade")
	}
	current := s.Install.Neo4jHome
	s.Version = "2026.09.0"
	s.Install.Neo4jHome = previous
	h := startNativeNeoAgent(t, s, "")
	h.cypher(t, "neo4j", "CREATE (:UpgradeProof {value: 'retained'})", nil)
	s = h.spec
	s.Version = "2026.10.0"
	s.Install.Neo4jHome = current
	s.RestartToken = "upgrade"
	h.reload(t, s)
	rows := h.cypher(t, "neo4j", "MATCH (n:UpgradeProof) RETURN n.value AS value", nil).rows()
	if len(rows) != 1 || rows[0]["value"] != "retained" {
		t.Fatalf("upgrade lost data: %#v", rows)
	}
}

func nativeUserAuthenticates(t *testing.T, h *nativeNeoAgent, user, password string, want bool) {
	t.Helper()
	request, err := http.NewRequest("POST", fmt.Sprintf("https://127.0.0.1:%d/db/system/query/v2", h.engine.httpsPort), strings.NewReader(`{"statement":"SHOW USERS"}`))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.SetBasicAuth(user, password)
	response, err := h.client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	authenticated := response.StatusCode == http.StatusOK || response.StatusCode == http.StatusAccepted
	if authenticated != want {
		t.Fatalf("application credential authentication=%t, want %t (HTTP %d)", authenticated, want, response.StatusCode)
	}
}

func nativeUserRequestStatus(t *testing.T, h *nativeNeoAgent, request neo4jspec.UserRequest, want int) {
	t.Helper()
	response, err := h.request("POST", "/v1/neo4j/users", request)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, 64<<10))
	response.Body.Close()
	if err != nil || response.StatusCode != want {
		t.Fatalf("user %s HTTP %d, want %d", request.Op, response.StatusCode, want)
	}
	if request.Password != "" && bytes.Contains(body, []byte(request.Password)) {
		t.Fatal("user error exposed password")
	}
}
