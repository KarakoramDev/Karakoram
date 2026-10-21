//go:build integration

// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agent"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	mysqldriver "github.com/go-sql-driver/mysql"
)

// coreTLS deliberately shares its CA signing key with its initial leaf. Issue
// another leaf with a fresh key so this exercises actual cert/key replacement.
func renewedProxyCertificate(t *testing.T, ca string, original myspec.Certificate) myspec.Certificate {
	t.Helper()
	caBlock, _ := pem.Decode([]byte(ca))
	keyBlock, _ := pem.Decode([]byte(original.KeyPEM))
	leafBlock, _ := pem.Decode([]byte(original.CertPEM))
	authority, err := x509.ParseCertificate(caBlock.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := x509.ParsePKCS8PrivateKey(keyBlock.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(leafBlock.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	leaf.SerialNumber = big.NewInt(3)
	der, err := x509.CreateCertificate(rand.Reader, leaf, authority, &key.PublicKey, signer)
	if err != nil {
		t.Fatal(err)
	}
	private, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return myspec.Certificate{CertPEM: string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})), KeyPEM: string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: private}))}
}

func nativeProxyTLSRotation(t *testing.T, backend myspec.Spec) {
	t.Helper()
	if os.Getenv("KARAKORAM_PROXYSQL_BIN") == "" {
		t.Skip("set KARAKORAM_PROXYSQL_BIN for native ProxySQL TLS rotation")
	}
	adminAddress, clientAddress := nativeTestAddress(t), nativeTestAddress(t)
	t.Setenv("KARAKORAM_PROXYSQL_ADMIN_ADDR", adminAddress)
	t.Setenv("KARAKORAM_PROXYSQL_CLIENT_ADDR", clientAddress)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	began := time.Now()
	_, port := nativeServerListener(backend)
	s := backend
	s.Member = "proxy-tls"
	s.Role = myspec.RoleProxy
	s.AllowInitialize = false
	s.Proxy = &myspec.Proxy{Admin: myspec.Credentials{Username: "manager", Password: "native-proxy-password"}, Monitor: s.Monitor, Backends: []myspec.Peer{{Name: backend.Member, Host: "127.0.0.1", Port: port}}, Users: []myspec.ProxyUser{{Username: s.Admin.Username, Password: s.Admin.Password, RequireTLS: true}}, MaxConnections: 30}
	a := &groupRunnerAgent{t: t, spec: s, runner: New(), done: make(chan error, 1)}
	raw, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	a.config = agentapi.MemberConfig{Cluster: "proxy-tls", Member: s.Member, Role: s.Role, Engine: "mysql", EngineSpec: raw, ConfigVersion: 1, DesiredProcesses: []string{"proxysql"}, AgentTLS: agentapi.Certificate{CertPEM: s.ServerCert.CertPEM, KeyPEM: s.ServerCert.KeyPEM}}
	pair, err := tls.X509KeyPair([]byte(s.ServerCert.CertPEM), []byte(s.ServerCert.KeyPEM))
	if err != nil {
		t.Fatal(err)
	}
	cp := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" || r.URL.Path != "/internal/agent/v1/config" || !agentapi.AuthenticateBearer(r, "native-proxy-token") {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		a.mu.Lock()
		defer a.mu.Unlock()
		json.NewEncoder(w).Encode(a.config)
	}))
	cp.TLS = &tls.Config{Certificates: []tls.Certificate{pair}, MinVersion: tls.VersionTLS12}
	cp.StartTLS()
	defer cp.Close()
	dir := nativeTestDir(t)
	listen := nativeTestAddress(t)
	a.client, err = agentapi.NewClient(agentapi.ClientOptions{BaseURL: "https://" + listen, Token: "native-proxy-token", CAPEM: []byte(s.CAPEM)})
	if err != nil {
		t.Fatal(err)
	}
	runCtx, stop := context.WithCancel(context.Background())
	a.cancel = stop
	defer a.close()
	go func() {
		a.done <- agent.Run(runCtx, agent.Options{ControlPlaneURL: cp.URL, InternalCAPEM: s.CAPEM, Token: "native-proxy-token", Cluster: a.config.Cluster, Member: s.Member, DataDir: dir, Listen: listen, Engine: "mysql", Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}, a.runner)
	}()
	nativeGroupPoll(t, 65*time.Second, "proxy initial config", func() bool { st, e := a.client.Status(ctx); return e == nil && st.ConfigVersion == 1 && st.Error == "" })
	const tlsName = "native-proxy-tls-rotation"
	if err = mysqldriver.RegisterTLSConfig(tlsName, tlsConfig(x509Pool(s.CAPEM), "localhost")); err != nil {
		t.Fatal(err)
	}
	defer mysqldriver.DeregisterTLSConfig(tlsName)
	cfg := mysqldriver.NewConfig()
	cfg.User = s.Admin.Username
	cfg.Passwd = s.Admin.Password
	cfg.Net = "tcp"
	cfg.Addr = clientAddress
	cfg.TLSConfig = tlsName
	cfg.Timeout = time.Second
	cfg.ReadTimeout = 3 * time.Second
	cfg.WriteTimeout = 3 * time.Second
	app, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	defer app.Close()
	conn, err := app.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	var uuid string
	if err = conn.QueryRowContext(ctx, "SELECT @@server_uuid").Scan(&uuid); err != nil {
		t.Fatal(err)
	}
	before, err := a.client.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	renewed := renewedProxyCertificate(t, s.CAPEM, *s.ServerCert)
	extraCA, _ := coreTLS(t)
	a.spec.ServerCert = &renewed
	a.spec.CAPEM += extraCA
	phase := time.Now()
	a.publish(ctx, []string{"proxysql"})
	after, err := a.client.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if after.Processes["proxysql"].PID != before.Processes["proxysql"].PID {
		t.Fatal("TLS rotation restarted ProxySQL")
	}
	var status myspec.ProxyStatusResponse
	a.action(ctx, myspec.ActionProxyStatus, struct{}{}, &status)
	if status.TLSReloadFallback {
		t.Fatal("TLS rotation used restart fallback")
	}
	var sameUUID string
	if err = conn.QueryRowContext(ctx, "SELECT @@server_uuid").Scan(&sameUUID); err != nil || sameUUID != uuid {
		t.Fatalf("existing session disrupted: UUID=%s error=%v", sameUUID, err)
	}
	expected, _ := pem.Decode([]byte(renewed.CertPEM))
	secure := tlsConfig(x509Pool(a.spec.CAPEM), "localhost")
	secure.VerifyConnection = func(state tls.ConnectionState) error {
		if len(state.PeerCertificates) == 0 || !bytes.Equal(state.PeerCertificates[0].Raw, expected.Bytes) {
			return fmt.Errorf("frontend still presents old certificate")
		}
		return nil
	}
	const newTLSName = "native-proxy-tls-renewed"
	if err = mysqldriver.RegisterTLSConfig(newTLSName, secure); err != nil {
		t.Fatal(err)
	}
	defer mysqldriver.DeregisterTLSConfig(newTLSName)
	cfg.TLSConfig = newTLSName
	fresh, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Close()
	if err = fresh.QueryRowContext(ctx, "SELECT @@server_uuid").Scan(&sameUUID); err != nil || sameUUID != uuid {
		t.Fatalf("new TLS frontend/backend session failed: UUID=%s error=%v", sameUUID, err)
	}
	admin, err := proxyAdmin(ctx, a.spec)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	for variable, file := range map[string]string{"mysql-ssl_p2s_ca": "ca.pem", "mysql-ssl_p2s_cert": "server-cert.pem", "mysql-ssl_p2s_key": "server-key.pem"} {
		var value string
		if err = admin.QueryRowContext(ctx, "SELECT variable_value FROM runtime_global_variables WHERE variable_name="+proxyQuote(variable)).Scan(&value); err != nil || value != filepath.Join(dataPaths(dir).root, file) {
			t.Fatalf("backend TLS variable %s=%s error=%v", variable, value, err)
		}
	}
	t.Logf("ProxySQL cert/key/CA reload: %s; unchanged PID, old session retained, new verified certificate and backend TLS variables; total %s", time.Since(phase), time.Since(began))
}

func TestNativeMySQLProxyListeners(t *testing.T) {
	if os.Getenv("KARAKORAM_PROXYSQL_BIN") == "" {
		t.Skip("set KARAKORAM_PROXYSQL_BIN for native listener confinement")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	dir := nativeTestDir(t)
	s := testProxySpec()
	ca, cert := coreTLS(t)
	for name, data := range map[string]string{"ca.pem": ca, "server-cert.pem": cert.CertPEM, "server-key.pem": cert.KeyPEM, "proxysql-ca.pem": ca, "proxysql-cert.pem": cert.CertPEM, "proxysql-key.pem": cert.KeyPEM} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("KARAKORAM_PROXYSQL_ADMIN_ADDR", nativeTestAddress(t))
	t.Setenv("KARAKORAM_PROXYSQL_CLIENT_ADDR", nativeTestAddress(t))
	text, err := RenderProxyConfig(s, dir, dir)
	if err != nil {
		t.Fatal(err)
	}
	config := filepath.Join(dir, "proxysql.cnf")
	if err = os.WriteFile(config, []byte(text), 0600); err != nil {
		t.Fatal(err)
	}
	process := exec.CommandContext(ctx, nativeProxyBinary(), "-f", "--initial", "-c", config)
	process.Stdout, process.Stderr = io.Discard, io.Discard
	if err = process.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- process.Wait() }()
	t.Cleanup(func() {
		_ = process.Process.Signal(syscall.SIGTERM)
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			_ = process.Process.Kill()
			<-done
		}
	})
	var admin *sql.DB
	nativeGroupPoll(t, 15*time.Second, "native MySQL-only proxy admin", func() bool {
		admin, err = proxyAdmin(ctx, s)
		return err == nil
	})
	defer admin.Close()
	assertNativeMySQLOnlyProxy(t, admin)
}
