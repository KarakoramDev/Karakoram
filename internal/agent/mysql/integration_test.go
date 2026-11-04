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
	"crypto/x509/pkix"
	"database/sql"
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
	"sync"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agent"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	mysqldriver "github.com/go-sql-driver/mysql"
)

func coreTLS(t *testing.T) (string, myspec.Certificate) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "integration CA"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
	caDER, err := x509.CreateCertificate(rand.Reader, ca, ca, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	server := &x509.Certificate{SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "localhost"}, NotBefore: ca.NotBefore, NotAfter: ca.NotAfter, KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}, DNSNames: []string{"localhost", "mysql1"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}}
	der, err := x509.CreateCertificate(rand.Reader, server, ca, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	private, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})), myspec.Certificate{CertPEM: string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})), KeyPEM: string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: private}))}
}
func TestNativeMySQLAgent(t *testing.T) {
	if os.Getenv("KARAKORAM_MYSQL_BIN_DIR") == "" {
		t.Skip("set KARAKORAM_MYSQL_BIN_DIR for real agent integration")
	}
	dir := nativeTestDir(t)
	t.Setenv("KARAKORAM_MYSQL_PORT", fmt.Sprint(nativeTestPort(t)))
	s := coreTestSpec()
	ca, cert := coreTLS(t)
	s.CAPEM = ca
	s.ServerCert = &cert
	raw, _ := json.Marshal(s)
	config := agentapi.MemberConfig{Cluster: "integration", Member: s.Member, Role: s.Role, Engine: "mysql", EngineSpec: raw, DesiredProcesses: []string{"mysqld"}, ConfigVersion: 1, AgentTLS: agentapi.Certificate{CertPEM: cert.CertPEM, KeyPEM: cert.KeyPEM}}
	pair, err := tls.X509KeyPair([]byte(cert.CertPEM), []byte(cert.KeyPEM))
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	cp := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/internal/agent/v1/config" || r.Header.Get("Authorization") != "Bearer native-token" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		mu.Lock()
		defer mu.Unlock()
		_ = json.NewEncoder(w).Encode(config)
	}))
	cp.TLS = &tls.Config{Certificates: []tls.Certificate{pair}, MinVersion: tls.VersionTLS12}
	cp.StartTLS()
	defer cp.Close()
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM([]byte(ca))
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}}, Timeout: 70 * time.Second}
	defer client.CloseIdleConnections()
	var cancel context.CancelFunc
	var done chan error
	var address string
	var runner *Runner
	stop := func() {
		if cancel == nil {
			return
		}
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("agent shutdown: %v", err)
			}
		case <-time.After(65 * time.Second):
			t.Error("agent shutdown timed out")
		}
		cancel = nil
	}
	defer stop()
	start := func() {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		address = "https://" + l.Addr().String()
		listen := l.Addr().String()
		_ = l.Close()
		ctx, c := context.WithCancel(context.Background())
		cancel = c
		done = make(chan error, 1)
		runner = New()
		go func() {
			done <- agent.Run(ctx, agent.Options{ControlPlaneURL: cp.URL, InternalCAPEM: ca, Token: "native-token", Cluster: "integration", Member: s.Member, DataDir: dir, Listen: listen, Engine: "mysql", Logger: slog.New(slog.NewTextHandler(os.Stderr, nil))}, runner)
		}()
	}
	request := func(path string, value any) (int, []byte, error) {
		b, _ := json.Marshal(value)
		req, _ := http.NewRequest("POST", address+path, bytes.NewReader(b))
		req.Header.Set("Authorization", "Bearer native-token")
		resp, err := client.Do(req)
		if err != nil {
			return 0, nil, err
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		return resp.StatusCode, body, err
	}
	status := func() myspec.StatusResponse {
		deadline := time.Now().Add(55 * time.Second)
		for time.Now().Before(deadline) {
			code, b, err := request("/v1/mysql/status", struct{}{})
			if err == nil && code == 200 {
				var out myspec.StatusResponse
				if json.Unmarshal(b, &out) == nil && out.ServerUUID != "" {
					return out
				}
			}
			time.Sleep(100 * time.Millisecond)
		}
		log, _ := os.ReadFile(filepath.Join(dataPaths(dir).root, "mysql-error.log"))
		t.Fatalf("agent did not initialize: %s", log)
		return myspec.StatusResponse{}
	}
	start()
	first := status()
	if !first.DataIdentity.Initialized || first.MemberState != "ONLINE" {
		t.Fatalf("status %+v", first)
	}
	ctx := context.Background()
	db, err := runner.LocalAccessDB(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var users int
	if err = db.QueryRow("SELECT COUNT(*) FROM mysql.user WHERE user IN ('root','kk_agent','kk_backup','kk_monitor','kk_repl','admin')").Scan(&users); err != nil || users != 6 {
		t.Fatalf("system accounts=%d err=%v", users, err)
	}
	// The client verifies the CA and hostname; transport encryption is mandatory.
	tlsName := "native-agent-mysql"
	if err = mysqldriver.RegisterTLSConfig(tlsName, &tls.Config{RootCAs: roots, ServerName: "localhost", MinVersion: tls.VersionTLS12}); err != nil {
		t.Fatal(err)
	}
	defer mysqldriver.DeregisterTLSConfig(tlsName)
	login := func(secure bool) (*sql.DB, error) {
		cfg := mysqldriver.NewConfig()
		cfg.User = s.Admin.Username
		cfg.Passwd = s.Admin.Password
		cfg.Net = "tcp"
		_, port := nativeListener()
		cfg.Addr = fmt.Sprintf("127.0.0.1:%d", port)
		cfg.Timeout = 3 * time.Second
		if secure {
			cfg.TLSConfig = tlsName
		}
		db, err := sql.Open("mysql", cfg.FormatDSN())
		if err == nil {
			err = db.Ping()
		}
		return db, err
	}
	app, err := login(true)
	if err != nil {
		t.Fatal(err)
	}
	var fileDirectory, scratch string
	var loaded sql.NullString
	// This char-pointer system variable exposes the literal disabled sentinel,
	// not a protocol SQL NULL. Empty/path values permit file access.
	if err = app.QueryRow("SELECT @@secure_file_priv,@@tmpdir").Scan(&fileDirectory, &scratch); err != nil || fileDirectory != "NULL" {
		t.Fatalf("server-side SQL file access not disabled: value=%q err=%v", fileDirectory, err)
	}
	if filepath.Clean(scratch) != filepath.Clean(os.TempDir()) {
		t.Fatalf("native server did not retain the caller's temporary directory: %s", scratch)
	}
	marker := filepath.Join(dir, "private-runtime-marker")
	if err = os.WriteFile(marker, []byte("runtime-file-must-not-be-readable"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = app.QueryRow("SELECT LOAD_FILE(?)", marker).Scan(&loaded); err != nil || loaded.Valid {
		t.Fatalf("SQL administrator can read runtime files: %v", err)
	}
	if _, err = app.Exec("GRANT FILE ON *.* TO 'admin'@'%'"); err == nil {
		t.Fatal("SQL administrator restored its own FILE authority")
	}
	if _, err = app.Exec("ALTER USER 'kk_agent'@'localhost' IDENTIFIED BY 'forbidden-password'"); err == nil {
		t.Fatal("admin modified protected agent account")
	}
	_ = app.Close()
	app, err = login(false)
	if app != nil {
		_ = app.Close()
	}
	if err == nil {
		t.Fatal("non-TLS login accepted")
	}
	metrics := runner.Metrics(ctx, nil)
	names := map[string]bool{}
	for _, m := range metrics {
		names[m.Name] = true
	}
	for _, name := range []string{"karakoram_mysql_connections", "karakoram_mysql_questions_total", "karakoram_mysql_max_connections"} {
		if !names[name] {
			t.Errorf("missing metric %s", name)
		}
	}
	time.Sleep(10 * time.Millisecond)
	names = map[string]bool{}
	for _, m := range runner.Metrics(ctx, nil) {
		names[m.Name] = true
	}
	if !names["karakoram_mysql_questions_per_second"] {
		t.Error("missing Questions-derived QPS")
	}
	t.Run("ProxyTLSRotation", func(t *testing.T) { nativeProxyTLSRotation(t, s) })
	api, err := agentapi.NewClient(agentapi.ClientOptions{BaseURL: address, Token: "native-token", CAPEM: []byte(ca)})
	if err != nil {
		t.Fatal(err)
	}
	setSlowShutdown := func(slow bool) {
		t.Helper()
		before, err := api.Status(ctx)
		if err != nil {
			t.Fatal(err)
		}
		s.SlowShutdown = slow
		mu.Lock()
		config.EngineSpec, err = json.Marshal(s)
		config.ConfigVersion++
		revision := config.ConfigVersion
		mu.Unlock()
		if err != nil {
			t.Fatal(err)
		}
		if err = api.Reload(ctx); err != nil {
			t.Fatal(err)
		}
		want := 1
		if slow {
			want = 0
		}
		nativeGroupPoll(t, 20*time.Second, "live slow-shutdown revision", func() bool {
			after, err := api.Status(ctx)
			if err != nil || after.ConfigVersion != revision {
				return false
			}
			if after.Processes["mysqld"].PID != before.Processes["mysqld"].PID {
				t.Fatal("slow-shutdown revision restarted mysqld")
			}
			local, err := runner.LocalAccessDB(ctx)
			if err != nil {
				return false
			}
			var value int
			return local.QueryRowContext(ctx, "SELECT @@innodb_fast_shutdown").Scan(&value) == nil && value == want
		})
	}
	// Simulate an existing/restored account policy with the same durable password
	// receipt. A normal restart must revoke FILE without re-keying accounts.
	beforeRestart, err := runner.LocalAccessDB(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = beforeRestart.ExecContext(ctx, "GRANT FILE ON *.* TO 'admin'@'%'"); err != nil {
		t.Fatal(err)
	}
	if !credentialsRecorded(s, dataPaths(dir)) {
		t.Fatal("existing-account privilege regression requires an unchanged password receipt")
	}
	setSlowShutdown(true)
	stop()
	start()
	second := status()
	if second.ServerUUID != first.ServerUUID {
		t.Fatal("restart changed server identity")
	}
	local, err := runner.LocalAccessDB(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var filePrivilege string
	if err = local.QueryRowContext(ctx, "SELECT File_priv FROM mysql.user WHERE User='admin' AND Host='%'").Scan(&filePrivilege); err != nil || filePrivilege != "N" {
		t.Fatalf("existing-account startup retained FILE privilege: value=%q err=%v", filePrivilege, err)
	}
	var fastShutdown int
	if err = local.QueryRowContext(ctx, "SELECT @@innodb_fast_shutdown").Scan(&fastShutdown); err != nil || fastShutdown != 0 {
		t.Fatalf("restart did not retain slow shutdown: value=%d error=%v", fastShutdown, err)
	}
	api, err = agentapi.NewClient(agentapi.ClientOptions{BaseURL: address, Token: "native-token", CAPEM: []byte(ca)})
	if err != nil {
		t.Fatal(err)
	}
	setSlowShutdown(false)
	stop()
	// Restored data may contain source credentials. The startup init-file must
	// replace them with target sealed credentials without knowing the source.
	s.Agent.Password = "new-agent-password-123"
	s.Superuser.Password = "new-root-password-123"
	s.Admin.Password = "new-admin-password-123"
	mu.Lock()
	config.EngineSpec, _ = json.Marshal(s)
	config.ConfigVersion++
	mu.Unlock()
	start()
	rekeyed := status()
	if rekeyed.ServerUUID != first.ServerUUID {
		t.Fatal("re-keying changed data identity")
	}
	app, err = login(true)
	if err != nil {
		t.Fatalf("sealed credential re-key: %v", err)
	}
	_ = app.Close()
	deadlineApplied := time.Now().Add(20 * time.Second)
	for {
		b, err := os.ReadFile(filepath.Join(dir, "karakoram", "config.json"))
		var accepted agentapi.MemberConfig
		if err == nil && json.Unmarshal(b, &accepted) == nil && accepted.ConfigVersion == config.ConfigVersion {
			break
		}
		if time.Now().After(deadlineApplied) {
			t.Fatal("re-key configuration was not durably acknowledged")
		}
		time.Sleep(100 * time.Millisecond)
	}
	// Stop intent must precede the temporary socket-only GTID observation.
	stop()
	mu.Lock()
	config.DesiredProcesses = []string{}
	config.ConfigVersion++
	mu.Unlock()
	start()
	deadline := time.Now().Add(20 * time.Second)
	var offline myspec.OfflineGTIDResponse
	var lastResponse string
	for time.Now().Before(deadline) {
		code, b, err := request("/v1/mysql/offline-gtid", struct{}{})
		lastResponse = fmt.Sprintf("HTTP %d: %s (%v)", code, b, err)
		if err == nil && code == 200 && json.Unmarshal(b, &offline) == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !offline.Stopped || offline.ServerUUID != first.ServerUUID {
		log, _ := os.ReadFile(filepath.Join(dataPaths(dir).root, "offline-error.log"))
		t.Fatalf("offline observation %+v; %s\n%s", offline, lastResponse, log)
	}
	stop()
	// A foreign non-empty directory is retained, never initialized or deleted.
	if err = os.RemoveAll(filepath.Join(dir, "mysql")); err != nil {
		t.Fatal(err)
	}
	if err = os.RemoveAll(filepath.Join(dir, "karakoram", "mysql")); err != nil {
		t.Fatal(err)
	}
	if err = os.Mkdir(filepath.Join(dir, "mysql"), 0700); err != nil {
		t.Fatal(err)
	}
	sentinel := filepath.Join(dir, "mysql", "foreign")
	if err = os.WriteFile(sentinel, []byte("foreign-data"), 0600); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	config.DesiredProcesses = []string{"mysqld"}
	config.ConfigVersion++
	mu.Unlock()
	start()
	time.Sleep(2 * time.Second)
	b, err := os.ReadFile(sentinel)
	if err != nil || string(b) != "foreign-data" {
		t.Fatal("foreign data was modified")
	}
	if _, err = os.Stat(filepath.Join(dir, "mysql", "mysql")); !os.IsNotExist(err) {
		t.Fatal("foreign data was initialized")
	}
	stop()
	t.Log("real agent: initialization, six accounts, verified TLS-only admin, protected system user, metrics/QPS, live slow-shutdown toggle without restart and restart retention, restart identity, stopped offline GTID and foreign-data refusal passed")
}
