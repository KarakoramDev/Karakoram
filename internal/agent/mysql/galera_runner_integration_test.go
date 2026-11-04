//go:build integration

// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agent"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

// Only the fake control-plane transport gets a test-local certificate. Every
// engine and agent-serving certificate/credential is retained from Core export.
func startNativeGaleraAgent(t *testing.T, ctx context.Context, fixture nativeGaleraConfig, s myspec.Spec, dir string) *groupRunnerAgent {
	t.Helper()
	// Native provisioning supplies the empty durable directory, just as a
	// backend mounts a newly allocated volume before launching the agent.
	// Keep the CP VolumeIdentity intact: production volumeSafety verifies the
	// empty-volume authorization and creates its own durable sentinel.
	memberDir := filepath.Join(dir, s.Member)
	if err := privateDir(memberDir); err != nil {
		t.Fatal(err)
	}
	// Keep harness diagnostics outside the durable volume: an unknown log in
	// a fresh volume would correctly make volumeSafety refuse initialization.
	logPath := filepath.Join(dir, s.Member+"-agent.log")
	agentLog, err := os.OpenFile(logPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = agentLog.Close() })
	a := &groupRunnerAgent{t: t, spec: s, runner: New(), done: make(chan error, 1)}
	a.config = fixture.config
	raw, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	a.config.EngineSpec = raw
	a.config.ConfigVersion = 1
	if s.Role == myspec.RoleProxy {
		a.config.DesiredProcesses = []string{"proxysql"}
	} else {
		a.config.DesiredProcesses = []string{"mysqld"}
	}
	token := "native-galera-token-" + s.Member
	ca, cert := coreTLS(t)
	pair, err := tls.X509KeyPair([]byte(cert.CertPEM), []byte(cert.KeyPEM))
	if err != nil {
		t.Fatal(err)
	}
	cp := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" || r.URL.Path != "/internal/agent/v1/config" || !agentapi.AuthenticateBearer(r, token) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		a.mu.Lock()
		defer a.mu.Unlock()
		_ = json.NewEncoder(w).Encode(a.config)
	}))
	cp.TLS = &tls.Config{Certificates: []tls.Certificate{pair}, MinVersion: tls.VersionTLS12}
	cp.StartTLS()
	port := nativeTestPort(t)
	address := galeraAddress("127.0.0.1", port)
	hostname := nativeCertificateName(t, a.config.AgentTLS.CertPEM)
	a.client, err = agentapi.NewClient(agentapi.ClientOptions{BaseURL: "https://" + galeraAddress(hostname, port), Token: token, CAPEM: []byte(s.CAPEM), Timeout: 4 * time.Minute, EngineTimeout: 3 * time.Minute, DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, address)
	}})
	if err != nil {
		t.Fatal(err)
	}
	runCtx, cancel := context.WithCancel(context.Background())
	a.cancel = cancel
	t.Cleanup(func() {
		if t.Failed() {
			current := a.spec
			_ = agentLog.Sync()
			observation, cancelObservation := context.WithTimeout(context.Background(), 2*time.Second)
			status, statusErr := a.client.Status(observation)
			cancelObservation()
			t.Logf("%s failure snapshot revision=%d maintenance=%v apply=%s status=%s", current.Member, status.ConfigVersion, status.Maintenance, nativeGaleraRedact(current, token, status.Error), nativeGaleraRedact(current, token, fmt.Sprint(statusErr)))
			for name, process := range status.Processes {
				t.Logf("%s owned process=%s state=%s pid=%d restarts=%d error=%s", current.Member, name, process.State, process.PID, process.Restarts, nativeGaleraRedact(current, token, process.Error))
			}
			if status.Volume != nil {
				t.Logf("%s volume verified=%v lost=%v diagnostic=%s", current.Member, status.Volume.Verified, status.Volume.Lost, nativeGaleraRedact(current, token, status.Volume.Message))
			}
			nativeGaleraStartupDiagnostics(t, current, token, memberDir, logPath)
		}
		a.close()
		a.client.Close()
		cp.Close()
	})
	go func() {
		a.done <- agent.Run(runCtx, agent.Options{ControlPlaneURL: cp.URL, InternalCAPEM: ca, Token: token, Cluster: a.config.Cluster, Member: s.Member, DataDir: memberDir, Listen: address, Engine: "mysql", Logger: slog.New(slog.NewTextHandler(agentLog, nil))}, a.runner)
	}()
	readiness, stopReadiness := context.WithTimeout(ctx, 3*time.Minute)
	defer stopReadiness()
	var lastStatus agentapi.Status
	var lastStatusErr error
	lastError := ""
	for readiness.Err() == nil {
		select {
		case runErr := <-a.done:
			// Preserve the terminal result for the existing cleanup owner.
			a.done <- runErr
			_ = agentLog.Sync()
			nativeGaleraStartupDiagnostics(t, s, token, memberDir, logPath)
			t.Fatalf("%s agent exited before configuration acknowledgement: %s", s.Member, nativeGaleraRedact(s, token, fmt.Sprint(runErr)))
		default:
		}
		// Status is an observation, not a lifecycle mutation. Do not inherit
		// the four-minute synchronous Reload deadline for an individual poll.
		observation, cancelObservation := context.WithTimeout(readiness, 2*time.Second)
		status, statusErr := a.client.Status(observation)
		cancelObservation()
		lastStatus, lastStatusErr = status, statusErr
		if statusErr == nil && status.ConfigVersion == 1 && status.Error == "" {
			return a
		}
		message := status.Error
		if statusErr != nil {
			message = statusErr.Error()
		}
		if message != "" && message != lastError {
			lastError = message
			t.Logf("%s configuration not ready: %s", s.Member, nativeGaleraRedact(s, token, message))
		}
		select {
		case <-readiness.Done():
		case <-time.After(100 * time.Millisecond):
		}
	}
	_ = agentLog.Sync()
	for name, process := range lastStatus.Processes {
		t.Logf("%s owned process=%s state=%s pid=%d restarts=%d error=%s", s.Member, name, process.State, process.PID, process.Restarts, nativeGaleraRedact(s, token, process.Error))
	}
	nativeGaleraStartupDiagnostics(t, s, token, memberDir, logPath)
	t.Fatalf("%s initial revision not acknowledged: observed=%d apply=%s status=%s deadline=%v", s.Member, lastStatus.ConfigVersion, nativeGaleraRedact(s, token, lastStatus.Error), nativeGaleraRedact(s, token, fmt.Sprint(lastStatusErr)), readiness.Err())
	return a
}

func nativeGaleraRedact(s myspec.Spec, token, text string) string {
	values := []string{token, s.Superuser.Password, s.Agent.Password, s.BackupUser.Password, s.Replication.Password, s.Monitor.Password, s.Admin.Password}
	if s.ServerCert != nil {
		values = append(values, s.ServerCert.CertPEM, s.ServerCert.KeyPEM)
	}
	if s.Galera != nil && s.Galera.TrafficCert != nil {
		values = append(values, s.Galera.TrafficCert.CertPEM, s.Galera.TrafficCert.KeyPEM)
	}
	if s.Proxy != nil {
		values = append(values, s.Proxy.Admin.Password, s.Proxy.Monitor.Password)
		for _, user := range s.Proxy.Users {
			values = append(values, user.Password)
		}
	}
	if s.Backup != nil {
		values = append(values, s.Backup.Repository.S3.AccessKey, s.Backup.Repository.S3.SecretKey)
	}
	for _, value := range values {
		if value == "" {
			continue
		}
		text = strings.ReplaceAll(text, value, "[redacted]")
		// A bounded log tail may contain only individual PEM body lines.
		for _, line := range strings.Split(value, "\n") {
			if len(line) > 16 {
				text = strings.ReplaceAll(text, line, "[redacted]")
			}
		}
	}
	lines := strings.Split(text, "\n")
	for i, line := range lines {
		lower := strings.ToLower(line)
		if strings.Contains(lower, "password") || strings.Contains(lower, "passwd") || strings.Contains(lower, "private key") || strings.Contains(lower, "identified by") || strings.Contains(lower, "authorization") || strings.Contains(lower, "bearer ") {
			lines[i] = "[redacted credential-bearing log line]"
		}
	}
	return strings.Join(lines, "\n")
}

// Read only owned diagnostic logs, never configuration, credentials or data.
// Bound traversal, file count and bytes while children are still alive, before
// registered cleanups remove the failed campaign's evidence.
func nativeGaleraStartupDiagnostics(t *testing.T, s myspec.Spec, token, memberDir, agentLog string) {
	t.Helper()
	logs := []string{agentLog}
	durableDirs := []string{memberDir}
	owned := map[string]bool{s.Member: true}
	for _, peer := range s.Peers {
		if !owned[peer.Name] {
			owned[peer.Name] = true
			durableDirs = append(durableDirs, filepath.Join(filepath.Dir(memberDir), peer.Name))
			logs = append(logs, filepath.Join(filepath.Dir(memberDir), peer.Name+"-agent.log"))
		}
	}
	var roots []string
	for _, durable := range durableDirs {
		p := dataPaths(durable)
		roots = append(roots, p.root, p.data)
	}
	for _, root := range roots {
		visited := 0
		_ = filepath.WalkDir(root, func(name string, entry os.DirEntry, walkErr error) error {
			if walkErr != nil {
				return nil
			}
			visited++
			if visited > 512 || len(logs) >= 24 {
				return filepath.SkipAll
			}
			if entry.IsDir() {
				if name != root {
					relative, _ := filepath.Rel(root, name)
					base := strings.ToLower(entry.Name())
					if strings.Count(relative, string(os.PathSeparator)) >= 2 || !strings.Contains(base, "sst") && !strings.HasPrefix(base, "joiner") && !strings.HasPrefix(base, "donor") && !strings.HasPrefix(base, "tmp") {
						return filepath.SkipDir
					}
				}
				return nil
			}
			base := strings.ToLower(entry.Name())
			if (strings.HasSuffix(base, ".log") || strings.HasSuffix(base, ".err")) && (strings.Contains(base, "error") || strings.HasPrefix(base, "innobackup") || strings.HasPrefix(base, "xtrabackup") || strings.HasPrefix(base, "sst") || strings.HasPrefix(base, "wsrep") || strings.HasPrefix(base, "galera-supervisor-recover")) {
				logs = append(logs, name)
			}
			return nil
		})
	}
	for _, name := range logs {
		info, err := os.Lstat(name)
		if err != nil || !info.Mode().IsRegular() {
			t.Logf("%s diagnostic log unavailable: %s", s.Member, filepath.Base(name))
			continue
		}
		file, err := os.Open(name)
		if err != nil {
			t.Logf("%s diagnostic log open failed: %s", s.Member, filepath.Base(name))
			continue
		}
		const maximum = 16 << 10
		truncated := info.Size() > maximum
		if truncated {
			_, err = file.Seek(-maximum, io.SeekEnd)
		}
		var raw []byte
		if err == nil {
			raw, err = io.ReadAll(io.LimitReader(file, maximum))
		}
		_ = file.Close()
		if err != nil {
			t.Logf("%s diagnostic log read failed: %s", s.Member, filepath.Base(name))
			continue
		}
		text := string(raw)
		if truncated {
			_, text, _ = strings.Cut(text, "\n")
		}
		relative, _ := filepath.Rel(filepath.Dir(memberDir), name)
		t.Logf("%s owned diagnostic %s (tail, sanitized):\n%s", s.Member, relative, nativeGaleraRedact(s, token, text))
	}
}
