//go:build integration

// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package temporal

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
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agent"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine/temporal/tspec"
	"github.com/KarakoramDev/Karakoram/internal/pki"
	"github.com/jackc/pgx/v5"
)

type nativeRunner struct {
	*Runner
	node chan *agent.Node
}

// Routes captures the toolkit exposed by the real agent for native assertions.
func (r *nativeRunner) Routes(mux *http.ServeMux, n *agent.Node) {
	r.Runner.Routes(mux, n)
	select {
	case r.node <- n:
	default:
	}
}

func freePort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	listener.Close()
	return port
}

// TestNativeTemporal exercises native schema, mTLS, upgrade and namespace lifecycles.
func TestNativeTemporal(t *testing.T) {
	install, previous, ui, pg := os.Getenv("KARAKORAM_TEMPORAL_DIR"), os.Getenv("KARAKORAM_TEMPORAL_PREV_DIR"), os.Getenv("KARAKORAM_TEMPORAL_UI_DIR"), os.Getenv("KARAKORAM_PG_BIN_DIR")
	if install == "" || ui == "" || pg == "" {
		t.Skip("native Temporal, UI and PostgreSQL artifact directories are required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	root := t.TempDir()
	ca, err := pki.NewCA("native-temporal", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	cert, key, err := ca.Issue(pki.CertRequest{CommonName: "native", DNSNames: []string{"internode.test", "frontend.test"}, IPs: []net.IP{net.ParseIP("127.0.0.1")}, Server: true, Client: true, Validity: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	write := func(path string, data []byte) {
		t.Helper()
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	write(filepath.Join(root, "postgres.pem"), cert)
	write(filepath.Join(root, "postgres-key.pem"), key)
	pgdata := filepath.Join(root, "pgdata")
	init := exec.CommandContext(ctx, filepath.Join(pg, "initdb"), "-D", pgdata, "-U", "postgres", "--auth-local=trust", "--auth-host=trust", "--no-locale", "--encoding=UTF8")
	if output, err := init.CombinedOutput(); err != nil {
		t.Fatalf("initdb: %v %s", err, output)
	}
	write(filepath.Join(pgdata, "pg_hba.conf"), []byte("local all all trust\nhostssl all all 127.0.0.1/32 scram-sha-256\n"))
	port := freePort(t)
	log, err := os.OpenFile(filepath.Join(root, "postgres.log"), os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(ctx, filepath.Join(pg, "postgres"), "-D", pgdata, "-h", "127.0.0.1", "-p", strconv.Itoa(port), "-k", root, "-c", "ssl=on", "-c", "ssl_cert_file="+filepath.Join(root, "postgres.pem"), "-c", "ssl_key_file="+filepath.Join(root, "postgres-key.pem"), "-c", "shared_buffers=32MB", "-c", "max_connections=80")
	cmd.Stdout, cmd.Stderr = log, log
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGINT)
		done := make(chan struct{})
		go func() { _ = cmd.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
			<-done
		}
		log.Close()
	})
	var admin *pgx.Conn
	for deadline := time.Now().Add(15 * time.Second); time.Now().Before(deadline); {
		admin, err = pgx.Connect(ctx, fmt.Sprintf("host=%s port=%d user=postgres dbname=postgres sslmode=disable", root, port))
		if err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil {
		t.Fatal("PostgreSQL did not start")
	}
	defer admin.Close(context.Background())
	for _, sql := range []string{"CREATE ROLE temporal_owner LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD 'native-password'", "CREATE DATABASE temporal OWNER temporal_owner", "CREATE DATABASE temporal_visibility OWNER temporal_owner"} {
		if _, err := admin.Exec(ctx, sql); err != nil {
			t.Fatal(err)
		}
	}
	s := testSpec()
	s.TLS = tspec.TLS{CAPEM: string(ca.CertPEM), InternodeCertPEM: string(cert), InternodeKeyPEM: string(key), InternodeServerName: "internode.test", FrontendCertPEM: string(cert), FrontendKeyPEM: string(key), FrontendServerName: "frontend.test", RequireClientAuth: true}
	s.Persistence = &tspec.Database{Host: "127.0.0.1", Port: port, Database: "temporal", VisibilityDatabase: "temporal_visibility", Username: "temporal_owner", Password: "native-password", CAPEM: string(ca.CertPEM), MaxConns: 12, VisibilityMaxConns: 5}
	selected := install
	if previous != "" {
		selected = previous
		s.Version = "1.31.4"
		s.RestartToken = "previous"
	}
	dataDir := filepath.Join(root, "server")
	_, files, err := render(s, dataDir, "127.0.0.1", "127.0.0.1", selected)
	if err != nil {
		t.Fatal(err)
	}
	for name, data := range files {
		write(filepath.Join(dataDir, "karakoram", name), data)
	}
	// Independent sessions contend on the same advisory lock. Both must safely
	// converge on the initial binary's schema as the non-superuser bound owner.
	var wg sync.WaitGroup
	schemaErrors := make(chan error, 2)
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, err := ensureSchema(ctx, s, selected, filepath.Join(dataDir, "karakoram", "temporal"))
			schemaErrors <- err
		}()
	}
	wg.Wait()
	close(schemaErrors)
	for err := range schemaErrors {
		if err != nil {
			t.Fatal(err)
		}
	}
	t.Log("concurrent schema ensure: two locked migrations succeeded as NOSUPERUSER owner")
	configuration := agentapi.MemberConfig{Engine: tspec.Engine, Cluster: "test", Member: "testn0", ConfigVersion: 1, AgentTLS: agentapi.Certificate{CertPEM: string(cert), KeyPEM: string(key)}}
	configuration.EngineSpec, _ = json.Marshal(s)
	var configMu sync.RWMutex
	cp := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/internal/agent/v1/config" {
			http.NotFound(w, request)
			return
		}
		configMu.RLock()
		defer configMu.RUnlock()
		writeJSON(w, configuration)
	}))
	pair, err := tls.X509KeyPair(cert, key)
	if err != nil {
		t.Fatal(err)
	}
	cp.TLS = &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{pair}}
	cp.StartTLS()
	t.Cleanup(cp.Close)
	startAgent := func(r *nativeRunner, dir, member string, c agentapi.MemberConfig) (*agent.Node, context.CancelFunc, <-chan error) {
		t.Helper()
		// Each agent pulls an immutable fixture from its own authenticated TLS endpoint.
		fixture := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, c) }))
		fixture.TLS = &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{pair}}
		fixture.StartTLS()
		t.Cleanup(fixture.Close)
		runCtx, stop := context.WithCancel(ctx)
		done := make(chan error, 1)
		options := agent.Options{Engine: tspec.Engine, Cluster: "test", Member: member, Token: "native-token", ControlPlaneURL: fixture.URL, InternalCAPEM: string(ca.CertPEM), DataDir: dir, Listen: "127.0.0.1:0", Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
		go func() { defer close(done); done <- agent.Run(runCtx, options, r) }()
		t.Cleanup(func() {
			stop()
			select {
			case err := <-done:
				if err != nil {
					t.Error(err)
				}
			case <-time.After(70 * time.Second):
				t.Error("native agent did not shut down")
			}
		})
		select {
		case node := <-r.node:
			return node, stop, done
		case err := <-done:
			t.Fatalf("agent startup: %v", err)
		case <-ctx.Done():
			t.Fatal("agent startup timed out")
		}
		return nil, nil, nil
	}
	// Main server uses the mutable control-plane fixture to exercise real reloads.
	r := &nativeRunner{Runner: &Runner{installDir: selected, bindAddress: "127.0.0.1"}, node: make(chan *agent.Node, 1)}
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		defer close(done)
		done <- agent.Run(runCtx, agent.Options{Engine: tspec.Engine, Cluster: "test", Member: "testn0", Token: "native-token", ControlPlaneURL: cp.URL, InternalCAPEM: string(ca.CertPEM), DataDir: dataDir, Listen: "127.0.0.1:0", Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}, r)
	}()
	t.Cleanup(func() {
		stop()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(70 * time.Second):
			t.Error("server agent did not shut down")
		}
	})
	var node *agent.Node
	select {
	case node = <-r.node:
	case err := <-done:
		t.Fatalf("server agent startup: %v", err)
	case <-ctx.Done():
		t.Fatal("server startup timed out")
	}
	waitReady := func() {
		t.Helper()
		for deadline := time.Now().Add(60 * time.Second); ; {
			if status := r.status(ctx, node); status.Ready {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("Temporal did not become ready: %+v", r.status(ctx, node))
			}
			time.Sleep(200 * time.Millisecond)
		}
	}
	waitReady()
	if err := r.Readiness(ctx, agent.Options{DataDir: dataDir}, []string{tspec.Server}); err != nil {
		t.Fatal(err)
	}
	initial := r.status(ctx, node)
	t.Logf("Temporal %s ready: persistence schema %s, visibility schema %s", s.Version, initial.SchemaVersion, initial.VisibilitySchemaVersion)
	if previous != "" {
		s.Version = "1.32.1"
		s.RestartToken = "upgrade"
		configMu.Lock()
		configuration.EngineSpec, _ = json.Marshal(s)
		configuration.ConfigVersion++
		next := configuration
		configMu.Unlock()
		if err := node.WithLock(ctx, func(locked *agent.Node) error {
			r.installDir = install
			_, err := r.Apply(ctx, locked, next)
			return err
		}); err != nil {
			t.Fatal(err)
		}
		waitReady()
		upgraded := r.status(ctx, node)
		if upgraded.SchemaVersion != "1.19" || upgraded.VisibilitySchemaVersion != "1.14" {
			t.Fatal("upgrade did not ensure the new binary's schema")
		}
		// Both locked releases currently embed the same latest SQL schemas.
		t.Logf("upgrade 1.31.4 -> 1.32.1 ready: persistence schema %s -> %s, visibility %s -> %s", initial.SchemaVersion, upgraded.SchemaVersion, initial.VisibilitySchemaVersion, upgraded.VisibilitySchemaVersion)
	}
	status := r.status(ctx, node)
	if status.SchemaVersion != "1.19" || status.VisibilitySchemaVersion != "1.14" {
		t.Fatalf("unexpected schema versions: %+v", status)
	}
	mux := http.NewServeMux()
	r.Runner.Routes(mux, node)
	action := func(name, body string) *httptest.ResponseRecorder {
		t.Helper()
		response := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodPost, "/v1/temporal/"+name, strings.NewReader(body)).WithContext(ctx)
		mux.ServeHTTP(response, request)
		return response
	}
	if response := action(tspec.ActionEnsureNamespaces, `[{"name":"default","retention":"72h"},{"name":"native-orders","retention":"48h","description":"created"}]`); response.Code != 200 {
		t.Fatalf("ensure namespaces: HTTP %d %s", response.Code, response.Body.String())
	}
	// Describe reads persistence, but Update's interceptor resolves namespaces
	// through the eventually consistent registry. Wait for the created identity.
	for deadline := time.Now().Add(30 * time.Second); ; {
		if _, err := frontendRequest(ctx, s, http.MethodGet, "/api/v1/namespaces/native-orders?weakConsistency=true", nil); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("created namespace did not enter the frontend registry")
		}
		time.Sleep(100 * time.Millisecond)
	}
	if response := action(tspec.ActionEnsureNamespaces, `[{"name":"native-orders","retention":"96h","description":"updated"}]`); response.Code != 200 {
		t.Fatalf("update namespace: HTTP %d %s", response.Code, response.Body.String())
	}
	response := action(tspec.ActionNamespaces, `{}`)
	if response.Code != 200 {
		t.Fatalf("list namespaces: %d", response.Code)
	}
	var namespaces []tspec.NamespaceStatus
	if err := json.Unmarshal(response.Body.Bytes(), &namespaces); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, namespace := range namespaces {
		if namespace.Name == "native-orders" {
			found = true
			if namespace.Retention != "96h0m0s" || namespace.Description != "updated" {
				t.Fatalf("namespace update did not converge: %+v", namespace)
			}
		}
	}
	if !found {
		t.Fatal("namespace missing")
	}
	if response := action(tspec.ActionDeleteNamespace, `{"name":"default"}`); response.Code != 409 {
		t.Fatalf("default deletion accepted: %d", response.Code)
	}
	if response := action(tspec.ActionDeleteNamespace, `{"name":"native-orders"}`); response.Code != 200 {
		t.Fatalf("namespace deletion: HTTP %d %s", response.Code, response.Body.String())
	}
	t.Log("namespace ensure/list/retention+description update/delete: passed; default deletion refused (409)")
	uiSpec := s
	uiSpec.Component = tspec.UI
	uiSpec.Version = "2.56.0"
	uiSpec.Persistence = nil
	uiSpec.FrontendAddress = "127.0.0.1:7233"
	uiSpec.UI = &tspec.UISettings{}
	uiSpec.RestartToken = "ui"
	uiConfig := configuration
	uiConfig.Member = "test-ui"
	uiConfig.EngineSpec, _ = json.Marshal(uiSpec)
	uiRunner := &nativeRunner{Runner: &Runner{installDir: ui, bindAddress: "127.0.0.1"}, node: make(chan *agent.Node, 1)}
	uiNode, _, _ := startAgent(uiRunner, filepath.Join(root, "ui"), "test-ui", uiConfig)
	for deadline := time.Now().Add(20 * time.Second); ; {
		if uiRunner.status(ctx, uiNode).Ready {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("UI not ready")
		}
		time.Sleep(100 * time.Millisecond)
	}
	uiResponse, err := http.Get("http://127.0.0.1:8080/")
	if err != nil {
		t.Fatal(err)
	}
	uiResponse.Body.Close()
	if uiResponse.StatusCode != 200 {
		t.Fatalf("UI: HTTP %d", uiResponse.StatusCode)
	}
	apiResponse, err := (&http.Client{Timeout: 10 * time.Second}).Get("http://127.0.0.1:8080/api/v1/namespaces")
	if err != nil {
		t.Fatal(err)
	}
	apiResponse.Body.Close()
	if apiResponse.StatusCode != 200 {
		t.Fatalf("UI mTLS namespace API: HTTP %d", apiResponse.StatusCode)
	}
	t.Log("ui-server 2.56.0: HTTP 8080 ready over mTLS frontend connection")
}
