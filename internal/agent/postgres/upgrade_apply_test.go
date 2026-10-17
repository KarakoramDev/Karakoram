// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agent"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
)

type upgradeApplyEngine struct {
	*Runner
	result chan error
	resume bool
}

func (e *upgradeApplyEngine) Apply(ctx context.Context, n *agent.Node, c agentapi.MemberConfig) ([]string, error) {
	changed, err := e.Runner.Apply(ctx, n, c)
	if err == nil && e.resume {
		p := dataPaths(n.DataDir())
		if _, statErr := os.Lstat(p.pgdata); !os.IsNotExist(statErr) {
			err = errors.New("cached Apply recreated intentionally absent PGDATA")
		} else {
			request := upgradeRequest()
			name, nameErr := receiptPath(p, "upgrade", request.Receipt)
			err = nameErr
			if err == nil {
				var receipt upgradeReceipt
				data, readErr := os.ReadFile(name)
				err = readErr
				if err == nil {
					err = json.Unmarshal(data, &receipt)
				}
				if err == nil {
					err = activateUpgrade(p, pathsForUpgrade(p, request), &receipt, name)
				}
			}
		}
	}
	e.result <- err
	return changed, err
}

func TestCachedApplyPreservesUpgradeActivationRenames(t *testing.T) {
	for _, phase := range []string{"linked", "old-retired"} {
		t.Run(phase, func(t *testing.T) {
			dir := t.TempDir()
			_, cert := testTLS(t)
			s := testSpec()
			raw, err := json.Marshal(s)
			if err != nil {
				t.Fatal(err)
			}
			c := agentapi.MemberConfig{Engine: pgspec.Engine, Cluster: "test", Member: s.Member, EngineSpec: raw, DesiredProcesses: []string{}, AgentTLS: agentapi.Certificate{CertPEM: cert.CertPEM, KeyPEM: cert.KeyPEM}}
			var unavailable atomic.Bool
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if unavailable.Load() {
					http.Error(w, "unavailable", http.StatusServiceUnavailable)
					return
				}
				if r.URL.Path == "/internal/agent/v1/config" {
					_ = json.NewEncoder(w).Encode(c)
					return
				}
				w.WriteHeader(http.StatusNoContent)
			}))
			defer server.Close()
			ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
			o := agent.Options{Engine: pgspec.Engine, ControlPlaneURL: server.URL, InternalCAPEM: string(ca), Cluster: c.Cluster, Member: c.Member, Token: "token", DataDir: dir, Listen: "127.0.0.1:0", Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
			run := func(resume bool) {
				t.Helper()
				e := &upgradeApplyEngine{Runner: New(), result: make(chan error, 4), resume: resume}
				ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
				defer cancel()
				done := make(chan error, 1)
				go func() { done <- agent.Run(ctx, o, e) }()
				select {
				case err := <-e.result:
					cancel()
					runErr := <-done
					if err != nil || runErr != nil {
						t.Fatalf("Apply: %v; run: %v", err, runErr)
					}
				case err := <-done:
					t.Fatalf("member exited before Apply: %v", err)
				case <-ctx.Done():
					cancel()
					<-done
					t.Fatal("member did not apply configuration")
				}
			}
			// Populate the real cached config, then interrupt activation before its
			// second rename and restart with no control-plane configuration available.
			run(false)
			p := dataPaths(dir)
			request := upgradeRequest()
			u := pathsForUpgrade(p, request)
			name, err := receiptPath(p, "upgrade", request.Receipt)
			if err != nil {
				t.Fatal(err)
			}
			makeVersion(t, p.pgdata, 16)
			makeVersion(t, u.target, 17)
			if err = os.Rename(p.pgdata, u.retired); err != nil {
				t.Fatal(err)
			}
			receipt := upgradeReceipt{Request: request, Phase: phase, Checked: true}
			if err = durableJSON(name, receipt); err != nil {
				t.Fatal(err)
			}
			if err = os.WriteFile(filepath.Join(filepath.Dir(name), ".receipt-interrupted"), []byte("{"), 0600); err != nil {
				t.Fatal(err)
			}
			unavailable.Store(true)
			run(true)
			if version, err := pgMajorAt(p.pgdata); err != nil || version != 17 {
				t.Fatal("activation did not resume after cached Apply", version, err)
			}
			if version, err := pgMajorAt(u.retired); err != nil || version != 16 {
				t.Fatal("retired data was changed", version, err)
			}
		})
	}
}

func TestUpgradeActivationKeepsGenuineConflicts(t *testing.T) {
	p := dataPaths(t.TempDir())
	request := upgradeRequest()
	u := pathsForUpgrade(p, request)
	name, err := receiptPath(p, "upgrade", request.Receipt)
	if err != nil {
		t.Fatal(err)
	}
	makeVersion(t, u.retired, 16)
	makeVersion(t, u.target, 17)
	makeVersion(t, p.pgdata, 17)
	receipt := upgradeReceipt{Request: request, Phase: "old-retired", Checked: true}
	if err := activateUpgrade(p, u, &receipt, name); err == nil {
		t.Fatal("genuine conflicting directories accepted")
	}
	if receipt.Done {
		t.Fatal("conflicting activation marked done")
	}
}

// A paused Patroni exits on SIGTERM without stopping its separately grouped
// postmaster, which retains Patroni's stdout/stderr pipes.
func TestPausedUpgradeProcessHelper(t *testing.T) {
	mode := os.Getenv("KARAKORAM_PAUSED_UPGRADE_HELPER")
	if mode == "" {
		return
	}
	pidfile := filepath.Join(os.Getenv("KARAKORAM_PAUSED_UPGRADE_DATA"), "postmaster.pid")
	signals := make(chan os.Signal, 1)
	if mode == "postgres" {
		signal.Notify(signals, syscall.SIGINT)
		if err := os.WriteFile(pidfile, []byte(fmt.Sprint(os.Getpid())), 0600); err != nil {
			os.Exit(2)
		}
		<-signals
		_ = os.Remove(pidfile)
		os.Exit(0)
	}
	signal.Notify(signals, syscall.SIGTERM)
	child := exec.Command(os.Args[0], "-test.run=^TestPausedUpgradeProcessHelper$")
	child.Env = append(os.Environ(), "KARAKORAM_PAUSED_UPGRADE_HELPER=postgres")
	child.Stdout, child.Stderr = os.Stdout, os.Stderr
	child.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := child.Start(); err != nil {
		os.Exit(2)
	}
	go func() { _ = child.Wait() }()
	<-signals
	os.Exit(0)
}

type pausedUpgradeApplyEngine struct {
	*Runner
	result chan error
}

func (*pausedUpgradeApplyEngine) Version(context.Context) string { return "17.11.1" }
func (e *pausedUpgradeApplyEngine) Apply(ctx context.Context, n *agent.Node, c agentapi.MemberConfig) ([]string, error) {
	if c.ConfigVersion != 1 {
		changed, err := e.Runner.Apply(ctx, n, c)
		e.result <- err
		return changed, err
	}
	p := dataPaths(n.DataDir())
	if err := os.MkdirAll(p.pgdata, 0700); err != nil {
		return nil, err
	}
	err := n.Configure("patroni", agent.ProcessSpec{
		Binary: os.Args[0], Args: []string{"-test.run=^TestPausedUpgradeProcessHelper$"},
		Env: []string{"KARAKORAM_PAUSED_UPGRADE_HELPER=patroni", "KARAKORAM_PAUSED_UPGRADE_DATA=" + p.pgdata},
	})
	if err == nil {
		err = n.Action(ctx, "patroni", "start")
	}
	if err == nil {
		for {
			if _, err = os.Stat(filepath.Join(p.pgdata, "postmaster.pid")); err == nil {
				break
			}
			select {
			case <-ctx.Done():
				err = ctx.Err()
			case <-time.After(5 * time.Millisecond):
				continue
			}
			break
		}
	}
	e.result <- err
	return nil, err
}

func TestOfflineApplyStopsPausedPostmasterBeforePatroni(t *testing.T) {
	dir := t.TempDir()
	p := dataPaths(dir)
	bin := t.TempDir()
	// Model pg_ctl fast shutdown without a native PostgreSQL dependency.
	script := "#!/bin/sh\nkill -INT \"$(cat \"$2/postmaster.pid\")\" || exit 1\nfor i in $(seq 1 200); do\n test -e \"$2/postmaster.pid\" || exit 0\n sleep .01\ndone\nexit 1\n"
	if err := os.WriteFile(filepath.Join(bin, "pg_ctl"), []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	stopDetached := func() {
		if data, err := os.ReadFile(filepath.Join(p.pgdata, "postmaster.pid")); err == nil {
			var pid int
			if _, err = fmt.Sscan(string(data), &pid); err == nil {
				_ = syscall.Kill(pid, syscall.SIGINT)
			}
		}
	}
	t.Cleanup(stopDetached)
	s := testSpec()
	s.Topology, s.BinDir = pgspec.TopologyHA, bin
	s.Etcd = &pgspec.Etcd{InitialCluster: "member0=https://member0:2380", State: "new", Token: "test"}
	ca, cert := testTLS(t)
	s.CAPEM, s.ServerCert = ca, cert
	s.Peers = []pgspec.Peer{{Name: s.Member, Host: s.Member}}
	raw, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	config := agentapi.MemberConfig{Engine: pgspec.Engine, Cluster: "test", Member: s.Member, ConfigVersion: 1, EngineSpec: raw, AgentTLS: agentapi.Certificate{CertPEM: cert.CertPEM, KeyPEM: cert.KeyPEM}}
	var mu sync.Mutex
	cp := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		_ = json.NewEncoder(w).Encode(config)
	}))
	defer cp.Close()
	internalCA := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cp.Certificate().Raw})
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	_ = listener.Close()
	engine := &pausedUpgradeApplyEngine{Runner: New(), result: make(chan error, 4)}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- agent.Run(ctx, agent.Options{Engine: pgspec.Engine, ControlPlaneURL: cp.URL, InternalCAPEM: string(internalCA), Cluster: config.Cluster, Member: config.Member, Token: "token", DataDir: dir, Listen: address, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}, engine)
	}()
	defer func() {
		stopDetached()
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()
	select {
	case err := <-engine.result:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("initial paused process did not start")
	}
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM([]byte(ca))
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}}, Timeout: 3 * time.Second}
	defer client.CloseIdleConnections()
	mu.Lock()
	config.ConfigVersion = 2
	config.DesiredProcesses = []string{}
	mu.Unlock()
	var response *http.Response
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://"+address+"/v1/reload", nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer token")
		response, err = client.Do(req)
		if err == nil {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("offline reload did not acknowledge stopped intent:", err)
		case <-time.After(5 * time.Millisecond):
		}
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(response.Body)
		t.Fatalf("offline reload: %d %s", response.StatusCode, body)
	}
	if err := assertOfflineData(p.pgdata); err != nil {
		t.Fatal("offline Apply left the paused postmaster running:", err)
	}
}
