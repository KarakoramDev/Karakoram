// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agent"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
)

var errInterruptedBootstrap = errors.New("interrupted restore")

type bootstrapResumeEngine struct {
	*Runner
	spec   pgspec.Spec
	fail   bool
	calls  int
	result chan error
}

func (e *bootstrapResumeEngine) Apply(ctx context.Context, n *agent.Node, _ agentapi.MemberConfig) ([]string, error) {
	p := dataPaths(n.DataDir())
	err := privateDir(p.pgdata)
	if err == nil {
		err = n.Configure("postgres", agent.ProcessSpec{Binary: "/bin/sleep", Args: []string{"30"}})
	}
	if err == nil {
		err = n.Action(ctx, "postgres", "start")
	}
	if err == nil {
		err = initSingleRestore(ctx, n, e.spec, p, func(runCtx context.Context, _ pgspec.Spec, _ paths, args ...string) ([]byte, error) {
			e.calls++
			states, _ := n.Snapshot()
			if states["postgres"].PID != 0 {
				return nil, errors.New("restore ran while PostgreSQL was running")
			}
			if runCtx != ctx {
				return nil, errors.New("bootstrap restore acquired a short-command context")
			}
			if !slices.Contains(args, "--delta") {
				return nil, errors.New("bootstrap did not resume delta restore")
			}
			if err := os.WriteFile(filepath.Join(p.pgdata, "PG_VERSION"), []byte("17\n"), 0600); err != nil {
				return nil, err
			}
			if e.fail {
				return nil, errInterruptedBootstrap
			}
			return nil, nil
		})
	}
	if e.fail && errors.Is(err, errInterruptedBootstrap) && e.calls == 1 {
		if _, receiptErr := os.Stat(filepath.Join(p.root, "bootstrap-generation.json")); !os.IsNotExist(receiptErr) {
			err = fmt.Errorf("interrupted restore published a completion receipt: %v", receiptErr)
		} else {
			err = nil // The failure is expected; inspect the stopped state below.
		}
	}
	states, _ := n.Snapshot()
	if err == nil && states["postgres"].PID != 0 {
		err = errors.New("bootstrap failure or completion started PostgreSQL before acknowledgement")
	}
	e.result <- err
	return nil, nil
}

func TestSingletonInterruptedBootstrapResumesAcrossCachedRestart(t *testing.T) {
	dir := t.TempDir()
	_, cert := testTLS(t)
	c := agentapi.MemberConfig{Engine: pgspec.Engine, Cluster: "test", Member: "member0", AgentTLS: agentapi.Certificate{CertPEM: cert.CertPEM, KeyPEM: cert.KeyPEM}}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/internal/agent/v1/config" {
			_ = json.NewEncoder(w).Encode(c)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	o := agent.Options{Engine: pgspec.Engine, ControlPlaneURL: server.URL, InternalCAPEM: string(ca), Cluster: c.Cluster, Member: c.Member, Token: "token", DataDir: dir, Listen: "127.0.0.1:0", Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	s := testSpec()
	s.Bootstrap = &pgspec.Bootstrap{Method: "restore", Generation: 0, Restore: &pgspec.Restore{Source: pgspec.Repository{Stanza: "kk"}}}
	for _, fail := range []bool{true, false} {
		e := &bootstrapResumeEngine{Runner: New(), spec: s, fail: fail, result: make(chan error, 4)}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		done := make(chan error, 1)
		go func() { done <- agent.Run(ctx, o, e) }()
		select {
		case err := <-e.result:
			if err != nil {
				cancel()
				<-done
				t.Fatal(err)
			}
		case err := <-done:
			cancel()
			t.Fatalf("member exited before restore: %v", err)
		case <-ctx.Done():
			cancel()
			<-done
			t.Fatal("member did not apply bootstrap configuration")
		}
		cancel()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		if e.calls != 1 {
			t.Fatal("PG_VERSION was mistaken for bootstrap completion", e.calls)
		}
	}
	// Even generation zero needs a present valid receipt; once published, the
	// ordinary initSingle entry point skips safely without touching the node.
	if err := initSingle(t.Context(), nil, s, dataPaths(dir)); err != nil {
		t.Fatal("completed zero-generation bootstrap was not recognized", err)
	}
}

func TestSingletonBootstrapRequiresValidMatchingReceipt(t *testing.T) {
	for _, receipt := range []string{"", "null", "invalid", "1", "{}"} {
		t.Run(receipt, func(t *testing.T) {
			p := dataPaths(t.TempDir())
			if err := privateDir(p.root); err != nil {
				t.Fatal(err)
			}
			if err := privateDir(p.pgdata); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(p.pgdata, "PG_VERSION"), []byte("17\n"), 0600); err != nil {
				t.Fatal(err)
			}
			if receipt != "" {
				if err := os.WriteFile(filepath.Join(p.root, "bootstrap-generation.json"), []byte(receipt), 0600); err != nil {
					t.Fatal(err)
				}
			}
			s := testSpec()
			s.Bootstrap = &pgspec.Bootstrap{Method: "restore", Generation: 0}
			// Missing/invalid/mismatched receipts must reach the stop path even
			// with PG_VERSION present. A cancelled context avoids a real command.
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			// The helper checks its lifecycle before using the node.
			if err := initSingle(ctx, nil, s, p); !errors.Is(err, context.Canceled) {
				t.Fatal("invalid receipt skipped interrupted bootstrap", err)
			}
		})
	}
}
