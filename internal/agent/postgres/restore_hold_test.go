// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agent"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
)

type restoreHoldEngine struct {
	*Runner
	prepare bool
	result  chan error
}

func (e *restoreHoldEngine) Apply(ctx context.Context, n *agent.Node, c agentapi.MemberConfig) ([]string, error) {
	var err error
	for _, process := range []string{"postgres", "patroni"} {
		if err = n.Configure(process, agent.ProcessSpec{Binary: "/bin/sleep", Args: []string{"30"}}); err != nil {
			break
		}
		if e.prepare {
			s := testSpec()
			if process == "patroni" {
				s.Topology = pgspec.TopologyHA
			}
			err = prepareRestore(ctx, n, s, dataPaths(n.DataDir()), pgspec.PrepareRestoreRequest{Receipt: "operation/" + process, Wipe: true})
		}
		if err == nil {
			err = n.Action(ctx, process, "start")
		}
		states, _ := n.Snapshot()
		if err == nil && (!n.Stopped(process) || states[process].PID != 0) {
			err = fmt.Errorf("%s preparation hold did not survive configuration application", process)
		}
		if err != nil {
			break
		}
	}
	e.result <- err
	return nil, err
}

func TestPrepareRestoreHoldSurvivesCachedRestart(t *testing.T) {
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
	for _, prepare := range []bool{true, false} {
		e := &restoreHoldEngine{Runner: New(), prepare: prepare, result: make(chan error, 4)}
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
			t.Fatalf("member exited before preparation: %v", err)
		case <-ctx.Done():
			cancel()
			<-done
			t.Fatal("member did not apply configuration")
		}
		cancel()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		data, err := os.ReadFile(filepath.Join(dir, "karakoram", "controls.json"))
		var controls struct {
			Stopped map[string]bool `json:"stopped"`
		}
		if err != nil || json.Unmarshal(data, &controls) != nil || !controls.Stopped["postgres"] || !controls.Stopped["patroni"] {
			t.Fatal("restore preparation was acknowledged without durable holds", err)
		}
	}
}
