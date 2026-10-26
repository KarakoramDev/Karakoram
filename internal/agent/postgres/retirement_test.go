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
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agent"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"gopkg.in/yaml.v3"
)

func TestRetirementTagsRenderedLocally(t *testing.T) {
	s := testSpec()
	s.Topology, s.Retiring = pgspec.TopologyHA, true
	text, err := renderPatroni(s, dataPaths(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	var config struct {
		Tags map[string]bool `yaml:"tags"`
	}
	if err := yaml.Unmarshal([]byte(text), &config); err != nil {
		t.Fatal(err)
	}
	clone, exists := config.Tags["clonefrom"]
	if !config.Tags["nofailover"] || !config.Tags["noloadbalance"] || !exists || clone {
		t.Fatalf("retirement tags not rendered: %v", config.Tags)
	}
	if _, exists := dynamicConfig(s, dataPaths(t.TempDir()))["tags"]; exists {
		t.Fatal("retirement exclusion must be node-local, not cluster-wide")
	}
}

func TestRetirementClusterTags(t *testing.T) {
	for _, tc := range []struct {
		tags string
		want bool
	}{
		{`{"nofailover":true,"noloadbalance":true}`, true},
		{`{"nofailover":true,"noloadbalance":true,"clonefrom":false,"failover_priority":0}`, true},
		{`{"noloadbalance":true,"failover_priority":0}`, true},
		{`{"nofailover":false,"noloadbalance":true,"failover_priority":0}`, false},
		{`{"nofailover":true,"noloadbalance":true,"clonefrom":true}`, false},
		{`{"nofailover":true}`, false},
		{`{}`, false},
	} {
		var cluster patroniCluster
		if err := json.Unmarshal([]byte(`{"members":[{"name":"member0","role":"replica","tags":`+tc.tags+`}]}`), &cluster); err != nil {
			t.Fatal(err)
		}
		if retirementTagsApplied(cluster.Members[0].Tags) != tc.want {
			t.Fatalf("incorrect retirement confirmation for %s", tc.tags)
		}
	}
}

func TestRetirementStopRefusesLeaderAndUnconfirmedTags(t *testing.T) {
	base := pgspec.StatusResponse{Role: "replica", Running: true, Leader: "member1", FailoverFenced: true}
	if !retirementCanStop(base, "member0") {
		t.Fatal("healthy fenced replica cannot stop")
	}
	for name, mutate := range map[string]func(*pgspec.StatusResponse){
		"primary":          func(s *pgspec.StatusResponse) { s.Role = "primary" },
		"leader changed":   func(s *pgspec.StatusResponse) { s.Leader = "member0" },
		"leader unknown":   func(s *pgspec.StatusResponse) { s.Leader = "" },
		"unconfirmed tags": func(s *pgspec.StatusResponse) { s.FailoverFenced = false },
		"not running":      func(s *pgspec.StatusResponse) { s.Running = false },
	} {
		t.Run(name, func(t *testing.T) {
			st := base
			mutate(&st)
			if retirementCanStop(st, "member0") {
				t.Fatal("unsafe retirement stop accepted")
			}
		})
	}
}

type retirementHoldEngine struct {
	*Runner
	first  bool
	result chan error
}

func (e *retirementHoldEngine) Apply(ctx context.Context, n *agent.Node, c agentapi.MemberConfig) ([]string, error) {
	if err := n.Configure("patroni", agent.ProcessSpec{Binary: "/bin/sleep", Args: []string{"30"}}); err != nil {
		e.result <- err
		return nil, err
	}
	s := testSpec()
	s.Topology, s.Retiring = pgspec.TopologyHA, true
	var err error
	if e.first {
		// The hold has committed, but the CP did not receive the stop response.
		err = n.Stop(ctx, "patroni")
	}
	if err == nil {
		err = e.retireStop(ctx, n, s, dataPaths(n.DataDir()))
	}
	if err == nil {
		err = n.Action(ctx, "patroni", "start")
	}
	if err == nil && !retirementStopped(n, s, dataPaths(n.DataDir())) {
		err = fmt.Errorf("retirement stop hold did not survive configuration application")
	}
	e.result <- err
	return nil, err
}

func TestRetirementHoldSurvivesCachedRestart(t *testing.T) {
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
	for _, first := range []bool{true, false} {
		e := &retirementHoldEngine{Runner: New(), first: first, result: make(chan error, 4)}
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
			t.Fatalf("member exited before retirement: %v", err)
		case <-ctx.Done():
			cancel()
			<-done
			t.Fatal("member did not apply retirement configuration")
		}
		cancel()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
}

func TestRetirementRenderRejectsPrimaryAndUnknownRole(t *testing.T) {
	s := testSpec()
	s.Topology, s.Retiring = pgspec.TopologyHA, true
	for _, tc := range []struct {
		name   string
		status pgspec.StatusResponse
		want   bool
	}{
		{"streaming replica", pgspec.StatusResponse{Role: "replica", Running: true, Streaming: true, Leader: "member1"}, true},
		{"primary", pgspec.StatusResponse{Role: "primary", Running: true, Leader: s.Member}, false},
		{"regained leader", pgspec.StatusResponse{Role: "replica", Running: true, Streaming: true, Leader: s.Member}, false},
		{"unknown role", pgspec.StatusResponse{}, false},
		{"unknown leader", pgspec.StatusResponse{Role: "replica", Running: true, Streaming: true}, false},
		{"not streaming", pgspec.StatusResponse{Role: "replica", Running: true, Leader: "member1"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			effective := retirementRenderSpec(s, tc.status)
			if effective.Retiring != tc.want {
				t.Fatal("retirement render applied unsafe nofailover intent")
			}
			text, err := renderPatroni(effective, dataPaths(t.TempDir()))
			if err != nil {
				t.Fatal(err)
			}
			var config struct {
				Tags map[string]bool `yaml:"tags"`
			}
			if err := yaml.Unmarshal([]byte(text), &config); err != nil || config.Tags["nofailover"] != tc.want {
				t.Fatal("effective local Patroni tags differ from safe role observation", err)
			}
			if _, exists := dynamicConfig(effective, dataPaths(t.TempDir()))["member_slots_ttl"]; exists {
				t.Fatal("retirement changed member slot retention instead of fixing ordering")
			}
		})
	}
}
