// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agent"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"

	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
)

func TestGracefulPatroniHandoff(t *testing.T) {
	for _, tc := range []struct {
		name, cluster, candidate string
		wantErr                  bool
	}{
		{"sync_standby", `{"members":[{"name":"p0","role":"leader"},{"name":"p1","role":"sync_standby","state":"streaming","receive_lsn":"0/10"},{"name":"p2","role":"replica","state":"streaming","receive_lsn":"0/20"}]}`, "p1", false},
		{"most_up_to_date", `{"members":[{"name":"p0","role":"leader"},{"name":"p2","role":"sync_standby","state":"streaming","receive_lsn":"0/20"},{"name":"p1","role":"sync_standby","state":"streaming","receive_lsn":"0/10"}]}`, "p2", false},
		{"no_async_fallback", `{"members":[{"name":"p0","role":"leader"},{"name":"p1","role":"replica","state":"streaming","receive_lsn":"0/20"}]}`, "", true},
		{"fenced", `{"members":[{"name":"p0","role":"leader"},{"name":"p1","role":"sync_standby","state":"streaming","receive_lsn":"0/20","tags":{"nofailover":true}}]}`, "", true},
		{"not_streaming", `{"members":[{"name":"p0","role":"leader"},{"name":"p1","role":"sync_standby","state":"stopped","receive_lsn":"0/20"}]}`, "", true},
		{"paused", `{"pause":true,"members":[{"name":"p0","role":"leader"}]}`, "", true},
		{"replica", `{"members":[{"name":"p1","role":"leader"},{"name":"p0","role":"sync_standby"}]}`, "", false},
		{"priority_fenced", `{"members":[{"name":"p0","role":"leader"},{"name":"p1","role":"sync_standby","state":"streaming","receive_lsn":"0/30","tags":{"failover_priority":0}},{"name":"p2","role":"sync_standby","state":"streaming","receive_lsn":"0/20"}]}`, "p2", false},
		{"negative_priority_fenced", `{"members":[{"name":"p0","role":"leader"},{"name":"p1","role":"sync_standby","state":"streaming","receive_lsn":"0/30","tags":{"failover_priority":-1}},{"name":"p2","role":"sync_standby","state":"streaming","receive_lsn":"0/20"}]}`, "p2", false},
		{"explicit_false_overrides_priority", `{"members":[{"name":"p0","role":"leader"},{"name":"p1","role":"sync_standby","state":"streaming","receive_lsn":"0/30","tags":{"nofailover":false,"failover_priority":0}},{"name":"p2","role":"sync_standby","state":"streaming","receive_lsn":"0/20"}]}`, "p1", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := testSpec()
			s.Topology = pgspec.TopologyHA
			s.Member = "p0"
			s.Postgres.Synchronous = pgspec.SyncStrict
			posts := 0
			err := gracefulHandoff(context.Background(), s, func(_ context.Context, _ pgspec.Spec, method, path string, input, output any) error {
				if method == "GET" && path == "/cluster" {
					return json.Unmarshal([]byte(tc.cluster), output)
				}
				if method != "POST" || path != "/switchover" {
					t.Fatalf("unexpected Patroni request %s %s", method, path)
				}
				posts++
				req := input.(map[string]string)
				if req["leader"] != "p0" || req["candidate"] != tc.candidate {
					t.Fatalf("unsafe switchover: %v", req)
				}
				return nil
			})
			if (err != nil) != tc.wantErr {
				t.Fatalf("handoff error=%v", err)
			}
			if (posts == 1) != (tc.candidate != "") {
				t.Fatalf("posts=%d", posts)
			}
		})
	}
}

func TestGracefulPatroniHandoffFailureDoesNotForceFailover(t *testing.T) {
	s := testSpec()
	s.Topology = pgspec.TopologyHA
	s.Member = "p0"
	calls := 0
	err := gracefulHandoff(context.Background(), s, func(_ context.Context, _ pgspec.Spec, method, path string, input, output any) error {
		calls++
		if calls == 1 {
			return json.Unmarshal([]byte(`{"members":[{"name":"p0","role":"leader"},{"name":"p1","role":"sync_standby","state":"streaming","receive_lsn":"0/10"}]}`), output)
		}
		return errors.New("deadline exceeded")
	})
	if err == nil || calls != 2 {
		t.Fatal("failed handoff must not retry as forced failover", err, calls)
	}
}

func TestPatroniHandoffTimingPreservesSafety(t *testing.T) {
	s := testSpec()
	s.Postgres.Synchronous = pgspec.SyncStrict
	config := dynamicConfig(s, dataPaths(t.TempDir()))
	loop := config["loop_wait"].(int)
	retry := config["retry_timeout"].(int)
	ttl := config["ttl"].(int)
	if loop != 1 || ttl != 20 || retry != 5 || loop+2*retry > ttl {
		t.Fatal("unsafe timing configuration", config)
	}
	if config["synchronous_mode"] != true || config["synchronous_mode_strict"] != true {
		t.Fatal("handoff weakened commit safety")
	}
}

// Exercise the production Shutdown entry with a real Node and its serialized
// configuration/stop controls, without starting native PostgreSQL processes.
type shutdownHoldEngine struct {
	*Runner
	stop   bool
	result chan error
}

func (e *shutdownHoldEngine) Apply(ctx context.Context, n *agent.Node, c agentapi.MemberConfig) ([]string, error) {
	s := testSpec()
	s.Topology, s.Retiring = pgspec.TopologyHA, c.Role == "retiring"
	e.mu.Lock()
	e.current = s
	e.mu.Unlock()
	if e.stop {
		if err := n.Configure("patroni", agent.ProcessSpec{Binary: "/bin/sleep", Args: []string{"30"}}); err != nil {
			return nil, err
		}
		if err := n.Stop(ctx, "patroni"); err != nil {
			return nil, err
		}
	}
	e.result <- e.Runner.Shutdown(ctx, n)
	return nil, nil
}

func TestGracefulPatroniHandoffHonorsLifecycleHolds(t *testing.T) {
	for _, tc := range []struct {
		name        string
		maintenance bool
		desired     []string
		retiring    bool
		stop        bool
		wantHandoff bool
	}{
		{name: "maintenance", maintenance: true},
		{name: "upgrade_or_suspend", desired: []string{}},
		{name: "patroni_not_desired", desired: []string{"etcd"}},
		{name: "authorized_stop", stop: true},
		{name: "retirement", retiring: true},
		{name: "ordinary_runtime_stop", wantHandoff: true},
		{name: "explicit_running_intent", desired: []string{"patroni", "etcd"}, wantHandoff: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, cert := testTLS(t)
			c := agentapi.MemberConfig{
				Engine: pgspec.Engine, Cluster: "test", Member: "member0",
				Maintenance: tc.maintenance, DesiredProcesses: tc.desired,
				AgentTLS: agentapi.Certificate{CertPEM: cert.CertPEM, KeyPEM: cert.KeyPEM},
			}
			if tc.retiring {
				c.Role = "retiring"
			}
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/internal/agent/v1/config" {
					_ = json.NewEncoder(w).Encode(c)
					return
				}
				w.WriteHeader(http.StatusNoContent)
			}))
			defer server.Close()
			ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
			o := agent.Options{
				Engine: pgspec.Engine, ControlPlaneURL: server.URL, InternalCAPEM: string(ca),
				Cluster: c.Cluster, Member: c.Member, Token: "test-token", DataDir: t.TempDir(),
				Listen: "127.0.0.1:0", Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
			}
			e := &shutdownHoldEngine{Runner: New(), stop: tc.stop, result: make(chan error, 8)}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- agent.Run(ctx, o, e) }()
			var err error
			select {
			case err = <-e.result:
			case err = <-done:
				t.Fatalf("agent exited before shutdown admission: %v", err)
			case <-ctx.Done():
				t.Fatal("agent did not apply lifecycle hold")
			}
			cancel()
			if runErr := <-done; runErr != nil {
				t.Fatal(runErr)
			}
			// The fixture deliberately has no native TLS identity: only a
			// permitted handoff reaches patroniRequest and returns this error.
			if tc.wantHandoff && (err == nil || err.Error() != "internal TLS certificate required") {
				t.Fatalf("ordinary runtime handoff was suppressed: %v", err)
			}
			if !tc.wantHandoff && err != nil {
				t.Fatalf("lifecycle hold attempted native handoff: %v", err)
			}
		})
	}
}
