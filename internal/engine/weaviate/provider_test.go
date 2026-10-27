// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package weaviate

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"encoding/pem"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/weaviate/wvspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/infra/fake"
	"github.com/KarakoramDev/Karakoram/internal/pki"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type coreArtifact struct{}

func (coreArtifact) Open(context.Context) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader("artifact")), nil
}

type coreDialer struct{ address string }

func (d coreDialer) DialContext(ctx context.Context, network, _ string) (net.Conn, error) {
	return (&net.Dialer{}).DialContext(ctx, network, d.address)
}

type coreAgentDialer struct {
	url string
	ca  []byte
}

func (d coreAgentDialer) Dial(_ context.Context, _ domain.ClusterRef, name string) (*agentapi.Client, error) {
	return agentapi.NewClient(agentapi.ClientOptions{BaseURL: d.url + "/" + name, CAPEM: d.ca, Token: "test-token"})
}

type coreFixture struct {
	p              *Provider
	rc             *engine.Context
	backend        *fake.Backend
	quorum         bool
	statusStale    bool
	readinessStale bool
	native         *httptest.Server
}

func coreSetup(t *testing.T, replicas int, handlers ...func(http.ResponseWriter, *http.Request) bool) *coreFixture {
	t.Helper()
	ctx := context.Background()
	seal, e := secrets.New(strings.Repeat("x", 32))
	if e != nil {
		t.Fatal(e)
	}
	st, e := store.Open(filepath.Join(t.TempDir(), "state.db"), seal)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = st.Close() })
	p := New(Options{Store: st})
	c := &domain.Cluster{Name: "demo"}
	if replicas == 1 {
		c.Spec.Topology.Type = "single"
	} else {
		c.Spec.Topology.Type = "cluster"
	}
	p.ApplyDefaults(&c.Spec)
	c.Spec.Components.Engine.Replicas = replicas
	if e = st.CreateCluster(ctx, c); e != nil {
		t.Fatal(e)
	}
	ca, e := pki.NewCA("test", 365*24*time.Hour)
	if e != nil {
		t.Fatal(e)
	}
	for k, v := range map[string][]byte{secretCA: ca.CertPEM, secretCAKey: ca.KeyPEM} {
		if e = st.SetClusterSecret(ctx, c.Ref(), k, v); e != nil {
			t.Fatal(e)
		}
	}
	names := []string{}
	for i := range 20 {
		names = append(names, NodeHostname("demo", i))
	}
	crt, key, e := ca.Issue(pki.CertRequest{DNSNames: names, Server: true})
	if e != nil {
		t.Fatal(e)
	}
	pair, e := tls.X509KeyPair(crt, key)
	if e != nil {
		t.Fatal(e)
	}
	f := &coreFixture{p: p, backend: fake.New(fake.Options{MaskSecrets: true}), quorum: true}
	f.rc = &engine.Context{Cluster: c, Store: st, Secrets: st, Backend: f.backend, Artifact: coreArtifact{}}
	native := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		for _, handler := range handlers {
			if handler(w, r) {
				return
			}
		}
		s, _ := load(c)
		switch r.URL.Path {
		case "/v1/meta":
			name := strings.Split(r.Host, ":")[0]
			m := s.find(name)
			_ = json.NewEncoder(w).Encode(map[string]string{"version": m.DesiredVersion})
		case "/v1/nodes":
			nodes := []any{}
			for _, m := range s.Members {
				status := "HEALTHY"
				if m.CleanupStep == "stopped" {
					cfg, err := f.p.MemberConfig(ctx, f.rc, m.Name)
					if err != nil {
						t.Error(err)
					}
					if len(cfg.DesiredProcesses) == 0 {
						continue
					}
				}
				nodes = append(nodes, map[string]any{"name": m.Name, "status": status, "version": m.DesiredVersion, "stats": map[string]int{"objectCount": 0, "shardCount": 0}})
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"nodes": nodes})
		case "/v1/cluster/statistics":
			stats := []any{}
			config := []any{}
			for _, name := range s.Voters {
				config = append(config, map[string]any{"id": name, "suffrage": 0})
			}
			for _, m := range s.Members {
				voter := false
				for _, n := range s.Voters {
					voter = voter || n == m.Name
				}
				stats = append(stats, map[string]any{"name": m.Name, "status": "HEALTHY", "dbLoaded": true, "open": true, "ready": true, "isVoter": voter, "leaderId": s.Voters[0], "raft": map[string]any{"latestConfiguration": config}})
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"statistics": stats, "synchronized": f.quorum})
		case "/v1/schema":
			_ = json.NewEncoder(w).Encode(map[string]any{"classes": []any{}})
		default:
			http.NotFound(w, r)
		}
	}))
	native.TLS = &tls.Config{Certificates: []tls.Certificate{pair}, MinVersion: tls.VersionTLS12}
	native.StartTLS()
	t.Cleanup(native.Close)
	f.native = native
	f.rc.Dialer = coreDialer{native.Listener.Addr().String()}
	agent := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-token" {
			t.Error("agent request unauthenticated")
		}
		parts := strings.SplitN(strings.TrimPrefix(r.URL.Path, "/"), "/", 2)
		s, _ := load(c)
		m := s.find(parts[0])
		if m == nil {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		switch "/" + parts[1] {
		case "/v1/status":
			version := m.ConfigVersion
			if f.statusStale {
				version--
			}
			_ = json.NewEncoder(w).Encode(agentapi.Status{ConfigVersion: version, EngineVersion: m.DesiredVersion, Processes: map[string]agentapi.ProcessStatus{"weaviate": {State: "running"}}})
		case "/v1/weaviate/readiness":
			version := m.ConfigVersion
			if f.readinessStale {
				version--
			}
			_ = json.NewEncoder(w).Encode(wvspec.ReadinessResponse{Ready: true, ConfigVersion: version, Version: m.DesiredVersion})
		default:
			_ = json.NewEncoder(w).Encode(map[string]bool{"success": true})
		}
	}))
	t.Cleanup(agent.Close)
	f.rc.Agents = coreAgentDialer{agent.URL, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: agent.Certificate().Raw})}
	return f
}
func coreConverge(t *testing.T, f *coreFixture) *state {
	t.Helper()
	for range 300 {
		before := len(f.backend.Calls())
		r, e := f.p.Sync(context.Background(), f.rc)
		if e != nil {
			t.Fatal(e)
		}
		calls := f.backend.Calls()[before:]
		mutations := 0
		for _, call := range calls {
			switch call {
			case "ImportServices", "Deploy", "Stop", "DeleteServiceByID", "SetResources", "EnsurePublicPort", "DeletePublicPorts", "SetEnvVars":
				mutations++
			}
		}
		if mutations > 1 {
			t.Fatalf("multiple mutations in pass: %v", calls)
		}
		s, e := load(f.rc.Cluster)
		if e != nil {
			t.Fatal(e)
		}
		if s.Phase == "ready" && s.Rolling == nil && s.Pending == nil && r.RequeueAfter == 30*time.Second {
			return s
		}
	}
	t.Fatal("did not converge")
	return nil
}
func TestFormationAndMemberConfig(t *testing.T) {
	for _, n := range []int{1, 3} {
		t.Run(string(rune('0'+n)), func(t *testing.T) {
			f := coreSetup(t, n)
			s := coreConverge(t, f)
			if len(s.Voters) != min(3, n) {
				t.Fatal(s.Voters)
			}
			cfg, e := f.p.MemberConfig(context.Background(), f.rc, s.Members[0].Name)
			if e != nil {
				t.Fatal(e)
			}
			var spec wvspec.MemberSpec
			if e = json.Unmarshal(cfg.EngineSpec, &spec); e != nil {
				t.Fatal(e)
			}
			if e = spec.Validate(); e != nil {
				t.Fatal(e)
			}
			if spec.MemoryLimitMiB != 1740 || spec.RootUser != AdminUser || len(spec.RaftJoin) != n || !spec.TLS || cfg.AgentTLS.CertPEM == "" || cfg.TLS.KeyPEM == "" || cfg.Volume == nil {
				t.Fatalf("unexpected member configuration: memory=%d seeds=%d", spec.MemoryLimitMiB, len(spec.RaftJoin))
			}
			admin, _ := secret(context.Background(), f.rc, secretAdmin)
			status, e := f.p.Status(context.Background(), f.rc)
			if e != nil {
				t.Fatal(e)
			}
			raw, _ := json.Marshal(status)
			if strings.Contains(string(raw), string(admin)) || strings.Contains(string(f.rc.Cluster.Internal), string(admin)) {
				t.Fatal("secret escaped encrypted configuration")
			}
			runtime, e := f.p.runtimeSpec(f.rc, s.Members[0], s.Members[0].Version)
			if e != nil {
				t.Fatal(e)
			}
			if len(runtime.Ports) != 7 {
				t.Fatal(runtime.Ports)
			}
			for _, port := range runtime.Ports {
				if port.Port == 2112 || port.HTTP {
					t.Fatal(port)
				}
			}
			if !strings.Contains(strings.Join(runtime.Readiness.Args, " "), "--startup") {
				t.Fatal("startup gated on native quorum")
			}
		})
	}
}
func TestScaleOutAndRollingQuorumGate(t *testing.T) {
	f := coreSetup(t, 3)
	coreConverge(t, f)
	f.rc.Cluster.Spec.Components.Engine.Replicas = 4
	s := coreConverge(t, f)
	if len(s.Voters) != 3 {
		t.Fatal(s.Voters)
	}
	f.rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"queryDefaultsLimit":42}`)
	f.quorum = false
	for range 8 {
		if _, e := f.p.Sync(context.Background(), f.rc); e != nil {
			t.Fatal(e)
		}
	}
	s, _ = load(f.rc.Cluster)
	if s.Rolling == nil || s.Rolling.Order[0] != s.Members[3].Name || s.Rolling.Step != "gate" {
		t.Fatal(s.Rolling)
	}
	f.quorum = true
	coreConverge(t, f)
}
func TestRestartReplaceAndCleanup(t *testing.T) {
	f := coreSetup(t, 3)
	s := coreConverge(t, f)
	name := s.Members[0].Name
	for _, kind := range []string{domain.OperationRestart, domain.OperationReplaceMember} {
		op := &domain.Operation{ID: kind, Kind: kind, Payload: json.RawMessage(`{"member":"` + name + `"}`)}
		done := false
		for range 150 {
			result, e := f.p.RunOperation(context.Background(), f.rc, op)
			if e != nil {
				t.Fatal(e)
			}
			op.Checkpoint = result.Checkpoint
			if result.Done {
				done = true
				break
			}
		}
		if !done {
			t.Fatal("operation did not complete", kind)
		}
	}
	for range 150 {
		done, e := f.p.Cleanup(context.Background(), f.rc, domain.DeleteOptions{KeepVolumes: true})
		if e != nil {
			t.Fatal(e)
		}
		if done {
			for _, m := range s.Members {
				v, e := f.backend.GetService(context.Background(), m.Volume)
				if e != nil || v == nil {
					t.Fatal("retained volume missing", e)
				}
			}
			return
		}
	}
	t.Fatal("cleanup did not complete")
}

var _ infra.Backend = (*fake.Backend)(nil)
