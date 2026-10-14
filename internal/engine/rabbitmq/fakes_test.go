// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package rabbitmq

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/rabbitmq/mgmt"
	"github.com/KarakoramDev/Karakoram/internal/engine/rabbitmq/rmqspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

type callLog struct {
	mu    sync.Mutex
	calls []string
}

func (l *callLog) add(call string)    { l.mu.Lock(); defer l.mu.Unlock(); l.calls = append(l.calls, call) }
func (l *callLog) snapshot() []string { l.mu.Lock(); defer l.mu.Unlock(); return slices.Clone(l.calls) }
func (l *callLog) clear()             { l.mu.Lock(); defer l.mu.Unlock(); l.calls = nil }

type fakeInfra struct {
	services     map[string]*infra.Service
	done, failed bool
	mutations    int
	imports      [][]infra.ServiceSpec
	deployments  []infra.Deployment
	publicPorts  []infra.PublicPortRoute
	subdomains   map[string]bool
	failures     map[string]error
	states       map[string]infra.OpState
	log          *callLog
	agents       *fakeAgents
}

var _ infra.Backend = (*fakeInfra)(nil)
var _ infra.SubdomainAccess = (*fakeInfra)(nil)

func (f *fakeInfra) Name() string { return "test" }
func (f *fakeInfra) ProjectInfo(context.Context) (infra.Project, error) {
	return infra.Project{ID: "project", PublicIPv6: "2001:db8::1", PublicIPv4: "192.0.2.1"}, nil
}
func (f *fakeInfra) op(action, name string) (infra.Op, error) {
	f.log.add("infra:" + name + ":" + action)
	if err := f.failures[action+":"+name]; err != nil {
		return infra.Op{}, err
	}
	if err := f.failures[action]; err != nil {
		return infra.Op{}, err
	}
	f.mutations++
	return infra.Op{ID: fmt.Sprintf("%s-%d", action, f.mutations), Kind: action, ServiceID: name}, nil
}
func (f *fakeInfra) ImportServices(_ context.Context, specs []infra.ServiceSpec) (infra.Op, error) {
	op, err := f.op("import", specs[0].Hostname)
	if err != nil {
		return op, err
	}
	f.imports = append(f.imports, slices.Clone(specs))
	for _, s := range specs {
		f.services[s.Hostname] = &infra.Service{ID: s.Hostname, Hostname: s.Hostname, Type: s.Type, Resources: s.Resources, Ownership: s.Env}
	}
	return op, nil
}
func (f *fakeInfra) GetService(_ context.Context, name string) (*infra.Service, error) {
	return f.services[name], nil
}
func (f *fakeInfra) ListServices(context.Context, string) ([]infra.Service, error) {
	out := []infra.Service{}
	for _, s := range f.services {
		out = append(out, *s)
	}
	return out, nil
}
func (f *fakeInfra) DeleteService(_ context.Context, name string) (infra.Op, error) {
	op, err := f.op("delete", name)
	if err == nil {
		delete(f.services, name)
	}
	return op, err
}
func (f *fakeInfra) Deploy(_ context.Context, name string, d infra.Deployment) (infra.Op, error) {
	if d.Artifact == nil {
		return infra.Op{}, errors.New("missing artifact")
	}
	if _, err := io.ReadAll(d.Artifact); err != nil {
		return infra.Op{}, err
	}
	op, err := f.op("deploy", name)
	if err != nil {
		return op, err
	}
	d.Artifact = nil
	f.deployments = append(f.deployments, d)
	if f.agents != nil {
		f.agents.redeploy(name)
	}
	return op, nil
}
func (f *fakeInfra) SetEnvSecrets(context.Context, string, map[string]string) error { return nil }
func (f *fakeInfra) Start(_ context.Context, name string) (infra.Op, error) {
	op, err := f.op("start", name)
	if err == nil {
		f.agents.redeploy(name)
	}
	return op, err
}
func (f *fakeInfra) Stop(_ context.Context, name string) (infra.Op, error) { return f.op("stop", name) }
func (f *fakeInfra) Restart(_ context.Context, name string) (infra.Op, error) {
	op, err := f.op("restart", name)
	if err == nil {
		f.agents.redeploy(name)
	}
	return op, err
}
func (f *fakeInfra) SetResources(_ context.Context, name string, r infra.Resources) (infra.Op, error) {
	op, err := f.op("resources", name)
	if err == nil && f.services[name] != nil {
		f.services[name].Resources = r
	}
	return op, err
}
func (f *fakeInfra) SetContainerCount(_ context.Context, name string, _, _ int) (infra.Op, error) {
	return f.op("containers", name)
}
func (f *fakeInfra) EnsurePublicPort(_ context.Context, name string, p infra.PublicPort) error {
	f.log.add(fmt.Sprintf("infra:%s:expose:%d", name, p.Port))
	for i, r := range f.publicPorts {
		if r.Hostname == name && r.IPType == p.IPType {
			f.publicPorts[i] = infra.PublicPortRoute{Hostname: name, PublicPort: p}
			return nil
		}
	}
	f.publicPorts = append(f.publicPorts, infra.PublicPortRoute{Hostname: name, PublicPort: p})
	return nil
}
func (f *fakeInfra) ListPublicPorts(context.Context) ([]infra.PublicPortRoute, error) {
	return slices.Clone(f.publicPorts), nil
}
func (f *fakeInfra) DeletePublicPorts(_ context.Context, name string) error {
	f.log.add("infra:" + name + ":unexpose")
	f.publicPorts = slices.DeleteFunc(f.publicPorts, func(r infra.PublicPortRoute) bool { return r.Hostname == name })
	return nil
}
func (f *fakeInfra) Containers(context.Context, string) ([]infra.Container, error) {
	return []infra.Container{{Status: "ACTIVE"}}, nil
}
func (f *fakeInfra) Logs(context.Context, string, infra.LogQuery) ([]infra.LogLine, error) {
	return nil, nil
}
func (f *fakeInfra) OpStatus(_ context.Context, op infra.Op) (infra.OpState, error) {
	if s, ok := f.states[op.ID]; ok {
		return s, nil
	}
	return infra.OpState{Done: f.done, Failed: f.failed, Message: "scripted platform failure"}, nil
}
func (f *fakeInfra) SetSubdomainAccess(_ context.Context, name string, enabled bool) (infra.Op, error) {
	op, err := f.op("subdomain", name)
	if err == nil {
		f.subdomains[name] = enabled
	}
	return op, err
}
func (f *fakeInfra) SubdomainURLs(_ context.Context, name string) ([]string, error) {
	if !f.subdomains[name] {
		return nil, nil
	}
	return []string{"https://" + name + ".zerops.app"}, nil
}

type fakeArtifact struct{}

func (fakeArtifact) Open(context.Context) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader("test-agent-artifact")), nil
}

type testClock struct{ at time.Time }

func (c *testClock) Now() time.Time { return c.at }

type fakeBroker struct {
	running      bool
	down         bool
	joined       bool
	critical     bool
	maintenance  bool
	appStopped   bool
	version      string
	agentVersion *string
	config       int64
	alarms       json.RawMessage
	partitions   json.RawMessage
}
type fakeAgents struct {
	t             *testing.T
	p             *Provider
	rc            *engine.Context
	log           *callLog
	brokers       map[string]*fakeBroker
	servers       map[string]*httptest.Server
	clients       map[string]*agentapi.Client
	failures      map[string]int
	streamHandler func(rmqspec.StreamReplicasRequest)
	queueSource   func() []mgmt.Queue
	forgetHandler func(string)
}

func (a *fakeAgents) broker(name string) *fakeBroker {
	b := a.brokers[name]
	if b == nil {
		b = &fakeBroker{running: true}
		a.brokers[name] = b
	}
	return b
}
func (a *fakeAgents) redeploy(name string) {
	b := a.broker(name)
	b.running = true
	b.maintenance = false
	b.version = a.rc.Cluster.Spec.Version
	b.config = 0
}
func (a *fakeAgents) Dial(_ context.Context, _ domain.ClusterRef, name string) (*agentapi.Client, error) {
	b := a.broker(name)
	if b.down {
		return nil, fmt.Errorf("member %s unavailable", name)
	}
	if client := a.clients[name]; client != nil {
		return client, nil
	}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-token" {
			a.t.Error("missing agent bearer authentication")
		}
		w.Header().Set("Content-Type", "application/json")
		action := strings.TrimPrefix(r.URL.Path, "/v1/rabbitmq/")
		if r.URL.Path != "/v1/status" && action != "status" {
			a.log.add("agent:" + name + ":" + action)
		}
		if code := a.failures[name+":"+action]; code != 0 {
			http.Error(w, "scripted agent failure", code)
			return
		}
		switch r.URL.Path {
		case "/v1/status":
			if b.config == 0 {
				cfg, err := a.p.MemberConfig(r.Context(), a.rc, name)
				if err != nil {
					http.Error(w, err.Error(), 500)
					return
				}
				b.config = cfg.ConfigVersion
				if b.version == "" {
					b.version = a.rc.Cluster.Spec.Version
				}
			}
			state := "stopped"
			if b.running {
				state = "running"
			}
			version := b.version
			if b.agentVersion != nil {
				version = *b.agentVersion
			}
			_ = json.NewEncoder(w).Encode(agentapi.Status{EngineVersion: version, ConfigVersion: b.config, HostFingerprint: "host-" + name, Processes: map[string]agentapi.ProcessStatus{"rabbitmq-server": {State: state}}})
		case "/v1/reload":
			cfg, err := a.p.MemberConfig(r.Context(), a.rc, name)
			if err != nil {
				http.Error(w, err.Error(), 500)
				return
			}
			b.config = cfg.ConfigVersion
			_ = json.NewEncoder(w).Encode(agentapi.ActionResponse{Success: true})
		case "/v1/maintenance":
			var req agentapi.Maintenance
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				a.t.Error(err)
			}
			b.maintenance = req.Enabled
			_ = json.NewEncoder(w).Encode(agentapi.ActionResponse{Success: true})
		case "/v1/rabbitmq/status":
			nodes := []string{}
			diskNodes := []string{}
			if b.joined {
				for peer, pb := range a.brokers {
					if pb.joined {
						diskNodes = append(diskNodes, "rabbit@"+peer)
					}
					if pb.joined && pb.running && !pb.down && !pb.appStopped {
						nodes = append(nodes, "rabbit@"+peer)
					}
				}
			} else if b.running && !b.appStopped {
				nodes = append(nodes, "rabbit@"+name)
				diskNodes = append(diskNodes, "rabbit@"+name)
			}
			slices.Sort(nodes)
			slices.Sort(diskNodes)
			alarms, partitions := b.alarms, b.partitions
			if len(alarms) == 0 {
				alarms = json.RawMessage(`[]`)
			}
			if len(partitions) == 0 {
				partitions = json.RawMessage(`[]`)
			}
			_ = json.NewEncoder(w).Encode(rmqspec.StatusResponse{NodeName: "rabbit@" + name, RunningNodes: nodes, DiskNodes: diskNodes, Alarms: alarms, Partitions: partitions, Maintenance: b.maintenance, Version: b.version})
		case "/v1/rabbitmq/quorum-critical":
			_ = json.NewEncoder(w).Encode(rmqspec.QuorumCriticalResponse{Critical: b.critical})
		case "/v1/rabbitmq/stream-replicas":
			var req rmqspec.StreamReplicasRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				a.t.Error(err)
			}
			a.log.add("agent:" + name + ":stream-replicas:" + req.Action + ":" + req.Node)
			if a.streamHandler != nil {
				a.streamHandler(req)
			}
			_ = json.NewEncoder(w).Encode(rmqspec.StreamReplicasResponse{Replicas: json.RawMessage(`[]`)})
		default:
			if r.Method != "POST" {
				http.Error(w, "POST required", 405)
				return
			}
			switch action {
			case "bootstrap":
				b.joined = true
			case "join":
				var req rmqspec.JoinRequest
				if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Seed == "" {
					a.t.Errorf("bad join request: %+v %v", req, err)
				}
				b.joined = true
			case "drain":
				b.maintenance = true
			case "stop-app":
				b.appStopped = true
			case "stop":
				b.running = false
			case "start":
				b.running = true
				b.appStopped = false
			case "revive":
				b.maintenance = false
				b.running = true
				b.appStopped = false
			case "forget":
				var req rmqspec.NodeRequest
				_ = json.NewDecoder(r.Body).Decode(&req)
				a.log.add("agent:" + name + ":forget:" + req.Node)
				if a.forgetHandler != nil {
					a.forgetHandler(req.Node)
				}
				if peer := a.brokers[strings.TrimPrefix(req.Node, "rabbit@")]; peer != nil {
					peer.joined = false
				}
			case "grow", "shrink":
				var req rmqspec.NodeRequest
				if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Node == "" {
					a.t.Errorf("bad %s request: %+v %v", action, req, err)
				}
				a.log.add("agent:" + name + ":" + action + ":" + req.Node)
			case "await-quorum", "rebalance", "enable-feature-flags", "clear-pem-cache", "set-password":
			default:
				http.Error(w, "unknown fake agent route: "+r.URL.Path, 404)
				return
			}
			_ = json.NewEncoder(w).Encode(rmqspec.ActionResponse{OK: true})
		}
	}))
	a.t.Cleanup(server.Close)
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	client, err := agentapi.NewClient(agentapi.ClientOptions{BaseURL: server.URL, Token: "test-token", CAPEM: ca})
	if err != nil {
		return nil, err
	}
	a.servers[name] = server
	a.clients[name] = client
	return client, nil
}

func setup(t *testing.T) (*Provider, *engine.Context, *fakeInfra, *fakeAgents) {
	t.Helper()
	sealer, err := secrets.New(strings.Repeat("x", 32))
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(filepath.Join(t.TempDir(), "state.db"), sealer)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	p := New(Options{Store: st, InternalCAPEM: "control-plane-ca", ControlPlaneURL: "https://control-plane.test"})
	c := &domain.Cluster{Name: "demo"}
	p.ApplyDefaults(&c.Spec)
	if err := st.CreateCluster(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	log := &callLog{}
	f := &fakeInfra{services: map[string]*infra.Service{}, done: true, subdomains: map[string]bool{}, failures: map[string]error{}, states: map[string]infra.OpState{}, log: log}
	rc := &engine.Context{Cluster: c, Store: st, Secrets: st, Backend: f, Artifact: fakeArtifact{}, Clock: &testClock{at: time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)}}
	a := &fakeAgents{t: t, p: p, rc: rc, log: log, brokers: map[string]*fakeBroker{}, servers: map[string]*httptest.Server{}, clients: map[string]*agentapi.Client{}, failures: map[string]int{}}
	rc.Agents = a
	f.agents = a
	setupManagement(t, rc, a)
	return p, rc, f, a
}
func state(t *testing.T, rc *engine.Context) *internalState {
	t.Helper()
	s, err := loadState(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	return s
}
func syncUntil(t *testing.T, p *Provider, rc *engine.Context, done func(*internalState) bool) {
	t.Helper()
	for range 250 {
		if _, err := p.Sync(context.Background(), rc); err != nil {
			t.Fatalf("sync: %v\nstate=%s", err, rc.Cluster.Internal)
		}
		if done(state(t, rc)) {
			return
		}
	}
	t.Fatalf("sync did not converge: %s", rc.Cluster.Internal)
}
func ready(t *testing.T, p *Provider, rc *engine.Context) {
	t.Helper()
	syncUntil(t, p, rc, func(s *internalState) bool {
		if len(s.Members) != rc.Cluster.Spec.Components.Engine.Replicas || !s.Bootstrapped || s.Pending != nil {
			return false
		}
		for _, m := range s.Members {
			if m.Stage != "ready" || m.Removing || m.ChangeStep != "" || m.Version != rc.Cluster.Spec.Version {
				return false
			}
		}
		return s.Phase == "ready"
	})
}
func cloneCluster(t *testing.T, c *domain.Cluster) *domain.Cluster {
	t.Helper()
	raw, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	out := new(domain.Cluster)
	if err = json.Unmarshal(raw, out); err != nil {
		t.Fatal(err)
	}
	out.Internal = slices.Clone(c.Internal)
	out.RequestedBy = c.RequestedBy
	return out
}
func assertBefore(t *testing.T, calls []string, first, last string) {
	t.Helper()
	a, b := slices.Index(calls, first), slices.Index(calls, last)
	if a < 0 || b < 0 || a >= b {
		t.Fatalf("%q must precede %q: %v", first, last, calls)
	}
}
func countCall(calls []string, want string) int {
	n := 0
	for _, call := range calls {
		if call == want {
			n++
		}
	}
	return n
}
