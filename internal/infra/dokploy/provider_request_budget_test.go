// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package dokploy

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/datamcp/companion"
	"github.com/KarakoramDev/Karakoram/internal/datamcp/companionspec"
	model "github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/neo4j"
	"github.com/KarakoramDev/Karakoram/internal/engine/neo4j/neo4jspec"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

// Keep real provider status and Dokploy HTTP paths. The TLS server substitutes
// only native/generic agent responses; pinned transports still reauthorize each
// connection against fresh Dokploy task inventory, just like the relay.
type providerBudgetAgents struct {
	backend  *Backend
	endpoint string
	ca       []byte
}

func (a *providerBudgetAgents) Dial(_ context.Context, _ model.ClusterRef, member string) (*agentapi.Client, error) {
	endpoint, err := url.Parse(a.endpoint)
	if err != nil {
		return nil, err
	}
	dial := func(ctx context.Context, network, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, endpoint.Host)
	}
	return agentapi.NewClient(agentapi.ClientOptions{BaseURL: a.endpoint, Token: member, CAPEM: a.ca, DialContext: dial,
		DialInstanceContext: func(ctx context.Context, network, logical, instance string) (net.Conn, error) {
			containers, err := a.backend.Containers(infra.WithFreshTaskInventory(ctx), member)
			if err != nil {
				return nil, err
			}
			for _, c := range containers {
				if c.Hostname == instance && c.Status == infra.ContainerRunning {
					return dial(ctx, network, logical)
				}
			}
			return nil, fmt.Errorf("instance is not in current inventory")
		}})
}

type providerGETCounts struct {
	mu        sync.Mutex
	endpoints map[string]int
	uris      map[string]int
}

func countProviderGETs(f *fakeAPI) *providerGETCounts {
	c := &providerGETCounts{endpoints: map[string]int{}, uris: map[string]int{}}
	f.mu.Lock()
	f.requestHook = func(_ http.ResponseWriter, r *http.Request) (bool, func()) {
		if r.Method == http.MethodGet {
			c.mu.Lock()
			c.endpoints[strings.TrimPrefix(r.URL.Path, "/api/")]++
			c.uris[r.URL.RequestURI()]++
			c.mu.Unlock()
		}
		return false, nil
	}
	f.mu.Unlock()
	return c
}
func (c *providerGETCounts) reset() {
	c.mu.Lock()
	clear(c.endpoints)
	clear(c.uris)
	c.mu.Unlock()
}
func (c *providerGETCounts) report(t *testing.T, budget map[string]int, idle bool) {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	total := 0
	for endpoint, count := range c.endpoints {
		total += count
		if budget != nil && count > budget[endpoint] {
			t.Errorf("GET %s=%d exceeds budget %d", endpoint, count, budget[endpoint])
		}
	}
	for uri, count := range c.uris {
		if count > 1 {
			t.Logf("repeated GET %s: %d", uri, count)
			if idle && budget != nil && !strings.HasPrefix(uri, "/api/docker.getServiceContainersByAppName?") {
				t.Errorf("duplicate same-pass non-task GET %s: %d", uri, count)
			}
		}
	}
	// Projection, not a wall-clock load test. ReadinessInterval is defined in
	// engine/observation.go; reconciler/availability.go availabilityLoop ticks at
	// that cadence. Reconcile also caps its requeue at ReadinessInterval in
	// reconciler/reconciler.go. One pass/tick gives the baseline; two independent
	// availability+reconcile passes/tick doubles it, excluding Sync/operations.
	t.Logf("GETs=%d endpoints=%v", total, c.endpoints)
	if idle {
		t.Logf("idle/min(one pass/tick)=%.0f idle/min(two passes/tick)=%.0f interval=%s", float64(total)*float64(time.Minute)/float64(engine.ReadinessInterval), 2*float64(total)*float64(time.Minute)/float64(engine.ReadinessInterval), engine.ReadinessInterval)
	}
}

func providerBudgetContext(t *testing.T, p engine.Provider) (*engine.Context, *fakeAPI, *Backend) {
	t.Helper()
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	sealer, err := secrets.New(strings.Repeat("x", 32))
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(filepath.Join(t.TempDir(), "state.db"), sealer)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	c := &model.Cluster{Name: "sample", Namespace: model.DefaultNamespace, Generation: 1}
	p.ApplyDefaults(&c.Spec)
	if err = st.CreateCluster(t.Context(), c); err != nil {
		t.Fatal(err)
	}
	return &engine.Context{Cluster: c, Backend: b, Store: st, Secrets: st, Clock: engine.RealClock{}, ObservationEpoch: "test-process"}, f, b
}
func providerBudgetState(t *testing.T, rc *engine.Context, key string, state any) {
	t.Helper()
	raw, err := json.Marshal(map[string]any{key: state})
	if err != nil {
		t.Fatal(err)
	}
	rc.Cluster.Internal = raw
}
func providerBudgetSecret(t *testing.T, rc *engine.Context, key string, v any) {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	if err = rc.Secrets.SetClusterSecret(t.Context(), rc.Cluster.Ref(), key, raw); err != nil {
		t.Fatal(err)
	}
}
func providerBudgetRuntime(t *testing.T, b *Backend, name, volume string, count int, proxy bool) string {
	t.Helper()
	spec := runtimeSpec(name, volume)
	spec.MinContainers, spec.MaxContainers = count, count
	if volume != "" {
		importFake(t, b, volumeSpec(volume))
	}
	op := importFake(t, b, spec)
	d := testDeployment(t, volume)
	if proxy {
		d.Runtime.Env = map[string]string{"KARAKORAM_PROXY_SERVING_PROBE": "1"}
	}
	deploy, err := b.Deploy(t.Context(), name, d)
	if err != nil {
		t.Fatal(err)
	}
	awaitFake(t, b, deploy)
	return op.Services[name]
}
func providerBudgetServer(t *testing.T, rc *engine.Context, handler http.HandlerFunc) {
	t.Helper()
	s := httptest.NewTLSServer(handler)
	t.Cleanup(s.Close)
	rc.Agents = &providerBudgetAgents{backend: rc.Backend.(*Backend), endpoint: s.URL, ca: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: s.Certificate().Raw})}
}

func TestPostgresProviderStatusRequestBudget(t *testing.T) {
	p := postgres.New(postgres.Options{})
	rc, f, b := providerBudgetContext(t, p)
	counts := countProviderGETs(f)
	rc.Cluster.Spec.Topology.Type = "single"
	proxy := rc.Cluster.Spec.Components.Extra["proxy"]
	proxy.Replicas = 2
	rc.Cluster.Spec.Components.Extra["proxy"] = proxy
	rc.Cluster.Spec.DataMcp = &model.DataMcpSpec{Enabled: true}
	dataID := providerBudgetRuntime(t, b, "samplep0", "samplep0v", 1, false)
	proxyID := providerBudgetRuntime(t, b, "samplepx", "", 2, true)
	providerBudgetRuntime(t, b, "samplemcp", "", 1, false)
	f.mu.Lock()
	for _, app := range f.apps {
		for _, task := range app.tasks {
			if task["state"] == "running" {
				task["health"] = "healthy"
			}
		}
	}
	f.mu.Unlock()
	providerBudgetState(t, rc, "postgres", map[string]any{"uid": "sample-identity", "configVersion": 1, "topology": "single", "formed": true, "members": []map[string]any{
		{"name": "samplep0", "role": "data", "stage": "joined", "serviceId": dataID, "volume": "samplep0v", "version": rc.Cluster.Spec.Version, "replicas": 1, "configVersion": 1},
		{"name": "samplepx", "role": "proxy", "stage": "joined", "serviceId": proxyID, "version": rc.Cluster.Spec.Version, "replicas": 2, "configVersion": 1},
	}})
	providerBudgetSecret(t, rc, "datamcp/state", map[string]any{"stage": "ready"})
	providerBudgetSecret(t, rc, "datamcp/config", agentapi.MemberConfig{Cluster: "sample", Member: "samplemcp", ConfigVersion: 1, DesiredProcesses: []string{companionspec.ProcessRead}})
	registry, err := engine.NewRegistry(p)
	if err != nil {
		t.Fatal(err)
	}
	controller := &companion.Controller{Registry: registry}
	providerBudgetServer(t, rc, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/status":
			_ = json.NewEncoder(w).Encode(agentapi.Status{ConfigVersion: 1})
		case "/v1/postgres/status":
			st := pgspec.StatusResponse{Running: true, Role: "standalone", Version: rc.Cluster.Spec.Version, CredentialReceipts: true, SlotStatusKnown: true, HealthCapabilities: &pgspec.HealthCapabilities{ReplicationSlots: true}}
			if strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ") == "samplepx" {
				st.Role = "proxy"
				st.ProxyBackends = []pgspec.ProxyBackend{{Host: "samplep0", Hostgroup: pgspec.ProxyWriterGroup, Status: "ONLINE"}}
			}
			_ = json.NewEncoder(w).Encode(st)
		case "/v1/datamcp/readiness":
			_ = json.NewEncoder(w).Encode(map[string]any{"configVersion": 1, "processes": map[string]any{companionspec.ProcessRead: map[string]bool{"healthy": true}}})
		default:
			http.NotFound(w, r)
		}
	})
	t.Log("create/import/deploy setup")
	counts.report(t, nil, false)
	for pass := range 12 {
		counts.reset()
		ctx := engine.WithObservationPass(t.Context())
		status, err := engine.ObserveStatus(ctx, p, rc)
		if err != nil {
			t.Fatal(err)
		}
		if status.Phase != model.PhaseReady {
			t.Fatalf("healthy provider status did not become ready: %+v", status)
		}
		if len(status.Members) != 2 {
			t.Fatalf("incomplete provider pass: %+v", status)
		}
		for _, m := range status.Members {
			if !m.Health || !m.Agent.Healthy {
				t.Fatalf("member lacks positive proof: %+v", m)
			}
		}
		if err = controller.Observe(ctx, rc, &status); err != nil {
			t.Fatal(err)
		}
		if status.DataMcp == nil || status.DataMcp.Phase != model.DataMcpPhaseReady {
			t.Fatalf("companion pass incomplete: %+v", status.DataMcp)
		}
		t.Logf("two-proxy status pass %d", pass+1)
		counts.report(t, map[string]int{
			"environment.one": 1, "application.one": 3, "cluster.getNodes": 1,
			"swarm.getNodeApps": 1, "docker.getConfig": 2, "deployment.all": 2,
			"docker.getServiceContainersByAppName": 6,
		}, true)
	}
	counts.reset()
	op, err := b.SetContainerCount(t.Context(), "samplepx", 3, 3)
	if err != nil {
		t.Fatal(err)
	}
	awaitFake(t, b, op)
	t.Log("backend marked proxy count change 2->3 including receipt completion (not full provider Sync)")
	counts.report(t, nil, false)
	proxy.Replicas = 3
	rc.Cluster.Spec.Components.Extra["proxy"] = proxy
	var root map[string]any
	if err = json.Unmarshal(rc.Cluster.Internal, &root); err != nil {
		t.Fatal(err)
	}
	root["postgres"].(map[string]any)["members"].([]any)[1].(map[string]any)["replicas"] = 3
	rc.Cluster.Internal, err = json.Marshal(root)
	if err != nil {
		t.Fatal(err)
	}
	for pass := range 12 {
		counts.reset()
		ctx := engine.WithObservationPass(t.Context())
		status, err := engine.ObserveStatus(ctx, p, rc)
		if err != nil {
			t.Fatal(err)
		}
		if status.Phase != model.PhaseReady {
			t.Fatalf("healthy post-scale provider status did not become ready: %+v", status)
		}
		for _, m := range status.Members {
			if !m.Health || !m.Agent.Healthy {
				t.Fatalf("post-scale member lacks positive proof: %+v", m)
			}
		}
		if err = controller.Observe(ctx, rc, &status); err != nil {
			t.Fatal(err)
		}
		if status.DataMcp == nil || status.DataMcp.Phase != model.DataMcpPhaseReady {
			t.Fatalf("companion pass incomplete: %+v", status.DataMcp)
		}
		t.Logf("three-proxy status pass %d", pass+1)
		counts.report(t, map[string]int{
			"environment.one": 1, "application.one": 3, "cluster.getNodes": 1,
			"swarm.getNodeApps": 1, "docker.getConfig": 2, "deployment.all": 2,
			"docker.getServiceContainersByAppName": 8,
		}, true)
	}
}

func TestNeo4jProviderStatusRequestBudget(t *testing.T) {
	p := neo4j.New(neo4j.Options{})
	rc, f, b := providerBudgetContext(t, p)
	counts := countProviderGETs(f)
	id := providerBudgetRuntime(t, b, "samplen0", "samplen0v", 1, false)
	providerBudgetState(t, rc, "neo4j", map[string]any{"uid": "sample-identity", "configVersion": 1, "member": map[string]any{"name": "samplen0", "volume": "samplen0v", "serviceId": id, "stage": "ready", "version": rc.Cluster.Spec.Version, "resources": rc.Cluster.Spec.Components.Engine.Resources}})
	for _, key := range []string{"tls/ca.crt", "tls/servers/samplen0.crt", "tls/servers/samplen0.key", "users/neo4j", "tls/agents/samplen0.crt", "tls/agents/samplen0.key"} {
		if err := rc.Secrets.SetClusterSecret(t.Context(), rc.Cluster.Ref(), key, []byte("fixture")); err != nil {
			t.Fatal(err)
		}
	}
	cfg, err := p.MemberConfig(t.Context(), rc, "samplen0")
	if err != nil {
		t.Fatal(err)
	}
	var spec neo4jspec.Spec
	if err = json.Unmarshal(cfg.EngineSpec, &spec); err != nil {
		t.Fatal(err)
	}
	providerBudgetServer(t, rc, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/status":
			_ = json.NewEncoder(w).Encode(agentapi.Status{ConfigVersion: 1})
		case "/v1/neo4j/readiness":
			_ = json.NewEncoder(w).Encode(neo4jspec.Readiness{Ready: true, ConfigVersion: 1, Version: rc.Cluster.Spec.Version, AppliedRestartToken: spec.RestartToken})
		default:
			http.NotFound(w, r)
		}
	})
	t.Log("create/import/deploy setup")
	counts.report(t, nil, false)
	for pass := range 12 {
		counts.reset()
		status, err := engine.ObserveStatus(engine.WithObservationPass(t.Context()), p, rc)
		if err != nil || status.Phase != model.PhaseReady {
			t.Fatalf("healthy single status: phase=%s err=%v", status.Phase, err)
		}
		t.Logf("status pass %d", pass+1)
		counts.report(t, map[string]int{
			"environment.one": 1, "application.one": 2, "cluster.getNodes": 1,
			"swarm.getNodeApps": 1, "docker.getConfig": 1, "deployment.all": 1,
			"docker.getServiceContainersByAppName": 1,
		}, true)
	}
}
