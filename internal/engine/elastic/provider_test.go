// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/elastic/agentspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/pki"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

type coreInfra struct {
	mu           sync.Mutex
	services     map[string]*infra.Service
	nodes        map[string]string
	mutations    int
	done, failed bool
	imports      [][]infra.ServiceSpec
	calls        []string
	version      func() string
	public       []infra.PublicPortRoute
}

func (f *coreInfra) Name() string { return "test" }
func (f *coreInfra) ProjectInfo(context.Context) (infra.Project, error) {
	return infra.Project{PublicIPv6: "2001:db8::1", PublicIPv4: "192.0.2.1"}, nil
}
func (f *coreInfra) ImportServices(_ context.Context, specs []infra.ServiceSpec) (infra.Op, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.mutations++
	f.imports = append(f.imports, specs)
	services := make(map[string]string, len(specs))
	for _, s := range specs {
		ownership := make(map[string]string, len(s.Env)+len(s.Vars))
		for key, value := range s.Env {
			ownership[key] = value
		}
		for key, value := range s.Vars {
			ownership[key] = value
		}
		f.services[s.Hostname] = &infra.Service{ID: s.Hostname, Hostname: s.Hostname, Type: s.Type, Resources: s.Resources, Ownership: ownership, MinContainers: s.MinContainers, MaxContainers: s.MaxContainers}
		services[s.Hostname] = s.Hostname
		f.calls = append(f.calls, "import:"+s.Hostname)
	}
	return infra.Op{ID: "import", Services: services}, nil
}
func (f *coreInfra) GetService(_ context.Context, name string) (*infra.Service, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.services[name], nil
}
func (f *coreInfra) ListServices(context.Context, string) ([]infra.Service, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := []infra.Service{}
	for _, s := range f.services {
		out = append(out, *s)
	}
	return out, nil
}
func (f *coreInfra) DeleteService(_ context.Context, name string) (infra.Op, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.mutations++
	f.calls = append(f.calls, "delete:"+name)
	delete(f.services, name)
	delete(f.nodes, name)
	return infra.Op{ID: "delete"}, nil
}
func (f *coreInfra) Deploy(_ context.Context, name string, d infra.Deployment) (infra.Op, error) {
	if _, err := io.Copy(io.Discard, d.Artifact); err != nil {
		return infra.Op{}, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.mutations++
	f.calls = append(f.calls, "deploy:"+name)
	if strings.Contains(d.ZeropsYAML, "downloads/elasticsearch/") {
		f.nodes[name] = f.version()
	}
	return infra.Op{ID: "deploy"}, nil
}
func (*coreInfra) SetEnvSecrets(context.Context, string, map[string]string) error { return nil }
func (f *coreInfra) SetEnvVars(_ context.Context, name string, vars map[string]string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	service := f.services[name]
	if service == nil {
		return errors.New("service " + name + " not found")
	}
	if service.Ownership == nil {
		service.Ownership = map[string]string{}
	}
	for key, value := range vars {
		service.Ownership[key] = value
	}
	return nil
}
func (f *coreInfra) lifecycle(name, action string) (infra.Op, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.mutations++
	f.calls = append(f.calls, action+":"+name)
	return infra.Op{ID: action}, nil
}
func (f *coreInfra) Start(_ context.Context, name string) (infra.Op, error) {
	return f.lifecycle(name, "start")
}
func (f *coreInfra) Stop(_ context.Context, name string) (infra.Op, error) {
	return f.lifecycle(name, "stop")
}
func (f *coreInfra) Restart(_ context.Context, name string) (infra.Op, error) {
	return f.lifecycle(name, "restart")
}
func (f *coreInfra) SetResources(_ context.Context, name string, r infra.Resources) (infra.Op, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.mutations++
	f.calls = append(f.calls, "resources:"+name)
	if service := f.services[name]; service != nil {
		service.Resources = r
	}
	return infra.Op{ID: "resources"}, nil
}
func (f *coreInfra) SetContainerCount(_ context.Context, name string, min, max int) (infra.Op, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.mutations++
	f.calls = append(f.calls, "containers:"+name)
	f.services[name].MinContainers = min
	f.services[name].MaxContainers = max
	return infra.Op{ID: "containers"}, nil
}
func (f *coreInfra) EnsurePublicPort(_ context.Context, name string, port infra.PublicPort) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.mutations++
	f.calls = append(f.calls, "public:"+name)
	f.public = append(f.public, infra.PublicPortRoute{Hostname: name, PublicPort: port})
	return nil
}
func (f *coreInfra) ListPublicPorts(context.Context) ([]infra.PublicPortRoute, error) {
	return f.public, nil
}
func (f *coreInfra) DeletePublicPorts(_ context.Context, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.mutations++
	f.calls = append(f.calls, "unpublic:"+name)
	f.public = slices.DeleteFunc(f.public, func(r infra.PublicPortRoute) bool { return r.Hostname == name })
	return nil
}

// Containers fails for a missing service like the Zerops backend does.
func (f *coreInfra) Containers(_ context.Context, name string) ([]infra.Container, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.services[name] == nil {
		return nil, errors.New("service " + name + " not found")
	}
	return []infra.Container{{Status: "ACTIVE", PlacementGroup: "group-a", Resources: &domain.ObservedResources{CPUCores: 1, MemoryGB: 2, DiskGB: 5}}}, nil
}
func (*coreInfra) Logs(context.Context, string, infra.LogQuery) ([]infra.LogLine, error) {
	return nil, nil
}
func (f *coreInfra) OpStatus(context.Context, infra.Op) (infra.OpState, error) {
	return infra.OpState{Done: f.done, Failed: f.failed, Message: "platform failed"}, nil
}
func (f *coreInfra) SetSubdomainAccess(_ context.Context, name string, enabled bool) (infra.Op, error) {
	return f.lifecycle(name, "subdomain:"+strconv.FormatBool(enabled))
}
func (*coreInfra) SubdomainURLs(_ context.Context, name string) ([]string, error) {
	return []string{"https://" + name + ".example.com"}, nil
}

type coreArtifact struct{}

func (coreArtifact) Open(context.Context) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader("artifact")), nil
}

type coreAgentDialer struct {
	url string
	ca  []byte
}

func (d coreAgentDialer) Dial(context.Context, domain.ClusterRef, string) (*agentapi.Client, error) {
	return agentapi.NewClient(agentapi.ClientOptions{BaseURL: d.url, CAPEM: d.ca, Token: "test-token"})
}

type coreDialer struct{ address string }

func (d coreDialer) DialContext(ctx context.Context, network, _ string) (net.Conn, error) {
	return (&net.Dialer{}).DialContext(ctx, network, d.address)
}

type coreClock struct{ at time.Time }

func (c *coreClock) Now() time.Time { return c.at }

type coreEvents struct{ events []domain.Event }

func (r *coreEvents) Record(_ context.Context, e domain.Event) error {
	r.events = append(r.events, e)
	return nil
}

type coreES struct {
	mu                       sync.Mutex
	infra                    *coreInfra
	uuid, master, health     string
	shards                   map[string]int
	replicas                 int
	autoExpand               string
	persistent               map[string]any
	transient                map[string]any
	mutations                []string
	tokens                   int
	snapshots                []string
	unassignedPrimary        bool
	relocating, initializing int
	voters                   []string
	shardRows                []map[string]string
	allocationExplain        map[string]any
}

func (f *coreES) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	reply := func(v any) { _ = json.NewEncoder(w).Encode(v) }
	if r.Method != "GET" {
		f.mutations = append(f.mutations, r.Method+" "+r.URL.Path)
	}
	switch {
	case r.URL.Path == "/":
		reply(map[string]any{"cluster_uuid": f.uuid, "cluster_name": "demo", "version": map[string]string{"number": "9.5.5"}})
	case r.URL.Path == "/_nodes":
		f.infra.mu.Lock()
		nodes := map[string]any{}
		for name, version := range f.infra.nodes {
			roles := []string{"master", "data"}
			if strings.HasPrefix(name, "demom") {
				roles = []string{"master"}
			}
			nodes[name] = map[string]any{"name": name, "version": version, "roles": roles}
		}
		f.infra.mu.Unlock()
		reply(map[string]any{"nodes": nodes})
	case r.URL.Path == "/_cluster/state/metadata":
		voters := f.voters
		if voters == nil {
			for name := range f.infra.nodes {
				voters = append(voters, name)
			}
		}
		reply(map[string]any{"metadata": map[string]any{"cluster_coordination": map[string]any{"last_committed_config": voters}}})
	case r.URL.Path == "/_cat/master":
		reply([]map[string]string{{"node": f.master}})
	case r.URL.Path == "/_cluster/health":
		reply(map[string]any{"status": f.health, "number_of_nodes": len(f.infra.nodes), "number_of_data_nodes": len(f.infra.nodes), "relocating_shards": f.relocating, "initializing_shards": f.initializing})
	case r.URL.Path == "/_cat/allocation":
		rows := []map[string]any{}
		f.infra.mu.Lock()
		for node := range f.infra.nodes {
			if strings.HasPrefix(node, "demom") {
				continue
			}
			rows = append(rows, map[string]any{"node": node, "shards": strconv.Itoa(f.shards[node])})
		}
		f.infra.mu.Unlock()
		reply(rows)
	case r.URL.Path == "/_cat/shards":
		rows := f.shardRows
		if rows == nil {
			names := []string{}
			for name := range f.infra.nodes {
				if strings.HasPrefix(name, "demon") {
					names = append(names, name)
				}
			}
			slices.Sort(names)
			for i := 0; i <= f.replicas; i++ {
				row := map[string]string{"index": "logs", "shard": "0", "state": "UNASSIGNED", "prirep": "r"}
				if i == 0 {
					row["prirep"] = "p"
				}
				if i < len(names) && !(i == 0 && f.unassignedPrimary) {
					row["state"], row["node"] = "STARTED", names[i]
				}
				rows = append(rows, row)
			}
			// Existing yellow fixtures represent at least one missing replica.
			if f.health == "yellow" && f.replicas == 0 && !f.unassignedPrimary {
				rows = append(rows, map[string]string{"index": "logs", "shard": "0", "state": "UNASSIGNED", "prirep": "r"})
			}
		}
		reply(rows)
	case r.URL.Path == "/_cluster/allocation/explain":
		reply(f.allocationExplain)
	case r.URL.Path == "/_all/_settings":
		settings := map[string]string{"index.number_of_replicas": strconv.Itoa(f.replicas)}
		if f.autoExpand != "" {
			settings["index.auto_expand_replicas"] = f.autoExpand
		}
		reply(map[string]any{"logs": map[string]any{"settings": settings}})
	case r.URL.Path == "/_cluster/settings":
		if r.Method == "PUT" {
			var body struct {
				Persistent map[string]any `json:"persistent"`
				Transient  map[string]any `json:"transient"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			for k, v := range body.Persistent {
				if v == nil {
					delete(f.persistent, k)
				} else {
					f.persistent[k] = v
				}
			}
			for k, v := range body.Transient {
				if v == nil {
					delete(f.transient, k)
				} else {
					if f.transient == nil {
						f.transient = map[string]any{}
					}
					f.transient[k] = v
				}
			}
		}
		reply(map[string]any{"persistent": f.persistent, "transient": f.transient})
	case r.URL.Path == "/_flush" || r.URL.Path == "/_cluster/voting_config_exclusions" || r.URL.Path == "/_nodes/reload_secure_settings":
		reply(map[string]any{"acknowledged": true})
	case r.URL.Path == "/_security/_authenticate":
		reply(map[string]string{"username": "elastic/kibana", "authentication_type": "token"})
	case strings.HasPrefix(r.URL.Path, "/_security/service/") && r.Method == "POST":
		f.tokens++
		reply(map[string]any{"created": true, "token": map[string]string{"name": "kk-token", "value": "test-kibana-token"}})
	case strings.HasPrefix(r.URL.Path, "/_security/"):
		reply(map[string]any{"created": true, "found": true})
	case strings.HasPrefix(r.URL.Path, "/_snapshot/") && strings.HasSuffix(r.URL.Path, "/_all"):
		out := []map[string]string{}
		for _, name := range f.snapshots {
			out = append(out, map[string]string{"snapshot": name, "state": "SUCCESS"})
		}
		reply(map[string]any{"snapshots": out})
	case strings.HasPrefix(r.URL.Path, "/_snapshot/") && r.Method == "DELETE":
		_, name, _ := strings.Cut(strings.TrimPrefix(r.URL.Path, "/_snapshot/"), "/")
		f.snapshots = slices.DeleteFunc(f.snapshots, func(s string) bool { return s == name })
		reply(map[string]bool{"acknowledged": true})
	case strings.HasSuffix(r.URL.Path, "/_verify"):
		reply(map[string]any{"nodes": map[string]any{"n": map[string]string{"name": "demon0"}}})
	case strings.HasPrefix(r.URL.Path, "/_snapshot/"):
		reply(map[string]bool{"acknowledged": true})
	default:
		http.Error(w, "unexpected endpoint", 404)
	}
}
func coreSetup(t *testing.T, replicas int) (*Provider, *engine.Context, *coreInfra, *coreES) {
	t.Helper()
	ctx := context.Background()
	sealer, err := secrets.New(strings.Repeat("x", 32))
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(filepath.Join(t.TempDir(), "state.db"), sealer)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	p := New(ProviderOptions{Store: st})
	c := &domain.Cluster{Name: "demo"}
	p.ApplyDefaults(&c.Spec)
	c.Spec.Components.Engine.Replicas = replicas
	if err = st.CreateCluster(ctx, c); err != nil {
		t.Fatal(err)
	}
	ca, err := NewCA("test", 365*24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if err = st.SetClusterSecret(ctx, c.Ref(), secretCA, ca.CertPEM); err != nil {
		t.Fatal(err)
	}
	if err = st.SetClusterSecret(ctx, c.Ref(), secretCAKey, ca.KeyPEM); err != nil {
		t.Fatal(err)
	}
	names := []string{"demokb", "demols", "demoapm"}
	for i := range 50 {
		names = append(names, NodeHostname("demo", i), MasterHostname("demo", i))
	}
	crt, key, err := ca.Issue(pki.CertRequest{CommonName: "test", DNSNames: names, Server: true, Validity: 365 * 24 * time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	cert, err := tls.X509KeyPair(crt, key)
	if err != nil {
		t.Fatal(err)
	}
	f := &coreInfra{services: map[string]*infra.Service{}, nodes: map[string]string{}, done: true, version: func() string { return c.Spec.Version }}
	es := &coreES{infra: f, uuid: "test-uuid", master: "demon0", health: "green", shards: map[string]int{}, persistent: map[string]any{}}
	server := httptest.NewUnstartedServer(es)
	server.TLS = &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
	server.StartTLS()
	t.Cleanup(server.Close)
	agentServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-token" {
			t.Error("missing agent authentication")
		}
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/v1/status":
			_ = json.NewEncoder(w).Encode(agentapi.Status{EngineVersion: c.Spec.Version, ConfigVersion: 1 << 30, Processes: map[string]agentapi.ProcessStatus{"elasticsearch": {State: "running"}, "kibana": {State: "running"}, "logstash": {State: "running"}, "apm-server": {State: "running"}}})
		case "/v1/elasticsearch/readiness":
			_ = json.NewEncoder(w).Encode(agentspec.Readiness{Ready: true, ConfigVersion: 1 << 30})
		case "/v1/reload":
			_ = json.NewEncoder(w).Encode(map[string]bool{"success": true})
		default:
			http.Error(w, "unknown", 404)
		}
	}))
	t.Cleanup(agentServer.Close)
	agentCA := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: agentServer.Certificate().Raw})
	rc := &engine.Context{Cluster: c, Store: st, Secrets: st, Backend: f, Agents: coreAgentDialer{agentServer.URL, agentCA}, Dialer: coreDialer{server.Listener.Addr().String()}, Artifact: coreArtifact{}, Clock: &coreClock{at: time.Now().UTC()}, Events: &coreEvents{}}
	return p, rc, f, es
}
func coreConverge(t *testing.T, p *Provider, rc *engine.Context, f *coreInfra) *state {
	t.Helper()
	for range 300 {
		before := f.mutations
		result, err := p.Sync(context.Background(), rc)
		if err != nil {
			t.Fatal(err)
		}
		if f.mutations-before > 1 {
			t.Fatal("multiple platform mutations per Sync")
		}
		s, err := load(rc.Cluster)
		if err != nil {
			t.Fatal(err)
		}
		if s.Phase == "ready" && s.Pending == nil && s.Rolling == nil {
			if result.RequeueAfter != 30*time.Second {
				t.Fatal("ready requeue", result)
			}
			return s
		}
	}
	t.Fatal("cluster did not converge")
	return nil
}
func cloneCluster(t *testing.T, c *domain.Cluster) *domain.Cluster {
	t.Helper()
	b, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	var out domain.Cluster
	if err = json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	return &out
}

func TestProviderCatalogDefaultsValidation(t *testing.T) {
	p, rc, _, _ := coreSetup(t, 1)
	info := p.Describe()
	if info.Name != "elasticsearch" || len(info.Versions) != 4 || len(info.Topologies) != 2 || len(info.Presets) != 4 || !slices.Contains(info.Capabilities, "elasticAccess") {
		t.Fatal(info)
	}
	for _, preset := range info.Presets {
		c := &domain.Cluster{Name: "preset", Spec: domain.ClusterSpec{Topology: preset.Topology, Components: preset.Components}}
		p.ApplyDefaults(&c.Spec)
		if err := p.Validate(context.Background(), nil, c); err != nil {
			t.Fatalf("preset %s: %v", preset.Name, err)
		}
	}
	if err := p.Validate(context.Background(), nil, rc.Cluster); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		change func(*domain.Cluster)
	}{{"name", func(c *domain.Cluster) { c.Name = "BAD" }}, {"provider", func(c *domain.Cluster) { c.Spec.Provider = "psmdb" }}, {"version", func(c *domain.Cluster) { c.Spec.Version = "9.0.0" }}, {"topology", func(c *domain.Cluster) { c.Spec.Topology.Type = "sharded" }}, {"replicas", func(c *domain.Cluster) { c.Spec.Components.Engine.Replicas = 10 }}, {"ram", func(c *domain.Cluster) { c.Spec.Components.Engine.Resources.MinRAM = 1 }}, {"tls", func(c *domain.Cluster) { c.Spec.TLS.Mode = "disabled" }}, {"cpu", func(c *domain.Cluster) { c.Spec.Components.Engine.Resources.MinCPU = 1.5 }}, {"options", func(c *domain.Cluster) {
		c.Spec.EngineOptions = json.RawMessage(`{"elasticsearch":{"settings":{"node.roles":["master"]}}}`)
	}}, {"engineConfig", func(c *domain.Cluster) { c.Spec.EngineConfig = "foo: bar" }}, {"storage", func(c *domain.Cluster) { c.Spec.Backup.Storage = "absent" }}, {"cron", func(c *domain.Cluster) {
		c.Spec.Backup.Schedules = []domain.BackupSchedule{{Cron: "bad", Type: "snapshot", RetainCount: 1}}
	}}, {"public", func(c *domain.Cluster) { c.Spec.Exposure.Public = true }}} {
		t.Run(tc.name, func(t *testing.T) {
			c := cloneCluster(t, rc.Cluster)
			tc.change(c)
			if err := p.Validate(context.Background(), nil, c); !errors.Is(err, engine.ErrInvalid) {
				t.Fatal("expected invalid", err)
			}
		})
	}
	for _, count := range []int{1, 2, 3, 9} {
		c := cloneCluster(t, rc.Cluster)
		c.Spec.Components.Engine.Replicas = count
		if err := p.Validate(context.Background(), nil, c); err != nil {
			t.Fatal(err)
		}
	}
	c := cloneCluster(t, rc.Cluster)
	c.Spec.Topology.Type = "dedicatedMasters"
	p.ApplyDefaults(&c.Spec)
	for _, count := range []int{3, 5} {
		master := c.Spec.Components.Extra["master"]
		master.Replicas = count
		c.Spec.Components.Extra["master"] = master
		if err := p.Validate(context.Background(), nil, c); err != nil {
			t.Fatal(err)
		}
	}
	master := c.Spec.Components.Extra["master"]
	master.Replicas = 4
	c.Spec.Components.Extra["master"] = master
	if err := p.Validate(context.Background(), nil, c); err == nil {
		t.Fatal("accepted four dedicated masters")
	}
	old := cloneCluster(t, rc.Cluster)
	old.Spec.Version = "9.4.8"
	n := cloneCluster(t, rc.Cluster)
	n.Spec.Components.Engine.Replicas = 3
	if err := p.Validate(context.Background(), old, n); err == nil {
		t.Fatal("accepted upgrade with scaling")
	}
	n = cloneCluster(t, rc.Cluster)
	if err := p.Validate(context.Background(), old, n); err != nil {
		t.Fatal("rejected upgrade", err)
	}
	var defaults domain.ClusterSpec
	p.ApplyDefaults(&defaults)
	if defaults.Components.Extra["kibana"].Replicas != 0 || defaults.TLS.Mode != "requireTLS" || !defaults.Monitoring.Enabled {
		t.Fatal(defaults)
	}
	defaults.Components.Engine.Resources.MinRAM = 8
	defaults.Components.Engine.Resources.MaxRAM = 10
	defaults.Components.Engine.Replicas = 2
	p.ApplyDefaults(&defaults)
	if defaults.Components.Engine.Resources.MinRAM != 8 || defaults.Components.Engine.Resources.MaxRAM != 10 || defaults.Components.Engine.Replicas != 2 {
		t.Fatal("defaults replaced explicit settings")
	}
}

func TestProviderExposureValidation(t *testing.T) {
	p, rc, _, _ := coreSetup(t, 1)
	for _, tc := range []struct {
		name     string
		exposure domain.Exposure
		legacy   bool
		valid    bool
	}{
		{"family mismatch", domain.Exposure{Public: true, IPType: "IPV6", Allowlist: []string{"192.0.2.0/24"}}, false, false},
		{"full family", domain.Exposure{Public: true, IPType: "IPV6", Allowlist: []string{"::/1", "8000::/1"}}, false, false},
		{"acknowledged", domain.Exposure{Public: true, IPType: "IPV6", Allowlist: []string{"::/0"}, AllowAllAddresses: true}, false, true},
		{"private", domain.Exposure{IPType: "IPV6", Allowlist: []string{"::/0"}}, false, true},
		{"unchanged legacy", domain.Exposure{Public: true, IPType: "IPV6", Allowlist: []string{"192.0.2.0/24"}}, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			next := cloneCluster(t, rc.Cluster)
			next.Spec.Exposure = tc.exposure
			var previous *domain.Cluster
			if tc.legacy {
				previous = cloneCluster(t, next)
				next.Spec.Monitoring.Enabled = !previous.Spec.Monitoring.Enabled
			}
			err := p.Validate(t.Context(), previous, next)
			if tc.valid && err != nil {
				t.Fatal(err)
			}
			if !tc.valid && !errors.Is(err, engine.ErrInvalid) {
				t.Fatalf("expected invalid exposure, got %v", err)
			}
		})
	}
}

func TestInitialProvisioningPendingBootstrapAndSecurity(t *testing.T) {
	p, rc, f, es := coreSetup(t, 3)
	ctx := context.Background()
	if _, err := p.Sync(ctx, rc); err != nil {
		t.Fatal(err)
	}
	s, _ := load(rc.Cluster)
	if len(s.InitialMasterNodes) != 3 {
		t.Fatal(s.InitialMasterNodes)
	}
	if _, err := p.Sync(ctx, rc); err != nil {
		t.Fatal(err)
	}
	f.done = false
	before := f.mutations
	for range 3 {
		if _, err := p.Sync(ctx, rc); err != nil {
			t.Fatal(err)
		}
	}
	s, _ = load(rc.Cluster)
	if f.mutations != before || s.Members[0].Stage != stagePlanned {
		t.Fatal("advanced pending import")
	}
	f.done = true
	if _, err := p.Sync(ctx, rc); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Sync(ctx, rc); err != nil {
		t.Fatal(err)
	}
	cfg, err := p.MemberConfig(ctx, rc, "demon0")
	if err != nil {
		t.Fatal(err)
	}
	var spec agentspec.Spec
	_ = json.Unmarshal(cfg.EngineSpec, &spec)
	token := spec.RestartToken
	if len(spec.Elasticsearch.InitialMasterNodes) != 3 || len(spec.Elasticsearch.SeedHosts) != 3 || spec.HeapMB != 768 {
		t.Fatal(spec)
	}
	s = coreConverge(t, p, rc, f)
	cfg, err = p.MemberConfig(ctx, rc, "demon0")
	if err != nil {
		t.Fatal(err)
	}
	spec = agentspec.Spec{}
	_ = json.Unmarshal(cfg.EngineSpec, &spec)
	if s.ClusterUUID != "test-uuid" || len(spec.Elasticsearch.InitialMasterNodes) != 0 || spec.RestartToken != token {
		t.Fatal("bootstrap removal restarted node", spec)
	}
	if es.tokens != 1 || !slices.Contains(es.mutations, "POST /_security/user/elastic/_password") || !slices.Contains(es.mutations, "PUT /_security/role/karakoram_logstash_writer") || !slices.Contains(es.mutations, "PUT /_security/user/karakoram_apm") {
		t.Fatal(es.mutations)
	}
	for _, imported := range f.imports {
		if len(imported) != 2 || imported[0].Type != "local-storage:single@1" || imported[1].ZeropsYAML != "" || imported[1].Env["KARAKORAM_AGENT_TOKEN"] == "" {
			t.Fatal(imported)
		}
	}
	if strings.Contains(string(rc.Cluster.Internal), "test-kibana-token") || strings.Contains(string(rc.Cluster.Internal), "BEGIN PRIVATE KEY") || strings.Contains(string(rc.Cluster.Internal), "password") {
		t.Fatal("secrets persisted in Internal")
	}
}

// Zerops reports unknown services as errors; status during provisioning
// must not fail on members whose runtime is not imported yet.
func TestStatusToleratesUnimportedMembers(t *testing.T) {
	p, rc, _, _ := coreSetup(t, 3)
	ctx := context.Background()
	for range 3 {
		if _, err := p.Sync(ctx, rc); err != nil {
			t.Fatal(err)
		}
	}
	s, _ := load(rc.Cluster)
	if s.Members[len(s.Members)-1].Stage != stagePlanned {
		t.Fatal("expected an unimported member")
	}
	status, err := p.Status(ctx, rc)
	if err != nil {
		t.Fatal(err)
	}
	if len(status.Members) != 3 {
		t.Fatalf("members=%d", len(status.Members))
	}
	if _, err = p.Placement(ctx, rc); err != nil {
		t.Fatal(err)
	}
}
func TestStackComponentProvisioningOrderAndReplicaChanges(t *testing.T) {
	p, rc, f, _ := coreSetup(t, 1)
	for _, kind := range []string{kindKibana, kindLogstash, kindAPM} {
		c := rc.Cluster.Spec.Components.Extra[kind]
		c.Replicas = 1
		rc.Cluster.Spec.Components.Extra[kind] = c
	}
	coreConverge(t, p, rc, f)
	deploys := []string{}
	for _, call := range f.calls {
		if strings.HasPrefix(call, "deploy:") {
			deploys = append(deploys, call)
		}
	}
	if !reflect.DeepEqual(deploys, []string{"deploy:demon0", "deploy:demokb", "deploy:demols", "deploy:demoapm"}) {
		t.Fatal(deploys)
	}
	kb := rc.Cluster.Spec.Components.Extra[kindKibana]
	kb.Replicas = 2
	rc.Cluster.Spec.Components.Extra[kindKibana] = kb
	coreConverge(t, p, rc, f)
	if !slices.Contains(f.calls, "containers:demokb") {
		t.Fatal(f.calls)
	}
	kb.Replicas = 0
	rc.Cluster.Spec.Components.Extra[kindKibana] = kb
	s := coreConverge(t, p, rc, f)
	if s.find("demokb") != nil || f.services["demokb"] != nil {
		t.Fatal("disabled component retained")
	}
}
func TestScaleUpSeedsAndDrainingBeforeDeletion(t *testing.T) {
	p, rc, f, es := coreSetup(t, 3)
	coreConverge(t, p, rc, f)
	rc.Cluster.Spec.Components.Engine.Replicas = 4
	coreConverge(t, p, rc, f)
	cfg, err := p.MemberConfig(context.Background(), rc, "demon0")
	if err != nil {
		t.Fatal(err)
	}
	var spec agentspec.Spec
	_ = json.Unmarshal(cfg.EngineSpec, &spec)
	if !slices.Contains(spec.Elasticsearch.SeedHosts, "demon3:9300") {
		t.Fatal(spec.Elasticsearch.SeedHosts)
	}
	rc.Cluster.Spec.Components.Engine.Replicas = 3
	es.shards["demon3"] = 2
	for range 3 {
		if _, err = p.Sync(context.Background(), rc); err != nil {
			t.Fatal(err)
		}
	}
	s, _ := load(rc.Cluster)
	if s.find("demon3").Stage != stageDraining || f.services["demon3"] == nil {
		t.Fatal("node deleted before drain")
	}
	if !strings.Contains(es.persistent["cluster.routing.allocation.exclude._name"].(string), "demon3") {
		t.Fatal(es.persistent)
	}
	es.shards["demon3"] = 0
	coreConverge(t, p, rc, f)
	if !slices.Contains(es.mutations, "POST /_cluster/voting_config_exclusions") || !slices.Contains(es.mutations, "DELETE /_cluster/voting_config_exclusions") {
		t.Fatal(es.mutations)
	}
	runtime := slices.Index(f.calls, "delete:demon3")
	volume := slices.Index(f.calls, "delete:demon3v")
	if runtime < 0 || volume <= runtime {
		t.Fatal(f.calls)
	}
	if _, ok := es.persistent["cluster.routing.allocation.exclude._name"]; ok {
		t.Fatal("allocation exclusion not cleared")
	}
}
func TestScaleDownPreflightBlocksInsufficientReplicas(t *testing.T) {
	p, rc, f, es := coreSetup(t, 3)
	coreConverge(t, p, rc, f)
	es.replicas = 2
	rc.Cluster.Spec.Components.Engine.Replicas = 2
	for range 4 {
		if _, err := p.Sync(context.Background(), rc); err != nil {
			t.Fatal(err)
		}
	}
	s, _ := load(rc.Cluster)
	if s.DrainBlocked == "" || s.find("demon2").Stage != stageJoined || f.services["demon2"] == nil {
		t.Fatal("unsafe scale down")
	}
}
func TestRollingUpgradeElectedMasterLastAndAllocationReset(t *testing.T) {
	p, rc, f, es := coreSetup(t, 3)
	rc.Cluster.Spec.Version = "9.4.8"
	coreConverge(t, p, rc, f)
	before := len(f.calls)
	es.master = "demon1"
	rc.Cluster.Spec.Version = "9.5.5"
	coreConverge(t, p, rc, f)
	deploys := []string{}
	for _, call := range f.calls[before:] {
		if strings.HasPrefix(call, "deploy:") {
			deploys = append(deploys, call)
		}
	}
	if !reflect.DeepEqual(deploys, []string{"deploy:demon0", "deploy:demon2", "deploy:demon1"}) {
		t.Fatal(deploys)
	}
	if _, ok := es.persistent["cluster.routing.allocation.enable"]; ok {
		t.Fatal("allocation not restored")
	}
	if count := strings.Count(strings.Join(es.mutations, "\n"), "PUT /_cluster/settings"); count != 6 {
		t.Fatal("expected primaries/null per member", es.mutations)
	}
}
func TestMemberConfigGatesStaticChangesAndRefreshesSecrets(t *testing.T) {
	p, rc, f, _ := coreSetup(t, 1)
	kb := rc.Cluster.Spec.Components.Extra[kindKibana]
	kb.Replicas = 1
	rc.Cluster.Spec.Components.Extra[kindKibana] = kb
	coreConverge(t, p, rc, f)
	ctx := context.Background()
	cfg, err := p.MemberConfig(ctx, rc, "demon0")
	if err != nil {
		t.Fatal(err)
	}
	var before agentspec.Spec
	_ = json.Unmarshal(cfg.EngineSpec, &before)
	rc.Cluster.Spec.Components.Engine.Resources.MinRAM = 4
	rc.Cluster.Spec.Components.Engine.Resources.MaxRAM = 4
	cfg, err = p.MemberConfig(ctx, rc, "demon0")
	if err != nil {
		t.Fatal(err)
	}
	var after agentspec.Spec
	_ = json.Unmarshal(cfg.EngineSpec, &after)
	if before.RestartToken != after.RestartToken || after.HeapMB != 768 {
		t.Fatal("static config escaped rolling gate")
	}
	if err = putSecret(ctx, rc, secretKibanaToken, []byte("rotated-token")); err != nil {
		t.Fatal(err)
	}
	cfg, err = p.MemberConfig(ctx, rc, "demokb")
	if err != nil {
		t.Fatal(err)
	}
	_ = json.Unmarshal(cfg.EngineSpec, &after)
	if after.Kibana.ServiceAccountToken != "rotated-token" {
		t.Fatal("credentials not refreshed")
	}
	if cfg.Engine != "elasticsearch" || cfg.AgentTLS.CertPEM == "" || before.Elasticsearch.FileUsers == "" || before.Elasticsearch.TransportCert.CertPEM == "" {
		t.Fatal("missing agent payload")
	}
}
func TestCleanupSnapshotsComponentsRuntimeVolumeOrdering(t *testing.T) {
	p, rc, f, es := coreSetup(t, 1)
	kb := rc.Cluster.Spec.Components.Extra[kindKibana]
	kb.Replicas = 1
	rc.Cluster.Spec.Components.Extra[kindKibana] = kb
	s := coreConverge(t, p, rc, f)
	s.Repositories["test"] = &repository{Name: "kk-test", BasePath: "owned"}
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	es.snapshots = []string{"one", "two"}
	before := len(f.calls)
	for i := range 30 {
		mutations := f.mutations
		done, err := p.Cleanup(context.Background(), rc, domain.DeleteOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if f.mutations-mutations > 1 {
			t.Fatal("multiple cleanup mutations")
		}
		if done {
			break
		}
		if i == 29 {
			t.Fatal("cleanup incomplete")
		}
	}
	calls := f.calls[before:]
	if !reflect.DeepEqual(calls, []string{"delete:demokb", "delete:demon0", "delete:demon0v"}) {
		t.Fatal(calls)
	}
	if len(es.snapshots) != 0 {
		t.Fatal("snapshots retained")
	}
	if _, err := secret(context.Background(), rc, secretAdmin); !missing(err) {
		t.Fatal("secrets not deleted")
	}
}
func TestCleanupKeepVolumesAndResume(t *testing.T) {
	p, rc, f, es := coreSetup(t, 1)
	s := coreConverge(t, p, rc, f)
	s.Rolling = &rolling{Reason: "restart", OperationID: "retired", Current: "demon0", Step: "apply"}
	s.Members[0].Removing = true
	s.Members[0].Replaces = "retired-member"
	s.Members[0].AllocationExcluded = true
	s.DrainBlocked = "old removal"
	es.persistent["cluster.routing.allocation.enable"] = "primaries"
	es.transient = map[string]any{"cluster.routing.allocation.exclude._name": "operator,demon0"}
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	for range 20 {
		done, err := p.Cleanup(context.Background(), rc, domain.DeleteOptions{KeepVolumes: true, KeepBackups: true})
		if err != nil {
			t.Fatal(err)
		}
		if done {
			break
		}
	}
	s, _ = load(rc.Cluster)
	if !s.Suspended || f.services["demon0v"] == nil || f.services["demon0"] != nil {
		t.Fatal("invalid retained-volume cleanup")
	}
	if s.Rolling != nil || s.DrainBlocked != "" || s.Members[0].Removing || s.Members[0].Replaces != "" || s.Members[0].AllocationExcluded || !s.ResumeAllocationReset {
		t.Fatal("retired operation bookkeeping survived cleanup", s)
	}
	retained, err := rc.Store.ListRetainedVolumes(context.Background(), rc.Cluster.Ref())
	if err != nil || len(retained) != 1 {
		t.Fatal(retained, err)
	}
	s = coreConverge(t, p, rc, f)
	if s.ClusterUUID != "test-uuid" || len(s.InitialMasterNodes) != 0 {
		t.Fatal("resume attempted bootstrap")
	}
	if _, ok := es.persistent["cluster.routing.allocation.enable"]; ok || len(s.ResumeExcludeNames) != 0 || s.ResumeAllocationReset {
		t.Fatal("retained resume did not restore shard allocation", s, es.persistent)
	}
	if es.persistent["cluster.routing.allocation.exclude._name"] != "operator" {
		t.Fatal("resume lost the operator's exclusion", es.persistent)
	}
}
func TestPlatformRetryPersistsActionAndDoesNotAdvance(t *testing.T) {
	p, rc, f, _ := coreSetup(t, 1)
	ctx := context.Background()
	_, _ = p.Sync(ctx, rc)
	_, _ = p.Sync(ctx, rc)
	f.failed = true
	_, err := p.Sync(ctx, rc)
	if err == nil {
		t.Fatal("platform failure not returned")
	}
	s, _ := load(rc.Cluster)
	if s.Retry == nil || s.Pending != nil {
		t.Fatal("retry not persisted")
	}
	before := f.mutations
	_, err = p.Sync(ctx, rc)
	var wait *engine.PlatformRetryWait
	if !errors.As(err, &wait) || f.mutations != before {
		t.Fatal("failed action immediately reissued", err)
	}
	rc.Clock.(*coreClock).at = rc.Clock.Now().Add(time.Minute)
	f.failed = false
	if _, err = p.Sync(ctx, rc); err != nil {
		t.Fatal(err)
	}
	s, _ = load(rc.Cluster)
	if s.Pending != nil {
		t.Fatal("adopting completed partial import should not mutate")
	}
	if s.Members[0].Stage != stageImported {
		t.Fatal("partial import not adopted")
	}
}
func TestRelayTargetsAndConnectionStatus(t *testing.T) {
	p, rc, f, _ := coreSetup(t, 1)
	for _, kind := range []string{kindKibana, kindLogstash, kindAPM} {
		c := rc.Cluster.Spec.Components.Extra[kind]
		c.Replicas = 1
		rc.Cluster.Spec.Components.Extra[kind] = c
	}
	rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"kibana":{"public":true},"apm":{"public":true}}`)
	coreConverge(t, p, rc, f)
	targets := p.RelayTargets(rc.Cluster)
	for _, target := range []string{"demon0:9443", "demon0:9200", "demokb:5601", "demols:9600", "demols:5044", "demols:8080", "demoapm:8200"} {
		if !slices.Contains(targets, target) {
			t.Fatal(targets)
		}
	}
	details, err := p.Connection(context.Background(), rc)
	if err != nil {
		t.Fatal(err)
	}
	if details.CACertificate == "" || len(details.Credentials) != 2 || details.Credentials[0].Username != "elastic" {
		t.Fatal(details)
	}
	status, err := p.Status(context.Background(), rc)
	if err != nil {
		t.Fatal(err)
	}
	if status.Phase != domain.PhaseReady || status.Members[0].State != "elected-master" {
		t.Fatal(status)
	}
	public := 0
	for _, e := range status.Connection.Endpoints {
		if e.Public {
			public++
		}
	}
	if public != 2 {
		t.Fatal(status.Connection)
	}
	if _, err = p.ListDatabaseUsers(context.Background(), rc); !errors.Is(err, engine.ErrNotSupported) {
		t.Fatal(err)
	}
}

func TestFormationUUIDGuard(t *testing.T) {
	p, rc, f, es := coreSetup(t, 1)
	es.uuid = "_na_"
	for range 10 {
		if _, err := p.Sync(context.Background(), rc); err != nil {
			t.Fatal(err)
		}
	}
	s, _ := load(rc.Cluster)
	if s.ClusterUUID != "" || len(s.InitialMasterNodes) != 1 || es.tokens != 0 {
		t.Fatal("formation advanced without UUID")
	}
	es.uuid = "formed"
	coreConverge(t, p, rc, f)
}

func TestDedicatedMasterPayloadAndRollingOrder(t *testing.T) {
	p, rc, f, es := coreSetup(t, 2)
	rc.Cluster.Spec.Topology.Type = "dedicatedMasters"
	p.ApplyDefaults(&rc.Cluster.Spec)
	rc.Cluster.Spec.Version = "9.4.8"
	coreConverge(t, p, rc, f)
	config, err := p.MemberConfig(context.Background(), rc, "demon0")
	if err != nil {
		t.Fatal(err)
	}
	var data agentspec.Spec
	if err = json.Unmarshal(config.EngineSpec, &data); err != nil {
		t.Fatal(err)
	}
	if slices.Contains(data.Elasticsearch.Roles, "master") || len(data.Elasticsearch.SeedHosts) != 3 {
		t.Fatal("incorrect dedicated data roles or seeds")
	}
	config, err = p.MemberConfig(context.Background(), rc, "demom0")
	if err != nil {
		t.Fatal(err)
	}
	var master agentspec.Spec
	if err = json.Unmarshal(config.EngineSpec, &master); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(master.Elasticsearch.Roles, []string{"master"}) {
		t.Fatal(master.Elasticsearch.Roles)
	}
	es.master = "demom1"
	before := len(f.calls)
	rc.Cluster.Spec.Version = "9.5.5"
	coreConverge(t, p, rc, f)
	deploys := []string{}
	for _, call := range f.calls[before:] {
		if strings.HasPrefix(call, "deploy:") {
			deploys = append(deploys, call)
		}
	}
	if !reflect.DeepEqual(deploys, []string{"deploy:demon0", "deploy:demon1", "deploy:demom0", "deploy:demom2", "deploy:demom1"}) {
		t.Fatal(deploys)
	}
}

func TestRestartOperationDoesNotTriggerAnotherConfigPass(t *testing.T) {
	p, rc, f, es := coreSetup(t, 3)
	coreConverge(t, p, rc, f)
	op := &domain.Operation{ID: "restart-one", Kind: domain.OperationRestart}
	done := false
	for range 100 {
		result, err := p.RunOperation(context.Background(), rc, op)
		if err != nil {
			t.Fatal(err)
		}
		op.Checkpoint = result.Checkpoint
		if result.Done {
			done = true
			break
		}
	}
	if !done {
		t.Fatal("restart did not complete")
	}
	before := len(es.mutations)
	coreConverge(t, p, rc, f)
	if len(es.mutations) != before {
		t.Fatal("Sync undid the imperative restart nonce", es.mutations[before:])
	}
}

func TestResourceChangesRollESHeapButNotCPUOrComponents(t *testing.T) {
	p, rc, f, es := coreSetup(t, 2)
	kb := rc.Cluster.Spec.Components.Extra[kindKibana]
	kb.Replicas = 1
	rc.Cluster.Spec.Components.Extra[kindKibana] = kb
	coreConverge(t, p, rc, f)
	before := len(es.mutations)
	rc.Cluster.Spec.Components.Engine.Resources.MinCPU = 2
	rc.Cluster.Spec.Components.Engine.Resources.MaxCPU = 2
	kb.Resources.MinRAM = 3
	kb.Resources.MaxRAM = 3
	rc.Cluster.Spec.Components.Extra[kindKibana] = kb
	coreConverge(t, p, rc, f)
	if len(es.mutations) != before {
		t.Fatal("CPU/component resize disrupted Elasticsearch")
	}
	cfg, err := p.MemberConfig(context.Background(), rc, "demokb")
	if err != nil {
		t.Fatal(err)
	}
	var spec agentspec.Spec
	if err = json.Unmarshal(cfg.EngineSpec, &spec); err != nil {
		t.Fatal(err)
	}
	if spec.HeapMB != KibanaHeapMB(1.5) {
		t.Fatal("stateless resize restarted with a different heap")
	}
	rc.Cluster.Spec.Components.Engine.Resources.MinRAM = 4
	rc.Cluster.Spec.Components.Engine.Resources.MaxRAM = 4
	coreConverge(t, p, rc, f)
	cfg, err = p.MemberConfig(context.Background(), rc, "demon0")
	if err != nil {
		t.Fatal(err)
	}
	spec = agentspec.Spec{}
	if err = json.Unmarshal(cfg.EngineSpec, &spec); err != nil {
		t.Fatal(err)
	}
	if spec.HeapMB != ElasticsearchHeapMB(4) {
		t.Fatal("ES heap resize was not rolled")
	}
	if len(es.mutations) == before {
		t.Fatal("ES heap resize did not use allocation procedure")
	}
}

func TestRepositorySettingsCredentialsAndReload(t *testing.T) {
	p, rc, f, es := coreSetup(t, 1)
	ctx := context.Background()
	storage := &domain.BackupStorage{Name: "archive", Type: "s3", EndpointURL: "https://objects.example.com", Region: "us-east-1", Bucket: "archive", Prefix: "snapshots", Credentials: domain.StorageCredentials{AccessKeyID: "test-access", SecretAccessKey: "test-secret"}}
	if err := rc.Store.(*store.Store).CreateBackupStorage(ctx, storage); err != nil {
		t.Fatal(err)
	}
	rc.Cluster.Spec.Backup.Storage = storage.Name
	s := coreConverge(t, p, rc, f)
	if len(s.Repositories) != 1 {
		t.Fatal("repository not registered")
	}
	cfg, err := p.MemberConfig(ctx, rc, "demon0")
	if err != nil {
		t.Fatal(err)
	}
	var spec agentspec.Spec
	if err = json.Unmarshal(cfg.EngineSpec, &spec); err != nil {
		t.Fatal(err)
	}
	client := S3ClientName(rc.Cluster.Ref().Namespace, storage.Name)
	if spec.Elasticsearch.Settings["s3.client."+client+".endpoint"] != storage.EndpointURL || spec.Elasticsearch.SecureSettings["s3.client."+client+".secret_key"] != "test-secret" {
		t.Fatal("S3 settings not rendered")
	}
	if strings.Contains(string(rc.Cluster.Internal), "test-secret") || strings.Contains(string(rc.Cluster.Internal), "test-access") {
		t.Fatal("storage credentials persisted in Internal")
	}
	storage.Credentials.SecretAccessKey = "rotated"
	if err = rc.Store.(*store.Store).UpdateBackupStorage(ctx, storage); err != nil {
		t.Fatal(err)
	}
	coreConverge(t, p, rc, f)
	if !slices.Contains(es.mutations, "POST /_nodes/reload_secure_settings") {
		t.Fatal("keystore rotation not reloaded")
	}
	cfg, err = p.MemberConfig(ctx, rc, "demon0")
	if err != nil {
		t.Fatal(err)
	}
	spec = agentspec.Spec{}
	if err = json.Unmarshal(cfg.EngineSpec, &spec); err != nil {
		t.Fatal(err)
	}
	if spec.Elasticsearch.SecureSettings["s3.client."+client+".secret_key"] != "rotated" {
		t.Fatal("rotated credentials not published")
	}
}

func TestDataSourceValidationAndCrashSafeInitialQueue(t *testing.T) {
	p, rc, f, es := coreSetup(t, 1)
	ctx := context.Background()
	source := &domain.Cluster{Name: "source"}
	p.ApplyDefaults(&source.Spec)
	if err := rc.Store.(*store.Store).CreateCluster(ctx, source); err != nil {
		t.Fatal(err)
	}
	backup := &domain.Backup{Cluster: source.Name, Name: "snapshot", Type: "snapshot", Provider: "elasticsearch", SourceVersion: "8.19.23", Phase: domain.PhaseSucceeded}
	if err := rc.Store.CreateBackup(ctx, backup); err != nil {
		t.Fatal(err)
	}
	rc.Cluster.Spec.DataSource = &domain.DataSource{Cluster: source.Name, Backup: backup.Name}
	if err := p.Validate(ctx, nil, rc.Cluster); err != nil {
		t.Fatal(err)
	}
	backup.SourceVersion = "9.5.5"
	if err := rc.Store.UpdateBackup(ctx, backup); err != nil {
		t.Fatal(err)
	}
	newer := cloneCluster(t, rc.Cluster)
	newer.Spec.Version = "9.4.8"
	if err := p.Validate(ctx, nil, newer); err == nil {
		t.Fatal("accepted newer snapshot on older target")
	}
	backup.Type = "physical"
	if err := rc.Store.UpdateBackup(ctx, backup); err != nil {
		t.Fatal(err)
	}
	if err := p.Validate(ctx, nil, rc.Cluster); err == nil {
		t.Fatal("accepted non-snapshot source")
	}
	backup.Type = "snapshot"
	if err := rc.Store.UpdateBackup(ctx, backup); err != nil {
		t.Fatal(err)
	}
	for range 15 {
		if _, err := p.Sync(ctx, rc); err != nil {
			t.Fatal(err)
		}
	}
	s, _ := load(rc.Cluster)
	if s.InitialRestore != "initial-restore" || es.tokens != 0 {
		t.Fatal("security preceded initial restore")
	}
	restore, err := rc.Store.GetRestore(ctx, rc.Cluster.Ref(), "initial-restore")
	if err != nil {
		t.Fatal(err)
	}
	if restore.Source.SourceCluster != source.Name || restore.SourceNamespace != rc.Cluster.Ref().Namespace {
		t.Fatal("incorrect source provenance")
	}
	s.InitialRestore = ""
	if err = save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	if _, err = p.Sync(ctx, rc); err != nil {
		t.Fatal("crash-safe queue adoption", err)
	}
	operations, err := rc.Store.(*store.Store).ListOperations(ctx, rc.Cluster.Ref())
	if err != nil || len(operations) != 1 {
		t.Fatal(operations, err)
	}
	s, _ = load(rc.Cluster)
	s.InitialRestore = "done"
	if err = save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	coreConverge(t, p, rc, f)
}

func TestOwnershipAndOccupiedNameSafety(t *testing.T) {
	p, rc, f, _ := coreSetup(t, 1)
	f.services["demon0v"] = &infra.Service{ID: "unrelated", Hostname: "demon0v", Type: "local-storage:single@1"}
	s := coreConverge(t, p, rc, f)
	if s.find("demon1") == nil || f.services["demon0v"].ID != "unrelated" {
		t.Fatal("adopted unowned existing volume")
	}
	f.services["demon1"].ID = "replaced-by-other-owner"
	f.services["demon1"].Ownership = nil
	rc.Cluster.Spec.Components.Engine.Resources.MinCPU = 2
	rc.Cluster.Spec.Components.Engine.Resources.MaxCPU = 2
	before := f.mutations
	if _, err := p.Sync(context.Background(), rc); !errors.Is(err, engine.ErrConflict) {
		t.Fatal("unowned replacement accepted", err)
	}
	if f.mutations != before {
		t.Fatal("unowned service mutated")
	}
}

func TestDetachedMountedVolumeHandoff(t *testing.T) {
	p, rc, f, _ := coreSetup(t, 3)
	s := coreConverge(t, p, rc, f)
	m := s.find("demon2")
	m.Removing = true
	m.Stage = "delete-volume"
	if _, err := f.DeleteService(context.Background(), m.Name); err != nil {
		t.Fatal(err)
	}
	v := &pending{Action: "delete-volume", Member: m.Name}
	for i := range 5 {
		rc.Clock.(*coreClock).at = rc.Clock.Now().Add(10 * time.Minute)
		err := p.failPending(context.Background(), rc, s, v, "volume still mounted")
		if i < 4 && err == nil {
			t.Fatal("premature handoff")
		}
		if i == 4 && err != nil {
			t.Fatal(err)
		}
	}
	if !m.VolumeOrphaned || s.Retry != nil {
		t.Fatal("volume was not handed off")
	}
	record, err := rc.Store.GetOrphanedVolume(context.Background(), rc.Cluster.Ref().Namespace, m.Volume)
	if err != nil || record.ServiceID != m.VolumeID {
		t.Fatal(record, err)
	}
	if err = save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	coreConverge(t, p, rc, f)
	if f.services["demon2v"] == nil {
		t.Fatal("provider deleted janitor-owned volume")
	}
}

func TestUnreachableSnapshotCleanupRecordsWarningAndContinues(t *testing.T) {
	p, rc, f, _ := coreSetup(t, 1)
	s := coreConverge(t, p, rc, f)
	s.Repositories["owned"] = &repository{Name: "kk-owned"}
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	rc.Dialer = coreDialer{address: "127.0.0.1:1"}
	done := false
	for range 20 {
		var err error
		done, err = p.Cleanup(context.Background(), rc, domain.DeleteOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if done {
			break
		}
	}
	if !done {
		t.Fatal("unreachable cluster blocked cleanup")
	}
	events := rc.Events.(*coreEvents).events
	if len(events) != 1 || events[0].Reason != "SnapshotsRetained" {
		t.Fatal(events)
	}
}

func TestCrossNamespaceReadOnlyRepositoryClient(t *testing.T) {
	p, rc, f, _ := coreSetup(t, 1)
	s := coreConverge(t, p, rc, f)
	ctx := context.Background()
	if err := rc.Store.(*store.Store).CreateNamespace(ctx, &domain.Namespace{Name: "other"}); err != nil {
		t.Fatal(err)
	}
	storage := &domain.BackupStorage{Namespace: "other", Name: "archive", Type: "s3", EndpointURL: "https://source.example.com", Region: "us-east-1", Bucket: "source", Credentials: domain.StorageCredentials{AccessKeyID: "source-access", SecretAccessKey: "source-secret"}}
	if err := rc.Store.(*store.Store).CreateBackupStorage(ctx, storage); err != nil {
		t.Fatal(err)
	}
	var repo string
	ready := false
	for range 50 {
		var err error
		repo, err = p.ensureRepository(ctx, rc, s, storage, true, "retained/source/uid")
		if err == nil {
			ready = true
			break
		}
		if !errors.Is(err, errWaiting) {
			t.Fatal(err)
		}
		if _, _, err = p.rollingStep(ctx, rc, s); err != nil {
			t.Fatal(err)
		}
		if err = save(rc.Cluster, s); err != nil {
			t.Fatal(err)
		}
	}
	if !ready || repo == RepositoryName(storage.Name) {
		t.Fatal("source repository not isolated")
	}
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	cfg, err := p.MemberConfig(ctx, rc, "demon0")
	if err != nil {
		t.Fatal(err)
	}
	var spec agentspec.Spec
	if err = json.Unmarshal(cfg.EngineSpec, &spec); err != nil {
		t.Fatal(err)
	}
	client := S3ClientName("other", "archive")
	if spec.Elasticsearch.Settings["s3.client."+client+".endpoint"] != storage.EndpointURL || spec.Elasticsearch.SecureSettings["s3.client."+client+".secret_key"] != "source-secret" {
		t.Fatal("source client resolved in wrong namespace")
	}
	record := s.Repositories["other/archive/source"]
	if record == nil || !record.ReadOnly || record.BasePath != "retained/source/uid" {
		t.Fatal("source repository provenance missing")
	}
}

func TestPublicCertificateSANsAndLeafRotationDoNotRestart(t *testing.T) {
	p, rc, f, _ := coreSetup(t, 1)
	rc.Cluster.Spec.Exposure.Public = true
	rc.Cluster.Spec.Exposure.Allowlist = []string{"2001:db8::/32"}
	s := coreConverge(t, p, rc, f)
	ctx := context.Background()
	cfg, err := p.MemberConfig(ctx, rc, "demon0")
	if err != nil {
		t.Fatal(err)
	}
	var before agentspec.Spec
	if err = json.Unmarshal(cfg.EngineSpec, &before); err != nil {
		t.Fatal(err)
	}
	block, _ := pem.Decode([]byte(before.Elasticsearch.HTTPCert.CertPEM))
	if block == nil {
		t.Fatal("HTTP certificate absent")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	for _, host := range []string{"demon0", "localhost", "127.0.0.1", "::1", "2001:db8::1", "192.0.2.1"} {
		if err = cert.VerifyHostname(host); err != nil {
			t.Fatal(host, err)
		}
	}
	if slices.Contains(cert.ExtKeyUsage, x509.ExtKeyUsageClientAuth) {
		t.Fatal("HTTP identity permits client authentication")
	}
	if err = p.issueLeafs(ctx, rc, s, s.find("demon0"), true); err != nil {
		t.Fatal(err)
	}
	if err = save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	cfg, err = p.MemberConfig(ctx, rc, "demon0")
	if err != nil {
		t.Fatal(err)
	}
	var after agentspec.Spec
	if err = json.Unmarshal(cfg.EngineSpec, &after); err != nil {
		t.Fatal(err)
	}
	if after.RestartToken != before.RestartToken || after.Elasticsearch.HTTPCert.CertPEM == before.Elasticsearch.HTTPCert.CertPEM {
		t.Fatal("leaf rotation restarted ES or did not rotate")
	}
}

type coreFailSecrets struct{ engine.SecretStore }

func (s coreFailSecrets) SetClusterSecret(ctx context.Context, ref domain.ClusterRef, key string, value []byte) error {
	if strings.HasSuffix(key, "/restart-spec") {
		return errors.New("secret persistence unavailable")
	}
	return s.SecretStore.SetClusterSecret(ctx, ref, key, value)
}
func TestRollingConfigurationFailureDoesNotAdvance(t *testing.T) {
	p, rc, f, _ := coreSetup(t, 1)
	s := coreConverge(t, p, rc, f)
	m := s.find("demon0")
	before := m.RestartToken
	s.Rolling = &rolling{Reason: "restart", Version: rc.Cluster.Spec.Version, Order: []string{m.Name}, Current: m.Name, Step: "apply", OperationID: "restart-failed", StartedAt: now(rc)}
	secrets := rc.Secrets
	rc.Secrets = coreFailSecrets{secrets}
	if _, _, err := p.rollingStep(context.Background(), rc, s); err == nil {
		t.Fatal("configuration failure not returned")
	}
	if s.Rolling.Step != "apply" || m.RestartToken != before {
		t.Fatal("rolling pass advanced without persisted config")
	}
	rc.Secrets = secrets
	if _, _, err := p.rollingStep(context.Background(), rc, s); err != nil {
		t.Fatal(err)
	}
	if s.Rolling.Step != "rejoin" || m.RestartToken == before {
		t.Fatal("rolling config did not resume")
	}
}

func TestAcceptableYellowAndConvergedGeneration(t *testing.T) {
	p, rc, f, es := coreSetup(t, 1)
	rc.Cluster.Generation = 8
	rc.Cluster.Status.ObservedGeneration = 7
	es.health = "yellow"
	es.unassignedPrimary = true
	for range 15 {
		if _, err := p.Sync(context.Background(), rc); err != nil {
			t.Fatal(err)
		}
	}
	s, _ := load(rc.Cluster)
	if s.Phase == "ready" || rc.Cluster.Status.ObservedGeneration != 7 {
		t.Fatal("unassigned primary reported converged")
	}
	es.unassignedPrimary = false
	es.initializing = 1
	if _, err := p.Sync(context.Background(), rc); err != nil {
		t.Fatal(err)
	}
	s, _ = load(rc.Cluster)
	if s.Phase == "ready" || rc.Cluster.Status.ObservedGeneration != 7 {
		t.Fatal("initializing shards reported converged")
	}
	es.initializing = 0
	es.relocating = 1
	if _, err := p.Sync(context.Background(), rc); err != nil {
		t.Fatal(err)
	}
	s, _ = load(rc.Cluster)
	if s.Phase == "ready" || rc.Cluster.Status.ObservedGeneration != 7 {
		t.Fatal("relocating shards reported converged")
	}
	es.relocating = 0
	coreConverge(t, p, rc, f)
	if rc.Cluster.Status.ObservedGeneration != 8 || rc.Cluster.Status.Phase != domain.PhaseReady {
		t.Fatal("applied generation not published")
	}
}

func TestUpgradeBeforeEnablingNewComponent(t *testing.T) {
	p, rc, f, _ := coreSetup(t, 2)
	rc.Cluster.Spec.Version = "9.4.8"
	coreConverge(t, p, rc, f)
	before := len(f.calls)
	rc.Cluster.Spec.Version = "9.5.5"
	apm := rc.Cluster.Spec.Components.Extra[kindAPM]
	apm.Replicas = 1
	rc.Cluster.Spec.Components.Extra[kindAPM] = apm
	coreConverge(t, p, rc, f)
	deploys := []string{}
	for _, call := range f.calls[before:] {
		if strings.HasPrefix(call, "deploy:") {
			deploys = append(deploys, call)
		}
	}
	if !reflect.DeepEqual(deploys, []string{"deploy:demon1", "deploy:demon0", "deploy:demoapm"}) {
		t.Fatal("new component preceded ES upgrade", deploys)
	}
}

func TestRestorePublishesEmptyComponentIntent(t *testing.T) {
	p, rc, f, _ := coreSetup(t, 1)
	for _, kind := range []string{kindKibana, kindLogstash, kindAPM} {
		comp := rc.Cluster.Spec.Components.Extra[kind]
		comp.Replicas = 2
		rc.Cluster.Spec.Components.Extra[kind] = comp
	}
	s := coreConverge(t, p, rc, f)
	s.RestoreActive = true
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	for _, m := range s.componentMembers() {
		config, err := p.MemberConfig(context.Background(), rc, m.Name)
		if err != nil {
			t.Fatal(err)
		}
		if config.DesiredProcesses == nil || len(config.DesiredProcesses) != 0 {
			t.Fatal("restore configuration permits component startup", m.Name, config.DesiredProcesses)
		}
	}
}

func TestRollingWaitsForEveryMaster(t *testing.T) {
	p, rc, f, es := coreSetup(t, 2)
	rc.Cluster.Spec.Topology.Type = "dedicatedMasters"
	p.ApplyDefaults(&rc.Cluster.Spec)
	s := coreConverge(t, p, rc, f)
	delete(f.nodes, "demom2")
	for _, step := range []string{"prepare", "apply"} {
		s.Rolling = &rolling{Reason: "restart", Current: "demom0", Order: []string{"demom0"}, Step: step}
		before := len(es.mutations)
		token := s.find("demom0").RestartToken
		done, msg, err := p.rollingStep(context.Background(), rc, s)
		if done || !errors.Is(err, errWaiting) || !strings.Contains(msg, "master-eligible") || s.Rolling.Step != step || len(es.mutations) != before || s.find("demom0").RestartToken != token {
			t.Fatalf("master disrupted without full quorum membership: %v %s %v %+v", done, msg, err, s.Rolling)
		}
	}
	s.Retry = &pending{Action: "resources", Member: "demom0", Resources: domain.Resources{MinRAM: 4}}
	before := f.mutations
	if _, _, err := p.retryPending(context.Background(), rc, s); !errors.Is(err, errWaiting) || f.mutations != before || s.Retry == nil {
		t.Fatal("platform retry bypassed master membership gate", err)
	}
	s.Retry = nil
	f.nodes["demom2"] = rc.Cluster.Spec.Version
	s.Rolling.Step = "prepare"
	if _, _, err := p.rollingStep(context.Background(), rc, s); err != nil || s.Rolling.Step != "flush" {
		t.Fatal("joined masters did not release the gate", err, s.Rolling)
	}
}

func TestYellowRequiresUnplaceableReplicas(t *testing.T) {
	p, rc, f, es := coreSetup(t, 3)
	s := coreConverge(t, p, rc, f)
	es.health = "yellow"
	for _, tc := range []struct {
		replicas                 int
		primary                  bool
		relocating, initializing int
		want                     bool
	}{{1, false, 0, 0, false}, {2, false, 0, 0, false}, {3, false, 0, 0, true}, {4, false, 0, 0, true}, {3, true, 0, 0, false}, {3, false, 1, 0, false}, {3, false, 0, 1, false}} {
		es.replicas, es.unassignedPrimary = tc.replicas, tc.primary
		es.relocating, es.initializing = tc.relocating, tc.initializing
		for _, step := range []string{"prepare", "recover"} {
			s.Rolling = &rolling{Reason: "restart", Current: "demon1", Order: []string{"demon1"}, Step: step}
			_, _, err := p.rollingStep(context.Background(), rc, s)
			if err != nil {
				t.Fatal(err)
			}
			advanced := s.Rolling.Step != step || s.Rolling.Current == ""
			if advanced != tc.want {
				t.Fatalf("yellow gate %+v step=%s rolling=%+v", tc, step, s.Rolling)
			}
		}
		s.Rolling = nil
		s.Phase = "ready"
		if err := save(rc.Cluster, s); err != nil {
			t.Fatal(err)
		}
		status, err := p.Status(context.Background(), rc)
		if err != nil {
			t.Fatal(err)
		}
		if (status.Phase == domain.PhaseReady) != tc.want {
			t.Fatalf("yellow readiness %+v: %+v", tc, status)
		}
	}
}

func TestProvisioningPrecedesSavedRolling(t *testing.T) {
	for _, kind := range []string{kindNode, kindKibana} {
		for _, stage := range []string{stagePlanned, stageImported} {
			t.Run(kind+"/"+stage, func(t *testing.T) {
				p, rc, f, _ := coreSetup(t, 1)
				if kind == kindKibana {
					kb := rc.Cluster.Spec.Components.Extra[kindKibana]
					kb.Replicas = 1
					rc.Cluster.Spec.Components.Extra[kindKibana] = kb
				}
				s := coreConverge(t, p, rc, f)
				m := s.members(kind)[0]
				m.Stage = stage
				s.Rolling = &rolling{Reason: "restart", Current: "demon0", Step: "prepare", Order: []string{"demon0"}}
				if stage == stagePlanned {
					delete(f.services, m.Name)
				}
				if err := save(rc.Cluster, s); err != nil {
					t.Fatal(err)
				}
				before := len(f.calls)
				if _, err := p.Sync(context.Background(), rc); err != nil {
					t.Fatal(err)
				}
				action := "import:"
				if stage == stageImported {
					action = "deploy:"
				}
				if !slices.Contains(f.calls[before:], action+m.Name) {
					t.Fatal("saved rolling suppressed provisioning", f.calls[before:])
				}
				s, _ = load(rc.Cluster)
				if s.Rolling.Step != "prepare" {
					t.Fatal("rolling advanced before provisioning", s.Rolling)
				}
			})
		}
	}
}

func TestRemovalPreservesOperatorExclusions(t *testing.T) {
	for _, alreadyExcluded := range []bool{false, true} {
		p, rc, f, es := coreSetup(t, 3)
		s := coreConverge(t, p, rc, f)
		m := s.find("demon2")
		excluded := "operator"
		if alreadyExcluded {
			excluded += ",demon2"
		}
		es.transient = map[string]any{"cluster.routing.allocation.exclude._name": excluded}
		m.Removing = true
		if err := p.removeStep(context.Background(), rc, s, m); err != nil {
			t.Fatal(err)
		}
		if m.Stage == stageJoined {
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			s, _ = load(rc.Cluster)
			m = s.find("demon2")
			if err := p.removeStep(context.Background(), rc, s, m); err != nil {
				t.Fatal(err)
			}
		}
		if es.persistent["cluster.routing.allocation.exclude._name"] != "operator,demon2" || es.transient["cluster.routing.allocation.exclude._name"] != nil || m.AllocationExcluded == alreadyExcluded {
			t.Fatal("effective drain exclusion was not applied with ownership", es.persistent, es.transient, m)
		}
		delete(f.nodes, m.Name)
		m.Stage = "removed"
		if err := p.removeStep(context.Background(), rc, s, m); err != nil {
			t.Fatal(err)
		}
		want := "operator"
		if alreadyExcluded {
			want += ",demon2"
		}
		if es.persistent["cluster.routing.allocation.exclude._name"] != want {
			t.Fatal("removal changed an operator-owned exclusion", es.persistent)
		}
	}
}
