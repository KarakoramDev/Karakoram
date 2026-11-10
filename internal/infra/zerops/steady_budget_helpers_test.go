// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package zerops

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
	"github.com/KarakoramDev/Karakoram/internal/agentpush"
	model "github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

type providerBudgetAgents struct {
	backend  *Backend
	endpoint string
	ca       []byte
	samples  *agentpush.Store
}

func (a *providerBudgetAgents) Dial(_ context.Context, ref model.ClusterRef, member string) (*agentapi.Client, error) {
	endpoint, err := url.Parse(a.endpoint)
	if err != nil {
		return nil, err
	}
	dial := func(ctx context.Context, network, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, endpoint.Host)
	}
	var samples agentapi.SampleSource
	if a.samples != nil {
		samples = a.samples.Source(agentpush.MemberKey{Namespace: ref.Namespace, Cluster: ref.Name, Member: member})
	}
	return agentapi.NewClient(agentapi.ClientOptions{BaseURL: a.endpoint, Token: member, CAPEM: a.ca, DialContext: dial, Samples: samples, SampleMember: member, DialInstanceContext: func(ctx context.Context, network, logical, instance string) (net.Conn, error) {
		containers, err := a.backend.Containers(infra.WithFreshTaskInventory(ctx), member)
		if err != nil {
			return nil, err
		}
		for _, c := range containers {
			if (c.IPv4 == instance || c.IPv6 == instance || c.Hostname == instance) && (c.Status == infra.ContainerRunning || c.Status == infra.ContainerActive) {
				return dial(ctx, network, logical)
			}
		}
		return nil, fmt.Errorf("instance is not in current inventory")
	}})
}
func providerBudgetContext(t *testing.T, p engine.Provider, b *Backend) *engine.Context {
	t.Helper()
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
	return &engine.Context{Cluster: c, Backend: b, Store: st, Secrets: st, Clock: engine.RealClock{}, ObservationEpoch: "budget-process"}
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
func awaitBudget(t *testing.T, b *Backend, op infra.Op) {
	t.Helper()
	st, err := b.OpStatus(t.Context(), op)
	if err != nil || !st.Done || st.Failed {
		t.Fatalf("operation=%+v err=%v", st, err)
	}
}
func providerBudgetRuntime(t *testing.T, b *Backend, name, volume string, count int, proxy bool) string {
	t.Helper()
	spec := infra.ServiceSpec{Hostname: name, Kind: infra.KindRuntime, Base: infra.BaseUbuntu2404, Volume: volume, MinContainers: count, MaxContainers: count}
	var specs []infra.ServiceSpec
	if volume != "" {
		specs = append(specs, infra.ServiceSpec{Hostname: volume, Kind: infra.KindVolume, Resources: infra.Resources{MinDisk: 5, MaxDisk: 5}})
	}
	specs = append(specs, spec)
	op, err := b.ImportServices(t.Context(), specs)
	if err != nil {
		t.Fatal(err)
	}
	awaitBudget(t, b, op)
	d := hardeningDeployment()
	d.Runtime.Volume = volume
	if proxy {
		d.Runtime.Env = map[string]string{"KARAKORAM_PROXY_SERVING_PROBE": "1"}
	}
	deploy, err := b.Deploy(t.Context(), name, d)
	if err != nil {
		t.Fatal(err)
	}
	awaitBudget(t, b, deploy)
	return op.Services[name]
}
func providerBudgetServer(t *testing.T, rc *engine.Context, handler http.HandlerFunc) {
	t.Helper()
	s := httptest.NewTLSServer(handler)
	t.Cleanup(s.Close)
	rc.Agents = &providerBudgetAgents{backend: rc.Backend.(*Backend), endpoint: s.URL, ca: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: s.Certificate().Raw})}
}

type providerGETCounts struct {
	mu       sync.Mutex
	requests map[string]int
	signed   map[string]int
}

func (c *providerGETCounts) RoundTrip(r *http.Request) (*http.Response, error) {
	path := r.URL.Path
	parts := strings.Split(strings.Trim(path, "/"), "/")
	// Preserve method and endpoint, collapsing only fixture-generated resource IDs.
	if len(parts) > 1 && (parts[0] == "service-stack" || parts[0] == "process" || parts[0] == "app-version") {
		parts[1] = "{id}"
		path = "/" + strings.Join(parts, "/")
	}
	c.mu.Lock()
	if r.URL.Path == "/signed-log" {
		c.signed[r.Method+" "+path]++
	} else {
		c.requests[r.Method+" "+path]++
	}
	c.mu.Unlock()
	return http.DefaultTransport.RoundTrip(r)
}
func (c *providerGETCounts) reset() { c.mu.Lock(); clear(c.requests); clear(c.signed); c.mu.Unlock() }
func newBudgetBackend(t *testing.T, base string, c *providerGETCounts) *Backend {
	t.Helper()
	b, err := New(Options{BaseURL: base, Token: "token", ProjectID: "project", HTTPClient: &http.Client{Transport: c}, MaxRetries: -1, MaxInflightDeploys: 32, PollInterval: time.Millisecond, WaitTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// Mutate only after a real steady GET has recorded its served snapshot epoch.
// A Dial hook races platform lookup and can mutate before any evidence exists.
type steadyMutationBackend struct {
	infra.Backend
	once   sync.Once
	mutate func()
}

func (b *steadyMutationBackend) GetService(ctx context.Context, name string) (*infra.Service, error) {
	s, err := b.Backend.GetService(ctx, name)
	if err == nil && infra.SteadyObservation(ctx) {
		b.once.Do(b.mutate)
	}
	return s, err
}
