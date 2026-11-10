// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package nomad

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

// Logical connections use the TLS agent, while every pinned dial authorizes
// the actual CNI address against fresh allocation inventory, as the relay does.
type providerBudgetAgents struct {
	backend  *Backend
	endpoint string
	ca       []byte
	samples  *agentpush.Store
	mu       sync.Mutex
	clients  map[providerBudgetAgentKey]*agentapi.Client
}

type providerBudgetAgentKey struct {
	namespace, cluster, member string
	samples                    *agentpush.Store
}

// The replay advances platform/store time without waiting for wall time.
// Translate client proof starts into that clock, retaining request elapsed age.
type providerBudgetSamples struct{ agentapi.SampleSource }

func (s providerBudgetSamples) BindLogical(session string, started time.Time) {
	s.SampleSource.BindLogical(session, infra.SnapshotNow().Add(-time.Since(started)))
}

func (a *providerBudgetAgents) Dial(_ context.Context, ref model.ClusterRef, member string) (*agentapi.Client, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	key := providerBudgetAgentKey{ref.Namespace, ref.Name, member, a.samples}
	if client := a.clients[key]; client != nil {
		return client, nil
	}
	endpoint, err := url.Parse(a.endpoint)
	if err != nil {
		return nil, err
	}
	dial := func(ctx context.Context, network, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, endpoint.Host)
	}
	var samples agentapi.SampleSource
	if a.samples != nil {
		samples = providerBudgetSamples{a.samples.Source(agentpush.MemberKey{Namespace: ref.Namespace, Cluster: ref.Name, Member: member})}
	}
	client, err := agentapi.NewClient(agentapi.ClientOptions{BaseURL: a.endpoint, Token: member, CAPEM: a.ca, DialContext: dial, Samples: samples, SampleMember: member, DialInstanceContext: func(ctx context.Context, network, logical, instance string) (net.Conn, error) {
		containers, err := a.backend.Containers(infra.WithFreshTaskInventory(ctx), member)
		if err != nil {
			return nil, err
		}
		for _, c := range containers {
			if (c.IPv4 == instance || c.IPv6 == instance) && c.Status == infra.ContainerRunning {
				return dial(ctx, network, logical)
			}
		}
		return nil, fmt.Errorf("instance is not in current inventory")
	}})
	if err == nil {
		if a.clients == nil {
			a.clients = make(map[providerBudgetAgentKey]*agentapi.Client)
		}
		a.clients[key] = client
	}
	return client, err
}

// Reuse immutable logical clients and their connection pools across simulated
// passes. Registered test cleanup releases their idle network resources.
func (a *providerBudgetAgents) closeClients() {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, client := range a.clients {
		client.Close()
	}
}

// Keep real sample timestamps, but separate the sweep and shared MySQL pass
// budgets from this nice-19, race-instrumented TLS request-count replay.
// Production's latency contracts remain covered by engine observation tests.
type providerBudgetClock struct{ engine.RealClock }

func (providerBudgetClock) ObservationSweepTimeoutForTest() time.Duration {
	return 5 * time.Minute
}

func (providerBudgetClock) ObservationReadTimeoutForTest() time.Duration {
	return 5 * time.Minute
}

func providerBudgetContext(t *testing.T, p engine.Provider) *engine.Context {
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
	return &engine.Context{Cluster: c, Store: st, Secrets: st, Clock: providerBudgetClock{}, ObservationEpoch: "test-process"}
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
	importFake(t, b, spec)
	d := testDeployment(t, volume)
	if proxy {
		d.Runtime.Env = map[string]string{"KARAKORAM_PROXY_SERVING_PROBE": "1"}
	}
	op, err := b.Deploy(t.Context(), name, d)
	if err != nil {
		t.Fatal(err)
	}
	awaitFake(t, b, op)
	return name
}
func providerBudgetServer(t *testing.T, rc *engine.Context, handler http.HandlerFunc) {
	t.Helper()
	s := httptest.NewTLSServer(handler)
	t.Cleanup(s.Close)
	rc.Agents = &providerBudgetAgents{backend: rc.Backend.(*Backend), endpoint: s.URL, ca: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: s.Certificate().Raw})}
	t.Cleanup(rc.Agents.(*providerBudgetAgents).closeClients)
}
