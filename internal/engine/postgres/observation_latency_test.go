// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type coldObservationAgents struct{ options agentapi.ClientOptions }

func (d coldObservationAgents) Dial(_ context.Context, _ domain.ClusterRef, name string) (*agentapi.Client, error) {
	o := d.options
	o.Token = name
	return agentapi.NewClient(o)
}

func waitObservationLatency(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestObservationColdTLSAndSlowNativeFitsSweep(t *testing.T) {
	p, rc, _ := setup(t, "ha")
	s := converge(t, p, rc)
	statuses := make(map[string]pgspec.StatusResponse, len(s.Members))
	for _, m := range s.Members {
		st, err := live(t.Context(), rc, m.Name)
		if err != nil {
			t.Fatal(err)
		}
		statuses[m.Name] = st
	}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		delay := time.Second
		if r.URL.Path == "/v1/postgres/status" {
			delay = 1500 * time.Millisecond
		}
		if err := waitObservationLatency(r.Context(), delay); err != nil {
			return
		}
		switch r.URL.Path {
		case "/v1/status":
			_ = json.NewEncoder(w).Encode(agentapi.Status{ConfigVersion: s.ConfigVersion})
		case "/v1/postgres/status":
			_ = json.NewEncoder(w).Encode(statuses[strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")])
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	rc.Clock = engine.RealClock{}
	rc.Agents = coldObservationAgents{options: agentapi.ClientOptions{
		BaseURL: server.URL, CAPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}),
		DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
			if err := waitObservationLatency(ctx, 1500*time.Millisecond); err != nil {
				return nil, err
			}
			return (&net.Dialer{}).DialContext(ctx, network, address)
		},
	}}
	// The data-member observation is the shared per-member budget boundary;
	// the two HTTP clients have independent cold TLS connections.
	ctx, cancel := context.WithTimeout(t.Context(), engine.ObservationSweepTimeout)
	defer cancel()
	observed := observeStatusMembers(ctx, rc, s.data())
	for i, sample := range observed {
		if sample.err != nil || !sample.agentHealthy || !sample.status.Running {
			t.Fatalf("cold healthy member %s lost fresh availability: %+v", s.data()[i].Name, sample)
		}
	}
}

// Dokploy serializes GetService under its lifecycle lock. Containers is an
// independent API read, and both must overlap the native/member observations.
type delayedStatusInventoryBackend struct {
	*fakeBackend
	runtimeMu        sync.Mutex
	runtimeDelay     time.Duration
	containerDelay   time.Duration
	runtimeErr       error
	containerErr     error
	proxyInventories atomic.Int32
	runtimeObserved  atomic.Bool
}

func (b *delayedStatusInventoryBackend) GetService(ctx context.Context, name string) (*infra.Service, error) {
	b.runtimeMu.Lock()
	defer b.runtimeMu.Unlock()
	if err := waitObservationLatency(ctx, b.runtimeDelay); err != nil {
		return nil, err
	}
	if b.runtimeErr != nil {
		return nil, b.runtimeErr
	}
	b.runtimeObserved.Store(true)
	return b.fakeBackend.GetService(ctx, name)
}

func (b *delayedStatusInventoryBackend) Containers(ctx context.Context, name string) ([]infra.Container, error) {
	if strings.HasSuffix(name, "px") {
		b.proxyInventories.Add(1)
	}
	if err := waitObservationLatency(ctx, b.containerDelay); err != nil {
		return nil, err
	}
	if b.containerErr != nil {
		return nil, b.containerErr
	}
	return b.fakeBackend.Containers(ctx, name)
}

func TestStatusOverlapsHAInfrastructureInventory(t *testing.T) {
	p, rc, base := setup(t, "ha")
	s := converge(t, p, rc)
	backend := &delayedStatusInventoryBackend{
		fakeBackend: base, runtimeDelay: 850 * time.Millisecond, containerDelay: 500 * time.Millisecond,
	}
	rc.Backend, rc.Clock = backend, engine.RealClock{}
	status, err := engine.ObserveStatus(t.Context(), p, rc)
	if err != nil || status.Phase != domain.PhaseReady || status.ObservedAt.IsZero() {
		t.Fatalf("healthy HA inventory exceeded readiness sweep: phase=%s observed=%s error=%v", status.Phase, status.ObservedAt, err)
	}
	if len(status.Members) != len(s.Members) {
		t.Fatalf("member inventory incomplete: got %d want %d", len(status.Members), len(s.Members))
	}
	for _, m := range status.Members {
		if !m.Health || !m.Agent.Healthy || m.PlacementGroup != "node-a" {
			t.Errorf("lost fresh member or placement evidence: %+v", m)
		}
	}
	if count := backend.proxyInventories.Load(); count != 1 {
		t.Fatalf("proxy task inventory repeated within one sweep: got %d want 1", count)
	}
}

func TestStatusInfrastructureErrorsRemainFailClosed(t *testing.T) {
	failure := errors.New("platform observation unavailable")
	for _, kind := range []string{"runtime", "containers"} {
		t.Run(kind, func(t *testing.T) {
			p, rc, base := setup(t, "ha")
			converge(t, p, rc)
			backend := &delayedStatusInventoryBackend{fakeBackend: base}
			if kind == "runtime" {
				backend.runtimeErr = failure
			} else {
				backend.containerErr = failure
			}
			rc.Backend = backend
			status, err := engine.ObserveStatus(t.Context(), p, rc)
			if !errors.Is(err, failure) || status.Phase == domain.PhaseReady || !status.ObservedAt.IsZero() {
				t.Fatalf("failed infrastructure read renewed readiness: phase=%s observed=%s error=%v", status.Phase, status.ObservedAt, err)
			}
			if !strings.Contains(err.Error(), "demop0 "+kind) {
				t.Fatalf("missing member/step attribution: %v", err)
			}
		})
	}
}

type runtimeBarrierAgents struct {
	fakeAgents
	observed *atomic.Bool
	early    *atomic.Bool
}

func (a runtimeBarrierAgents) Dial(ctx context.Context, ref domain.ClusterRef, name string) (*agentapi.Client, error) {
	if !a.observed.Load() {
		a.early.Store(true)
		return nil, errors.New("native read preceded public runtime barrier")
	}
	return a.fakeAgents.Dial(ctx, ref, name)
}

func TestStatusDirectNativeWaitsForRuntimeBarrier(t *testing.T) {
	p, rc, base := setup(t, "single")
	proxy := rc.Cluster.Spec.Components.Extra["proxy"]
	proxy.Replicas = 0
	rc.Cluster.Spec.Components.Extra["proxy"] = proxy
	converge(t, p, rc)
	backend := &delayedStatusInventoryBackend{fakeBackend: base, runtimeDelay: 50 * time.Millisecond}
	rc.Backend = backend
	var early atomic.Bool
	rc.Agents = runtimeBarrierAgents{fakeAgents: rc.Agents.(fakeAgents), observed: &backend.runtimeObserved, early: &early}
	status, err := engine.ObserveStatus(t.Context(), p, rc)
	if err != nil || status.Phase != domain.PhaseReady || early.Load() {
		t.Fatalf("direct endpoint lost runtime-before-native ordering: phase=%s error=%v", status.Phase, err)
	}
}
