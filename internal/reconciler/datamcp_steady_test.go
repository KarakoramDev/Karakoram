// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package reconciler

import (
	"context"
	"encoding/pem"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/agentpush"
	"github.com/KarakoramDev/Karakoram/internal/datamcp/companion"
	"github.com/KarakoramDev/Karakoram/internal/datamcp/companionspec"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	infrafake "github.com/KarakoramDev/Karakoram/internal/infra/fake"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

const companionReadyResponse = `{"configVersion":1,"processes":{"mcp":{"healthy":true}}}`

type companionEvidenceProvider struct {
	*companionObservationProvider
	snapshot *infra.TargetSnapshot
	calls    atomic.Int32
}

func (p *companionEvidenceProvider) Status(ctx context.Context, rc *engine.Context) (domain.ClusterStatus, error) {
	p.calls.Add(1)
	if p.snapshot != nil {
		p.snapshot.Lookup(ctx, "provider")
	}
	return p.companionObservationProvider.Status(ctx, rc)
}

type companionPushAgents struct {
	url            string
	ca             []byte
	store          *agentpush.Store
	snapshot       *infra.TargetSnapshot
	mutationTarget *infra.TargetSnapshot
	mutate         atomic.Bool
	freshDials     atomic.Int32
}

func (a *companionPushAgents) Dial(ctx context.Context, ref domain.ClusterRef, member string) (*agentapi.Client, error) {
	if !infra.SteadyObservation(ctx) {
		a.freshDials.Add(1)
	}
	if a.snapshot != nil {
		a.snapshot.Lookup(ctx, "companion")
	}
	key := agentpush.MemberKey{Namespace: domain.NormalizedNamespace(ref.Namespace), Cluster: ref.Name, Member: member}
	return agentapi.NewClient(agentapi.ClientOptions{BaseURL: a.url, Token: "fixture", CAPEM: a.ca,
		Samples: companionPushSource{SampleSource: a.store.Source(key), agents: a}, SampleMember: member})
}

type companionPushSource struct {
	agentapi.SampleSource
	agents *companionPushAgents
}

func (s companionPushSource) PushedSample(instance, engineName, action string, request []byte) ([]byte, time.Time, bool) {
	body, at, ok := s.SampleSource.PushedSample(instance, engineName, action, request)
	if ok && engineName == companionspec.Engine && action == "readiness" && s.agents.mutate.Swap(false) {
		done := s.agents.mutationTarget.BeginMutation(context.Background())
		done()
	}
	return body, at, ok
}

type companionPushFixture struct {
	manager     *Manager
	store       *store.Store
	cluster     *domain.Cluster
	provider    *companionEvidenceProvider
	clock       *maintenanceStepClock
	agents      *companionPushAgents
	readiness   atomic.Int32
	connections atomic.Int32
}

func newCompanionPushFixture(t *testing.T, providerTarget, companionTarget *infra.TargetSnapshot) *companionPushFixture {
	t.Helper()
	f := &companionPushFixture{clock: &maintenanceStepClock{now: time.Now().UTC()}}
	f.provider = &companionEvidenceProvider{companionObservationProvider: &companionObservationProvider{
		dataProvider: &dataProvider{fakeProvider: &fakeProvider{}, kind: engine.DataMcpCompanion}}, snapshot: providerTarget}
	f.manager, f.store = newSteadyTestManager(t, f.provider)
	f.manager.opts.Context.Clock = f.clock
	f.manager.opts.Context.ObservationEpoch = "companion-push-test"
	backend := infra.Backend(infrafake.New(infrafake.Options{}))
	if providerTarget != nil {
		backend = &steadySnapshotBackend{Backend: backend, snapshot: providerTarget}
	}
	f.manager.opts.Namespaces = engine.StaticNamespaces{DefaultBackend: backend}
	f.agents = &companionPushAgents{store: agentpush.NewStore(f.clock.Now), snapshot: companionTarget}
	f.manager.opts.Context.Agents = f.agents
	f.manager.opts.AgentPush = f.agents.store
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(agentapi.HeartbeatSessionHeader, "session")
		if r.URL.Path != "/v1/datamcp/readiness" {
			t.Errorf("unexpected live companion request: %s", r.URL.Path)
			http.NotFound(w, r)
			return
		}
		f.readiness.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(companionReadyResponse))
	}))
	server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			f.connections.Add(1)
		}
	}
	server.StartTLS()
	t.Cleanup(server.Close)
	f.agents.url = server.URL
	f.agents.ca = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	f.cluster = &domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: "fake", DataMcp: &domain.DataMcpSpec{Enabled: true}},
		Status: domain.ClusterStatus{Phase: domain.PhaseReady, ObservedGeneration: 1}}
	if err := f.store.CreateCluster(t.Context(), f.cluster); err != nil {
		t.Fatal(err)
	}
	for key, body := range map[string]string{
		"datamcp/state":  `{"stage":"ready"}`,
		"datamcp/config": `{"configVersion":1,"desiredProcesses":["mcp"]}`,
	} {
		if err := f.store.SetClusterSecret(t.Context(), f.cluster.Ref(), key, []byte(body)); err != nil {
			t.Fatal(err)
		}
	}
	return f
}

func (f *companionPushFixture) warm(t *testing.T, response string) time.Time {
	t.Helper()
	// The first production renewal is deliberately non-steady: it must learn
	// the readiness subscription and bind the session from the live response.
	if err := f.manager.renewAvailability(t.Context(), f.cluster.Ref()); err != nil {
		t.Fatal(err)
	}
	if f.readiness.Load() != 1 {
		t.Fatal("fresh renewal did not read companion readiness")
	}
	key := agentpush.MemberKey{Namespace: f.cluster.Ref().Namespace, Cluster: f.cluster.Name, Member: companion.Member(f.cluster.Name)}
	issuedAt := f.clock.Now()
	reply := f.agents.store.Ingest(key, agentapi.HeartbeatRequest{Session: "session"})
	learned := false
	for _, action := range reply.Requests {
		if action.Engine == companionspec.Engine && action.Action == "readiness" && string(action.Request) == "{}" {
			learned = true
		}
	}
	if !learned {
		t.Fatal("fresh production companion observation did not learn readiness subscription")
	}
	f.clock.now = f.clock.now.Add(time.Second)
	f.agents.store.Ingest(key, agentapi.HeartbeatRequest{Session: "session", Token: reply.Token,
		Samples: []agentapi.HeartbeatSample{{Engine: companionspec.Engine, Action: "readiness", Request: []byte("{}"), Code: 200, Response: []byte(response)}}})
	w := &worker{trigger: make(chan struct{}, 1)}
	f.manager.workers[f.cluster.Ref().Key()] = w
	var drift uint64
	if backend, err := f.manager.opts.Namespaces.Backend(domain.NormalizedNamespace(f.cluster.Namespace)); err == nil {
		if target, ok := backend.(infra.PlatformSnapshotter); ok {
			drift = target.PlatformSnapshot().DriftGeneration()
		}
	}
	if !f.manager.finishSteady(t.Context(), f.cluster.Ref(), w, w.triggers.Load(), f.cluster.Generation, drift) {
		t.Fatal("ready fixture did not enter steady mode")
	}
	f.readiness.Store(0)
	f.connections.Store(0)
	f.provider.calls.Store(0)
	f.agents.freshDials.Store(0)
	return issuedAt
}

func (f *companionPushFixture) renew(t *testing.T) domain.ClusterStatus {
	t.Helper()
	if err := f.manager.renewAvailability(t.Context(), f.cluster.Ref()); err != nil {
		t.Fatal(err)
	}
	stored, err := f.store.GetCluster(t.Context(), f.cluster.Ref())
	if err != nil {
		t.Fatal(err)
	}
	if stored.Status.DataMcp == nil || stored.Status.DataMcp.Phase != domain.DataMcpPhaseReady {
		t.Fatalf("companion did not publish Ready: %+v", stored.Status.DataMcp)
	}
	return stored.Status
}

func TestCompanionPushProductionRenewalAndStaleFallback(t *testing.T) {
	f := newCompanionPushFixture(t, nil, nil)
	issuedAt := f.warm(t, companionReadyResponse)
	status := f.renew(t)
	if f.readiness.Load() != 0 || f.connections.Load() != 0 {
		t.Fatal("warm production renewal issued live companion traffic", f.readiness.Load(), f.connections.Load())
	}
	if !status.ObservedAt.Equal(issuedAt) || !status.Members[0].Agent.LastSeen.Equal(issuedAt) {
		t.Fatal("companion evidence did not bound the published health lease", status)
	}
	f.clock.now = f.clock.now.Add(agentpush.SampleMaxAge + time.Second)
	status = f.renew(t)
	if f.readiness.Load() != 1 || f.agents.freshDials.Load() != 1 || !status.ObservedAt.After(issuedAt) {
		t.Fatal("stale companion push did not fall back to a fresh read", f.readiness.Load(), status)
	}
}

func TestCompanionPushNegativeAttemptFallsBackFresh(t *testing.T) {
	f := newCompanionPushFixture(t, nil, nil)
	issuedAt := f.warm(t, `{"configVersion":1,"processes":{"mcp":{"healthy":false}}}`)
	status := f.renew(t)
	if f.readiness.Load() != 1 || f.agents.freshDials.Load() != 1 {
		t.Fatal("negative pushed companion readiness did not rerun fresh")
	}
	if !status.ObservedAt.After(issuedAt) {
		t.Fatal("discarded companion evidence contaminated the fresh result anchor", status.ObservedAt)
	}
}

func TestCompanionPushRevalidatesProviderAndCompanionEpochs(t *testing.T) {
	for _, stage := range []string{"provider", "companion"} {
		t.Run(stage, func(t *testing.T) {
			target := &infra.TargetSnapshot{}
			target.SetListing(func(context.Context) (string, error) { return "stable", nil })
			var providerTarget, companionTarget *infra.TargetSnapshot
			if stage == "provider" {
				providerTarget = target
			} else {
				companionTarget = target
			}
			f := newCompanionPushFixture(t, providerTarget, companionTarget)
			issuedAt := f.warm(t, companionReadyResponse)
			f.agents.mutationTarget = target
			f.agents.mutate.Store(true)
			status := f.renew(t)
			wantProviderCalls := int32(1)
			if stage == "provider" {
				wantProviderCalls = 2
			}
			if f.provider.calls.Load() != wantProviderCalls || f.readiness.Load() != 1 || f.agents.freshDials.Load() != 1 {
				t.Fatal("invalidated proof did not select the required fresh fallback", f.provider.calls.Load(), f.readiness.Load())
			}
			if !status.ObservedAt.After(issuedAt) {
				t.Fatal("invalidated pushed sample renewed the published lease", status.ObservedAt)
			}
		})
	}
}
