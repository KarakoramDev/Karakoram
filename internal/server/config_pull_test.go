// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type configPullBackend struct {
	fakeBackend
	snapshot *infra.TargetSnapshot
}

func (b configPullBackend) PlatformSnapshot() *infra.TargetSnapshot { return b.snapshot }

type configBaseline struct{ eligible bool }

func (*configBaseline) Trigger(domain.ClusterRef) {}
func (b *configBaseline) SteadyConfigBaseline(context.Context, *domain.Cluster) bool {
	return b.eligible
}

type configPullProvider struct {
	fakeProvider
	calls  int
	render func(*agentapi.MemberConfig)
}

func (p *configPullProvider) MemberConfig(ctx context.Context, rc *engine.Context, member string) (agentapi.MemberConfig, error) {
	p.calls++
	rc.Backend.(infra.PlatformSnapshotter).PlatformSnapshot().ReadStart(ctx)
	cfg, err := p.fakeProvider.MemberConfig(ctx, rc, member)
	if p.render != nil {
		p.render(&cfg)
	}
	return cfg, err
}

func TestConditionalAgentConfigPull(t *testing.T) {
	s, _, _ := testServer(t)
	p := &configPullProvider{}
	s.Registry, _ = engine.NewRegistry(p)
	backend := configPullBackend{snapshot: infra.PlatformSnapshot(t.Name() + "|target")}
	s.namespaces = engine.StaticNamespaces{DefaultBackend: backend}
	backend.snapshot.SetListing(func(context.Context) (string, error) { return "same", nil })
	backend.snapshot.Lookup(infra.WithSteadyObservation(infra.ObservationContext(t.Context())), "baseline")
	baseline := &configBaseline{eligible: true}
	s.Trigger = baseline
	if err := s.Store.SetAgentToken(t.Context(), &domain.AgentToken{Cluster: "prod", Member: "prodm0", Token: "config-token"}); err != nil {
		t.Fatal(err)
	}
	pull := func(revision string) (int, string) {
		t.Helper()
		request := httptest.NewRequest("GET", "/internal/agent/v1/config?revision="+url.QueryEscape(revision), nil)
		request.Header.Set("Authorization", "Bearer config-token")
		response := httptest.NewRecorder()
		s.InternalHandler().ServeHTTP(response, request)
		var cfg agentapi.MemberConfig
		if response.Code == 200 {
			if err := json.Unmarshal(response.Body.Bytes(), &cfg); err != nil {
				t.Fatal(err)
			}
		}
		return response.Code, cfg.PullRevision
	}
	code, revision := pull("")
	if code != 200 || revision == "" || p.calls != 1 {
		t.Fatalf("initial status=%d revision=%q calls=%d", code, revision, p.calls)
	}
	if code, _ = pull(revision); code != 304 || p.calls != 1 {
		t.Fatal("unchanged pull performed platform reads", code, p.calls)
	}
	// Old agents never send a revision and must still receive a full response.
	if code, _ = pull(""); code != 200 || p.calls != 2 {
		t.Fatal("legacy pull changed", code, p.calls)
	}
	if err := s.Store.SetClusterSecret(t.Context(), domain.ClusterRef{Name: "prod"}, "changed", []byte("new-secret")); err != nil {
		t.Fatal(err)
	}
	code, changed := pull(revision)
	if code != 200 || changed == revision || p.calls != 3 {
		t.Fatal("secret update reused revision", code, p.calls)
	}
	backend.snapshot.Invalidate()
	// Listing drift, rather than mutation epoch alone, is certified by the manager.
	baseline.eligible = false
	if code, _ = pull(changed); code != 200 || p.calls != 4 {
		t.Fatal("lost steady baseline reused revision", code, p.calls)
	}
}

func TestConfigStaleListingAndFreshPayloadRevision(t *testing.T) {
	now := time.Now()
	defer infra.SetSnapshotClockForTest(func() time.Time { return now })()
	s, _, _ := testServer(t)
	p := &configPullProvider{}
	p.render = func(cfg *agentapi.MemberConfig) { cfg.EngineSpec = json.RawMessage(`{"peers":["original"]}`) }
	s.Registry, _ = engine.NewRegistry(p)
	snapshot := infra.PlatformSnapshot(t.Name() + "|target")
	s.namespaces = engine.StaticNamespaces{DefaultBackend: configPullBackend{snapshot: snapshot}}
	listings := 0
	snapshot.SetListing(func(ctx context.Context) (string, error) {
		if infra.RequestCaller(ctx) != infra.CallerListing {
			t.Fatal("on-demand refresh lost listing attribution")
		}
		listings++
		now = now.Add(250 * time.Millisecond)
		return "unchanged", nil
	})
	baseline := &configBaseline{eligible: true}
	s.Trigger = baseline
	if err := s.Store.SetAgentToken(t.Context(), &domain.AgentToken{Cluster: "prod", Member: "prodm0", Token: "token"}); err != nil {
		t.Fatal(err)
	}
	pull := func(revision string) (int, agentapi.MemberConfig) {
		t.Helper()
		request := httptest.NewRequest("GET", "/internal/agent/v1/config?revision="+url.QueryEscape(revision), nil)
		request.Header.Set("Authorization", "Bearer token")
		response := httptest.NewRecorder()
		s.InternalHandler().ServeHTTP(response, request)
		var cfg agentapi.MemberConfig
		if response.Code == 200 {
			if err := json.Unmarshal(response.Body.Bytes(), &cfg); err != nil {
				t.Fatal(err)
			}
		}
		return response.Code, cfg
	}
	code, initial := pull("")
	if code != 200 || initial.PullRevision == "" || p.calls != 1 || listings != 1 {
		t.Fatal("initial full response lacked a usable revision", code, p.calls, listings)
	}
	// A slow/jittered 65s listing schedule must add one shared listing,
	// not nine peer GETs and then another full pull to recover an empty token.
	for minute := range 60 {
		now = now.Add(65 * time.Second)
		if code, _ = pull(initial.PullRevision); code != 304 || p.calls != 1 || listings != minute+2 {
			t.Fatal("stale proof caused a full render or duplicate listing", minute, code, p.calls, listings)
		}
	}
	// Eligibility may become known during a full render. Certify the actual
	// new payload at completion, never reissue the old input-only token.
	baseline.eligible = false
	p.render = func(cfg *agentapi.MemberConfig) {
		cfg.EngineSpec = json.RawMessage(`{"peers":["fresh"]}`)
		baseline.eligible = true
	}
	code, fresh := pull(initial.PullRevision)
	if code != 200 || fresh.PullRevision == "" || fresh.PullRevision == initial.PullRevision {
		t.Fatal("fresh resolution did not get its own usable payload token", code, fresh.PullRevision)
	}
	renders := p.calls
	if code, _ = pull(fresh.PullRevision); code != 304 || p.calls != renders {
		t.Fatal("full response forced a second full pull to recover its token", code, p.calls)
	}
	if code, changed := pull(initial.PullRevision); code != 200 || changed.PullRevision != fresh.PullRevision {
		t.Fatal("older payload token hid a changed fresh peer set", code)
	}
}
