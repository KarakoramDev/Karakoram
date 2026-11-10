// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"net/url"
	"testing"

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
	calls int
}

func (p *configPullProvider) MemberConfig(ctx context.Context, rc *engine.Context, member string) (agentapi.MemberConfig, error) {
	p.calls++
	rc.Backend.(infra.PlatformSnapshotter).PlatformSnapshot().ReadStart(ctx)
	return p.fakeProvider.MemberConfig(ctx, rc, member)
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
