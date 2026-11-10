// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/agentpush"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func TestForcedConfigNoticeOneRenderPerUnchangedProxy(t *testing.T) {
	s, _, _ := testServer(t)
	s.AgentPush = agentpush.NewStore(nil)
	p := &configPullProvider{}
	p.render = func(cfg *agentapi.MemberConfig) { cfg.ConfigVersion = 10 }
	s.Registry, _ = engine.NewRegistry(p)
	snapshot := infra.PlatformSnapshot(t.Name() + "|target")
	snapshot.SetListing(func(context.Context) (string, error) { return "same", nil })
	s.namespaces = engine.StaticNamespaces{DefaultBackend: configPullBackend{snapshot: snapshot}}
	baseline := &configBaseline{eligible: true}
	s.Trigger = baseline
	if err := s.Store.SetAgentToken(t.Context(), &domain.AgentToken{Cluster: "prod", Member: "prodm0", Token: "token"}); err != nil {
		t.Fatal(err)
	}
	handler := s.InternalHandler()
	pull := func(revision string) (int, agentapi.MemberConfig, string) {
		t.Helper()
		r := httptest.NewRequest("GET", "/internal/agent/v1/config?revision="+url.QueryEscape(revision), nil)
		r.Header.Set("Authorization", "Bearer token")
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		var cfg agentapi.MemberConfig
		if w.Code == 200 {
			if err := json.Unmarshal(w.Body.Bytes(), &cfg); err != nil {
				t.Fatal(err)
			}
		}
		return w.Code, cfg, w.Header().Get(agentapi.ConfigChangeAckHeader)
	}
	heartbeat := func(cfg agentapi.MemberConfig, modern bool) agentapi.HeartbeatResponse {
		t.Helper()
		request := agentapi.HeartbeatRequest{Session: "session", ConfigRevision: cfg.ChangeRevision}
		if modern {
			request.ConfigContentRevision = cfg.ContentRevision
		}
		body, err := json.Marshal(request)
		if err != nil {
			t.Fatal(err)
		}
		r := httptest.NewRequest("POST", agentapi.HeartbeatPath, bytes.NewReader(body))
		r.Header.Set("Authorization", "Bearer token")
		if modern {
			r.Header.Set(agentapi.ConfigContentRevisionHeader, cfg.ContentRevision)
		}
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		var response agentapi.HeartbeatResponse
		if w.Code != 200 {
			t.Fatal(w.Code, w.Body.String())
		}
		if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
			t.Fatal(err)
		}
		return response
	}
	var proxies [2]agentapi.MemberConfig
	for i := range proxies {
		code, cfg, _ := pull("")
		if code != 200 || cfg.PullRevision == "" || cfg.ContentRevision == "" {
			t.Fatal("initial certificate missing", code)
		}
		proxies[i] = cfg
	}
	before := p.calls
	baseline.generation++
	baseline.eligible = false
	for range 5 {
		for _, proxy := range proxies {
			if heartbeat(proxy, true).ReloadConfig {
				t.Fatal("pure force invalidation reloaded unchanged content during deep pass")
			}
		}
	}
	if p.calls != before {
		t.Fatal("force window rendered before steady completion")
	}
	// Old agents omit the new content stamp and retain the existing behaviour.
	if !heartbeat(proxies[0], false).ReloadConfig {
		t.Fatal("legacy notice handling was silently disabled")
	}
	baseline.eligible = true
	for i := range proxies {
		notice := heartbeat(proxies[i], true)
		if !notice.ReloadConfig || notice.ConfigRevision == "" {
			t.Fatal("completed force did not identify its target stamp")
		}
		code, cfg, _ := pull(proxies[i].PullRevision)
		if code != 200 || cfg.ConfigVersion != 10 || cfg.ChangeRevision != notice.ConfigRevision {
			t.Fatal("forced full render did not acknowledge certified target", code)
		}
		proxies[i] = cfg
	}
	if p.calls-before != 2 {
		t.Fatal("unchanged force exceeded one full render per proxy", p.calls-before)
	}
	for range 8 {
		for _, proxy := range proxies {
			if heartbeat(proxy, true).ReloadConfig {
				t.Fatal("completed forced payload kept generating notices")
			}
		}
	}
	for _, proxy := range proxies {
		code, _, ack := pull(proxy.PullRevision)
		if code != 304 || ack != proxy.ChangeRevision {
			t.Fatal("304 acknowledgement was not certificate-bound", code, ack)
		}
	}
	// A real semantic change still propagates while nonsteady.
	baseline.eligible = false
	if err := s.Store.SetClusterSecret(t.Context(), domain.ClusterRef{Name: "prod"}, "changed", []byte("value")); err != nil {
		t.Fatal(err)
	}
	if !heartbeat(proxies[0], true).ReloadConfig {
		t.Fatal("nonsteady content change was suppressed with pure force notices")
	}
	// A full render crossing steady re-entry must publish its final certified
	// stamp, rather than leave the agent advertising its pre-render stamp.
	p.render = func(cfg *agentapi.MemberConfig) {
		cfg.ConfigVersion = 10
		baseline.eligible = true
	}
	code, crossed, _ := pull(proxies[0].PullRevision)
	if code != 200 || crossed.PullRevision == "" {
		t.Fatal("crossing render did not certify its fresh payload", code)
	}
	if notice := heartbeat(crossed, true); notice.ReloadConfig || notice.ConfigRevision != crossed.ChangeRevision {
		t.Fatal("render-end acknowledgement retained an obsolete pre-render stamp")
	}
	if code, _, ack := pull(crossed.PullRevision); code != 304 || ack != crossed.ChangeRevision {
		t.Fatal("crossing render certificate did not retain its own acknowledgement", code, ack)
	}
	if err := s.Store.SetClusterSecret(t.Context(), domain.ClusterRef{Name: "prod"}, "changed", []byte("newer")); err != nil {
		t.Fatal(err)
	}
	if code, _, ack := pull(crossed.PullRevision); code != 200 || ack != "" {
		t.Fatal("certificate acknowledged unrelated newer state", code, ack)
	}
}

func TestAgentConfigRequestMetricResultVocabulary(t *testing.T) {
	before := [3]uint64{agentConfigRequests[0].Load(), agentConfigRequests[1].Load(), agentConfigRequests[2].Load()}
	s, _, _ := testServer(t)
	p := &configPullProvider{}
	s.Registry, _ = engine.NewRegistry(p)
	snapshot := infra.PlatformSnapshot(t.Name() + "|target")
	snapshot.SetListing(func(context.Context) (string, error) { return "same", nil })
	s.namespaces = engine.StaticNamespaces{DefaultBackend: configPullBackend{snapshot: snapshot}}
	s.Trigger = &configBaseline{eligible: true}
	if err := s.Store.SetAgentToken(t.Context(), &domain.AgentToken{Cluster: "prod", Member: "prodm0", Token: "token"}); err != nil {
		t.Fatal(err)
	}
	h := s.InternalHandler()
	request := func(revision, token string) *httptest.ResponseRecorder {
		r := httptest.NewRequest("GET", "/internal/agent/v1/config?revision="+url.QueryEscape(revision), nil)
		if token != "" {
			r.Header.Set("Authorization", "Bearer "+token)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}
	full := request("", "token")
	var cfg agentapi.MemberConfig
	if full.Code != 200 || json.Unmarshal(full.Body.Bytes(), &cfg) != nil {
		t.Fatal("full fixture failed")
	}
	if request(cfg.PullRevision, "token").Code != 304 || request("", "").Code != 401 {
		t.Fatal("metric fixture result failed")
	}
	var text bytes.Buffer
	if err := writePollingPrometheus(&text); err != nil {
		t.Fatal(err)
	}
	for i, result := range [...]string{"full", "unchanged", "error"} {
		got := agentConfigRequests[i].Load()
		if got-before[i] != 1 {
			t.Fatal("wrong result counter delta", result, got-before[i])
		}
		line := fmt.Sprintf("karakoram_agent_config_requests_total{result=%q} %d", result, got)
		if !strings.Contains(text.String(), line) {
			t.Fatal("metric missing result-only series", line)
		}
	}
}
