// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/agentpush"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type noticeStormProvider struct {
	configPullProvider
	content string
}

func (p *noticeStormProvider) MemberConfigNoticeContent(context.Context, *engine.Context, string) (string, error) {
	return p.content, nil
}

func TestRunningOperationJournalChangesDoNotStormConfig(t *testing.T) {
	s, _, _ := testServer(t)
	p := &noticeStormProvider{content: "unchanged-rendered-proxy-content"}
	s.Registry, _ = engine.NewRegistry(p)
	s.AgentPush = agentpush.NewStore(nil)
	s.Trigger = &configBaseline{eligible: false}
	snapshot := infra.PlatformSnapshot(t.Name() + "|target")
	snapshot.SetListing(func(context.Context) (string, error) { return "same", nil })
	backend := configPullBackend{snapshot: snapshot}
	s.namespaces = engine.StaticNamespaces{DefaultBackend: backend}
	s.Context.Backend = backend
	if err := s.Store.SetAgentToken(t.Context(), &domain.AgentToken{Cluster: "prod", Member: "prodm0", Token: "token"}); err != nil {
		t.Fatal(err)
	}
	if err := s.Store.EnqueueOperation(t.Context(), &domain.Operation{Cluster: "prod", Kind: domain.OperationUpdate, Phase: domain.PhaseRunning}); err != nil {
		t.Fatal(err)
	}
	h := s.InternalHandler()
	pull := func() (agentapi.MemberConfig, string) {
		r := httptest.NewRequest("GET", "/internal/agent/v1/config", nil)
		r.Header.Set("Authorization", "Bearer token")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != 200 {
			t.Fatal(w.Code, w.Body.String())
		}
		var cfg agentapi.MemberConfig
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(w.Body.Bytes(), &cfg); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(w.Body.Bytes(), &fields); err != nil {
			t.Fatal(err)
		}
		var content string
		_ = json.Unmarshal(fields["contentRevision"], &content)
		return cfg, content
	}
	cfg, content := pull()
	heartbeat := func() bool {
		body, _ := json.Marshal(agentapi.HeartbeatRequest{Session: "running-operation", ConfigRevision: cfg.ChangeRevision})
		r := httptest.NewRequest("POST", agentapi.HeartbeatPath, bytes.NewReader(body))
		r.Header.Set("Authorization", "Bearer token")
		r.Header.Set("X-Karakoram-Config-Content-Revision", content)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != 200 {
			t.Fatal(w.Code, w.Body.String())
		}
		var response agentapi.HeartbeatResponse
		if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
			t.Fatal(err)
		}
		return response.ReloadConfig
	}
	before := p.calls
	for tick := range 20 {
		c, err := s.Store.GetCluster(t.Context(), domain.ClusterRef{Name: "prod"})
		if err != nil {
			t.Fatal(err)
		}
		c.Internal = json.RawMessage(fmt.Sprintf(`{"observationJournal":%d}`, tick))
		if tick%2 == 0 {
			c.Status.Phase = domain.PhaseRunning
		} else {
			c.Status.Phase = domain.PhaseReady
		}
		if err = s.Store.UpdateClusterState(t.Context(), c); err != nil {
			t.Fatal(err)
		}
		if err = s.Store.SetClusterSecret(t.Context(), c.Ref(), "mysql/gtid/history", []byte(fmt.Sprint(tick))); err != nil {
			t.Fatal(err)
		}
		if heartbeat() {
			cfg, content = pull()
		}
	}
	if renders := p.calls - before; renders != 0 {
		t.Fatalf("Running operation caused %d full renders in 20 heartbeat opportunities with unchanged proxy content", renders)
	}
	p.content = "peer-added"
	if !heartbeat() {
		t.Fatal("real peer change was delayed during running operation")
	}
	cfg, content = pull()
	if heartbeat() {
		t.Fatal("acknowledged changed peer content notified again")
	}
}
