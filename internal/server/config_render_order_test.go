// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type serializedConfigProvider struct {
	fakeProvider
	calls                  atomic.Int32
	withdrawn              atomic.Bool
	beforeRead, permitRead chan struct{}
}

func (p *serializedConfigProvider) MemberConfig(ctx context.Context, rc *engine.Context, member string) (agentapi.MemberConfig, error) {
	cfg, err := p.fakeProvider.MemberConfig(ctx, rc, member)
	call := p.calls.Add(1)
	if call == 2 {
		// R1 pauses BEFORE its fresh peer gate. Without serialization, the
		// later-admitted R2 could capture the older A observation first.
		close(p.beforeRead)
		<-p.permitRead
	}
	if p.withdrawn.Load() {
		cfg.EngineSpec = json.RawMessage(`{"peers":["B"]}`)
	} else {
		cfg.EngineSpec = json.RawMessage(`{"peers":["A"]}`)
	}
	return cfg, err
}

// SQL/context setup may evaluate Done before gate admission. Signal only once
// the production gate has pinned this request as the second holder/waiter.
type configGateAdmissionContext struct {
	context.Context
	server  *Server
	key     configPullKey
	once    sync.Once
	waiting chan struct{}
}

func (c *configGateAdmissionContext) Done() <-chan struct{} {
	c.server.configPullMu.Lock()
	gate := c.server.configRenderGates[c.key]
	waiting := gate != nil && gate.users > 1
	c.server.configPullMu.Unlock()
	if waiting {
		c.once.Do(func() { close(c.waiting) })
	}
	return c.Context.Done()
}

func TestSerializedConfigRenderPreventsReverseObservation(t *testing.T) {
	s, _, _ := testServer(t)
	p := &serializedConfigProvider{beforeRead: make(chan struct{}), permitRead: make(chan struct{})}
	s.Registry, _ = engine.NewRegistry(p)
	snapshot := infra.PlatformSnapshot(t.Name() + "|target")
	snapshot.SetListing(func(context.Context) (string, error) { return "same", nil })
	s.namespaces = engine.StaticNamespaces{DefaultBackend: configPullBackend{snapshot: snapshot}}
	s.Trigger = &configBaseline{eligible: true}
	if err := s.Store.SetAgentToken(t.Context(), &domain.AgentToken{Cluster: "prod", Member: "prodm0", Token: "token"}); err != nil {
		t.Fatal(err)
	}
	rc, _, err := s.context(t.Context(), domain.ClusterRef{Name: "prod"})
	if err != nil {
		t.Fatal(err)
	}
	key := memberConfigKey(rc, "prodm0")
	handler := s.InternalHandler()
	type result struct {
		code int
		cfg  agentapi.MemberConfig
		err  error
	}
	pull := func(ctx context.Context, revision string) result {
		request := httptest.NewRequest("GET", "/internal/agent/v1/config?revision="+url.QueryEscape(revision), nil).WithContext(ctx)
		request.Header.Set("Authorization", "Bearer token")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		r := result{code: response.Code}
		if r.code == 200 {
			r.err = json.Unmarshal(response.Body.Bytes(), &r.cfg)
		}
		return r
	}
	initial := pull(t.Context(), "")
	if initial.err != nil || initial.code != 200 || initial.cfg.PullRevision == "" {
		t.Fatal("initial A certificate missing", initial.code, initial.err)
	}
	first, second := make(chan result, 1), make(chan result, 1)
	go func() { first <- pull(t.Context(), "") }()
	<-p.beforeRead
	admission := &configGateAdmissionContext{Context: t.Context(), server: s, key: key, waiting: make(chan struct{})}
	go func() { second <- pull(admission, "") }()
	<-admission.waiting
	// R2 has reached the actual production gate but cannot observe A while
	// R1 is paused before reading the externally mutable peer state.
	calls := p.calls.Load()
	p.withdrawn.Store(true)
	close(p.permitRead)
	r1, r2 := <-first, <-second
	if calls != 2 {
		t.Fatal("R2 entered peer rendering concurrently with R1", calls)
	}
	if r1.err != nil || r2.err != nil || r1.code != 200 || r2.code != 200 {
		t.Fatal("serialized full render failed", r1.code, r2.code, r1.err, r2.err)
	}
	if r1.cfg.PullRevision == initial.cfg.PullRevision || r2.cfg.PullRevision != r1.cfg.PullRevision {
		t.Fatal("later request retained the pre-withdrawal observation")
	}
	if b := pull(t.Context(), r2.cfg.PullRevision); b.code != 304 {
		t.Fatal("authoritative B certificate missing", b.code)
	}
	if a := pull(t.Context(), initial.cfg.PullRevision); a.code != 200 || a.cfg.PullRevision != r2.cfg.PullRevision {
		t.Fatal("old A token was re-authorized for304", a.code, a.err)
	}
}

func TestConfigRenderGateCancellationAndParallelMembers(t *testing.T) {
	s := &Server{configPullState: &configPullState{}}
	a := configPullKey{cluster: domain.ClusterRef{Name: "cluster"}, member: "A"}
	b := configPullKey{cluster: domain.ClusterRef{Name: "cluster"}, member: "B"}
	holder, err := s.acquireConfigRender(t.Context(), a)
	if err != nil {
		t.Fatal(err)
	}
	defer s.releaseConfigRender(holder)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	admission := &configGateAdmissionContext{Context: ctx, server: s, key: a, waiting: make(chan struct{})}
	result := make(chan error, 1)
	go func() {
		gate, err := s.acquireConfigRender(admission, a)
		if gate != nil {
			s.releaseConfigRender(gate)
		}
		result <- err
	}()
	<-admission.waiting
	parallel, err := s.acquireConfigRender(t.Context(), b)
	if err != nil {
		cancel()
		t.Fatal("different member blocked behind A", err)
	}
	s.releaseConfigRender(parallel)
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatal("queued render ignored request cancellation", err)
	}
	s.configPullMu.Lock()
	users := holder.users
	s.configPullMu.Unlock()
	if users != 1 {
		t.Fatal("cancelled waiter leaked its active gate reference", users)
	}
}

func TestConfigRenderGateBoundedEvictionPreservesActiveKey(t *testing.T) {
	s := &Server{configPullState: &configPullState{}}
	active := configPullKey{cluster: domain.ClusterRef{Name: "cluster"}, member: "active"}
	holder, err := s.acquireConfigRender(t.Context(), active)
	if err != nil {
		t.Fatal(err)
	}
	defer s.releaseConfigRender(holder)
	for i := range maxConfigPullCertificates + 1 {
		key := configPullKey{cluster: domain.ClusterRef{Name: fmt.Sprintf("cluster%d", i)}, member: "idle"}
		gate, err := s.acquireConfigRender(t.Context(), key)
		if err != nil {
			t.Fatal(err)
		}
		s.releaseConfigRender(gate)
	}
	s.configPullMu.Lock()
	retained := s.configRenderGates[active]
	size := len(s.configRenderGates)
	s.configPullMu.Unlock()
	if retained != holder || size != maxConfigPullCertificates {
		t.Fatal("active key was evicted or gate map grew unbounded", size)
	}
}
