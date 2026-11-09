// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

// Package agentpush binds pushed observations to authenticated runtime sessions.
package agentpush

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/netip"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
)

const SampleMaxAge = 7 * time.Second
const SubscriptionTTL = 10 * time.Minute

// LogicalBindingMaxAge bounds how long an unobserved routing change can keep a
// bound predecessor serving logical reads. Refreshing proof costs only a live
// logical agent request, not a platform request.
const LogicalBindingMaxAge = 10 * time.Second

// Fixed per-member budgets bound ingestion and sweeper work even for a faulty
// authenticated agent. Subscription bytes also bound the complete reply envelope.
const (
	maxMemberSessions      = 16
	maxMemberSubscriptions = agentapi.HeartbeatMaxSamples - 1
	maxSubscriptionBytes   = 64 << 10
	maxSessionSampleBytes  = agentapi.HeartbeatMaxBytes
	sessionRetention       = 5 * time.Minute
)

var heartbeatCounters struct{ accepted, tokenRejected, invalid atomic.Uint64 }

// HeartbeatCounters returns process-lifetime ingestion results across all stores.
func HeartbeatCounters() (accepted, tokenRejected, invalid uint64) {
	return heartbeatCounters.accepted.Load(), heartbeatCounters.tokenRejected.Load(), heartbeatCounters.invalid.Load()
}

type MemberKey struct{ Namespace, Cluster, Member string }
type actionKey struct{ engine, action, request string }
type sample struct {
	body []byte
	code int
	at   time.Time
}
type issuedToken struct {
	value string
	at    time.Time
}
type session struct {
	identity          agentapi.InstanceIdentity
	seen              time.Time
	current, previous issuedToken
	samples           map[actionKey]sample
	newestSample      time.Time
}
type member struct {
	sessions       map[string]*session
	subscriptions  map[actionKey]time.Time
	logicalSession string
	logicalBoundAt time.Time
	overflowUntil  time.Time
	stale          bool
}
type clusterKey struct{ namespace, cluster string }

type Store struct {
	mu       sync.Mutex
	now      func() time.Time
	members  map[MemberKey]*member
	watchers map[clusterKey]map[chan struct{}]struct{}
	hooks    []func(MemberKey)
}

func NewStore(now func() time.Time) *Store {
	if now == nil {
		now = time.Now
	}
	return &Store{now: now, members: make(map[MemberKey]*member), watchers: make(map[clusterKey]map[chan struct{}]struct{})}
}

func (s *Store) memberLocked(key MemberKey) *member {
	m := s.members[key]
	if m == nil {
		m = &member{sessions: make(map[string]*session), subscriptions: make(map[actionKey]time.Time)}
		s.members[key] = m
	}
	return m
}

func (s *Store) notifyLocked(key MemberKey) {
	for ch := range s.watchers[clusterKey{key.Namespace, key.Cluster}] {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

func (s *Store) Ingest(key MemberKey, req agentapi.HeartbeatRequest) agentapi.HeartbeatResponse {
	reply := agentapi.HeartbeatResponse{IntervalMillis: agentapi.HeartbeatInterval.Milliseconds()}
	// Reject malformed envelopes as a whole, not a partial set of evidence.
	if req.Session == "" || len(req.Samples) > agentapi.HeartbeatMaxSamples {
		heartbeatCounters.invalid.Add(1)
		return reply
	}
	for _, v := range req.Samples {
		if len(v.Response) > agentapi.HeartbeatSampleMaxBytes || len(v.Request) > agentapi.HeartbeatSampleMaxBytes {
			heartbeatCounters.invalid.Add(1)
			return reply
		}
	}
	var random [32]byte
	if _, err := rand.Read(random[:]); err != nil {
		heartbeatCounters.invalid.Add(1)
		return reply
	}
	reply.Token = hex.EncodeToString(random[:])
	s.mu.Lock()
	now := s.now()
	m := s.memberLocked(key)
	s.pruneLocked(key, m, now)
	x := m.sessions[req.Session]
	changed := x == nil
	if x == nil {
		if len(m.sessions) >= maxMemberSessions {
			// Do not evict a live session and thereby elect another runtime.
			m.overflowUntil = now.Add(SampleMaxAge)
			s.notifyLocked(key)
			heartbeatCounters.invalid.Add(1)
			hooks := append([]func(MemberKey){}, s.hooks...)
			s.mu.Unlock()
			for _, hook := range hooks {
				hook(key)
			}
			reply.Token = ""
			return reply
		}
		x = &session{samples: make(map[actionKey]sample)}
		m.sessions[req.Session] = x
	} else {
		changed = !reflect.DeepEqual(x.identity, req.Instance)
	}
	if changed {
		// Identity changes must not reuse samples taken for the old runtime.
		x.samples = make(map[actionKey]sample)
		x.newestSample = time.Time{}
	}
	var sampledAt time.Time
	if req.Token != "" {
		if req.Token == x.current.value {
			sampledAt = x.current.at
		} else if req.Token == x.previous.value {
			sampledAt = x.previous.at
		}
	}
	x.identity = req.Instance
	x.identity.Addresses = append([]string(nil), req.Instance.Addresses...)
	x.seen = now
	s.pruneSamplesLocked(m, x, now)
	if !sampledAt.IsZero() {
		heartbeatCounters.accepted.Add(1)
		for _, v := range req.Samples {
			if !agentapi.PushableAction(v.Engine, v.Action) {
				continue
			}
			k := actionKey{v.Engine, v.Action, string(v.Request)}
			if v.Code < 200 || v.Code >= 300 {
				s.notifyLocked(key)
			}
			if !m.requested(k, now) || now.Sub(sampledAt) > SampleMaxAge {
				continue
			}
			if old, ok := x.samples[k]; !ok || !sampledAt.Before(old.at) {
				delete(x.samples, k)
				size := len(v.Response)
				for _, kept := range x.samples {
					size += len(kept.body)
				}
				if size <= maxSessionSampleBytes {
					x.samples[k] = sample{append([]byte(nil), v.Response...), v.Code, sampledAt}
					if v.Code >= 200 && v.Code < 300 {
						if sampledAt.After(x.newestSample) {
							x.newestSample = sampledAt
						}
						if m.logicalSession == "" || m.logicalSession == req.Session {
							m.stale = false
						}
					}
				}
			}
		}
	} else {
		heartbeatCounters.tokenRejected.Add(1)
	}
	x.previous, x.current = x.current, issuedToken{reply.Token, now}
	actions := map[actionKey]struct{}{{"", agentapi.GenericStatusAction, ""}: {}}
	for k, expires := range m.subscriptions {
		if !now.Before(expires) {
			delete(m.subscriptions, k)
			continue
		}
		actions[k] = struct{}{}
	}
	for k := range actions {
		reply.Requests = append(reply.Requests, agentapi.HeartbeatAction{Engine: k.engine, Action: k.action, Request: []byte(k.request)})
	}
	sort.Slice(reply.Requests, func(i, j int) bool {
		a, b := reply.Requests[i], reply.Requests[j]
		if a.Engine != b.Engine {
			return a.Engine < b.Engine
		}
		if a.Action != b.Action {
			return a.Action < b.Action
		}
		return bytes.Compare(a.Request, b.Request) < 0
	})
	var hooks []func(MemberKey)
	if changed {
		s.notifyLocked(key)
		hooks = append(hooks, s.hooks...)
	}
	s.mu.Unlock()
	for _, hook := range hooks {
		hook(key)
	}
	return reply
}

type source struct {
	store *Store
	key   MemberKey
}

func (s *Store) Source(key MemberKey) agentapi.SampleSource { return source{s, key} }
func (p source) Subscribe(_ string, engine, action string, request []byte) {
	if !agentapi.PushableAction(engine, action) || engine == "" || len(request) > maxSubscriptionBytes {
		return
	}
	s := p.store
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	m := s.memberLocked(p.key)
	s.pruneLocked(p.key, m, now)
	k := actionKey{engine, action, string(request)}
	if _, exists := m.subscriptions[k]; !exists {
		if len(m.subscriptions) >= maxMemberSubscriptions {
			return
		}
		size := len(request)
		for key := range m.subscriptions {
			size += len(key.request)
		}
		if size > maxSubscriptionBytes {
			return
		}
	}
	m.subscriptions[k] = now.Add(SubscriptionTTL)
}

// BindLogical records routing proof ordered and leased by request start, not
// response arrival. Empty headers clear proof with the same ordering rule.
func (p source) BindLogical(session string, started time.Time) {
	s := p.store
	s.mu.Lock()
	defer s.mu.Unlock()
	m := s.memberLocked(p.key)
	if started.Before(m.logicalBoundAt) {
		return
	}
	if m.logicalSession != session {
		m.logicalSession = session
		m.stale = false
	}
	m.logicalBoundAt = started
}

// Pushing is consulted after an exact sample miss. Logical reads require an
// unexpired live binding before fail-fast; pinned reads retain identity matching.
func (p source) Pushing(instance string) bool {
	s := p.store
	s.mu.Lock()
	defer s.mu.Unlock()
	m := s.members[p.key]
	if m == nil {
		return false
	}
	now := s.now()
	if instance == "" {
		return m.logicalSession != "" && now.Sub(m.logicalBoundAt) <= LogicalBindingMaxAge
	}
	if !m.overflowUntil.IsZero() && !now.After(m.overflowUntil) {
		return false
	}
	known, fresh := false, 0
	for _, x := range m.sessions {
		if instance != "" && !matches(instance, x.identity) {
			continue
		}
		age := now.Sub(x.seen)
		if age <= sessionRetention {
			known = true
		}
		if age <= SampleMaxAge {
			fresh++
			if fresh > 1 {
				return false
			}
		}
	}
	return known
}

func normalizedIP(host string) (netip.Addr, bool) {
	host = strings.TrimSuffix(strings.TrimPrefix(host, "["), "]")
	a, err := netip.ParseAddr(host)
	if err != nil {
		return netip.Addr{}, false
	}
	return a.WithZone("").Unmap(), true
}
func matches(instance string, id agentapi.InstanceIdentity) bool {
	if instance == id.Hostname || id.TaskID != "" && (instance == id.TaskID || strings.HasSuffix(instance, "."+id.TaskID)) {
		return true
	}
	a, ok := normalizedIP(instance)
	if !ok {
		return false
	}
	for _, host := range id.Addresses {
		if b, ok := normalizedIP(host); ok && a == b {
			return true
		}
	}
	return false
}

func (p source) PushedSample(instance, engine, action string, request []byte) ([]byte, time.Time, bool) {
	if !agentapi.PushableAction(engine, action) {
		return nil, time.Time{}, false
	}
	s := p.store
	s.mu.Lock()
	defer s.mu.Unlock()
	m := s.members[p.key]
	if m == nil {
		return nil, time.Time{}, false
	}
	now := s.now()
	if !m.overflowUntil.IsZero() && !now.After(m.overflowUntil) {
		return nil, time.Time{}, false
	}
	var selected *session
	if instance == "" {
		if m.logicalSession == "" || now.Sub(m.logicalBoundAt) > LogicalBindingMaxAge {
			return nil, time.Time{}, false
		}
		selected = m.sessions[m.logicalSession]
	} else {
		for _, x := range m.sessions {
			if now.Sub(x.seen) > SampleMaxAge || !matches(instance, x.identity) {
				continue
			}
			if selected != nil {
				return nil, time.Time{}, false
			}
			selected = x
		}
	}
	if selected == nil || now.Sub(selected.seen) > SampleMaxAge {
		return nil, time.Time{}, false
	}
	v, ok := selected.samples[actionKey{engine, action, string(request)}]
	if !ok || v.code < 200 || v.code >= 300 || now.Sub(v.at) > SampleMaxAge {
		return nil, time.Time{}, false
	}
	return append([]byte(nil), v.body...), v.at, true
}

func (s *Store) Watch(namespace, cluster string) (<-chan struct{}, func()) {
	ch := make(chan struct{}, 1)
	key := clusterKey{namespace, cluster}
	s.mu.Lock()
	if s.watchers[key] == nil {
		s.watchers[key] = make(map[chan struct{}]struct{})
	}
	s.watchers[key][ch] = struct{}{}
	s.mu.Unlock()
	return ch, func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		delete(s.watchers[key], ch)
		if len(s.watchers[key]) == 0 {
			delete(s.watchers, key)
		}
	}
}
func (s *Store) OnIdentityChange(fn func(MemberKey)) {
	if fn == nil {
		return
	}
	s.mu.Lock()
	s.hooks = append(s.hooks, fn)
	s.mu.Unlock()
}
func (m *member) requested(k actionKey, now time.Time) bool {
	if k.engine == "" {
		return k.action == agentapi.GenericStatusAction && k.request == ""
	}
	expires, ok := m.subscriptions[k]
	return ok && now.Before(expires)
}

func (s *Store) pruneSamplesLocked(m *member, x *session, now time.Time) {
	for k, v := range x.samples {
		if now.Sub(v.at) > SampleMaxAge || !m.requested(k, now) {
			delete(x.samples, k)
		}
	}
}

func (s *Store) pruneLocked(key MemberKey, m *member, now time.Time) {
	var newest time.Time
	for id, x := range m.sessions {
		if (m.logicalSession == "" || m.logicalSession == id) && x.newestSample.After(newest) {
			newest = x.newestSample
		}
		s.pruneSamplesLocked(m, x, now)
		if now.Sub(x.seen) > sessionRetention {
			delete(m.sessions, id)
			if m.logicalSession == id {
				m.logicalSession = ""
			}
		}
	}
	if !newest.IsZero() && now.Sub(newest) > SampleMaxAge && !m.stale {
		m.stale = true
		s.notifyLocked(key)
	}
	for k, expires := range m.subscriptions {
		if !now.Before(expires) {
			delete(m.subscriptions, k)
		}
	}
}

func (s *Store) sweep() {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	for key, m := range s.members {
		s.pruneLocked(key, m, now)
		if len(m.sessions) == 0 && len(m.subscriptions) == 0 {
			delete(s.members, key)
		}
	}
}
func (s *Store) Run(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.sweep()
		}
	}
}
