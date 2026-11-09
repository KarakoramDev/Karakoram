// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package agentpush

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
)

func TestHeartbeatCounters(t *testing.T) {
	a, r, i := HeartbeatCounters()
	s := NewStore(nil)
	key := MemberKey{"ns", "cluster", "member"}
	s.Ingest(key, agentapi.HeartbeatRequest{})
	s.Ingest(key, agentapi.HeartbeatRequest{Session: "one", Samples: []agentapi.HeartbeatSample{{Response: make([]byte, agentapi.HeartbeatSampleMaxBytes+1)}}})
	s.Ingest(key, agentapi.HeartbeatRequest{Session: "one", Samples: make([]agentapi.HeartbeatSample, agentapi.HeartbeatMaxSamples+1)})
	first := s.Ingest(key, agentapi.HeartbeatRequest{Session: "one"})
	s.Ingest(key, agentapi.HeartbeatRequest{Session: "one", Token: first.Token})
	s.Ingest(key, agentapi.HeartbeatRequest{Session: "one", Token: "unknown"})
	// A second store shares the process counters, not per-cluster series.
	NewStore(nil).Ingest(key, agentapi.HeartbeatRequest{})
	aa, rr, ii := HeartbeatCounters()
	if aa-a != 1 || rr-r != 2 || ii-i != 4 {
		t.Fatalf("accepted=%d token_rejected=%d invalid=%d, want 1 2 4", aa-a, rr-r, ii-i)
	}
	t.Logf("accepted=%d token_rejected=%d invalid=%d", aa-a, rr-r, ii-i)
}

func TestStoreTokenProtocol(t *testing.T) {
	now := time.Unix(100, 0)
	s := NewStore(func() time.Time { return now })
	key := MemberKey{"ns", "cluster", "member"}
	src := s.Source(key)
	src.BindLogical("one", now)
	req := agentapi.HeartbeatRequest{Session: "one", Samples: []agentapi.HeartbeatSample{{Action: "status", Code: 200, Response: []byte(`{"configVersion":1}`)}}}
	first := s.Ingest(key, req)
	if first.Token == "" || first.IntervalMillis != 3000 {
		t.Fatal(first)
	}
	if _, _, ok := src.PushedSample("", "", "status", nil); ok {
		t.Fatal("accepted missing token")
	}
	now = now.Add(3 * time.Second)
	req.Token = first.Token
	second := s.Ingest(key, req)
	_, at, ok := src.PushedSample("", "", "status", nil)
	if !ok || !at.Equal(time.Unix(100, 0)) {
		t.Fatal("not anchored to issue time", at, ok)
	}
	// The immediately previous token remains valid after a lost reply.
	req.Samples[0].Response = []byte(`{"configVersion":2}`)
	s.Ingest(key, req)
	body, _, ok := src.PushedSample("", "", "status", nil)
	if !ok || string(body) != `{"configVersion":2}` {
		t.Fatal("previous token rejected")
	}
	req.Token = "unknown"
	req.Samples[0].Response = []byte(`{"configVersion":3}`)
	r := s.Ingest(key, req)
	body, _, _ = src.PushedSample("", "", "status", nil)
	if r.Token == "" || string(body) != `{"configVersion":2}` || second.Token == first.Token {
		t.Fatal("token rotation/discard failed")
	}
	now = time.Unix(107, 0)
	if _, _, ok := src.PushedSample("", "", "status", nil); !ok {
		t.Fatal("boundary must be usable")
	}
	now = now.Add(time.Nanosecond)
	if _, _, ok := src.PushedSample("", "", "status", nil); ok {
		t.Fatal("stale sample accepted")
	}
}

func TestStoreInstanceAndAmbiguity(t *testing.T) {
	now := time.Unix(100, 0)
	s := NewStore(func() time.Time { return now })
	key := MemberKey{"ns", "cluster", "member"}
	s.Source(key).Subscribe("", "mysql", "status", []byte(`{}`))
	s.Source(key).BindLogical("one", now)
	id := agentapi.InstanceIdentity{TaskID: "task123", Hostname: "runtime", Addresses: []string{"192.0.2.1", "fe80::1%eth0", "::ffff:192.0.2.2"}}
	push := func(session string, identity agentapi.InstanceIdentity) {
		r := agentapi.HeartbeatRequest{Session: session, Instance: identity}
		r.Token = s.Ingest(key, r).Token
		r.Samples = []agentapi.HeartbeatSample{{Engine: "mysql", Action: "status", Request: []byte(`{}`), Response: []byte(`{}`), Code: 200}}
		s.Ingest(key, r)
	}
	push("one", id)
	for _, instance := range []string{"", "runtime", "task123", "svc.1.task123", "192.0.2.1", "[fe80::1%eth1]", "192.0.2.2"} {
		if _, _, ok := s.Source(key).PushedSample(instance, "mysql", "status", []byte(`{}`)); !ok {
			t.Errorf("instance %q missed", instance)
		}
	}
	for _, instance := range []string{"other", "not-task123", "192.0.2.3"} {
		if _, _, ok := s.Source(key).PushedSample(instance, "mysql", "status", []byte(`{}`)); ok {
			t.Errorf("instance %q matched", instance)
		}
	}
	if _, _, ok := s.Source(key).PushedSample("", "mysql", "status", nil); ok {
		t.Fatal("request bytes not exact")
	}
	push("two", agentapi.InstanceIdentity{Hostname: "other"})
	if _, _, ok := s.Source(key).PushedSample("", "mysql", "status", []byte(`{}`)); !ok {
		t.Fatal("bound logical session lost to overlapping session")
	}
	if _, _, ok := s.Source(key).PushedSample("runtime", "mysql", "status", []byte(`{}`)); !ok {
		t.Fatal("unambiguous pinned read missed")
	}
	now = now.Add(8 * time.Second)
	push("one", id)
	if _, _, ok := s.Source(key).PushedSample("", "mysql", "status", []byte(`{}`)); !ok {
		t.Fatal("stale second session remained ambiguous")
	}
}

func TestStoreSubscriptionsAndWatch(t *testing.T) {
	now := time.Unix(100, 0)
	s := NewStore(func() time.Time { return now })
	key := MemberKey{"ns", "cluster", "member"}
	ch, cancel := s.Watch(key.Namespace, key.Cluster)
	defer cancel()
	other, stopOther := s.Watch("ns", "other")
	defer stopOther()
	hooks := 0
	s.OnIdentityChange(func(k MemberKey) { hooks++; s.Source(k).Subscribe("", "mysql", "status", []byte(`{}`)) })
	src := s.Source(key)
	src.Subscribe("a", "mysql", "status", []byte(`{}`))
	src.Subscribe("b", "mysql", "status", []byte(`{}`))
	src.Subscribe("", "mysql", "stop", nil)
	r := agentapi.HeartbeatRequest{Session: "one"}
	reply := s.Ingest(key, r)
	if hooks != 1 || len(reply.Requests) != 2 {
		t.Fatal(hooks, reply.Requests)
	}
	select {
	case <-ch:
	default:
		t.Fatal("new member not notified")
	}
	select {
	case <-other:
		t.Fatal("cross-cluster notification")
	default:
	}
	r.Token = reply.Token
	r.Samples = []agentapi.HeartbeatSample{{Action: "status", Code: 503, Response: []byte(`{}`)}}
	s.Ingest(key, r)
	select {
	case <-ch:
	default:
		t.Fatal("failure not notified")
	}
	if _, _, ok := src.PushedSample("", "", "status", nil); ok {
		t.Fatal("non-2xx served")
	}
	r.Instance.Hostname = "changed"
	s.Ingest(key, r)
	select {
	case <-ch:
	default:
		t.Fatal("identity not notified")
	}
	if hooks != 2 {
		t.Fatal(hooks)
	}
	r.Token = s.Ingest(key, agentapi.HeartbeatRequest{Session: r.Session, Instance: r.Instance}).Token
	r.Samples[0].Code = 200
	s.Ingest(key, r)
	now = now.Add(SampleMaxAge)
	s.sweep()
	select {
	case <-ch:
		t.Fatal("stale at inclusive boundary")
	default:
	}
	now = now.Add(time.Nanosecond)
	s.sweep()
	select {
	case <-ch:
	default:
		t.Fatal("staleness not notified")
	}
	s.sweep()
	select {
	case <-ch:
		t.Fatal("duplicate staleness notification")
	default:
	}
	now = now.Add(SubscriptionTTL)
	r.Samples = nil
	reply = s.Ingest(key, r)
	if len(reply.Requests) != 1 || reply.Requests[0].Action != "status" {
		t.Fatal(reply.Requests)
	}
	now = now.Add(6 * time.Minute)
	s.sweep()
	if m := s.members[key]; m != nil && len(m.sessions) != 0 {
		t.Fatal("idle session not pruned")
	}
	// The new-session hook may have renewed a subscription after the long gap.
	now = now.Add(SubscriptionTTL)
	s.sweep()
	if len(s.members) != 0 {
		t.Fatal("expired member subscriptions not reclaimed")
	}
}

func TestStoreValidationAndConcurrency(t *testing.T) {
	s := NewStore(nil)
	key := MemberKey{"ns", "cluster", "member"}
	for _, r := range []agentapi.HeartbeatRequest{{}, {Session: "one", Samples: make([]agentapi.HeartbeatSample, agentapi.HeartbeatMaxSamples+1)}, {Session: "one", Samples: []agentapi.HeartbeatSample{{Response: make([]byte, agentapi.HeartbeatSampleMaxBytes+1)}}}} {
		if s.Ingest(key, r).Token != "" {
			t.Fatal("invalid request accepted")
		}
	}
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 20 {
				ch, cancel := s.Watch("ns", "cluster")
				_ = ch
				s.Source(key).Subscribe("", "mysql", "status", nil)
				s.Ingest(key, agentapi.HeartbeatRequest{Session: "one"})
				s.Source(key).PushedSample("", "mysql", "status", nil)
				s.sweep()
				cancel()
			}
		}()
	}
	wg.Wait()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s.Run(ctx)
}

func TestStoreSampleClockWatchAndPushing(t *testing.T) {
	start := time.Unix(100, 0)
	now := start
	s := NewStore(func() time.Time { return now })
	key := MemberKey{"ns", "cluster", "member"}
	src := s.Source(key)
	if src.Pushing("") {
		t.Fatal("legacy member identified as a pusher")
	}
	ch, cancel := s.Watch(key.Namespace, key.Cluster)
	defer cancel()
	req := agentapi.HeartbeatRequest{Session: "one", Instance: agentapi.InstanceIdentity{TaskID: "task123", Addresses: []string{"2001:db8::1"}}}
	req.Token = s.Ingest(key, req).Token
	src.BindLogical(req.Session, now)
	<-ch // Initial identity notification.
	for _, instance := range []string{"", "task123", "svc.1.task123", "[2001:db8::1]"} {
		if !src.Pushing(instance) {
			t.Fatalf("accepted heartbeat not tracked for %q", instance)
		}
	}
	if src.Pushing("other") {
		t.Fatal("unmatched instance identified as a pusher")
	}
	now = start.Add(3 * time.Second)
	req.Samples = []agentapi.HeartbeatSample{{Action: "status", Code: 200, Response: []byte(`{}`)}}
	reply := s.Ingest(key, req)
	now = start.Add(SampleMaxAge)
	s.sweep()
	if _, _, ok := src.PushedSample("", "", "status", nil); !ok {
		t.Fatal("sample unavailable at inclusive boundary")
	}
	select {
	case <-ch:
		t.Fatal("watch fired before sample expired")
	default:
	}
	now = now.Add(time.Nanosecond)
	s.sweep()
	if _, _, ok := src.PushedSample("", "", "status", nil); ok {
		t.Fatal("expired conservative sample served")
	}
	select {
	case <-ch:
	default:
		t.Fatal("watch used heartbeat receive time instead of sample issue time")
	}
	// Empty heartbeats cannot postpone/rearm the old sample's crossing.
	req.Token = reply.Token
	req.Samples = nil
	s.Ingest(key, req)
	s.sweep()
	select {
	case <-ch:
		t.Fatal("empty heartbeat rearmed stale notification")
	default:
	}
	now = now.Add(5 * time.Minute)
	if !src.Pushing("task123") {
		t.Fatal("pusher forgotten at inclusive retention boundary")
	}
	now = now.Add(time.Nanosecond)
	if src.Pushing("task123") {
		t.Fatal("pusher remembered beyond retention without sweeping")
	}
}
