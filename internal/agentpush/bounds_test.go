// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package agentpush

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
)

func TestStoreRetainedBounds(t *testing.T) {
	now := time.Unix(100, 0)
	s := NewStore(func() time.Time { return now })
	key := MemberKey{"ns", "cluster", "member"}
	src := s.Source(key)
	r := agentapi.HeartbeatRequest{Session: "live", Instance: agentapi.InstanceIdentity{Hostname: "live"}}
	r.Token = s.Ingest(key, r).Token
	large := []byte(`"` + strings.Repeat("x", agentapi.HeartbeatSampleMaxBytes-2) + `"`)
	for i := range 100 {
		now = now.Add(time.Second)
		request := []byte(fmt.Sprintf(`{"iteration":%d}`, i))
		src.Subscribe("", "mysql", "status", request)
		r.Samples = []agentapi.HeartbeatSample{
			{Action: "status", Code: 200, Response: []byte(`{}`)},
			{Engine: "mysql", Action: "status", Request: request, Code: 200, Response: large},
			// Arbitrary echoed request keys were never subscribed.
			{Engine: "postgres", Action: "status", Request: request, Code: 200, Response: large},
		}
		reply := s.Ingest(key, r)
		r.Token = reply.Token
		s.Ingest(key, agentapi.HeartbeatRequest{Session: fmt.Sprintf("rotating-%d", i)})
		m := s.members[key]
		if len(m.sessions) > maxMemberSessions || len(m.subscriptions) > maxMemberSubscriptions {
			t.Fatal("unbounded sessions/subscriptions", len(m.sessions), len(m.subscriptions))
		}
		subscriptionBytes := 0
		for k := range m.subscriptions {
			subscriptionBytes += len(k.request)
		}
		if subscriptionBytes > maxSubscriptionBytes {
			t.Fatal("unbounded subscription bytes", subscriptionBytes)
		}
		for _, x := range m.sessions {
			bytes := 0
			for k, sample := range x.samples {
				if k.engine == "postgres" {
					t.Fatal("unsolicited sample retained")
				}
				bytes += len(sample.body)
			}
			if len(x.samples) > agentapi.HeartbeatMaxSamples || bytes > maxSessionSampleBytes {
				t.Fatal("unbounded sample storage", len(x.samples), bytes)
			}
		}
		encoded, err := json.Marshal(reply)
		if err != nil || len(encoded) > agentapi.HeartbeatMaxBytes || len(reply.Requests) > agentapi.HeartbeatMaxSamples {
			t.Fatal("unbounded reply", len(encoded), len(reply.Requests), err)
		}
	}
	if len(s.members[key].sessions) != maxMemberSessions {
		t.Fatal("session cap not exercised")
	}
	if _, _, ok := src.PushedSample("", "", "status", nil); ok {
		t.Fatal("session overflow elected a logical runtime without binding")
	}
	if _, _, ok := src.PushedSample("live", "", "status", nil); ok {
		t.Fatal("session overflow silently trusted a pinned runtime")
	}
	// Keep the original session alive while sweeping away its old evidence.
	now = now.Add(SampleMaxAge + time.Nanosecond)
	r.Samples = nil
	s.Ingest(key, r)
	s.sweep()
	if len(s.members[key].sessions["live"].samples) != 0 {
		t.Fatal("live session retained expired sample keys")
	}
}

func TestStoreSubscriptionByteBudget(t *testing.T) {
	now := time.Unix(100, 0)
	s := NewStore(func() time.Time { return now })
	key := MemberKey{"ns", "cluster", "member"}
	src := s.Source(key)
	for i := range 8 {
		request := []byte(fmt.Sprintf(`{"n":%d,"data":"%s"}`, i, strings.Repeat("x", maxSubscriptionBytes/2-32)))
		src.Subscribe("", "mysql", "status", request)
	}
	m := s.members[key]
	if len(m.subscriptions) != 2 {
		t.Fatal("byte cap did not reject excess subscriptions", len(m.subscriptions))
	}
	src.Subscribe("", "mysql", "status", []byte(strings.Repeat("x", maxSubscriptionBytes+1)))
	if len(m.subscriptions) != 2 {
		t.Fatal("oversized subscription admitted")
	}
	reply := s.Ingest(key, agentapi.HeartbeatRequest{Session: "live"})
	encoded, err := json.Marshal(reply)
	if err != nil || len(encoded) > agentapi.HeartbeatMaxBytes {
		t.Fatal("reply envelope not bounded", len(encoded), err)
	}
	now = now.Add(SubscriptionTTL)
	s.sweep()
	if len(m.subscriptions) != 0 {
		t.Fatal("expired subscriptions retained")
	}
}
