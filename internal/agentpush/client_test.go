// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package agentpush

import (
	"context"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func TestStoreClientSubscriptionWarmup(t *testing.T) {
	now := time.Unix(100, 0)
	store := NewStore(func() time.Time { return now })
	key := MemberKey{"ns", "cluster", "member"}
	req := agentapi.HeartbeatRequest{Session: "one"}
	req.Token = store.Ingest(key, req).Token
	var calls atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set(agentapi.HeartbeatSessionHeader, req.Session)
		if r.URL.Path != "/v1/mysql/status" && r.URL.Path != "/v1/status" {
			t.Errorf("unexpected path %q", r.URL.Path)
		}
		_, _ = w.Write([]byte(`{"ready":true}`))
	}))
	defer server.Close()
	client, err := agentapi.NewClient(agentapi.ClientOptions{
		BaseURL: server.URL, Token: "member-bearer",
		CAPEM:   pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}),
		Samples: store.Source(key), SampleMember: key.Member,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if _, err := client.Status(context.Background()); err != nil {
		t.Fatal("initial live logical probe failed", err)
	}
	steady := infra.WithSteadyObservation(infra.ObservationContext(context.Background()))
	var result struct {
		Ready bool `json:"ready"`
	}
	if err := client.Engine(steady, "mysql", "status", struct{}{}, &result); err != agentapi.ErrPushedSampleUnavailable {
		t.Fatal("newly needed action must reject the first steady attempt", err)
	}
	if calls.Load() != 1 {
		t.Fatal("steady miss pulled from a known pusher")
	}
	// The ordinary fresh fallback learns the newly needed subscription.
	if err := client.Engine(infra.WithoutSteadyObservation(steady), "mysql", "status", struct{}{}, &result); err != nil || !result.Ready {
		t.Fatal("fresh fallback failed", result, err)
	}
	if calls.Load() != 2 {
		t.Fatal("fresh fallback did not pull")
	}
	now = now.Add(agentapi.HeartbeatInterval)
	reply := store.Ingest(key, req)
	found := false
	for _, action := range reply.Requests {
		if action.Engine == "mysql" && action.Action == "status" && string(action.Request) == `{}` {
			found = true
			req.Samples = append(req.Samples, agentapi.HeartbeatSample{Engine: action.Engine, Action: action.Action, Request: action.Request, Code: 200, Response: []byte(`{"ready":true}`)})
		}
	}
	if !found {
		t.Fatal("fresh fallback subscription did not survive into heartbeat reply", reply.Requests)
	}
	req.Token = reply.Token
	now = now.Add(agentapi.HeartbeatInterval)
	store.Ingest(key, req)
	attempt, evidence := infra.WithEvidence(steady)
	result.Ready = false
	if err := client.Engine(attempt, "mysql", "status", struct{}{}, &result); err != nil || !result.Ready {
		t.Fatal("subscribed pushed sample did not satisfy the next steady attempt", result, err)
	}
	if calls.Load() != 2 {
		t.Fatal("warmed steady read pulled")
	}
	if at, ok := evidence.MemberOldest(key.Member); !ok || !at.Equal(now.Add(-agentapi.HeartbeatInterval)) {
		t.Fatal("steady attempt lacked conservative pushed evidence", at, ok)
	}
	// Native pushed reads must keep their subscription alive beyond its original
	// TTL. Live generic probes renew routing proof, not the native subscription.
	for range 210 {
		now = now.Add(agentapi.HeartbeatInterval)
		req.Token = store.Ingest(key, req).Token
		client.Close() // Routing proof is renewed only on a newly dialed connection.
		if _, err := client.Status(context.Background()); err != nil {
			t.Fatal("live logical binding renewal failed", err)
		}
		before := calls.Load()
		if err := client.Engine(steady, "mysql", "status", struct{}{}, &result); err != nil {
			t.Fatal("steady hit did not renew subscription", now, err)
		}
		if calls.Load() != before {
			t.Fatal("native subscription expired during steady renewal")
		}
	}
	if calls.Load() != 212 {
		t.Fatal("unexpected requests beyond generic binding renewals")
	}
}

func TestStoreClientAmbiguousLogicalPull(t *testing.T) {
	for _, kind := range []string{"legacy", "stale", "ambiguous"} {
		t.Run(kind, func(t *testing.T) {
			now := time.Unix(100, 0)
			store := NewStore(func() time.Time { return now })
			key := MemberKey{"ns", "cluster", "proxy"}
			store.Source(key).Subscribe("", "mysql", "proxy-status", []byte(`{}`))
			sessions := []string(nil)
			switch kind {
			case "stale":
				sessions = []string{"one"}
			case "ambiguous":
				sessions = []string{"one", "two", "three"}
			}
			for _, session := range sessions {
				req := agentapi.HeartbeatRequest{Session: session, Instance: agentapi.InstanceIdentity{Hostname: session}}
				req.Token = store.Ingest(key, req).Token
				req.Samples = []agentapi.HeartbeatSample{
					{Action: agentapi.GenericStatusAction, Code: 200, Response: []byte(`{"configVersion":1}`)},
					{Engine: "mysql", Action: "proxy-status", Request: []byte(`{}`), Code: 200, Response: []byte(`{"ready":true}`)},
				}
				store.Ingest(key, req)
			}
			if kind == "stale" {
				now = now.Add(SampleMaxAge + time.Nanosecond)
			}
			var calls atomic.Int32
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if kind != "legacy" {
					w.Header().Set(agentapi.HeartbeatSessionHeader, "one")
				}
				_, _ = w.Write([]byte(`{"configVersion":2,"ready":true}`))
			}))
			defer server.Close()
			client, err := agentapi.NewClient(agentapi.ClientOptions{
				BaseURL: server.URL, Token: "member-bearer",
				CAPEM:   pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}),
				Samples: store.Source(key), SampleMember: key.Member,
			})
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			steady := infra.WithSteadyObservation(infra.ObservationContext(context.Background()))
			if kind == "stale" {
				if _, err := client.Status(context.Background()); err != nil {
					t.Fatal("live probe did not bind stale session", err)
				}
				calls.Store(0)
			}
			status, statusErr := client.Status(steady)
			var native struct {
				Ready bool `json:"ready"`
			}
			engineErr := client.Engine(steady, "mysql", "proxy-status", struct{}{}, &native)
			if kind == "stale" {
				if statusErr != agentapi.ErrPushedSampleUnavailable || engineErr != agentapi.ErrPushedSampleUnavailable || calls.Load() != 0 {
					t.Fatal("single stale pusher must fail fast", statusErr, engineErr, calls.Load())
				}
			} else if kind == "legacy" && (statusErr != nil || engineErr != nil || status.ConfigVersion != 2 || !native.Ready || calls.Load() != 2) {
				t.Fatal("legacy logical reads must pull", status, statusErr, engineErr, calls.Load())
			} else if kind == "ambiguous" && (statusErr != nil || engineErr != nil || status.ConfigVersion != 2 || !native.Ready || calls.Load() != 1) {
				t.Fatal("live logical read must bind a proxy for native push", status, statusErr, engineErr, calls.Load())
			}
			if kind == "ambiguous" {
				pinned, err := client.ForInstance("one")
				if err != nil {
					t.Fatal(err)
				}
				defer pinned.Close()
				status, err := pinned.Status(steady)
				if err != nil || status.ConfigVersion != 1 || calls.Load() != 1 {
					t.Fatal("unambiguous pinned sample must remain usable", status, err, calls.Load())
				}
			}
		})
	}
}
