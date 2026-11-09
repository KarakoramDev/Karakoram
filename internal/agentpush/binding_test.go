// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package agentpush

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func TestStoreLogicalReplacementBinding(t *testing.T) {
	now := time.Now()
	key := MemberKey{"ns", "cluster", "member"}
	s := NewStore(func() time.Time { return now })
	push := func(store *Store, id string, version int64) {
		r := agentapi.HeartbeatRequest{Session: id, Instance: agentapi.InstanceIdentity{Hostname: id}}
		r.Token = store.Ingest(key, r).Token
		body, _ := json.Marshal(agentapi.Status{ConfigVersion: version})
		r.Samples = []agentapi.HeartbeatSample{{Action: "status", Code: 200, Response: body}}
		store.Ingest(key, r)
	}
	var route atomic.Value
	route.Store("A")
	var dead atomic.Bool
	var calls atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		id := route.Load().(string)
		w.Header().Set(agentapi.HeartbeatSessionHeader, id)
		if dead.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		version := int64(1)
		if id == "B" {
			version = 2
		}
		_ = json.NewEncoder(w).Encode(agentapi.Status{ConfigVersion: version})
	}))
	defer server.Close()
	clientFor := func(store *Store) *agentapi.Client {
		c, err := agentapi.NewClient(agentapi.ClientOptions{BaseURL: server.URL, Token: "member-bearer", CAPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), Samples: store.Source(key), SampleMember: key.Member})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(c.Close)
		return c
	}
	client := clientFor(s)
	steady := infra.WithSteadyObservation(infra.ObservationContext(context.Background()))
	push(s, "A", 1)
	if _, _, ok := s.Source(key).PushedSample("", "", "status", nil); ok {
		t.Fatal("unbound process served logical health")
	}
	if _, err := client.Status(context.Background()); err != nil {
		t.Fatal(err)
	}
	if v, err := client.Status(steady); err != nil || v.ConfigVersion != 1 {
		t.Fatal(v, err)
	}
	push(s, "B", 2)
	route.Store("B")
	client.Close() // The simulated successor must answer on a new connection.
	if _, err := client.Status(context.Background()); err != nil {
		t.Fatal(err)
	}
	if v, err := client.Status(steady); err != nil || v.ConfigVersion != 2 {
		t.Fatal("live logical probe did not bind successor", v, err)
	}
	now = now.Add(SampleMaxAge + time.Nanosecond)
	push(s, "A", 1)
	dead.Store(true)
	before := calls.Load()
	if _, err := client.Status(steady); err != agentapi.ErrPushedSampleUnavailable || calls.Load() != before {
		t.Fatal("predecessor took over dead successor or stale binding pulled", err, calls.Load())
	}
	if _, _, ok := s.Source(key).PushedSample("A", "", "status", nil); !ok {
		t.Fatal("fixture predecessor was not still alive")
	}
	if _, err := client.Status(context.Background()); err == nil {
		t.Fatal("black-holed successor fixture unexpectedly healthy")
	}
	if s.members[key].logicalSession != "B" {
		t.Fatal("failed live probe rebound logical session")
	}
	// Restart forgets both tokens and routing proof, never electing the surviving A.
	restarted := NewStore(func() time.Time { return now })
	push(restarted, "A", 1)
	if restarted.Source(key).Pushing("") {
		t.Fatal("CP restart invented logical binding")
	}
	afterRestart := clientFor(restarted)
	before = calls.Load()
	if _, err := afterRestart.Status(steady); err == nil || calls.Load() != before+1 {
		t.Fatal("unbound restart did not probe logical endpoint", err)
	}
	dead.Store(false)
	if v, err := afterRestart.Status(steady); err != nil || v.ConfigVersion != 2 {
		t.Fatal("restart live probe failed", v, err)
	}
	if _, err := afterRestart.Status(steady); err != agentapi.ErrPushedSampleUnavailable {
		t.Fatal("bound unseen B borrowed A's sample", err)
	}
	push(restarted, "B", 2)
	if v, err := afterRestart.Status(steady); err != nil || v.ConfigVersion != 2 {
		t.Fatal(v, err)
	}
	now = now.Add(sessionRetention + time.Nanosecond)
	restarted.sweep()
	if restarted.Source(key).Pushing("") {
		t.Fatal("pruned bound session retained binding")
	}
}

func TestStoreLogicalBindingLeaseForcesRoutingProbe(t *testing.T) {
	for _, successor := range []string{"legacy", "dead", "session"} {
		t.Run(successor, func(t *testing.T) {
			start := time.Now()
			now := start
			key := MemberKey{"ns", "cluster", "member"}
			store := NewStore(func() time.Time { return now })
			src := store.Source(key)
			push := func(id string, version int64) {
				req := agentapi.HeartbeatRequest{Session: id, Instance: agentapi.InstanceIdentity{Hostname: id}}
				req.Token = store.Ingest(key, req).Token
				body, err := json.Marshal(agentapi.Status{ConfigVersion: version})
				if err != nil {
					t.Fatal(err)
				}
				req.Samples = []agentapi.HeartbeatSample{{Action: "status", Code: 200, Response: body}}
				store.Ingest(key, req)
			}
			var calls atomic.Int32
			serverA := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				calls.Add(1)
				w.Header().Set(agentapi.HeartbeatSessionHeader, "A")
				_ = json.NewEncoder(w).Encode(agentapi.Status{ConfigVersion: 1})
			}))
			defer serverA.Close()
			serverB := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if r.Header.Get("Authorization") != "Bearer member-bearer" {
					t.Error("routing probe lost bearer authentication")
				}
				if successor == "dead" {
					w.WriteHeader(http.StatusServiceUnavailable)
					return
				}
				if successor == "session" {
					w.Header().Set(agentapi.HeartbeatSessionHeader, "B")
				}
				_ = json.NewEncoder(w).Encode(agentapi.Status{ConfigVersion: 2})
			}))
			defer serverB.Close()
			var destination atomic.Value
			destination.Store(serverA.Listener.Addr().String())
			var dials atomic.Int32
			ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: serverA.Certificate().Raw})
			ca = append(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: serverB.Certificate().Raw})...)
			client, err := agentapi.NewClient(agentapi.ClientOptions{
				BaseURL: serverA.URL, Token: "member-bearer",
				CAPEM: ca, Samples: src, SampleMember: key.Member,
				DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
					dials.Add(1)
					return (&net.Dialer{}).DialContext(ctx, network, destination.Load().(string))
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			steady := infra.WithSteadyObservation(infra.ObservationContext(context.Background()))
			push("A", 1)
			if _, err := client.Status(context.Background()); err != nil {
				t.Fatal("initial live binding failed", err)
			}
			start = store.members[key].logicalBoundAt
			destination.Store(serverB.Listener.Addr().String())
			// Ordinary fresh reads retain keep-alive behavior, but cannot renew
			// routing proof from that old connection to the still-running A.
			now = start.Add(time.Second)
			beforeDials := dials.Load()
			if status, err := client.Status(context.Background()); err != nil || status.ConfigVersion != 1 || dials.Load() != beforeDials {
				t.Fatal("fixture did not retain A's keep-alive connection", status, err, dials.Load())
			}
			if !store.members[key].logicalBoundAt.Equal(start) {
				t.Fatal("fresh read over reused connection renewed orphan binding")
			}
			// The orphan remains alive throughout the lease, including its exact
			// inclusive boundary. Heartbeats must never refresh routing proof.
			for _, age := range []time.Duration{LogicalBindingMaxAge - time.Second, LogicalBindingMaxAge} {
				now = start.Add(age)
				push("A", 1)
				before := calls.Load()
				if status, err := client.Status(steady); err != nil || status.ConfigVersion != 1 || calls.Load() != before {
					t.Fatal("fresh binding did not serve predecessor sample", status, err, calls.Load())
				}
				if !src.Pushing("") {
					t.Fatal("fresh binding lost fail-fast eligibility")
				}
			}
			now = now.Add(time.Nanosecond)
			push("A", 1)
			if _, _, ok := src.PushedSample("", "", "status", nil); ok {
				t.Fatal("expired binding served orphan's fresh sample")
			}
			if src.Pushing("") {
				t.Fatal("expired binding would fail fast instead of probing routing")
			}
			if _, at, ok := src.PushedSample("A", "", "status", nil); !ok || !at.Equal(now) || !src.Pushing("A") {
				t.Fatal("binding lease changed pinned serving or conservative sample time", at, ok)
			}
			before := calls.Load()
			beforeDials = dials.Load()
			status, err := client.Status(steady)
			if calls.Load() != before+1 {
				t.Fatal("expired binding did not force live logical pull", calls.Load())
			}
			if dials.Load() != beforeDials+1 {
				t.Fatal("expired binding borrowed A's pooled connection", dials.Load())
			}
			if successor == "dead" {
				if err == nil || src.Pushing("") || store.members[key].logicalSession != "A" {
					t.Fatal("dead successor renewed routing proof", err)
				}
				return
			}
			if err != nil || status.ConfigVersion != 2 {
				t.Fatal("live successor response was not used", status, err)
			}
			if successor == "legacy" {
				if store.members[key].logicalSession != "" || src.Pushing("") {
					t.Fatal("legacy response did not clear predecessor binding")
				}
				return
			}
			if store.members[key].logicalSession != "B" || !src.Pushing("") {
				t.Fatal("live successor session did not acquire binding")
			}
			before = calls.Load()
			if _, err := client.Status(steady); err != agentapi.ErrPushedSampleUnavailable || calls.Load() != before {
				t.Fatal("fresh binding without samples did not fail fast", err)
			}
			push("B", 2)
			if status, err := client.Status(steady); err != nil || status.ConfigVersion != 2 || calls.Load() != before {
				t.Fatal("fresh successor binding did not serve its sample", status, err)
			}
			// A live response from the same process renews the lease as well.
			now = now.Add(LogicalBindingMaxAge - time.Second)
			client.Close() // Same-session proof requires a newly dialed response.
			if _, err := client.Status(context.Background()); err != nil {
				t.Fatal(err)
			}
			now = store.members[key].logicalBoundAt.Add(2 * time.Second)
			push("B", 2)
			before = calls.Load()
			if status, err := client.Status(steady); err != nil || status.ConfigVersion != 2 || calls.Load() != before {
				t.Fatal("same-session live response did not renew binding", status, err)
			}
		})
	}
}

func TestStoreLogicalProofOrderingAndStartLease(t *testing.T) {
	start := time.Unix(100, 0)
	now := start.Add(4 * time.Second)
	store := NewStore(func() time.Time { return now })
	key := MemberKey{"ns", "cluster", "member"}
	src := store.Source(key)
	src.BindLogical("B", start)
	if !store.members[key].logicalBoundAt.Equal(start) {
		t.Fatal("lease began at response arrival")
	}
	now = start.Add(LogicalBindingMaxAge)
	if !src.Pushing("") {
		t.Fatal("inclusive proof-start lease boundary rejected")
	}
	now = now.Add(time.Nanosecond)
	if src.Pushing("") {
		t.Fatal("response delay extended the lease")
	}
	src.BindLogical("A", start.Add(-time.Second))
	src.BindLogical("", start.Add(-time.Second))
	if store.members[key].logicalSession != "B" {
		t.Fatal("older bind or clear superseded newer proof")
	}
	src.BindLogical("", now)
	src.BindLogical("A", start)
	if store.members[key].logicalSession != "" {
		t.Fatal("late predecessor resurrected a cleared binding")
	}
	src.BindLogical("expired", now.Add(time.Second))
	now = now.Add(LogicalBindingMaxAge + 2*time.Second)
	src.BindLogical("old-response", now.Add(-LogicalBindingMaxAge-time.Nanosecond))
	if src.Pushing("") {
		t.Fatal("already expired proof acquired a new lease on arrival")
	}
}

func TestStoreLateFirstResponseCannotReplaceSuccessor(t *testing.T) {
	for _, successor := range []string{"B", ""} {
		t.Run("successor="+successor, func(t *testing.T) {
			entered, release := make(chan struct{}), make(chan struct{})
			serverA := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				close(entered)
				select {
				case <-release:
				case <-r.Context().Done():
					return
				}
				w.Header().Set(agentapi.HeartbeatSessionHeader, "A")
				_, _ = w.Write([]byte(`{}`))
			}))
			defer serverA.Close()
			defer close(release)
			serverB := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set(agentapi.HeartbeatSessionHeader, successor)
				_, _ = w.Write([]byte(`{}`))
			}))
			defer serverB.Close()
			var destination atomic.Value
			destination.Store(serverA.Listener.Addr().String())
			store := NewStore(nil)
			key := MemberKey{"ns", "cluster", "member"}
			ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: serverA.Certificate().Raw})
			ca = append(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: serverB.Certificate().Raw})...)
			client, err := agentapi.NewClient(agentapi.ClientOptions{
				BaseURL: serverA.URL, Token: "member-bearer", CAPEM: ca, Samples: store.Source(key), Timeout: 5 * time.Second,
				DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
					return (&net.Dialer{}).DialContext(ctx, network, destination.Load().(string))
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			done := make(chan error, 1)
			go func() { _, err := client.Status(t.Context()); done <- err }()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("A did not start")
			}
			destination.Store(serverB.Listener.Addr().String())
			if _, err := client.Status(t.Context()); err != nil {
				t.Fatal(err)
			}
			proof := store.members[key].logicalBoundAt
			// A is still blocked. Advance beyond B's lease before its late
			// first-use response arrives; neither B nor legacy may resurrect A.
			store.now = func() time.Time { return proof.Add(LogicalBindingMaxAge + time.Second) }
			// Release exactly once while retaining failure-path cleanup.
			release <- struct{}{}
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			if store.members[key].logicalSession != successor || !store.members[key].logicalBoundAt.Equal(proof) || store.Source(key).Pushing("") {
				t.Fatal("late A response superseded or renewed successor proof")
			}
		})
	}
}
