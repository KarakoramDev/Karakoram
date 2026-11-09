// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package infra

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"weak"
)

func snapshotContext(t *testing.T) context.Context {
	return WithSteadyObservation(ObservationContext(t.Context()))
}

func snapshotBaseline(t *testing.T, s *TargetSnapshot) {
	t.Helper()
	if !s.ensureListing(snapshotContext(t)) {
		t.Fatal("listing baseline failed")
	}
}

func TestSnapshotProviderClockRoundTrip(t *testing.T) {
	now := time.Unix(100, 0)
	defer SetSnapshotClockForTest(func() time.Time { return now })()
	s := &TargetSnapshot{}
	s.SetListing(func(context.Context) (string, error) { return "same", nil })
	snapshotBaseline(t, s)
	token, fetchedAt := s.ReadStart(t.Context()), SnapshotNow()
	now = now.Add(2 * time.Second) // A response arrives after its request started.
	s.Populate("get", token, fetchedAt, []byte("body"))
	if body, ok := s.Lookup(snapshotContext(t), "get"); !ok || string(body) != "body" {
		t.Fatal("provider-clock round trip missed", string(body), ok)
	}
	now = fetchedAt.Add(SnapshotMaxAge + time.Nanosecond)
	if _, ok := s.Lookup(snapshotContext(t), "get"); ok {
		t.Fatal("entry lifetime was renewed at response completion")
	}
}

func TestRequestCallerPrecedence(t *testing.T) {
	ctx := t.Context()
	if got := RequestCaller(ctx); got != CallerOther {
		t.Fatal(got)
	}
	outer := WithRequestCaller(ctx, CallerAvailability)
	inner := WithRequestCaller(outer, CallerDialAuthorization)
	if RequestCaller(outer) != CallerAvailability || RequestCaller(inner) != CallerDialAuthorization {
		t.Fatal("inner tag did not win")
	}
	if got := RequestCaller(WithRequestCaller(inner, "unbounded-user-value")); got != CallerOther {
		t.Fatal("unknown caller escaped vocabulary", got)
	}
	for _, caller := range requestCallers {
		if got := RequestCaller(WithRequestCaller(ctx, caller)); got != caller {
			t.Fatalf("caller %q became %q", caller, got)
		}
	}
}

func TestSnapshotCounters(t *testing.T) {
	now := time.Unix(100, 0)
	defer SetSnapshotClockForTest(func() time.Time { return now })()
	provider := t.Name()
	target := fmt.Sprintf("%s|%d", provider, time.Now().UnixNano())
	s := PlatformSnapshot(target + "|first-target")
	other := PlatformSnapshot(target + "|second-target")
	read := func() PlatformCounterValues {
		for _, c := range PlatformCounters() {
			if c.Provider == provider {
				return c
			}
		}
		t.Fatal("missing provider counters")
		return PlatformCounterValues{}
	}
	before := read()
	s.Invalidate()
	fingerprint := "a"
	var listingErr error
	s.SetListing(func(ctx context.Context) (string, error) {
		if RequestCaller(ctx) != CallerListing {
			t.Fatal("listing did not override caller")
		}
		s.ReadStart(ctx)
		return fingerprint, listingErr
	})
	snapshotBaseline(t, s)
	ctx := WithRequestCaller(t.Context(), CallerAvailability)
	steady := WithSteadyObservation(ObservationContext(ctx))
	s.Populate("get", s.ReadStart(ctx), now, []byte("body"))
	other.ReadStart(WithRequestCaller(ctx, "unbounded-user-value"))
	if _, ok := s.Lookup(t.Context(), "get"); ok {
		t.Fatal("non-steady hit")
	}
	if _, ok := s.Lookup(steady, "missing"); ok {
		t.Fatal("missing entry hit")
	}
	if _, ok := s.Lookup(steady, "get"); !ok {
		t.Fatal("expected hit")
	}
	fingerprint = "b"
	now = now.Add(SnapshotListingInterval)
	if _, ok := s.Lookup(steady, "get"); ok {
		t.Fatal("drift served stale entry")
	}
	listingErr = errors.New("listing failed")
	now = now.Add(SnapshotListingInterval)
	s.Lookup(steady, "get")
	done := other.BeginMutation(WithRequestCaller(ctx, CallerOperation))
	done()
	done() // Completion is idempotent and must not count another mutation.
	after := read()
	got := []uint64{0, 0, 0, after.Unchanged - before.Unchanged, after.Drift - before.Drift, after.Errors - before.Errors}
	for i, requests := range after.Requests {
		old := before.Requests[i]
		got[0] += requests.Reads - old.Reads
		got[1] += requests.Hits - old.Hits
		got[2] += requests.Mutations - old.Mutations
		switch requests.Caller {
		case CallerListing:
			if requests.Reads-old.Reads != 3 {
				t.Fatal("listing attribution", requests, old)
			}
		case CallerAvailability:
			if requests.Reads-old.Reads != 1 || requests.Hits-old.Hits != 1 {
				t.Fatal("availability attribution", requests, old)
			}
		case CallerOperation:
			if requests.Mutations-old.Mutations != 1 {
				t.Fatal("operation attribution", requests, old)
			}
		case CallerOther:
			if requests.Reads-old.Reads != 1 {
				t.Fatal("unknown caller attribution", requests, old)
			}
		default:
			if requests != old {
				t.Fatal("unused caller changed", requests, old)
			}
		}
	}
	want := []uint64{5, 1, 1, 1, 1, 1}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("counter deltas %v, want %v", got, want)
		}
	}
	t.Logf("read/network=%d read/snapshot=%d mutation/network=%d listing unchanged=%d drift=%d error=%d", got[0], got[1], got[2], got[3], got[4], got[5])
}

func TestSnapshotContextAndEvidence(t *testing.T) {
	now := time.Now()
	s := &TargetSnapshot{}
	s.SetListing(func(ctx context.Context) (string, error) {
		if SteadyObservation(ctx) {
			t.Error("listing inherited steady marker")
		}
		return "same", nil
	})
	snapshotBaseline(t, s)
	s.Populate("get", s.ReadStart(t.Context()), now, []byte("body"))
	steady := snapshotContext(t)
	for _, ctx := range []context.Context{t.Context(), WithSteadyObservation(t.Context()), ObservationContext(t.Context()), WithFreshTaskInventory(steady), WithoutObservation(steady), WithoutSteadyObservation(steady)} {
		if _, ok := s.Lookup(ctx, "get"); ok {
			t.Fatal("served outside steady observation")
		}
	}
	ctx, ev := WithEvidence(steady)
	body, ok := s.Lookup(ctx, "get")
	if !ok || string(body) != "body" || !ev.SnapshotValid() {
		t.Fatal("missing valid snapshot")
	}
	body[0] = 'x'
	body, _ = s.Lookup(ctx, "get")
	if string(body) != "body" {
		t.Fatal("caller mutated stored bytes")
	}
	NoteAgentSample(ctx, "m0", now)
	NoteAgentSample(ctx, "m0", now.Add(-time.Second))
	NoteAgentSample(ctx, "m1", now.Add(-2*time.Second))
	if at, ok := ev.Oldest(); !ok || !at.Equal(now.Add(-2*time.Second)) {
		t.Fatal(at, ok)
	}
	if at, ok := ev.MemberOldest("m0"); !ok || !at.Equal(now.Add(-time.Second)) {
		t.Fatal(at, ok)
	}
	done := s.BeginMutation(t.Context())
	if ev.SnapshotValid() {
		t.Fatal("in-flight mutation preserved evidence")
	}
	if _, ok := s.Lookup(steady, "get"); ok {
		t.Fatal("served during mutation")
	}
	done()
	done()
	if ev.SnapshotValid() {
		t.Fatal("completed mutation preserved evidence")
	}
}

func TestSnapshotMutationReadOverlap(t *testing.T) {
	for _, mode := range []string{"begin-after-read", "read-during-mutation", "invalidate"} {
		t.Run(mode, func(t *testing.T) {
			s := &TargetSnapshot{}
			s.SetListing(func(context.Context) (string, error) { return "same", nil })
			var done func()
			if mode == "read-during-mutation" {
				done = s.BeginMutation(t.Context())
			}
			token := s.ReadStart(t.Context())
			switch mode {
			case "begin-after-read":
				done = s.BeginMutation(t.Context())
			case "invalidate":
				s.Invalidate()
			}
			if done != nil {
				done()
			}
			s.Populate("get", token, time.Now(), []byte("stale"))
			if _, ok := s.Lookup(snapshotContext(t), "get"); ok {
				t.Fatal("overlapping read populated")
			}
		})
	}
}

func TestSnapshotListingDriftFailureAndAge(t *testing.T) {
	now := time.Now()
	restore := SetSnapshotClockForTest(func() time.Time { return now })
	defer restore()
	s := &TargetSnapshot{}
	fingerprint := "first"
	fail := false
	calls := 0
	s.SetListing(func(context.Context) (string, error) {
		calls++
		if fail {
			return "", errors.New("listing unavailable")
		}
		return fingerprint, nil
	})
	snapshotBaseline(t, s)
	ctx := snapshotContext(t)
	s.Populate("get", s.ReadStart(t.Context()), now, []byte("body"))
	if _, ok := s.Lookup(ctx, "get"); !ok {
		t.Fatal("initial miss")
	}
	now = now.Add(SnapshotListingInterval)
	fail = true
	if _, ok := s.Lookup(ctx, "get"); ok {
		t.Fatal("served after failed listing")
	}
	for range 10 {
		s.Lookup(ctx, "get")
	}
	if calls != 2 {
		t.Fatal("failure retry hammered listing", calls)
	}
	fail = false
	now = now.Add(5 * time.Second)
	if _, ok := s.Lookup(ctx, "get"); !ok {
		t.Fatal("did not recover")
	}
	ctx, evidence := WithEvidence(ctx)
	s.Lookup(ctx, "get")
	now = now.Add(SnapshotListingInterval)
	fingerprint = "second"
	if _, ok := s.Lookup(ctx, "get"); ok || s.DriftGeneration() != 1 || evidence.SnapshotValid() {
		t.Fatal("drift retained old facts")
	}
	s.Populate("get", s.ReadStart(t.Context()), now, []byte("new"))
	if _, ok := s.Lookup(ctx, "get"); !ok {
		t.Fatal("post-drift populate failed")
	}
	now = now.Add(SnapshotMaxAge)
	if _, ok := s.Lookup(ctx, "get"); !ok {
		t.Fatal("entry expired at inclusive boundary")
	}
	now = now.Add(time.Nanosecond)
	if _, ok := s.Lookup(ctx, "get"); ok {
		t.Fatal("entry survived max age")
	}
}

func TestSnapshotConcurrentSingleFlight(t *testing.T) {
	s := &TargetSnapshot{}
	now := time.Now()
	defer SetSnapshotClockForTest(func() time.Time { return now })()
	s.SetListing(func(context.Context) (string, error) { return "same", nil })
	snapshotBaseline(t, s)
	now = now.Add(SnapshotListingInterval)
	var calls atomic.Int32
	entered, release := make(chan struct{}), make(chan struct{})
	s.SetListing(func(context.Context) (string, error) {
		if calls.Add(1) == 1 {
			close(entered)
		}
		<-release
		return "same", nil
	})
	s.Populate("get", s.ReadStart(t.Context()), time.Now(), []byte("body"))
	ctx, ev := WithEvidence(snapshotContext(t))
	var workers sync.WaitGroup
	for range 32 {
		workers.Go(func() {
			if _, ok := s.Lookup(ctx, "get"); !ok {
				t.Error("concurrent lookup missed")
			}
			NoteAgentSample(ctx, "m0", time.Now())
			ev.SnapshotValid()
		})
	}
	<-entered
	close(release)
	workers.Wait()
	if calls.Load() != 1 {
		t.Fatal("listing not single-flight", calls.Load())
	}
	for range 16 {
		workers.Go(func() {
			for range 50 {
				token := s.ReadStart(t.Context())
				done := s.BeginMutation(t.Context())
				s.Populate("get", token, time.Now(), []byte("discard"))
				done()
				s.Lookup(ctx, "get")
				ev.SnapshotValid()
			}
		})
	}
	workers.Wait()
	if ev.SnapshotValid() {
		t.Fatal("concurrent mutations preserved evidence")
	}
}

func TestSnapshotBoundedEntriesAndBodies(t *testing.T) {
	s := &TargetSnapshot{}
	now := time.Now()
	for i := range snapshotMaxEntries + 5 {
		s.Populate(fmt.Sprint(i), s.ReadStart(t.Context()), now, []byte("body"))
	}
	if len(s.entries) != snapshotMaxEntries {
		t.Fatal(len(s.entries))
	}
	s.Populate("retained", s.ReadStart(t.Context()), now, []byte("body"))
	s.Populate("retained", s.ReadStart(t.Context()), now, make([]byte, SnapshotMaxBody+1))
	if string(s.entries["retained"].body) != "body" {
		t.Fatal("oversized body replaced entry")
	}
}

func TestSnapshotMissRetainsFirstEpoch(t *testing.T) {
	s := &TargetSnapshot{}
	s.SetListing(func(context.Context) (string, error) { return "same", nil })
	ctx, ev := WithEvidence(snapshotContext(t))
	if _, ok := s.Lookup(ctx, "missing"); ok {
		t.Fatal("unexpected hit")
	}
	first := s.ReadStart(t.Context())
	done := s.BeginMutation(t.Context())
	done()
	s.Lookup(ctx, "missing")
	if ev.SnapshotValid() || ev.targets[s].epoch != first {
		t.Fatal("miss forgot first epoch")
	}
}

func TestSnapshotWaiterCancellation(t *testing.T) {
	s := &TargetSnapshot{}
	entered, release := make(chan struct{}), make(chan struct{})
	s.SetListing(func(context.Context) (string, error) {
		close(entered)
		<-release
		return "same", nil
	})
	finished := make(chan struct{})
	go func() { s.Lookup(snapshotContext(t), "get"); close(finished) }()
	<-entered
	ctx, cancel := context.WithCancel(snapshotContext(t))
	cancel()
	if _, ok := s.Lookup(ctx, "get"); ok {
		t.Fatal("cancelled caller served")
	}
	close(release)
	<-finished
}

func TestSnapshotFirstListingDropsUncertifiedFacts(t *testing.T) {
	s := &TargetSnapshot{}
	fingerprint := "old"
	s.SetListing(func(context.Context) (string, error) { return fingerprint, nil })
	token := s.ReadStart(t.Context())
	s.Populate("old", token, SnapshotNow(), []byte("old facts"))
	fingerprint = "new"
	if _, ok := s.Lookup(snapshotContext(t), "old"); ok {
		t.Fatal("first listing certified facts fetched before its baseline")
	}
	s.Populate("overlapping", token, SnapshotNow(), []byte("old facts"))
	if len(s.entries) != 0 || s.entryBytes != 0 || s.ReadStart(t.Context()) == token {
		t.Fatal("first baseline retained pre-baseline entries or overlapping reads")
	}
	if s.DriftGeneration() != 0 {
		t.Fatal("first baseline reported drift")
	}
	s.Populate("current", s.ReadStart(t.Context()), SnapshotNow(), []byte("current facts"))
	if _, ok := s.Lookup(snapshotContext(t), "current"); !ok {
		t.Fatal("post-baseline facts did not serve")
	}
}

func TestSnapshotMarkerCannotReviveAfterStripping(t *testing.T) {
	steady := snapshotContext(t)
	for _, ctx := range []context.Context{
		ObservationContext(WithoutObservation(steady)),
		ObservationContext(WithoutObservation(WithSteadyObservation(t.Context()))),
		WithFreshTaskInventory(steady),
	} {
		if marked, _ := ctx.Value(steadyObservationKey{}).(bool); marked || SteadyObservation(ctx) {
			t.Fatal("safety context retained steady marker")
		}
	}
}

func TestSnapshotAggregateBudgetEvictsOldest(t *testing.T) {
	s := &TargetSnapshot{}
	now := SnapshotNow()
	body := make([]byte, SnapshotMaxBody)
	count := SnapshotMaxBytes / SnapshotMaxBody
	for i := range count {
		s.Populate(fmt.Sprint(i), s.ReadStart(t.Context()), now.Add(time.Duration(i-count)*time.Second), body)
	}
	s.Populate("newest", s.ReadStart(t.Context()), now, body)
	if s.entryBytes != SnapshotMaxBytes || len(s.entries) != count {
		t.Fatal("aggregate byte budget not enforced", s.entryBytes, len(s.entries))
	}
	if _, ok := s.entries["0"]; ok {
		t.Fatal("oldest response retained at expense of newer response")
	}
	s.Populate("newest", s.ReadStart(t.Context()), now, []byte("small"))
	if s.entryBytes != SnapshotMaxBytes-SnapshotMaxBody+len("small") {
		t.Fatal("replacement did not release previous bytes", s.entryBytes)
	}
	s.Invalidate()
	if s.entryBytes != 0 || len(s.entries) != 0 {
		t.Fatal("invalidation retained byte accounting")
	}
}

func TestSnapshotExpiredBodiesReclaimedWithoutPopulate(t *testing.T) {
	now := time.Now()
	defer SetSnapshotClockForTest(func() time.Time { return now })()
	for _, operation := range []string{"lookup", "read-start", "rejected-populate"} {
		t.Run(operation, func(t *testing.T) {
			s := &TargetSnapshot{}
			s.Populate("expired", s.ReadStart(t.Context()), now, []byte("body"))
			now = now.Add(SnapshotMaxAge + time.Nanosecond)
			switch operation {
			case "lookup":
				s.Lookup(t.Context(), "unrelated")
			case "read-start":
				s.ReadStart(t.Context())
			case "rejected-populate":
				s.Populate("rejected", s.epoch+1, now, nil)
			}
			if len(s.entries) != 0 || s.entryBytes != 0 {
				t.Fatal("expired bodies require a successful Populate to be reclaimed", s.entryBytes)
			}
		})
	}
}

type snapshotListingClient struct{ snapshot *TargetSnapshot }

func warmUnreferencedSnapshot(t *testing.T, key string) (weak.Pointer[TargetSnapshot], weak.Pointer[snapshotListingClient]) {
	t.Helper()
	s := PlatformSnapshot(key)
	client := &snapshotListingClient{snapshot: s}
	s.SetListing(func(context.Context) (string, error) { return fmt.Sprint(client.snapshot.DriftGeneration()), nil })
	snapshotBaseline(t, s)
	s.Populate("body", s.ReadStart(t.Context()), SnapshotNow(), make([]byte, 1024))
	return weak.Make(s), weak.Make(client)
}

func TestSnapshotRegistryReclaimsTargetChurn(t *testing.T) {
	const count = 32
	targets := make([]weak.Pointer[TargetSnapshot], 0, count)
	clients := make([]weak.Pointer[snapshotListingClient], 0, count)
	keys := make([]string, 0, count)
	for i := range count {
		key := fmt.Sprintf("%s|%d", t.Name(), i)
		target, client := warmUnreferencedSnapshot(t, key)
		keys = append(keys, key)
		targets = append(targets, target)
		clients = append(clients, client)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		runtime.GC()
		reclaimed := true
		for i := range count {
			if targets[i].Value() != nil || clients[i].Value() != nil {
				reclaimed = false
			}
		}
		platformSnapshots.Lock()
		for _, key := range keys {
			if _, ok := platformSnapshots.values[key]; ok {
				reclaimed = false
			}
		}
		platformSnapshots.Unlock()
		if reclaimed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("registry retained removed targets, listing clients, or registration keys")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestSnapshotRegistrySharesLiveClients(t *testing.T) {
	const count = 32
	targets := make([]*TargetSnapshot, count)
	var workers sync.WaitGroup
	for i := range count {
		workers.Go(func() { targets[i] = PlatformSnapshot(t.Name()) })
	}
	workers.Wait()
	for _, target := range targets {
		if target != targets[0] {
			t.Fatal("simultaneous clients do not share mutation epochs")
		}
	}
	runtime.GC()
	if PlatformSnapshot(t.Name()) != targets[0] {
		t.Fatal("live client lost its shared snapshot")
	}
	runtime.KeepAlive(targets)
}
