// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func TestObservationPassCachedMetadataKeepsOriginalBoundary(t *testing.T) {
	at := time.Date(2026, time.October, 30, 12, 0, 0, 0, time.UTC)
	rc := &Context{Clock: observationClock{at}, Cluster: &domain.Cluster{}, ObservationEpoch: "process"}
	calls := 0
	p := observationProvider{observe: func(ctx context.Context, rc *Context) (domain.ClusterStatus, error) {
		ObservationPassStart(ctx, rc.Clock.Now())
		calls++
		status := readyObservation(at)
		status.Phase = domain.PhaseDegraded
		rc.Clock = observationClock{at.Add(time.Second)}
		return status, nil
	}}
	ctx := WithObservationPass(t.Context())
	status, err := ObserveStatus(ctx, p, rc)
	if err != nil || calls != 2 || !status.Members[0].Health || !status.Members[0].Agent.LastSeen.Equal(at) || !status.ObservedAt.Equal(at) {
		t.Fatalf("same-pass retry relabelled or rejected raw metadata: %+v calls=%d error=%v", status, calls, err)
	}
	// The same old payload is invalid in a new pass, despite matching epoch.
	status, _ = ObserveStatus(WithObservationPass(ctx), p, rc)
	if !status.ObservedAt.IsZero() || status.Members[0].Health {
		t.Fatal("old metadata crossed the pass boundary")
	}
}

func TestObservationPassTimestampOptInAndIsolation(t *testing.T) {
	at := time.Date(2026, time.October, 30, 12, 0, 0, 0, time.UTC)
	ctx := WithObservationPass(t.Context())
	if _, ok := ObservationPassStartedAt(ctx); ok {
		t.Fatal("creating a cache silently opted unrelated providers into timestamp reuse")
	}
	if got := ObservationPassStart(ctx, at); !got.Equal(at) {
		t.Fatal("clock domain was replaced with wall time")
	}
	if got := ObservationPassStart(ctx, at.Add(time.Hour)); !got.Equal(at) {
		t.Fatal("cache hit advanced the observation boundary")
	}
	next := WithObservationPass(ctx)
	if _, ok := ObservationPassStartedAt(next); ok {
		t.Fatal("new pass inherited its parent's timestamp")
	}
	if ObservationPassCache(ctx) == ObservationPassCache(next) {
		t.Fatal("new pass inherited its parent's payload map")
	}
}

func TestStatusInfrastructureSnapshotScope(t *testing.T) {
	at := time.Now()
	rc := &Context{Clock: observationClock{at}, Cluster: &domain.Cluster{}, ObservationEpoch: "process"}
	outer := WithObservationPass(t.Context())
	calls := 0
	var first any
	p := observationProvider{observe: func(ctx context.Context, _ *Context) (domain.ClusterStatus, error) {
		cache := infra.ObservationCache(ctx)
		if cache == nil {
			t.Fatal("status has no infrastructure snapshot")
		}
		if calls == 0 {
			first = cache
			cache.Store("proof", true)
		} else if cache == first {
			t.Fatal("a retry or later status sweep reused the earlier snapshot")
		} else if _, ok := cache.Load("proof"); ok {
			t.Fatal("an earlier attempt's ownership proof leaked into a new attempt")
		}
		calls++
		status := readyObservation(at)
		if calls == 1 {
			status.Phase = domain.PhaseDegraded
		}
		return status, nil
	}}
	if _, err := ObserveStatus(outer, p, rc); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatalf("expected same-sweep retry, got %d calls", calls)
	}
	if infra.ObservationCache(outer) != nil {
		t.Fatal("read-only snapshot leaked into the mutation context")
	}
	if _, err := ObserveStatus(outer, p, rc); err != nil {
		t.Fatal(err)
	}
}

func TestObservationBudgetDeferralPreservesFactsWithoutRenewal(t *testing.T) {
	at := time.Date(2026, time.October, 30, 12, 0, 0, 0, time.UTC)
	previous := readyObservation(at)
	previous.ObservationEpoch = "original-process"
	rc := &Context{Clock: observationClock{at.Add(4 * time.Second)}, Cluster: &domain.Cluster{Status: previous}, ObservationEpoch: "new-process"}
	calls := 0
	p := observationProvider{observe: func(context.Context, *Context) (domain.ClusterStatus, error) {
		calls++
		return DeferredObservation(previous, rc.Clock.Now()), &ObservationBudgetError{}
	}}
	status, err := ObserveStatus(t.Context(), p, rc)
	if !errors.Is(err, ErrObservationBudgetExhausted) || calls != 1 {
		t.Fatalf("deferral became an ordinary failed/retried observation: %v calls=%d", err, calls)
	}
	if !status.Members[0].Health || !status.Members[0].Agent.Healthy ||
		!status.Members[0].Agent.LastSeen.Equal(at) || !status.ObservedAt.Equal(at) || status.ObservationEpoch != previous.ObservationEpoch {
		t.Fatal("budget deferral withdrew or renewed historical member facts")
	}
	if status.Phase != domain.PhaseDegraded {
		t.Fatal("deferred history was advertised as current Ready")
	}
	expired := FreshStatus(status, at.Add(ReadinessMaxAge+time.Second))
	if expired.Members[0].Health || expired.Members[0].Agent.Healthy {
		t.Fatal("budget deferral extended the original availability lease")
	}
}

func TestObservationBudgetRebasePreservesNewerPublicationAndFreshFailures(t *testing.T) {
	at := time.Date(2026, time.October, 30, 12, 0, 0, 0, time.UTC)
	partial := readyObservation(at)
	latest := readyObservation(at.Add(time.Second))
	latest.ObservationEpoch = "current"
	latest.Members[0].Health, latest.Members[0].Agent.Healthy = false, false
	latest.Members = append(latest.Members, domain.MemberStatus{Name: "m1", Health: true, Agent: domain.AgentStatus{Healthy: true, LastSeen: latest.ObservedAt}})
	for _, agentFailure := range []bool{false, true} {
		status := RebaseDeferredObservation(latest, partial, []ObservationFailure{{Member: "m1", Agent: agentFailure}}, at.Add(4*time.Second))
		if status.Members[0].Health || status.Members[0].Agent.Healthy {
			t.Fatal("stale worker snapshot overwrote a newer failed publication")
		}
		if status.Members[1].Health || status.Members[1].Agent.Healthy == agentFailure {
			t.Fatal("fresh native/agent failure was lost during budget deferral")
		}
		if !status.ObservedAt.Equal(latest.ObservedAt) || status.ObservationEpoch != latest.ObservationEpoch {
			t.Fatal("rebasing renewed or retagged the persisted sample")
		}
	}
	if !latest.Members[1].Health || !latest.Members[1].Agent.Healthy {
		t.Fatal("deferred projection mutated the stored publication")
	}
}
