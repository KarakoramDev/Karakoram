// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package engine

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func TestSteadyCounters(t *testing.T) {
	a, f := SteadyCounters()
	at := time.Now()
	for _, mode := range []string{"fresh", "accepted", "fallback"} {
		ctx := t.Context()
		if mode != "fresh" {
			ctx = WithSteadyEligible(ctx)
		}
		p := observationProvider{observe: func(ctx context.Context, rc *Context) (domain.ClusterStatus, error) {
			status := readyObservation(at)
			if mode == "fallback" && infra.SteadyObservation(ctx) {
				return status, errors.New("steady failed")
			}
			return status, nil
		}}
		if _, err := ObserveStatus(ctx, p, &Context{Clock: observationClock{at}, Cluster: &domain.Cluster{}}); err != nil {
			t.Fatal(err)
		}
	}
	aa, ff := SteadyCounters()
	if aa-a != 1 || ff-f != 1 {
		t.Fatalf("accepted=%d fallback=%d, want 1 1", aa-a, ff-f)
	}
	t.Logf("accepted=%d fallback=%d; noneligible observation uncounted", aa-a, ff-f)
}

func TestSteadyAcceptedAnchorsEvidence(t *testing.T) {
	at := time.Now()
	ctx, outcome := WithSteadyOutcome(WithSteadyEligible(WithObservationPass(t.Context())))
	parentCache := ObservationPassCache(ctx)
	calls := 0
	p := observationProvider{observe: func(ctx context.Context, rc *Context) (domain.ClusterStatus, error) {
		calls++
		if !infra.SteadyObservation(ctx) || ObservationPassCache(ctx) == parentCache {
			t.Fatal("steady attempt did not isolate pass")
		}
		if deadline, ok := ctx.Deadline(); !ok || time.Until(deadline) > ObservationSweepTimeout {
			t.Fatal("unbounded steady sweep")
		}
		ObservationPassStart(ctx, at.Add(-time.Second))
		infra.NoteAgentSample(ctx, "m0", at.Add(-2*time.Second))
		infra.NoteAgentSample(ctx, "m0", at.Add(-3*time.Second))
		infra.NoteAgentSample(ctx, "m1", at.Add(-4*time.Second))
		status := readyObservation(at)
		status.Members = append(status.Members, domain.MemberStatus{Name: "m1", Health: true, Agent: domain.AgentStatus{Healthy: true, LastSeen: at}})
		return status, nil
	}}
	rc := &Context{Clock: observationClock{at}, Cluster: &domain.Cluster{}, ObservationEpoch: "epoch"}
	got, err := ObserveStatus(ctx, p, rc)
	if err != nil || calls != 1 || !outcome.Attempted || !outcome.Accepted || got.ObservationEpoch != "epoch" {
		t.Fatal(got, outcome, err, calls)
	}
	if !got.ObservedAt.Equal(at.Add(-4*time.Second)) || !got.Members[0].Agent.LastSeen.Equal(at.Add(-3*time.Second)) || !got.Members[1].Agent.LastSeen.Equal(at.Add(-4*time.Second)) {
		t.Fatal("evidence timestamps renewed", got)
	}
}

func TestSteadyRejectedRunsFreshFallback(t *testing.T) {
	for _, mode := range []string{"negative", "partial", "error", "budget", "epoch", "unknown", "tagged", "missing-time"} {
		t.Run(mode, func(t *testing.T) {
			at := time.Now()
			ctx, outcome := WithSteadyOutcome(WithSteadyEligible(WithObservationPass(t.Context())))
			parentCache := ObservationPassCache(ctx)
			var steadyCache any
			snapshot := infra.PlatformSnapshot(t.Name())
			snapshot.SetListing(func(context.Context) (string, error) { return "same", nil })
			snapshot.Lookup(infra.WithSteadyObservation(infra.ObservationContext(t.Context())), "baseline")
			snapshot.Populate("get", snapshot.ReadStart(t.Context()), at, []byte("body"))
			calls := 0
			p := observationProvider{observe: func(ctx context.Context, rc *Context) (domain.ClusterStatus, error) {
				calls++
				status := readyObservation(at)
				if calls == 1 {
					if !infra.SteadyObservation(ctx) {
						t.Fatal("missing steady marker")
					}
					steadyCache = ObservationPassCache(ctx)
					switch mode {
					case "negative":
						status.Phase = domain.PhaseDegraded
					case "partial":
						return domain.ClusterStatus{}, nil
					case "error":
						return status, errors.New("read failed")
					case "budget":
						return status, ErrObservationBudgetExhausted
					case "epoch":
						if _, ok := snapshot.Lookup(ctx, "get"); !ok {
							t.Fatal("snapshot miss")
						}
						done := snapshot.BeginMutation(t.Context())
						done()
					case "unknown":
						status.Conditions[0].Status = "Unknown"
					case "tagged":
						status.ObservationEpoch = "old"
					case "missing-time":
						status.Members[0].Agent.LastSeen = time.Time{}
					}
					return status, nil
				}
				if infra.SteadyObservation(ctx) || ObservationPassCache(ctx) == steadyCache || ObservationPassCache(ctx) != parentCache {
					t.Fatal("fallback reused steady context")
				}
				if calls == 2 {
					return status, errors.New("first fresh attempt failed")
				}
				return status, nil
			}}
			got, err := ObserveStatus(ctx, p, &Context{Clock: observationClock{at}, Cluster: &domain.Cluster{}})
			if err != nil || calls != 3 || !outcome.Attempted || outcome.Accepted || got.Phase != domain.PhaseReady || !got.ObservedAt.Equal(at) {
				t.Fatal(got, outcome, err, calls)
			}
		})
	}
}

func TestSteadyNonEligibleUnchanged(t *testing.T) {
	at := time.Now()
	var results []domain.ClusterStatus
	for _, withOutcome := range []bool{false, true} {
		ctx := t.Context()
		var outcome *SteadyOutcome
		if withOutcome {
			ctx, outcome = WithSteadyOutcome(ctx)
		}
		calls := 0
		p := observationProvider{observe: func(ctx context.Context, rc *Context) (domain.ClusterStatus, error) {
			calls++
			if infra.SteadyObservation(ctx) {
				t.Fatal("non-eligible observation became steady")
			}
			return readyObservation(at), nil
		}}
		got, err := ObserveStatus(ctx, p, &Context{Clock: observationClock{at}, Cluster: &domain.Cluster{}})
		if err != nil || calls != 1 || outcome != nil && *outcome != (SteadyOutcome{}) {
			t.Fatal(err, calls, outcome)
		}
		results = append(results, got)
	}
	if !reflect.DeepEqual(results[0], results[1]) {
		t.Fatal("non-eligible behavior changed")
	}
}
