// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package engine

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

// SteadyResyncInterval is the backstop deep revalidation of a steady cluster.
// Health renews separately from heartbeats; platform drift visible in the
// target listing, agent session changes and non-Ready results reconcile at once.
const SteadyResyncInterval = 30 * time.Minute

type steadyEligibleKey struct{}
type steadyOutcomeKey struct{}

var steadyCounters struct{ accepted, fallback atomic.Uint64 }

// SteadyCounters returns process-lifetime results of eligible steady attempts.
func SteadyCounters() (accepted, fallback uint64) {
	return steadyCounters.accepted.Load(), steadyCounters.fallback.Load()
}

func WithSteadyEligible(ctx context.Context) context.Context {
	return context.WithValue(ctx, steadyEligibleKey{}, true)
}

func WithoutSteadyEligible(ctx context.Context) context.Context {
	return context.WithValue(ctx, steadyEligibleKey{}, false)
}
func SteadyEligible(ctx context.Context) bool {
	eligible, _ := ctx.Value(steadyEligibleKey{}).(bool)
	return eligible
}

type SteadyOutcome struct {
	Attempted, Accepted bool
	DriftGeneration     uint64
	evidence            *infra.Evidence
}

// SnapshotValid rechecks provider proofs after the caller observes companions.
func (o *SteadyOutcome) SnapshotValid() bool {
	return o != nil && o.evidence != nil && o.evidence.SnapshotValid()
}

func WithSteadyOutcome(ctx context.Context) (context.Context, *SteadyOutcome) {
	outcome := &SteadyOutcome{}
	return context.WithValue(ctx, steadyOutcomeKey{}, outcome), outcome
}

func observeSteady(ctx context.Context, p Provider, rc *Context, clock Clock) (domain.ClusterStatus, bool) {
	outcome, _ := ctx.Value(steadyOutcomeKey{}).(*SteadyOutcome)
	if outcome != nil {
		*outcome = SteadyOutcome{Attempted: true}
	}
	started := clock.Now()
	// A rejected push result must not seed provider memos used by live retries.
	probe, cancel := context.WithTimeout(WithObservationPass(ctx), observationSweepTimeout(clock))
	defer cancel()
	probe = infra.WithObservationCache(infra.WithSteadyObservation(infra.ObservationContext(probe)), new(sync.Map))
	probe, evidence := infra.WithEvidence(probe)
	if outcome != nil {
		outcome.evidence = evidence
	}
	status, err := p.Status(probe, rc)
	status.Members = slices.Clone(status.Members)
	status.Conditions = slices.Clone(status.Conditions)
	if !errors.Is(err, ErrObservationBudgetExhausted) {
		err = errors.Join(err, ApplyInitialDataSourceStatus(p, rc.Cluster, &status))
		err = errors.Join(err, observePlacementFailures(probe, rc, &status, true))
		err = errors.Join(err, ObserveRuntimeStops(probe, rc, &status, true))
	}
	err = errors.Join(err, probe.Err())
	if passStarted, ok := ObservationPassStartedAt(probe); ok && passStarted.Before(started) {
		started = passStarted
	}
	if oldest, ok := evidence.Oldest(); ok && oldest.Before(started) {
		started = oldest
	}
	accepted := err == nil && status.Phase == domain.PhaseReady && status.ObservationEpoch == ""
	for _, condition := range status.Conditions {
		if readinessCondition(condition) && (condition.Status == "Unknown" || condition.Reason == "Unreachable" || condition.Reason == "NoWritablePrimary") {
			accepted = false
		}
	}
	for i := range status.Members {
		member := &status.Members[i]
		if !member.Agent.Healthy || member.Agent.LastSeen.IsZero() || member.Agent.LastSeen.Before(started) {
			accepted = false
		}
		if oldest, ok := evidence.MemberOldest(member.Name); ok && oldest.Before(member.Agent.LastSeen) {
			member.Agent.LastSeen = oldest
		}
	}
	accepted = accepted && evidence.SnapshotValid()
	if outcome != nil {
		outcome.Accepted, outcome.DriftGeneration = accepted, evidence.DriftGeneration()
	}
	if accepted {
		steadyCounters.accepted.Add(1)
		status.ObservedAt, status.ObservationEpoch = started, rc.ObservationEpoch
		ObserveAgentVersions(&status, CurrentAgentBuild(), clock.Now())
	} else {
		steadyCounters.fallback.Add(1)
	}
	return status, accepted
}
