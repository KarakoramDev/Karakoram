package reconciler

import (
	"context"
	"errors"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func (m *Manager) observeStatus(ctx context.Context, p engine.Provider, rc *engine.Context) (domain.ClusterStatus, error) {
	return m.observeStatusWithOutcome(ctx, p, rc, nil)
}

func (m *Manager) observeStatusWithOutcome(ctx context.Context, p engine.Provider, rc *engine.Context, outcome *engine.SteadyOutcome) (domain.ClusterStatus, error) {
	if engine.SteadyEligible(ctx) && outcome == nil {
		ctx, outcome = engine.WithSteadyOutcome(ctx)
	}
	if engine.ObservationPassCache(ctx) == nil {
		ctx = engine.WithObservationPass(ctx)
	}
	// Publication owners release this gate only after their state write, not
	// when probes return: a delayed positive sample must not overwrite a
	// newer failed renewal. Sync runs outside this observation gate.
	if err := holdObservation(ctx); err != nil {
		return domain.ClusterStatus{}, err
	}
	started := time.Now()
	status, err := engine.ObserveStatus(ctx, p, rc)
	m.logStage(rc.Cluster.Ref(), "availability.observe", started)
	var deferred *engine.ObservationBudgetError
	if errors.As(err, &deferred) {
		// Sync runs outside the publication gate. A concurrent availability
		// renewal may be newer than rc.Cluster.Status; retain that publication,
		// not an invalidated worker proof, and never renew its original lease.
		latest, _, lookupErr := m.opts.Store.GetClusterObservation(ctx, rc.Cluster.Ref())
		if lookupErr != nil {
			return domain.ClusterStatus{}, errors.Join(deferred.Cause, lookupErr)
		}
		status = engine.RebaseDeferredObservation(latest.Status, status, deferred.Failures, rc.Clock.Now())
		return status, deferred.Cause
	}
	started = time.Now()
	providerAccepted := outcome != nil && outcome.Accepted
	companionErr := m.observeCompanion(ctx, rc, &status, outcome)
	m.logStage(rc.Cluster.Ref(), "companion.observe", started)
	if providerAccepted && !outcome.SnapshotValid() {
		// Companion work can overlap a target mutation after provider acceptance.
		// Discard both halves rather than publish the now-invalid provider proof.
		outcome.Accepted = false
		fresh := engine.WithObservationPass(engine.WithoutSteadyEligible(ctx))
		return m.observeStatusWithOutcome(fresh, p, rc, nil)
	}
	return status, errors.Join(err, companionErr)
}

func (m *Manager) observeCompanion(ctx context.Context, rc *engine.Context, status *domain.ClusterStatus, outcome *engine.SteadyOutcome) error {
	if engine.SteadyEligible(ctx) && outcome != nil && outcome.Accepted {
		probe := infra.WithSteadyObservation(infra.ObservationContext(engine.WithObservationPass(ctx)))
		probe, evidence := infra.WithEvidence(probe)
		candidate := *status
		err := m.opts.Companion.Observe(probe, rc, &candidate)
		outcome.DriftGeneration = max(outcome.DriftGeneration, evidence.DriftGeneration())
		positive := err == nil && (candidate.DataMcp == nil || candidate.DataMcp.Phase == domain.DataMcpPhaseReady)
		if positive && evidence.SnapshotValid() && outcome.SnapshotValid() {
			if oldest, ok := evidence.Oldest(); ok {
				if oldest.Before(candidate.ObservedAt) {
					candidate.ObservedAt = oldest
				}
				for i := range candidate.Members {
					if oldest.Before(candidate.Members[i].Agent.LastSeen) {
						candidate.Members[i].Agent.LastSeen = oldest
					}
				}
			}
			*status = candidate
			return nil
		}
		outcome.Accepted = false
		if !outcome.SnapshotValid() {
			return nil // The caller retries the entire provider/companion sample.
		}
	}
	// Fresh observations also learn subscriptions. A rejected attempt must not
	// seed the live retry's platform/pass caches or contribute sample timestamps.
	fresh := infra.WithoutSteadyObservation(infra.ObservationContext(engine.WithObservationPass(ctx)))
	return m.opts.Companion.Observe(fresh, rc, status)
}
