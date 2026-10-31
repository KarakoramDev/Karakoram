package reconciler

import (
	"context"
	"errors"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"time"
)

func (m *Manager) observeStatus(ctx context.Context, p engine.Provider, rc *engine.Context) (domain.ClusterStatus, error) {
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
	companionErr := m.opts.Companion.Observe(ctx, rc, &status)
	m.logStage(rc.Cluster.Ref(), "companion.observe", started)
	return status, errors.Join(err, companionErr)
}
