package reconciler

import (
	"context"
	"errors"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"time"
)

func (m *Manager) observeStatus(ctx context.Context, p engine.Provider, rc *engine.Context) (domain.ClusterStatus, error) {
	started := time.Now()
	status, err := engine.ObserveStatus(ctx, p, rc)
	m.logStage(rc.Cluster.Ref(), "availability.observe", started)
	started = time.Now()
	companionErr := m.opts.Companion.Observe(ctx, rc, &status)
	m.logStage(rc.Cluster.Ref(), "companion.observe", started)
	return status, errors.Join(err, companionErr)
}
