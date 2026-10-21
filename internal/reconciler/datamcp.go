package reconciler

import (
	"context"
	"errors"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

func (m *Manager) observeStatus(ctx context.Context, p engine.Provider, rc *engine.Context) (domain.ClusterStatus, error) {
	status, err := engine.ObserveStatus(ctx, p, rc)
	companionErr := m.opts.Companion.Observe(ctx, rc, &status)
	return status, errors.Join(err, companionErr)
}
