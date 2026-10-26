// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package psmdb

import (
	"context"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

type balancerControl interface {
	Balancer(context.Context, *engine.Context) (domain.BalancerStatus, error)
	SetBalancer(context.Context, *engine.Context, bool) error
}

// stopUpgradeBalancer separates observation from mutation by a durable return.
// Replaying a successful disable must never reinterpret it as operator intent.
func stopUpgradeBalancer(ctx context.Context, rc *engine.Context, s *internalState, control balancerControl) error {
	if !s.BalancerObserved {
		status, err := control.Balancer(ctx, rc)
		if err != nil {
			return err
		}
		s.BalancerWasEnabled = status.Enabled
		s.BalancerObserved = true
		return nil
	}
	if err := control.SetBalancer(ctx, rc, false); err != nil {
		return err
	}
	s.BalancerStopped = true
	return nil
}
