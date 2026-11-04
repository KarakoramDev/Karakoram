// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package agent

import (
	"context"
	"errors"
	"time"
)

// ShutdownEngine optionally hands off leadership before the supervisor signals
// children. The context is independent of the canceled runtime context.
// Engines must not wait for control-plane publication.
// An unsuccessful handoff never skips the normal dependency-ordered shutdown.
type ShutdownEngine interface {
	Shutdown(context.Context, *Node) error
}

// ShutdownBudgetEngine optionally reserves more than the default four seconds
// for native session draining. The agent caps this at 45 seconds to leave time
// for dependency-ordered child shutdown within its 60-second total budget.
type ShutdownBudgetEngine interface {
	ShutdownBudget() time.Duration
}

func (a *nodeAgent) shutdown(ctx context.Context) error {
	a.o.Logger.Info("member runtime shutdown started")
	a.sup.closeLaunches()
	var handoffErr error
	if engine, ok := a.engine.(ShutdownEngine); ok {
		started := time.Now()
		budget := 4 * time.Second
		if draining, ok := a.engine.(ShutdownBudgetEngine); ok {
			if requested := draining.ShutdownBudget(); requested > 0 {
				budget = min(requested, 45*time.Second)
			}
		}
		handoff, cancel := context.WithTimeout(ctx, budget)
		handoffErr = engine.Shutdown(handoff, &Node{a: a})
		cancel()
		a.o.Logger.Info("engine graceful handoff finished", "duration", time.Since(started), "error", handoffErr)
	}
	return errors.Join(handoffErr, a.sup.shutdown(ctx))
}
