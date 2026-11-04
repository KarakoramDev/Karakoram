// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

// finishCleanupAccess resolves only a previously sealed request before removing
// runtimes. It never admits new requests through the public deleting fence.
// Applied receipts publish without repeating SQL; ambiguous requests replay the
// exact durable request ID so the native journal decides applied versus rejected.
func (p *Core) finishCleanupAccess(ctx context.Context, rc *engine.Context) error {
	intent, err := loadAccessIntent(ctx, rc)
	if err != nil || intent == nil {
		return err
	}
	recovery := *rc
	cluster := *rc.Cluster
	cluster.Deleting = false
	cluster.OperationID = ""
	if cluster.Status.Phase == domain.PhaseDeleting {
		cluster.Status.Phase = domain.PhaseReady
	}
	recovery.Cluster = &cluster
	// All durable formation, restore, offline and suspension fences still apply.
	err = p.completeAccessIntent(ctx, &recovery, intent)
	rc.Cluster.Internal = cluster.Internal
	if err != nil {
		pending, loadErr := loadAccessIntent(ctx, rc)
		if loadErr != nil {
			return loadErr
		}
		if pending != nil {
			return err
		}
		// A definitive rejection restored the previous sealed indexes and cleared
		// the journal. It is an API failure, but no longer blocks cleanup.
	}
	return nil
}
