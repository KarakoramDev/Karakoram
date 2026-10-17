// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"context"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
)

// proxyApplied waits for every live instance, not whichever service DNS selects.
// A reload response is only a request; a later status observation is the ack.
func proxyApplied(ctx context.Context, rc *engine.Context, m *member, version int64, requireRunning bool) (bool, error) {
	all, count, err := engine.VisitLiveInstances(ctx, rc, m.Name, func(a *agentapi.Client) (bool, error) {
		status, err := a.Status(ctx)
		if err != nil {
			return false, err
		}
		if status.ConfigVersion < version {
			return false, a.Reload(ctx)
		}
		if requireRunning {
			var status pgspec.StatusResponse
			if err := a.Engine(ctx, "postgres", "status", struct{}{}, &status); err != nil {
				return false, err
			}
			return status.Running, nil
		}
		return true, nil
	})
	return all && (!requireRunning || count > 0), err
}
