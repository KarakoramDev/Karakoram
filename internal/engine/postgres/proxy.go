// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"context"
	"fmt"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
)

// proxyApplied waits for every live instance, not whichever service DNS selects.
// A reload response is only a request; a later status observation is the ack.
// This is the configuration/native-health barrier, not rollout completion:
// Sync must still be able to correct a failed rollout's desired configuration.
func proxyApplied(ctx context.Context, rc *engine.Context, m *member, version int64, requireRunning bool) (bool, error) {
	all, count, err := engine.VisitLiveInstances(ctx, rc, m.Name, func(a *agentapi.Client) (bool, error) {
		probeContext, cancel := context.WithTimeout(ctx, engine.ObservationTimeout)
		defer cancel()
		status, err := a.Status(probeContext)
		if err != nil {
			return false, err
		}
		diagnostic := domain.MemberStatus{Name: m.Name, Health: true}
		if status.Error != "" || len(engine.ApplyProcessDiagnostics(&diagnostic, status.Processes, "", now(rc))) > 0 {
			rc.SyncWait = "waiting for proxy " + m.Name + " healthy supervised processes"
			return false, nil
		}
		if status.ConfigVersion < version {
			rc.SyncWait = fmt.Sprintf("waiting for proxy %s configuration %d (observed %d)", m.Name, version, status.ConfigVersion)
			return false, a.Reload(ctx)
		}
		if requireRunning {
			var status pgspec.StatusResponse
			if err := a.Engine(probeContext, "postgres", "status", struct{}{}, &status); err != nil {
				return false, err
			}
			if !status.Running {
				rc.SyncWait = "waiting for proxy " + m.Name + " native process to run"
			}
			return status.Running, nil
		}
		return true, nil
	})
	if requireRunning && count == 0 {
		rc.SyncWait = "waiting for proxy " + m.Name + " live instance inventory"
	}
	return all && (!requireRunning || count > 0), err
}
