// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"context"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

// steadyResult follows fresh readiness, access publication, profile bootstrap
// and Citus metadata registration. These are receipt/spec-driven, not timers.
// Health and runtime identity changes wake Sync independently; certificate
// renewal has a long lead time and remains covered by periodic deep resync.
func steadyResult(ctx context.Context, rc *engine.Context, s *state) (engine.Result, error) {
	pending := engine.Result{RequeueAfter: 2 * time.Second}
	if s.Pending != nil || s.Rolling != nil || s.Replacement != nil || s.Upgrade != nil || s.RestoreRecovery != nil || s.CitusScale != nil || s.Rebalance != nil || s.CitusOperation != "" || s.ExtensionsPending {
		return pending, nil
	}
	for _, m := range s.Members {
		if m.ImportBatch != nil || m.DeployOp != nil || m.Removing || m.RetirementOp != nil || m.TLSPending || m.TLSReloadPending || m.ConfigRestartPending || m.UnplacedRetirement != nil || m.ConfigVersion != s.ConfigVersion {
			rc.SyncWait = "waiting for final member configuration acknowledgements"
			return pending, nil
		}
		for _, receipt := range m.ProxyClockRecovery {
			if receipt != nil && receipt.Message != "" {
				return pending, nil
			}
		}
	}
	for _, c := range rc.Cluster.Status.Conditions {
		if c.Type == "MaintenanceWindow" && c.Status == "False" {
			return rc.WaitForWindow(), nil
		}
	}
	result := engine.Result{Steady: true}
	if s.Topology == "distributed" && rc.Cluster.Spec.Backup.PITR.Enabled && rc.Cluster.Spec.Backup.Storage != "" && rc.Store != nil {
		items, err := rc.Store.ListBackups(ctx, rc.Cluster.Ref())
		if err != nil {
			return pending, err
		}
		for _, b := range items {
			if b.Phase == domain.PhasePending || b.Phase == domain.PhaseRunning {
				return pending, nil
			}
		}
		// scheduleCitusPoint uses deterministic UTC quarter-hour buckets. Preserve
		// that boundary rather than delaying it to the next ten-minute resync.
		at := now(rc)
		result.RequeueAfter = at.UTC().Truncate(15 * time.Minute).Add(15 * time.Minute).Sub(at)
	}
	return result, nil
}
