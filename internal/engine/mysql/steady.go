// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

// steadyResult is reached only after fresh native readiness and exposure checks.
// Receipts and maintenance journals must keep the ordinary convergence cadence.
// Health, agent replacement, desired configuration and platform drift wake Sync
// independently; certificate renewal's 30-day horizon tolerates deep resync.
func steadyResult(rc *engine.Context, s *state) engine.Result {
	pending := engine.Result{RequeueAfter: 2 * time.Second}
	if s.Pending != nil || s.Rolling != nil || s.Replacement != nil || s.TLSRotation != nil || s.GaleraTLS != nil || s.Formation.reforming() {
		return pending
	}
	if c := s.Formation.GTIDCertification; c != nil && !c.Legacy {
		proof := c.Checkpoint.GTIDCertification
		if proof == nil || !proof.Certified || proof.FencesOwned || proof.CleanupPending {
			return pending
		}
	}
	for _, m := range s.Members {
		if m.DeployOp != nil || m.ImportBatch != nil || m.Removing || m.RetirementOp != nil || m.ProxyDraining || m.ProxyDrainOperation != "" || m.PendingReplicas != 0 || m.ConfigVersion != s.ConfigVersion {
			return pending
		}
	}
	for _, c := range rc.Cluster.Status.Conditions {
		if c.Type == "MaintenanceWindow" && c.Status == "False" {
			return rc.WaitForWindow()
		}
	}
	result := engine.Result{Steady: true}
	policy := rc.Cluster.Spec.Backup
	if !policy.PITR.Enabled || policy.PITR.AutoReanchor != nil && !*policy.PITR.AutoReanchor {
		return result
	}
	r := &s.PITRReanchor
	var due time.Time
	switch r.LastReason {
	case "PITRReanchorSkippedCooldown", "PITRReanchorSkippedWriterNotStable":
		due = r.FirstSeen.Add(domain.PITRReanchorStabilityPeriod)
		if cooldown := r.LastTriggered.Add(time.Hour); !r.LastTriggered.IsZero() && cooldown.After(due) {
			due = cooldown
		}
	case "PITRReanchorSkippedBaselineUnavailable":
		due = r.VerifyAfter
	case "", "PITRReanchorSkippedNoStorage":
		return result
	default:
		// Backup/operation admission, writer identity and verification are unfinished.
		return pending
	}
	if due.IsZero() || !due.After(now(rc)) {
		return pending
	}
	result.RequeueAfter = due.Sub(now(rc))
	return result
}
