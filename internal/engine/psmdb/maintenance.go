// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package psmdb

import (
	"context"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/pki"
)

// Only an already-pending emergency database leaf bypasses planned restart windows.
func urgentMemberCertificate(ctx context.Context, rc *engine.Context, m *member) (bool, error) {
	pending, err := hasPendingCertificate(ctx, rc, m)
	if err != nil || !pending {
		return false, err
	}
	leaf, err := secret(ctx, rc, "tls/members/"+m.Name+".pem")
	if err != nil {
		return false, err
	}
	return pki.NeedsRenewal(leaf, 7*24*time.Hour, now(rc))
}

func mergeMaintenanceWait(rc *engine.Context, result *engine.Result) {
	for _, c := range rc.Cluster.Status.Conditions {
		if c.Type == "MaintenanceWindow" && c.Reason == "WaitingForWindow" {
			wait := rc.WaitForWindow()
			if wait.RequeueAfter > 0 && (result.RequeueAfter == 0 || wait.RequeueAfter < result.RequeueAfter) {
				result.RequeueAfter = wait.RequeueAfter
			}
			return
		}
	}
}
