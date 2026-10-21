// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package rabbitmq

import (
	"context"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/pki"
)

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

// Gate every new member disruption, including operations claimed in an earlier
// window. Once drain has succeeded, always let that member finish rejoining.
func changeWindow(ctx context.Context, rc *engine.Context, m *member) (engine.Result, error) {
	switch m.ChangeStep {
	case "", "quorum", "critical", "drain":
	default:
		return engine.Result{}, nil
	}
	if rc.Operation != nil {
		return rc.WaitForWindow(), nil
	}
	urgent, err := urgentMemberCertificate(ctx, rc, m)
	if err != nil || urgent {
		return engine.Result{}, err
	}
	return rc.WaitForWindow(), nil
}
