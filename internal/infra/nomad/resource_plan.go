// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package nomad

import (
	"context"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

// Nomad's scheduler dry run accounts for allocations replaced by this job; a
// free-capacity comparison would incorrectly require room for both writers.
// This is admission, not a reservation: concurrent external jobs can still race
// the subsequent stop/register sequence.
func (b *Backend) planResourceUpdate(ctx context.Context, r *record) error {
	body, err := b.renderJob(ctx, r)
	if err != nil {
		return err
	}
	// Job-plan and evaluation responses share FailedTGAllocs metrics.
	var plan evaluation
	if err := b.c.post(ctx, "/v1/job/"+r.Hostname+"/plan", nil, body, &plan); err != nil {
		return err // Never stop when the scheduler cannot prove feasibility.
	}
	if len(plan.FailedTGAllocs) == 0 {
		return nil
	}
	placement := *r
	if r.Volume != "" {
		volume, err := b.require(ctx, r.Volume)
		if err != nil {
			return err
		}
		placement.Node = volume.Node
	}
	plan.Status = "rejected resource update"
	return &infra.ResourceUpdateRejectedError{Message: "Resource update rejected before stopping; running allocation unchanged. " + b.placementMessage(&placement, plan) + "; restore capacity or choose smaller resources"}
}
