// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package nomad

import (
	"context"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

// PlanResources checks the same replacement specification used by SetResources,
// without saving bounds, claiming an operation, or stopping the current job.
func (b *Backend) PlanResources(ctx context.Context, hostname string, desired infra.Resources) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	r, err := b.require(ctx, hostname)
	if err != nil {
		return err
	}
	proposed := *r
	proposed.Resources = mergeResources(r.Resources, desired)
	if err := resourceValidation(proposed.Resources); err != nil {
		return err
	}
	relay := r.Deployment != nil && len(r.Deployment.Runtime.Args) != 0 && r.Deployment.Runtime.Args[0] == "relay"
	if r.Kind != infra.KindRuntime || r.Deployment == nil || r.Desired != "running" || r.Volume == "" && !relay {
		return nil
	}
	proposed.Pending = &operation{Action: "resources", Issued: time.Now().UTC()}
	work, cancel := operationContext(ctx)
	defer cancel()
	return b.planResourceUpdate(work, &proposed)
}

// Nomad's scheduler dry run accounts for allocations replaced by this job; a
// free-capacity comparison would incorrectly require room for both writers.
// This is admission, not a reservation: concurrent external jobs can still race
// the subsequent stop/register sequence.
func (b *Backend) planResourceUpdate(ctx context.Context, r *record) error {
	body, err := b.renderJob(ctx, r)
	if err != nil {
		return err
	}
	placement := *r
	if r.Volume != "" {
		volume, err := b.require(ctx, r.Volume)
		if err != nil {
			return err
		}
		placement.Node = volume.Node
		var node node
		if _, err := b.c.get(ctx, "/v1/node/"+volume.Node, nil, &node); err != nil {
			return err
		}
		if node.Status != "ready" || node.SchedulingEligibility != "eligible" || node.Drain {
			return &infra.ResourceUpdateRejectedError{Message: "Resource update rejected before stopping on node " + volume.Node + ": node is not ready, eligible and undrained; running allocation unchanged"}
		}
	}
	// Job-plan and evaluation responses share FailedTGAllocs metrics.
	var plan struct {
		evaluation
		Annotations struct {
			DesiredTGUpdates map[string]struct {
				Place, DestructiveUpdate int
			}
		}
	}
	if err := b.c.post(ctx, "/v1/job/"+r.Hostname+"/plan", nil, body, &plan); err != nil {
		return err // Never stop when the scheduler cannot prove feasibility.
	}
	if len(plan.FailedTGAllocs) != 0 {
		plan.Status = "rejected resource update"
		return &infra.ResourceUpdateRejectedError{Message: "Resource update rejected before stopping; running allocation unchanged. " + b.placementMessage(&placement, plan.evaluation) + "; restore capacity or choose smaller resources"}
	}
	update := plan.Annotations.DesiredTGUpdates["member"]
	if update.Place+update.DestructiveUpdate < 1 {
		return &infra.ResourceUpdateRejectedError{Message: "Resource update rejected before stopping on node " + placement.Node + ": scheduler plan did not evaluate replacement placement (no-op or in-place update); running allocation unchanged"}
	}
	return nil
}
