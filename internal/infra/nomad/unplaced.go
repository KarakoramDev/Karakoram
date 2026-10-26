// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package nomad

import (
	"context"
	"errors"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"net/url"
	"time"
)

// RuntimeNeverPlaced is read-only. Retained terminal allocations disqualify a
// runtime, unlike Containers (which intentionally hides terminal allocations).
// An unchanged original blocked-evaluation chain supplies positive scheduling
// evidence; missing/GC'd evaluation history never becomes proof of nonexecution.
func (b *Backend) RuntimeNeverPlaced(ctx context.Context, name string) (bool, error) {
	return b.unplacedAdmission(ctx, name, false)
}

// A collected registration evaluation is not proof of never executing. Only
// nondestructive stateless correction may use its still-blocked direct child;
// volume resize and destructive retirement retain the stronger original proof.
func (b *Backend) unplacedAdmission(ctx context.Context, name string, allowCollectedStateless bool) (bool, error) {
	ctx = infra.ObservationContext(ctx)
	r, err := b.load(ctx, name)
	if err != nil {
		return false, err
	}
	if r == nil {
		var job observedJob
		_, err = b.c.get(ctx, "/v1/job/"+url.PathEscape(name), nil, &job)
		if errors.Is(err, infra.ErrNotFound) {
			return true, nil
		}
		if err != nil {
			return false, err
		}
		return false, jobOwnershipConflict()
	}
	if r.Kind != infra.KindRuntime {
		return false, nil
	}
	as, err := b.allocations(ctx, name)
	if err != nil {
		return false, err
	}
	if len(as) != 0 {
		// A blocked stateless resource rollout may retain healthy predecessors.
		// Never relax RuntimeNeverPlaced's history proof, or admit a successor
		// that has already allocated (even if that allocation is now terminal).
		p := r.Pending
		relay := r.Deployment != nil && len(r.Deployment.Runtime.Args) > 0 && r.Deployment.Runtime.Args[0] == "relay"
		if !allowCollectedStateless || r.Volume != "" || relay || p == nil || p.Action != "resources" || p.Phase != "starting" {
			return false, nil
		}
		for _, a := range as {
			if a.JobVersion >= p.JobVersion {
				return false, nil
			}
		}
		var deployment *struct {
			JobVersion uint64
			Status     string
		}
		_, err := b.c.get(ctx, "/v1/job/"+url.PathEscape(name)+"/deployment", nil, &deployment)
		if errors.Is(err, infra.ErrNotFound) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		if deployment == nil || deployment.JobVersion != p.JobVersion || deployment.Status != "running" {
			return false, nil
		}
	}
	_, job, err := b.operationJob(ctx, r)
	if errors.Is(err, infra.ErrNotFound) {
		return r.Deployment == nil || r.Pending != nil && r.Pending.Action == "deploy" && r.Pending.EvalID == "" && r.Pending.JobModifyIndex == 0, nil
	}
	if err != nil {
		return false, err
	}
	p := r.Pending
	if p == nil || (p.Action != "deploy" && p.Action != "resources") || p.EvalID == "" || job.Stop || job.Version != p.JobVersion || job.JobModifyIndex != p.JobModifyIndex || job.Meta["karakoram.operation"] != p.Issued.Format(time.RFC3339Nano) {
		return false, nil
	}
	original := evaluation{}
	_, err = b.c.get(ctx, "/v1/evaluation/"+url.PathEscape(p.EvalID), nil, &original)
	collected := errors.Is(err, infra.ErrNotFound)
	if collected {
		relay := r.Deployment != nil && len(r.Deployment.Runtime.Args) > 0 && r.Deployment.Runtime.Args[0] == "relay"
		if !allowCollectedStateless || r.Volume != "" || relay {
			return false, nil
		}
	} else if err != nil {
		return false, err
	}
	latest, err := b.currentEvaluation(ctx, r, p.JobModifyIndex)
	if errors.Is(err, infra.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if collected {
		return latest.Status == "blocked" && latest.PreviousEval == p.EvalID, nil
	}
	if original.JobModifyIndex != p.JobModifyIndex || latest.Status != "blocked" {
		return false, nil
	}
	return latest.ID == original.ID || latest.ID == original.BlockedEval, nil
}
