// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package nomad

import (
	"context"
	"errors"
	"reflect"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

// ResizeUnplacedRuntime owns both admission and mutation. Initial deployments
// require no allocation history; stateless resource rollouts can retain older
// serving allocations while their blocked successor has never allocated.
// Stateless updates retain predecessors even if placement wins. Volume updates
// fence future placement by stopping the old job, then reject any allocation
// history before registering a replacement.
func (b *Backend) ResizeUnplacedRuntime(ctx context.Context, name string, expected infra.Op, desired infra.Resources) (infra.Op, bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	o, err := decodeOp(expected)
	if err != nil {
		return infra.Op{}, false, err
	}
	r, err := b.require(ctx, name)
	if err != nil {
		return infra.Op{}, false, err
	}
	if r.Pending != nil && r.Pending.Phase == "failed" && r.Pending.UnplacedPrevious != nil {
		return infra.Op{}, false, unplacedResizeFailure(r)
	}
	if r.Pending == nil || r.ID != o.RecordID || r.Kind != infra.KindRuntime || r.Desired != "running" || r.Deployment == nil {
		return infra.Op{}, false, nil
	}
	if !r.Pending.Issued.Equal(o.Issued) {
		// The conditional successor may already be durable while the provider
		// still holds its predecessor receipt. Only this exact correction can
		// be adopted; an ordinary or different-target successor stays fenced.
		correction := r.Pending.ResourceCorrection
		if r.Pending.Action == "resources" && correction != nil && correction.Predecessor.Equal(o.Issued) &&
			reflect.DeepEqual(correction.Resources, r.Resources) &&
			reflect.DeepEqual(mergeResources(correction.Resources, desired), correction.Resources) {
			return encodeOp(*r.Pending), true, nil
		}
		return infra.Op{}, false, nil
	}
	unplaced, err := b.unplacedAdmission(ctx, name, true)
	if err != nil || !unplaced {
		return infra.Op{}, false, err
	}
	previous := r.Resources
	r.Resources = mergeResources(r.Resources, desired)
	if err := resourceValidation(r.Resources); err != nil {
		return infra.Op{}, false, err
	}
	op, err := b.beginOperation(ctx, r, "resources", false, &previous, nil)
	return op, err == nil, err
}

func (b *Backend) checkUnplacedResize(ctx context.Context, r *record) error {
	as, err := b.allocations(ctx, r.Hostname)
	if err != nil {
		return err
	}
	if len(as) == 0 {
		return nil
	}
	message := "UnplacedResizeDeferred: allocation history appeared while fencing the initial deployment; replacement registration refused; inspect the stopped bootstrap allocation and retry the cluster update through its normal lifecycle"
	// Preserve old applied bounds on the terminal path. A later ordinary
	// retry must not combine corrected RAM with the old native cache config.
	r.Resources = *r.Pending.UnplacedPrevious
	_, saveErr := b.failOperation(ctx, r, infra.OpState{}, message)
	return errors.Join(unplacedResizeFailure(r), saveErr)
}

func unplacedResizeFailure(r *record) error {
	return &infra.DeploymentExhaustedError{Service: r.Hostname, LastError: r.Pending.Message, PreviousResources: r.Pending.UnplacedPrevious}
}
