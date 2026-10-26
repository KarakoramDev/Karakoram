// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package engine

import (
	"context"
	"fmt"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

// UnplacedRetirement owns infrastructure-only retirement of a scale-up member
// which never joined the data plane. The provider must persist this intent before
// advancing it, retain import/ownership evidence, and exclude provisioning until
// retirement finishes. Native members must use their normal retirement workflow.
type UnplacedRetirement struct {
	Phase string    `json:"phase,omitempty"`
	Op    *infra.Op `json:"op,omitempty"`
}

// RuntimeUnplaced is only an admission check for a provider-proven uninitialized
// member. Empty current allocations alone never prove an existing replica's data
// disposable. Reads fail closed; the subsequent delete supplies mutation fencing.
func RuntimeUnplaced(ctx context.Context, rc *Context, name string) (bool, error) {
	if !CleanupSupersedesDeployment(rc.Backend, infra.Op{Kind: "deploy"}) {
		return false, nil
	}
	observer, ok := rc.Backend.(infra.UnplacedRuntimeObserver)
	if !ok {
		return false, nil
	}
	return observer.RuntimeNeverPlaced(ctx, name)
}

// AdvanceUnplacedRetirement never deploys/restarts or creates storage. One step
// either observes an exact deletion or submits one, and its handle survives CP
// restart. Volume deletion follows authoritative logical runtime absence.
func AdvanceUnplacedRetirement(ctx context.Context, rc *Context, q *UnplacedRetirement, name, id, volume, volumeID string) (bool, error) {
	rc.Cluster.Status.Message = "Scaling down: retiring unplaced member " + name
	if q.Op != nil {
		state, err := rc.Backend.OpStatus(ctx, *q.Op)
		if err != nil {
			return false, err
		}
		if state.Failed {
			q.Op = nil
			return false, fmt.Errorf("retiring unplaced member %s: %s", name, state.Message)
		}
		if !state.Done {
			return false, nil
		}
		q.Op = nil
		if q.Phase == "volume" {
			return true, nil
		}
		// A successful exact deletion is authoritative absence evidence.
		q.Phase = "volume"
		return false, nil
	}
	if q.Phase == "" {
		// Submit exact-ID deletion before ordinary service observation: Nomad
		// GetService may otherwise advance/restore the obsolete deployment.
		op, err := rc.Backend.DeleteServiceByID(ctx, name, id)
		if err != nil {
			return false, err
		}
		if op.ID == "" {
			q.Phase = "volume"
		} else {
			q.Phase, q.Op = "runtime", &op
		}
		return false, nil
	}
	target, expected := name, id
	kind := infra.KindRuntime
	if q.Phase == "volume" {
		target, expected = volume, volumeID
		kind = infra.KindVolume
		if target == "" {
			return true, nil
		}
	}
	service, err := rc.Backend.GetService(ctx, target)
	if err != nil {
		return false, err
	}
	if service == nil {
		if q.Phase == "volume" {
			return true, nil
		}
		q.Phase = "volume"
		return false, nil
	}
	if expected == "" || service.ID != expected || service.Kind != kind {
		return false, UnverifiedOwnership(service)
	}
	op, err := rc.Backend.DeleteServiceByID(ctx, target, expected)
	if err != nil {
		return false, err
	}
	if op.ID == "" {
		if kind == infra.KindVolume {
			return true, nil
		}
		q.Phase = "volume"
		return false, nil
	}
	q.Op = &op
	return false, nil
}
