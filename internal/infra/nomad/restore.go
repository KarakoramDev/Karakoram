// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package nomad

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

const (
	registrationGrace    = 5 * time.Second
	restoreSubmitTimeout = 10 * time.Second
	// A claimed request must be delivered and committed within this window,
	// including Nomad server work after the client deadline. Lifecycle owners
	// retain the record until then, even when the submit response is lost.
	restoreFence = 60 * time.Second
)

type observedJob struct {
	Version, JobModifyIndex uint64
	Namespace               string
	Meta                    map[string]string
	Stop                    bool
}

func (b *Backend) ownsJob(r *record, job observedJob) bool {
	return job.Namespace == b.target.ID && job.Meta["karakoram.id"] == r.ID &&
		job.Meta["karakoram.kind"] == "runtime" && job.Meta["KARAKORAM_OWNER"] == r.Vars["KARAKORAM_OWNER"]
}

func jobOwnershipConflict() error {
	return &APIError{Status: 409, Op: "observe", Message: "job ownership does not match the durable service record; refusing adoption"}
}

func (b *Backend) now() time.Time {
	if b.clock != nil {
		return b.clock()
	}
	return time.Now()
}

func (b *Backend) restorationFenced(r *record) bool {
	return !r.RegistrationIssued.IsZero() && b.now().Sub(r.RegistrationIssued) < restoreFence
}

// restoreMissingJob repairs a derived object, never the durable identity or data.
// Serialize with lifecycle mutations, reload the desired state, and claim the
// registration with Variables CAS before submitting an absent-only jobs write.
func (b *Backend) restoreMissingJob(ctx context.Context, stale *record) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	r, err := b.load(ctx, stale.Hostname)
	if err != nil {
		return infra.StatusCreating, err
	}
	if r == nil || r.ID != stale.ID {
		return infra.StatusFailed, infra.ErrConflict
	}
	*stale = *r
	if r.Desired == "stopped" {
		if b.restorationFenced(r) {
			return infra.StatusCreating, nil
		}
		return infra.StatusStopped, nil
	}
	if r.Desired == "deleting" {
		return infra.StatusDeleting, nil
	}
	if r.Kind != infra.KindRuntime || r.Deployment == nil || r.Desired != "running" {
		return infra.StatusReadyToDeploy, nil
	}
	// Completed operation handles remain durable; only active/failed operations
	// exclude repair. In particular, never race an in-flight lifecycle register.
	if r.Pending != nil && r.Pending.Phase != "done" {
		if r.Pending.Phase == "failed" {
			return infra.StatusFailed, nil
		}
		return infra.StatusCreating, nil
	}
	if b.operationLeaseBlocked(r) {
		return infra.StatusCreating, nil
	}
	issued := r.RegistrationIssued
	if r.Pending != nil && r.Pending.Started.After(issued) {
		issued = r.Pending.Started
	}
	if !issued.IsZero() && b.now().Sub(issued) < registrationGrace {
		return infra.StatusCreating, nil
	}
	var existing observedJob
	_, err = b.c.get(ctx, "/v1/job/"+r.Hostname, nil, &existing)
	if err == nil {
		if !b.ownsJob(r, existing) {
			return infra.StatusFailed, jobOwnershipConflict()
		}
		return infra.StatusCreating, nil
	}
	if !errors.Is(err, infra.ErrNotFound) {
		return infra.StatusCreating, err
	}
	as, err := b.allocations(ctx, r.Hostname)
	if err != nil {
		return infra.StatusCreating, err
	}
	for _, a := range as {
		if !allocationTerminal(a) {
			return infra.StatusCreating, nil
		}
	}
	if r.Volume != "" {
		ok, err := b.restorableVolume(ctx, r)
		if err != nil {
			return infra.StatusFailed, err
		}
		if !ok {
			// Expose absence to engine replacement, rather than creating empty
			// storage or repeatedly scheduling an unmountable data member.
			return infra.StatusFailed, nil
		}
	}
	job, err := b.renderJob(ctx, r)
	if err != nil {
		return infra.StatusFailed, err
	}
	// Start the deadline before publishing the claim, so a delayed CAS response
	// cannot postpone submission beyond the durable fence's delivery window.
	submit, cancel := context.WithTimeout(ctx, restoreSubmitTimeout)
	defer cancel()
	r.RegistrationIssued = b.now().UTC()
	if err = b.save(submit, r); err != nil {
		if errors.Is(err, infra.ErrConflict) {
			return infra.StatusCreating, nil // Another observer/mutation won the claim.
		}
		return infra.StatusCreating, err
	}
	// Retain the claim generation for diagnosis; only a superseding lifecycle
	// operation owns cleanup, never this observer.
	generation := strconv.FormatUint(r.Index, 10)
	job["Job"].(map[string]any)["Meta"].(map[string]string)["karakoram.restore"] = generation
	job["EnforceIndex"] = true
	job["JobModifyIndex"] = uint64(0)
	if err = b.c.post(submit, "/v1/jobs", nil, job, nil); err != nil {
		if errors.Is(err, infra.ErrConflict) {
			if _, getErr := b.c.get(ctx, "/v1/job/"+r.Hostname, nil, &existing); getErr == nil && !b.ownsJob(r, existing) {
				return infra.StatusFailed, jobOwnershipConflict()
			}
		}
		return infra.StatusFailed, err
	}
	// Success, cancellation and lost responses all leave the same durable
	// obligation: Stop/Delete/replacement must wait out the claim's fence and
	// re-observe the hostname before finalizing. No compensating write here
	// can accidentally stop or purge a successor.
	return infra.StatusCreating, nil
}

func (b *Backend) restorableVolume(ctx context.Context, runtime *record) (bool, error) {
	v, err := b.load(ctx, runtime.Volume)
	if err != nil || v == nil {
		return false, err
	}
	if v.Kind != infra.KindVolume || v.VolumeID == "" || v.VolumeName == "" || v.Node == "" || v.Node != runtime.Node {
		return false, nil
	}
	var actual struct{ ID, Name, Namespace, NodeID, State string }
	_, err = b.c.get(ctx, "/v1/volume/host/"+v.VolumeID, nil, &actual)
	if errors.Is(err, infra.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return actual.ID == v.VolumeID && actual.Name == v.VolumeName && actual.Namespace == b.target.ID && actual.NodeID == v.Node && actual.State == "ready", nil
}
