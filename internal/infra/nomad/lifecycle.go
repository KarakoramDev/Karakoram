// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package nomad

import (
	"context"
	"errors"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func mergeResources(old, change infra.Resources) infra.Resources {
	if change.CPUMode != "" {
		old.CPUMode = change.CPUMode
	}
	if change.MinCPU != 0 {
		old.MinCPU = change.MinCPU
	}
	if change.MaxCPU != 0 {
		old.MaxCPU = change.MaxCPU
	}
	if change.MinRAM != 0 {
		old.MinRAM = change.MinRAM
	}
	if change.MaxRAM != 0 {
		old.MaxRAM = change.MaxRAM
	}
	if change.MinDisk != 0 {
		old.MinDisk = change.MinDisk
	}
	if change.MaxDisk != 0 {
		old.MaxDisk = change.MaxDisk
	}
	if change.StartCPU != nil && *change.StartCPU != 0 {
		old.StartCPU = change.StartCPU
	}
	return old
}
func (b *Backend) Deploy(ctx context.Context, hostname string, d infra.Deployment) (infra.Op, error) {
	if err := d.Validate(); err != nil {
		return infra.Op{}, reject(err.Error())
	}
	if d.Runtime.Base != infra.BaseUbuntu2404 {
		return infra.Op{}, reject("unsupported runtime base")
	}
	if err := validateEnvironment(d.Runtime.Env); err != nil {
		return infra.Op{}, err
	}
	source, checksum, err := b.artifacts.resolve(ctx, d.Artifact, b.artifactURL)
	if err != nil {
		return infra.Op{}, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	r, err := b.require(ctx, hostname)
	if err != nil {
		return infra.Op{}, err
	}
	if r.Kind != infra.KindRuntime {
		return infra.Op{}, reject("only runtime services can deploy")
	}
	if d.Runtime.Volume != r.Volume {
		return infra.Op{}, reject("deployment volume must match imported runtime")
	}
	// Reject admission before changing the import-owned environment. Otherwise
	// replaying the original import can no longer match its reservation.
	if r.Importing || b.operationLeaseBlocked(r) {
		return infra.Op{}, infra.ErrConflict
	}
	// Engine runtime descriptions repeat identity values imported as secrets.
	// Preserve their sensitivity instead of promoting them to readable metadata.
	if err = b.mergeEnv(ctx, r, d.Runtime.Env, false, true); err != nil {
		return infra.Op{}, err
	}
	diskMB := b.allocationDiskMB(d.Runtime)
	// Environment is held only in its dedicated Variable; the saved spec is safe metadata.
	d.Runtime.Env = nil
	r.Deployment = &storedDeployment{Runtime: d.Runtime, Source: source, Checksum: checksum, Binary: d.Artifact.Binary, EphemeralDiskMB: diskMB}
	r.Desired = "running"
	return b.begin(ctx, r, "deploy")
}
func (b *Backend) Start(ctx context.Context, hostname string) (infra.Op, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	r, err := b.require(ctx, hostname)
	if err != nil {
		return infra.Op{}, err
	}
	if r.Kind != infra.KindRuntime || r.Deployment == nil {
		return infra.Op{}, reject("service has no deployment to start")
	}
	r.Desired = "running"
	return b.begin(ctx, r, "start")
}
func (b *Backend) Stop(ctx context.Context, hostname string) (infra.Op, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	r, err := b.require(ctx, hostname)
	if err != nil {
		return infra.Op{}, err
	}
	if r.Kind != infra.KindRuntime {
		return infra.Op{}, reject("only runtime services can stop")
	}
	r.Desired = "stopped"
	return b.begin(ctx, r, "stop")
}
func (b *Backend) Restart(ctx context.Context, hostname string) (infra.Op, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	r, err := b.require(ctx, hostname)
	if err != nil {
		return infra.Op{}, err
	}
	if r.Kind != infra.KindRuntime {
		return infra.Op{}, reject("only runtime services can restart")
	}
	if r.Pending != nil && r.Pending.Phase == "failed" && r.Desired == "running" && r.Deployment != nil {
		return b.begin(ctx, r, "restart")
	}
	if b.operationLeaseBlocked(r) {
		return infra.Op{}, infra.ErrConflict
	}
	work, cancel := operationContext(ctx)
	defer cancel()
	lease, err := b.newOperationLease()
	if err != nil {
		return infra.Op{}, err
	}
	r.RestartLease = lease
	if err = b.save(work, r); err != nil {
		return infra.Op{}, b.releaseFailedOperation(work, r, lease, err)
	}
	as, err := b.allocations(work, hostname)
	if err != nil {
		return infra.Op{}, b.releaseFailedOperation(work, r, lease, err)
	}
	running := false
	for _, a := range as {
		if a.ClientStatus == "running" {
			if !b.leaseUsable(lease) {
				return infra.Op{}, b.releaseFailedOperation(work, r, lease, infra.ErrConflict)
			}
			if err = operationMutation(work, func() error {
				return b.c.post(work, "/v1/client/allocation/"+a.ID+"/restart", nil, map[string]any{"AllTasks": true}, nil)
			}); err != nil {
				return infra.Op{}, b.releaseFailedOperation(work, r, lease, err)
			}
			running = true
		}
	}
	if err = b.releaseExactOperationLease(work, r, lease); err != nil {
		return infra.Op{}, err
	}
	if !running && r.Desired == "running" && r.Deployment != nil {
		// A failed/lost allocation (or a purged job) cannot be restarted via
		// the allocation endpoint. Recreate the saved job through its normal
		// rollout policy, retaining the single-writer fence for volume members.
		current, err := b.require(ctx, hostname)
		if err != nil {
			return infra.Op{}, err
		}
		return b.begin(ctx, current, "restart")
	}
	return infra.Op{}, nil
}

// ResourceUpdatesDeployRuntime reports SetResources' stored-deployment restart.
func (*Backend) ResourceUpdatesDeployRuntime() bool { return true }

func (b *Backend) SetResources(ctx context.Context, hostname string, change infra.Resources) (infra.Op, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	r, err := b.require(ctx, hostname)
	if err != nil {
		return infra.Op{}, err
	}
	if r.Kind == infra.KindVolume && (change.MinDisk != 0 && change.MinDisk != r.Resources.MinDisk || change.MaxDisk != 0 && change.MaxDisk != r.Resources.MaxDisk) {
		return infra.Op{}, infra.ErrUnsupported
	}
	r.Resources = mergeResources(r.Resources, change)
	if err = resourceValidation(r.Resources); err != nil {
		return infra.Op{}, err
	}
	if r.Kind == infra.KindRuntime && r.Deployment != nil && r.Desired == "running" {
		return b.begin(ctx, r, "resources")
	}
	return infra.Op{}, b.save(ctx, r)
}
func (b *Backend) SetContainerCount(ctx context.Context, hostname string, min, max int) (infra.Op, error) {
	if min < 1 || max < min {
		return infra.Op{}, reject("invalid container bounds")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	r, err := b.require(ctx, hostname)
	if err != nil {
		return infra.Op{}, err
	}
	if r.Kind != infra.KindRuntime || r.Volume != "" && max != 1 {
		return infra.Op{}, reject("volume services and volume runtimes require one replica")
	}
	if op, retained, err := b.retainedScaleReceipt(ctx, r, min, max); err != nil || retained {
		return op, err
	}
	r.Min = min
	r.Max = max
	if r.Deployment != nil && r.Desired == "running" {
		return b.begin(ctx, r, "scale")
	}
	return infra.Op{}, b.save(ctx, r)
}

// RecoverFailedScale starts a new operation only for a placement-free recovery.
// The failed handle remains terminal; a failed automatic recovery is not retried.
func (b *Backend) RecoverFailedScale(ctx context.Context, hostname string, min, max int) (infra.Op, bool, error) {
	if min < 1 || max < min {
		return infra.Op{}, false, reject("invalid container bounds")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	r, err := b.require(ctx, hostname)
	if err != nil {
		return infra.Op{}, false, err
	}
	if op, retained, err := b.retainedScaleReceipt(ctx, r, min, max); err != nil || retained {
		return op, retained, err
	}
	if r.Kind != infra.KindRuntime || r.Volume != "" || r.Desired != "running" || r.Deployment == nil ||
		r.Pending == nil || r.Pending.Phase != "failed" || r.Pending.RetainOnly ||
		len(r.Deployment.Runtime.Args) > 0 && r.Deployment.Runtime.Args[0] == "relay" {
		return infra.Op{}, false, nil
	}
	job, observed, err := b.operationJob(ctx, r)
	if errors.Is(err, infra.ErrNotFound) {
		return infra.Op{}, false, nil
	}
	if err != nil {
		return infra.Op{}, false, err
	}
	if observed.Stop {
		return infra.Op{}, false, nil
	}
	candidate := *r
	candidate.Min, candidate.Max = min, max
	retained, err := b.retainedScaleJob(ctx, &candidate, job, false, true)
	if err != nil || retained == nil {
		return infra.Op{}, false, err
	}
	op, err := b.beginOperation(ctx, &candidate, "scale", true, nil)
	return op, err == nil, err
}

// Admission or completion can become durable before its caller saves a receipt.
// A same-target retry must adopt that outcome, never relax its retain-only policy.
func (b *Backend) retainedScaleReceipt(ctx context.Context, r *record, min, max int) (infra.Op, bool, error) {
	current := r.Pending
	if current == nil || !current.RetainOnly || current.Phase == "failed" ||
		r.Desired != "running" || r.Min != min || r.Max != max {
		return infra.Op{}, false, nil
	}
	if current.Phase == "done" {
		_, job, err := b.operationJob(ctx, r)
		if err != nil {
			return infra.Op{}, false, err
		}
		if !operationMatchesJob(current, job) {
			return infra.Op{}, false, nil
		}
	}
	return encodeOp(*current), true, nil
}
func (b *Backend) DeleteService(ctx context.Context, hostname string) (infra.Op, error) {
	return b.deleteService(ctx, hostname, "", false)
}
func (b *Backend) DeleteServiceByID(ctx context.Context, hostname, id string) (infra.Op, error) {
	return b.deleteService(ctx, hostname, id, true)
}
func (b *Backend) deleteService(ctx context.Context, hostname, id string, fenced bool) (infra.Op, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	r, err := b.load(ctx, hostname)
	if err != nil || r == nil {
		return infra.Op{}, err
	}
	if fenced && r.ID != id {
		return infra.Op{}, infra.ErrConflict
	}
	if r.Importing {
		return infra.Op{}, &APIError{Status: 409, Op: "delete", Message: "import outcome is unresolved; retry the original import before deleting its reserved resources"}
	}
	if r.Kind == infra.KindVolume {
		rs, incomplete, e := b.records(ctx, "")
		if e != nil {
			return infra.Op{}, e
		}
		// A corrupt runtime record can hide a retained volume reference.
		// Listing tolerates foreign records; destructive deletion fails closed.
		if incomplete {
			return infra.Op{}, &APIError{Status: 409, Op: "volume delete", Message: "an undecodable karakoram/services record may still reference this volume"}
		}
		for _, other := range rs {
			if other.Kind == infra.KindRuntime && other.Volume == hostname {
				return infra.Op{}, infra.ErrConflict
			}
		}
		if err = ignoreNotFound(b.c.delete(ctx, "/v1/volume/host/"+r.VolumeID, nil, nil)); err != nil {
			return infra.Op{}, err
		}
		_, err = b.finishDelete(ctx, r)
		return infra.Op{}, err
	}
	r.Desired = "deleting"
	return b.begin(ctx, r, "delete")
}
func (b *Backend) finishDelete(ctx context.Context, r *record) (bool, error) {
	if b.restorationFenced(r) {
		return false, nil
	}
	v, err := b.readVariable(ctx, "nomad/jobs/"+r.Hostname)
	if err != nil {
		return false, err
	}
	if v.ModifyIndex != 0 {
		if err = b.removeVariable(ctx, v.Path, v.ModifyIndex); err != nil {
			return false, err
		}
	}
	err = b.removeVariable(ctx, recordPrefix+r.Hostname, r.Index)
	return err == nil, err
}
