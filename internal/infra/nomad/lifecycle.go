// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package nomad

import (
	"context"

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
	// Engine runtime descriptions repeat identity values imported as secrets.
	// Preserve their sensitivity instead of promoting them to readable metadata.
	if err = b.mergeEnv(ctx, r, d.Runtime.Env, false, true); err != nil {
		return infra.Op{}, err
	}
	// Environment is held only in its dedicated Variable; the saved spec is safe metadata.
	d.Runtime.Env = nil
	r.Deployment = &storedDeployment{Runtime: d.Runtime, Source: source, Checksum: checksum, Binary: d.Artifact.Binary}
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
	as, err := b.allocations(ctx, hostname)
	if err != nil {
		return infra.Op{}, err
	}
	running := false
	for _, a := range as {
		if a.ClientStatus == "running" {
			if err = b.c.post(ctx, "/v1/client/allocation/"+a.ID+"/restart", nil, map[string]any{"AllTasks": true}, nil); err != nil {
				return infra.Op{}, err
			}
			running = true
		}
	}
	if !running && r.Desired == "running" && r.Deployment != nil {
		// A failed/lost allocation (or a purged job) cannot be restarted via
		// the allocation endpoint. Recreate the saved job through the same
		// stop/terminal/register fence as other destructive lifecycle changes.
		return b.begin(ctx, r, "restart")
	}
	return infra.Op{}, nil
}
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
	r.Min = min
	r.Max = max
	if r.Deployment != nil && r.Desired == "running" {
		return b.begin(ctx, r, "scale")
	}
	return infra.Op{}, b.save(ctx, r)
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
		return infra.Op{}, b.finishDelete(ctx, r)
	}
	r.Desired = "deleting"
	return b.begin(ctx, r, "delete")
}
func (b *Backend) finishDelete(ctx context.Context, r *record) error {
	v, err := b.readVariable(ctx, "nomad/jobs/"+r.Hostname)
	if err != nil {
		return err
	}
	if v.ModifyIndex != 0 {
		if err = b.removeVariable(ctx, v.Path, v.ModifyIndex); err != nil {
			return err
		}
	}
	return b.removeVariable(ctx, recordPrefix+r.Hostname, r.Index)
}
