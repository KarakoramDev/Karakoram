// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package dokploy

import (
	"context"
	"errors"
	"strings"
	"time"

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
func runningTasks(tasks []task) bool {
	for _, t := range tasks {
		if strings.EqualFold(t.State, "running") || strings.HasPrefix(strings.ToLower(t.CurrentState), "running") {
			return true
		}
	}
	return false
}
func (b *Backend) reloadUpdated(ctx context.Context, a *application, m metadata, settings map[string]any) (infra.Op, error) {
	var o operation
	var err error
	reload := m.Deployed
	if reload && a.Status == "idle" {
		tasks, err := b.tasks(ctx, a.AppName)
		if err != nil {
			return infra.Op{}, err
		}
		reload = false
		for _, t := range tasks {
			if strings.EqualFold(t.State, "running") {
				reload = true
				break
			}
		}
	}
	if reload {
		o, err = b.operation(ctx, a, m, "restart")
		if err != nil {
			return infra.Op{}, err
		}
	}
	if err = b.update(ctx, a, m, settings); err != nil {
		return infra.Op{}, err
	}
	if !reload {
		return infra.Op{}, nil
	}
	if err = b.c.post(ctx, "application.reload", map[string]any{"applicationId": a.ID, "appName": a.AppName}, nil); err != nil {
		return infra.Op{}, err
	}
	return encodeOp(o), nil
}
func (b *Backend) SetResources(ctx context.Context, hostname string, r infra.Resources) (infra.Op, error) {
	if err := resourceValidation(r); err != nil {
		return infra.Op{}, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	a, m, err := b.require(ctx, hostname)
	if err != nil {
		return infra.Op{}, err
	}
	if m.Kind == infra.KindVolume && (r.MinDisk != 0 && r.MinDisk != m.Resources.MinDisk || r.MaxDisk != 0 && r.MaxDisk != m.Resources.MaxDisk) {
		return infra.Op{}, infra.ErrUnsupported
	}
	m.Resources = mergeResources(m.Resources, r)
	if err = resourceValidation(m.Resources); err != nil {
		return infra.Op{}, err
	}
	return b.reloadUpdated(ctx, a, m, resourceSettings(m.Resources))
}
func (b *Backend) SetContainerCount(ctx context.Context, hostname string, min, max int) (infra.Op, error) {
	if min < 1 || max < min {
		return infra.Op{}, reject("invalid container bounds")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	a, m, err := b.require(ctx, hostname)
	if err != nil {
		return infra.Op{}, err
	}
	if m.Kind == infra.KindVolume || m.Volume != "" && max != 1 {
		return infra.Op{}, reject("a volume service or runtime requires one replica")
	}
	m.Min = min
	m.Max = max
	return b.reloadUpdated(ctx, a, m, map[string]any{"replicas": max})
}
func (b *Backend) DeleteService(ctx context.Context, hostname string) (infra.Op, error) {
	return b.deleteService(ctx, hostname, "", false)
}
func (b *Backend) DeleteServiceByID(ctx context.Context, hostname, expectedID string) (infra.Op, error) {
	return b.deleteService(ctx, hostname, expectedID, true)
}
func (b *Backend) deleteService(ctx context.Context, hostname, expectedID string, fenced bool) (infra.Op, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	a, err := b.application(ctx, hostname)
	if err != nil {
		return infra.Op{}, err
	}
	if a == nil {
		return infra.Op{}, nil
	}
	if fenced && a.ID != expectedID {
		return infra.Op{}, infra.ErrConflict
	}
	m, _ := parseMetadata(a.Description)
	if m.Kind == infra.KindVolume {
		e, err := b.environment(ctx)
		if err != nil {
			return infra.Op{}, err
		}
		for _, other := range e.Applications {
			om, ok := parseMetadata(other.Description)
			if ok && om.Kind == infra.KindRuntime && om.Volume == hostname {
				return infra.Op{}, infra.ErrConflict
			}
		}
		o := operation{Action: "wipe", AppID: a.ID, AppName: a.AppName, Hostname: hostname, Issued: time.Now().UTC(), Node: m.Node, PreviousTasks: m.WipePreviousTasks}
		if m.WipePreviousDeployment != "" {
			o.PreviousDeployments = []string{m.WipePreviousDeployment}
		}
		ds, err := b.deployments(ctx, a.ID)
		if err != nil {
			if errors.Is(err, infra.ErrNotFound) {
				return infra.Op{}, nil
			}
			return infra.Op{}, err
		}
		tasks, err := b.tasks(ctx, a.AppName)
		if err != nil {
			return infra.Op{}, err
		}
		failed := false
		for _, t := range tasks {
			if contains(o.PreviousTasks, t.ID) {
				continue
			}
			current := strings.ToLower(t.CurrentState)
			if strings.HasPrefix(current, "failed") || strings.HasPrefix(current, "rejected") {
				failed = true
			}
		}
		if len(ds) > 0 && !contains(o.PreviousDeployments, ds[0].ID) && (ds[0].Status == "error" || ds[0].Status == "cancelled") {
			failed = true
		}
		if !m.Wiping || failed {
			o.PreviousTasks = nil
			for _, t := range tasks {
				o.PreviousTasks = append(o.PreviousTasks, t.ID)
			}
			o.PreviousDeployments = nil
			m.WipePreviousDeployment = ""
			if len(ds) > 0 {
				o.PreviousDeployments = []string{ds[0].ID}
				m.WipePreviousDeployment = ds[0].ID
			}
			m.Wiping = true
			m.WipePreviousTasks = o.PreviousTasks
			settings := map[string]any{"sourceType": "docker", "dockerImage": baseImage, "command": "find", "args": []string{"/data", "-mindepth", "1", "-delete"}, "replicas": 1, "restartPolicySwarm": map[string]any{"Condition": "none"}, "placementSwarm": map[string]any{"Constraints": []string{"node.hostname==" + m.Node}}}
			if err = b.update(ctx, a, m, settings); err != nil {
				if errors.Is(err, infra.ErrNotFound) {
					return infra.Op{}, nil
				}
				return infra.Op{}, err
			}
			if err = b.c.post(ctx, "application.deploy", map[string]any{"applicationId": a.ID}, nil); err != nil {
				if errors.Is(err, infra.ErrNotFound) {
					return infra.Op{}, nil
				}
				m.Wiping = false
				_ = b.update(ctx, a, m, nil)
				return infra.Op{}, err
			}
		}
		return encodeOp(o), nil
	}
	o := operation{Action: "delete", AppID: a.ID, AppName: a.AppName, Hostname: hostname, Issued: time.Now().UTC()}
	if m.Deployed {
		if err = b.c.post(ctx, "application.stop", map[string]any{"applicationId": a.ID}, nil); err != nil && !errors.Is(err, infra.ErrNotFound) {
			return infra.Op{}, err
		}
	}
	return encodeOp(o), nil
}
func (b *Backend) advanceWipe(ctx context.Context, o operation, a *application, ts []task) (infra.OpState, error) {
	s := infra.OpState{Status: "wiping", ServiceIDs: []string{o.AppID}}
	complete := false
	for _, t := range ts {
		if contains(o.PreviousTasks, t.ID) {
			continue
		}
		current := strings.ToLower(t.CurrentState)
		if strings.HasPrefix(current, "complete") {
			complete = true
		}
		if strings.HasPrefix(current, "failed") || strings.HasPrefix(current, "rejected") {
			s.Failed = true
			s.Status = "failed"
			s.Message = "volume wipe task failed; the holder was retained"
			return s, nil
		}
	}
	if !complete {
		ds, err := b.deployments(ctx, a.ID)
		if err != nil {
			if errors.Is(err, infra.ErrNotFound) {
				return b.finishWipe(ctx, o, s)
			}
			return s, err
		}
		if len(ds) > 0 && !contains(o.PreviousDeployments, ds[0].ID) && (ds[0].Status == "error" || ds[0].Status == "cancelled") {
			s.Failed = true
			s.Status = "failed"
			s.Message = "volume wipe deployment failed; the holder was retained"
		}
		return s, nil
	}
	if err := b.c.post(ctx, "application.delete", map[string]any{"applicationId": a.ID}, nil); err != nil && !errors.Is(err, infra.ErrNotFound) {
		return s, err
	}
	return b.finishWipe(ctx, o, s)
}

// A removed service can leave its exited task container referencing the local
// volume briefly. Keep the durable operation pending until Docker releases it.
func (b *Backend) finishWipe(ctx context.Context, o operation, s infra.OpState) (infra.OpState, error) {
	ns, err := b.nodes(ctx)
	if err != nil {
		return s, err
	}
	manager := false
	for _, n := range ns {
		if n.Description.Hostname == o.Node && n.Spec.Role == "manager" {
			manager = true
			break
		}
	}
	if manager {
		var volumes []struct {
			Name string `json:"Name"`
		}
		if err = b.c.get(ctx, "dockerVolume.getVolumes", nil, &volumes); err != nil {
			return s, err
		}
		exists := false
		for _, v := range volumes {
			if v.Name == o.AppName {
				exists = true
				break
			}
		}
		if exists {
			err = b.c.post(ctx, "dockerVolume.removeVolume", map[string]any{"volumeName": o.AppName}, nil)
			if err != nil && !errors.Is(err, infra.ErrNotFound) {
				if errors.Is(err, infra.ErrRejected) || errors.Is(err, infra.ErrConflict) || errors.Is(err, infra.ErrUnavailable) {
					return s, nil
				}
				return s, err
			}
		}
	}
	s.Done = true
	s.Status = "done"
	return s, nil
}
