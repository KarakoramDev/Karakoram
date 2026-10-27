// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package dokploy

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
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
func (b *Backend) reloadUpdated(ctx context.Context, a *application, m metadata, settings map[string]any, action string, stopFirst bool) (infra.Op, error) {
	var o operation
	var err error
	reload := m.Deployed && m.Desired != "stopped" && m.Desired != "deleting"
	if reload && m.Desired == "" && a.Status == "idle" {
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
	var restoreConfig json.RawMessage
	if stopFirst && reload || m.Pending != nil && len(m.Pending.RestoreUpdateConfig) > 0 {
		if settings == nil {
			settings = map[string]any{}
		}
		if stopFirst && reload {
			restoreConfig, err = json.Marshal(a.UpdateConfig)
			if err != nil {
				return infra.Op{}, err
			}
			if m.Pending != nil && len(m.Pending.RestoreUpdateConfig) > 0 {
				restoreConfig = m.Pending.RestoreUpdateConfig
			}
			parallelism := 1
			if m.PortsPending || action == "endpoint-repair" {
				// Endpoint-mode/port changes must replace the entire replica
				// set in one stop-first batch. Serial stop-first retains an
				// old endpoint while another slot starts its replacement.
				parallelism = 0 // Swarm: all replicas in the same batch.
			}
			settings["updateConfigSwarm"] = map[string]any{"Parallelism": parallelism, "Order": "stop-first", "FailureAction": "continue"}
		} else {
			settings["updateConfigSwarm"] = m.Pending.RestoreUpdateConfig
		}
	}
	if reload {
		digest, err := b.applicationSpecification(ctx, a.ID, settings, m.DeploymentDigest)
		if err != nil {
			return infra.Op{}, err
		}
		o, err = b.operation(ctx, a, m, action)
		if err != nil {
			return infra.Op{}, err
		}
		o.RestoreUpdateConfig = restoreConfig
		o.Specification = digest
		m.Pending = &o
		if m.PortChange != nil {
			m.PortChange.ReloadPending = true
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
	if err := b.mu.Lock(ctx); err != nil {
		return infra.Op{}, err
	}
	defer b.mu.Unlock()
	a, m, err := b.require(ctx, hostname)
	if err != nil {
		return infra.Op{}, err
	}
	if err = b.mutationReady(ctx, a, &m); err != nil {
		return infra.Op{}, err
	}
	if m.Kind == infra.KindVolume && (r.MinDisk != 0 && r.MinDisk != m.Resources.MinDisk || r.MaxDisk != 0 && r.MaxDisk != m.Resources.MaxDisk) {
		return infra.Op{}, infra.ErrUnsupported
	}
	m.Resources = mergeResources(m.Resources, r)
	if err = resourceValidation(m.Resources); err != nil {
		return infra.Op{}, err
	}
	return b.reloadUpdated(ctx, a, m, resourceSettings(m.Resources), "restart", false)
}
func (b *Backend) SetContainerCount(ctx context.Context, hostname string, min, max int) (infra.Op, error) {
	if min < 1 || max < min {
		return infra.Op{}, reject("invalid container bounds")
	}
	if err := b.mu.Lock(ctx); err != nil {
		return infra.Op{}, err
	}
	defer b.mu.Unlock()
	a, m, err := b.require(ctx, hostname)
	if err != nil {
		return infra.Op{}, err
	}
	if err = b.mutationReady(ctx, a, &m); err != nil {
		return infra.Op{}, err
	}
	if m.Kind == infra.KindVolume || m.Volume != "" && max != 1 {
		return infra.Op{}, reject("a volume service or runtime requires one replica")
	}
	// Reload forces every Swarm task to restart. With multiple surviving
	// replicas, stop-first frees one slot without taking the service offline.
	// Keep start-first for a singleton: replacing its only task must not
	// deliberately interrupt all serving traffic.
	scaleBack := false
	specification, err := b.applicationSpecification(ctx, a.ID, nil, m.DeploymentDigest)
	if err != nil {
		return infra.Op{}, err
	}
	if m.Volume == "" && max > 1 && max <= a.Replicas && m.Deployed && m.Desired != "stopped" && m.Desired != "deleting" &&
		(m.Pending == nil || m.Pending.Action == "scale") && m.AppliedSpecification != "" && specification == m.AppliedSpecification {
		tasks, err := b.tasks(ctx, a.AppName)
		if err != nil {
			return infra.Op{}, err
		}
		running := 0
		for _, task := range tasks {
			if strings.EqualFold(task.State, "running") && strings.HasPrefix(strings.ToLower(task.CurrentState), "running") {
				running++
			}
		}
		scaleBack = running >= max
	}
	m.Min = min
	m.Max = max
	return b.reloadUpdated(ctx, a, m, map[string]any{"replicas": max}, "scale", scaleBack)
}
func (b *Backend) DeleteService(ctx context.Context, hostname string) (infra.Op, error) {
	return b.deleteService(ctx, hostname, "", false)
}
func (b *Backend) DeleteServiceByID(ctx context.Context, hostname, expectedID string) (infra.Op, error) {
	return b.deleteService(ctx, hostname, expectedID, true)
}

// Deletion owns the receipt fence: cleanup need not first make a failed or
// obsolete deployment healthy. deleteService still waits for terminal delivery.
func (*Backend) SupersedesDeploymentsOnDelete() bool { return true }
func (b *Backend) deleteService(ctx context.Context, hostname, expectedID string, fenced bool) (infra.Op, error) {
	if err := b.mu.Lock(ctx); err != nil {
		return infra.Op{}, err
	}
	defer b.mu.Unlock()
	a, err := b.application(ctx, hostname)
	if err != nil {
		return infra.Op{}, err
	}
	if fenced {
		var expected application
		err = b.c.get(ctx, "application.one", map[string]string{"applicationId": expectedID}, &expected)
		if err != nil && !errors.Is(err, infra.ErrNotFound) {
			return infra.Op{}, err
		}
		if err == nil {
			m, ok := parseMetadata(expected.Description)
			if !ok || m.Hostname != hostname || expected.Name != hostname || expected.Environment != b.target.ID || m.Environment != "" && m.Environment != b.target.ID || a != nil && a.ID != expectedID {
				return infra.Op{}, restoreConflict("expected application ID does not match hostname, environment or ownership; refusing fenced deletion")
			}
			a = &expected
			b.remember(hostname, expected.ID)
		} else if a != nil {
			return infra.Op{}, infra.ErrConflict
		}
	}
	if a == nil {
		_, err := b.orphanService(ctx, hostname)
		return infra.Op{}, err
	}
	if fenced && a.ID != expectedID {
		return infra.Op{}, infra.ErrConflict
	}
	m, _ := parseMetadata(a.Description)
	if m.Kind == infra.KindVolume {
		if err = b.checkPinnedNode(ctx, m.Node); err != nil {
			return infra.Op{}, err
		}
		e, err := b.environment(ctx)
		if err != nil {
			return infra.Op{}, err
		}
		searched, err := b.searchApplications(ctx, "")
		if err != nil {
			return infra.Op{}, err
		}
		e.Applications = append(e.Applications, searched...)
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
		if !m.Wiping {
			releaseSlot, err := b.deploymentSlot(ctx, a.ID)
			if err != nil {
				return infra.Op{}, err
			}
			submitted := false
			defer func() { releaseSlot(submitted) }()
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
			submitted = true
			if err = b.c.post(ctx, "application.deploy", map[string]any{"applicationId": a.ID}, nil); err != nil {
				if enqueueRefused(err) {
					submitted = false
					m.Wiping = false
					m.WipePreviousDeployment = ""
					m.WipePreviousTasks = nil
					if saveErr := b.update(ctx, a, m, nil); saveErr != nil {
						return infra.Op{}, errors.Join(err, saveErr)
					}
				}
				return infra.Op{}, err
			}
		}
		return encodeOp(o), nil
	}
	m.Desired = "deleting"
	if err = b.update(ctx, a, m, nil); err != nil {
		return infra.Op{}, err
	}
	if err = b.mutationReady(ctx, a, &m); err != nil {
		return infra.Op{}, err
	}
	o := operation{Action: "delete", AppID: a.ID, AppName: a.AppName, Hostname: hostname, Issued: b.nowTime().UTC()}
	if m.Pending != nil && m.Pending.Action == "delete" {
		if err := b.ensureDeleteStopped(ctx, a, &m, *m.Pending); err != nil {
			return infra.Op{}, err
		}
		return encodeOp(*m.Pending), nil
	}
	service, err := b.inspectService(ctx, a.AppName)
	if err != nil {
		return infra.Op{}, err
	}
	if service != nil {
		if !b.ownsService(*a, m, service) {
			return infra.Op{}, restoreConflict("cleanup service ownership changed")
		}
		o.ServiceIdentity = service.ID
	}
	m.Pending = &o
	m.DeleteStopConfirmed = false
	if err = b.update(ctx, a, m, nil); err != nil {
		return infra.Op{}, err
	}
	if err := b.ensureDeleteStopped(ctx, a, &m, o); err != nil {
		return infra.Op{}, err
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
	if err := b.retireDeployment(ctx, o.AppID); err != nil {
		return s, err
	}
	ns, err := b.nodes(ctx)
	if err != nil {
		return s, err
	}
	manager, found := false, false
	for _, n := range ns {
		if n.Description.Hostname != o.Node {
			continue
		}
		found = true
		if n.Status.State != "ready" {
			return s, silentFailure("volume wipe completed but its original node is unavailable; restore the node before confirming final volume cleanup")
		}
		manager = n.Spec.Role == "manager"
		break
	}
	if !found {
		return s, silentFailure("volume wipe cleanup cannot identify the original node; restore its Swarm identity before acknowledging deletion")
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
	b.forgetDeleted(o.Hostname, o.AppID)
	s.Done = true
	s.Status = "done"
	return s, nil
}

// Hash the application fields that mechanizeDockerContainer turns into the
// native task specification. Counts and update order deliberately do not
// participate; staged environment/port/resource changes must not scale stop-first.
func (b *Backend) applicationSpecification(ctx context.Context, id string, changes map[string]any, deployment string) (string, error) {
	var application map[string]any
	if err := b.c.get(ctx, "application.one", map[string]string{"applicationId": id}, &application); err != nil {
		return "", err
	}
	specification := make(map[string]any)
	specification["deployment"] = deployment
	for _, key := range []string{
		"sourceType", "dockerImage", "registryId", "buildRegistryId", "env", "mounts", "ports", "networkSwarm",
		"cpuLimit", "cpuReservation", "memoryLimit", "memoryReservation", "command", "args", "healthCheckSwarm",
		"restartPolicySwarm", "placementSwarm", "labelsSwarm", "modeSwarm", "stopGracePeriodSwarm", "ulimitsSwarm", "endpointSpecSwarm",
	} {
		specification[key] = application[key]
		if value, ok := changes[key]; ok {
			specification[key] = value
		}
	}
	if environment, ok := application["environment"].(map[string]any); ok {
		specification["environmentEnv"] = environment["env"]
		if project, ok := environment["project"].(map[string]any); ok {
			specification["projectEnv"] = project["env"]
		}
	}
	data, err := json.Marshal(specification)
	if err != nil {
		return "", err
	}
	// The API credential is held by the control plane, not stored in application
	// metadata. Rotation safely invalidates old receipts until the next apply.
	return secrets.KeyedHash([]byte(b.c.currentToken()), append([]byte("dokploy-application-specification-v1\x00"), data...)), nil
}
