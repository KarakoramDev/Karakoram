// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package dokploy

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

var _ infra.ServingProxyUpdates = (*Backend)(nil)
var _ infra.ProxyDeploymentLookup = (*Backend)(nil)

// Keep failed-roll admission authority independently of the engine receipt and
// the current submission journal. A rejected/unsubmitted retry cannot erase it.
type proxyAdmissionFailure struct {
	Issued      time.Time `json:"issued"`
	ForceUpdate uint64    `json:"forceUpdate"`
}

func (f *proxyAdmissionFailure) matches(o *operation) bool {
	return o != nil && f.Issued.Equal(o.Issued) && f.ForceUpdate == o.ForceUpdate
}

func proxyTaskAdmissionAllowed(m metadata, service *swarmService, taskID string, forceUpdate uint64) bool {
	switch service.UpdateStatus.State {
	case "paused", "rollback_started", "rollback_paused", "rollback_completed":
		return false
	}
	if forceUpdate != service.Spec.TaskTemplate.ForceUpdate {
		return false
	}
	pending := m.Pending
	if pending != nil && pending.HealthGated && pending.Failed {
		return false
	}
	if failure := m.ProxyAdmissionFailure; failure != nil {
		// A new request alone cannot release failure authority: require
		// the accepted live generation and exclude retained predecessors.
		return pending != nil && pending.HealthGated && !pending.Failed && !failure.matches(pending) &&
			pending.ForceUpdate == forceUpdate && !contains(pending.PreviousTasks, taskID)
	}
	return true
}

// Legacy images retain the ordinary redeploy path, including their first agent
// upgrade: installing policy before uploading the new image must be safe for the
// incumbent image too. Only a successfully deployed probe marker proves that.
func (b *Backend) ProxyUpdatesPreserveServing(ctx context.Context, hostname string) (bool, error) {
	ctx = infra.ObservationContext(ctx)
	a, err := b.application(ctx, hostname)
	if err != nil || a == nil {
		return false, err
	}
	m, ok := parseMetadata(a.Description)
	return ok && m.Kind == infra.KindRuntime && m.Volume == "" && m.Deployed && m.ProxyServingProbe && m.Desired != "stopped" && m.Desired != "deleting", nil
}

// LookupProxyDeployment reads only the pre-upload journal. Request identity
// remains valid even after the control-plane build changes; no artifact render,
// policy update, upload or provider mutation is allowed during cancellation.
func (b *Backend) LookupProxyDeployment(ctx context.Context, hostname, requestID string) (*infra.Op, error) {
	ctx = infra.ObservationContext(ctx)
	_, m, err := b.require(ctx, hostname)
	if err != nil {
		return nil, err
	}
	if m.Pending == nil {
		return nil, nil
	}
	o := m.Pending
	// Released legacy deployment receipts have neither request identity nor a
	// serving gate. They cannot be an ambiguous serving upgrade, so cancellation
	// may retain native-readiness settlement. Check the gate explicitly: an
	// unidentified health-gated receipt is not evidence of a legacy deployment.
	if o.RequestID == "" && !o.HealthGated {
		return nil, nil
	}
	if requestID == "" || o.RequestID != requestID || o.Action != "deploy" || !o.HealthGated {
		return nil, restoreConflict("cannot attribute pending proxy deployment to the cancelled operation; manual recovery required")
	}
	op := encodeOp(*o)
	return &op, nil
}

func (b *Backend) prepareProxyRollout(ctx context.Context, a *application, m metadata, settings map[string]any, o *operation) error {
	service, err := b.inspectService(ctx, a.AppName)
	if err != nil {
		return err
	}
	if service == nil || !b.ownsService(*a, m, service) {
		return restoreConflict("cannot inspect the owned Swarm service before proxy rollout")
	}
	o.HealthGated = true
	o.ExpectedReplicas = m.Max
	o.ForceUpdate = service.Spec.TaskTemplate.ForceUpdate + 1
	// Install only on a running update, never bootstrap: initial deployment must
	// not hold DNS/deployment admission while engine dependencies are forming.
	env, err := proxyAdmissionEnv(a.Env, true)
	if err != nil {
		return err
	}
	settings["env"] = env
	settings["healthCheckSwarm"] = map[string]any{
		"Test":     []string{"CMD", layout.AppDir + "/karakoram", "agent", "readiness", "--serving-proxy", "--data-dir=" + layout.DataDir("")},
		"Interval": int64(5 * time.Second), "Timeout": int64(10 * time.Second), "StartPeriod": int64(60 * time.Second), "Retries": 3,
	}
	settings["updateConfigSwarm"] = map[string]any{"Parallelism": 1, "Order": "start-first", "FailureAction": "pause", "Monitor": int64(30 * time.Second), "MaxFailureRatio": 0}
	settings["rollbackConfigSwarm"] = map[string]any{"Parallelism": 1, "Order": "start-first", "FailureAction": "pause", "Monitor": int64(30 * time.Second), "MaxFailureRatio": 0}
	return nil
}

// These values belong to the Swarm task template, never the image's shell env
// file: Swarm expands the task ID separately for every replacement.
func proxyAdmissionEnv(current string, enabled bool) (string, error) {
	values, err := decodeEnvironment(current)
	if err != nil {
		return "", err
	}
	delete(values, "KARAKORAM_TASK_ID")
	delete(values, "KARAKORAM_PROXY_SERVING_ADMISSION")
	if enabled {
		values["KARAKORAM_TASK_ID"] = "{{.Task.ID}}"
		values["KARAKORAM_PROXY_SERVING_ADMISSION"] = "1"
	}
	return encodeEnvironment(values)
}

func (b *Backend) proxyRolloutStatus(ctx context.Context, o operation, a application, tasks []task, state infra.OpState) (infra.OpState, error) {
	service, err := b.inspectService(ctx, a.AppName)
	if err != nil {
		return state, err
	}
	if service == nil {
		return state, restoreConflict("proxy rollout service inspection is unavailable")
	}
	switch service.UpdateStatus.State {
	case "paused", "rollback_started", "rollback_paused", "rollback_completed":
		state.Failed = true
		state.Status = "failed"
		state.Message = "ProxySQL start-first rollout paused or rolled back; inspect incumbent tasks, replacement health, config pull access, writer readiness and spare capacity before retrying"
		return state, nil
	}
	if service.Spec.TaskTemplate.ForceUpdate != o.ForceUpdate {
		state.Failed = true
		state.Status = "failed"
		state.Message = "ProxySQL rollout spec changed or rolled back; refusing to commit a stale replacement receipt"
		return state, nil
	}
	ready, oldRunning := 0, false
	for _, task := range tasks {
		running := strings.HasPrefix(strings.ToLower(task.CurrentState), "running")
		if contains(o.PreviousTasks, task.ID) {
			oldRunning = oldRunning || running
		} else if strings.EqualFold(task.State, "running") && running {
			// Swarm's executor reports Running and activates DNS/VIP binding only after
			// the healthcheck succeeds. A starting/unhealthy replacement cannot count.
			ready++
		}
	}
	if ready == o.ExpectedReplicas && !oldRunning && service.UpdateStatus.State == "completed" && service.Spec.Mode.Replicated != nil && service.Spec.Mode.Replicated.Replicas != nil && int(*service.Spec.Mode.Replicated.Replicas) == o.ExpectedReplicas {
		state.Done, state.Status = true, "done"
		return state, nil
	}
	state.Message = fmt.Sprintf("waiting for %d/%d new healthy ProxySQL tasks and all old tasks to retire", ready, o.ExpectedReplicas)
	if !b.nowTime().Before(o.Issued.Add(taskStartupWindow)) {
		state.Failed, state.Status = true, "failed"
		state.Message = "ProxySQL start-first rollout timed out; inspect healthcheck/config pull, ONLINE writer and spare capacity; do not switch to stop-first to bypass admission"
	}
	return state, nil
}
