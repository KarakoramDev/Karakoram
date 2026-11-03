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

// Legacy images must retain the disruptive path: a global capability cannot
// establish that a previously deployed agent implements the local serving probe.
func (b *Backend) ProxyUpdatesPreserveServing(ctx context.Context, hostname string) (bool, error) {
	a, err := b.application(ctx, hostname)
	if err != nil || a == nil {
		return false, err
	}
	m, ok := parseMetadata(a.Description)
	return ok && m.Kind == infra.KindRuntime && m.Volume == "" && m.Deployed && m.ProxyServingProbe, nil
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
	settings["healthCheckSwarm"] = map[string]any{
		"Test":     []string{"CMD", layout.AppDir + "/karakoram", "agent", "readiness", "--serving-proxy", "--data-dir=" + layout.DataDir("")},
		"Interval": int64(5 * time.Second), "Timeout": int64(10 * time.Second), "StartPeriod": int64(60 * time.Second), "Retries": 3,
	}
	settings["updateConfigSwarm"] = map[string]any{"Parallelism": 1, "Order": "start-first", "FailureAction": "pause", "Monitor": int64(30 * time.Second), "MaxFailureRatio": 0}
	settings["rollbackConfigSwarm"] = map[string]any{"Parallelism": 1, "Order": "start-first", "FailureAction": "pause", "Monitor": int64(30 * time.Second), "MaxFailureRatio": 0}
	return nil
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
		state.Message = "ProxySQL start-first rollout paused or rolled back; incumbents were retained. Inspect replacement health, config pull access, writer readiness and spare capacity before retrying"
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
