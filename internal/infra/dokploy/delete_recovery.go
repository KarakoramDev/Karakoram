// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package dokploy

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

const deleteProgressWindow = 3 * time.Minute

// Attempted is persisted before the removal call. An uncertain response must not
// replay a destructive mutation, even after control-plane reconstruction.
type deleteRemoval struct {
	ServiceID string `json:"serviceId"`
	Attempted bool   `json:"attempted"`
}

type deploymentQueueRow struct {
	State string `json:"state"`
	Data  struct {
		ApplicationID string `json:"applicationId"`
	} `json:"data"`
}

func taskStateName(value string) string {
	fields := strings.Fields(strings.ToLower(value))
	if len(fields) != 0 {
		switch fields[0] {
		case "new", "allocated", "pending", "assigned", "accepted", "preparing", "ready", "starting", "running", "complete", "shutdown", "failed", "rejected", "remove", "orphaned":
			return fields[0]
		}
	}
	return "unknown"
}

func taskCounts(ts []task, desired bool) string {
	counts := map[string]int{}
	for _, t := range ts {
		value := t.CurrentState
		if desired {
			value = t.State
		}
		counts[taskStateName(value)]++
	}
	keys := make([]string, 0, len(counts))
	for key := range counts {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, fmt.Sprintf("%s=%d", key, counts[key]))
	}
	return strings.Join(parts, ",")
}

func (b *Backend) agedShutdown(ctx context.Context, o operation, a application, ts []task, state infra.OpState) (infra.OpState, error) {
	age := b.nowTime().Sub(o.Issued)
	if age < deleteProgressWindow {
		return state, nil
	}
	state.Message = fmt.Sprintf("Dokploy %s pending for %s; tasks desired[%s] current[%s]", o.Action, age.Truncate(time.Second), taskCounts(ts, true), taskCounts(ts, false))
	service, err := b.inspectService(ctx, o.AppName)
	m, ok := parseMetadata(a.Description)
	if err != nil || !ok || service == nil || !b.ownsService(a, m, service) || service.Spec.Mode.Replicated == nil || service.Spec.Mode.Replicated.Replicas == nil {
		state.Message += "; exact owned-service inspection unavailable; waiting without escalation"
		return state, nil
	}
	replicas := *service.Spec.Mode.Replicated.Replicas
	inconsistent := false
	knownTasks := len(ts) > 0
	for _, t := range ts {
		knownTasks = knownTasks && t.ID != "" && strings.HasPrefix(t.Name, o.AppName+".") && taskStateName(t.State) != "unknown" && taskStateName(t.CurrentState) != "unknown"
		inconsistent = inconsistent || strings.EqualFold(t.State, "running")
	}
	state.Message += fmt.Sprintf("; service desired replicas=%d", replicas)
	if replicas == 0 && inconsistent {
		state.Message += "; Swarm inconsistency: desired 0 but a task is desired-running"
	} else {
		state.Message += "; tasks still stopping"
		return state, nil
	}
	if m.DeleteRemoval != nil && m.DeleteRemoval.Attempted {
		state.Message += "; escalated removal already submitted; observing absence without replay"
		return state, nil
	}
	intent, authorized := infra.CleanupIntentFromContext(ctx)
	labels := service.Spec.TaskTemplate.ContainerSpec.Labels
	if o.Action != "delete" || !authorized || !intent.FullDelete || intent.Services[o.Hostname] != o.AppID ||
		a.ID != o.AppID || a.AppName != o.AppName || a.Name != o.Hostname || a.Environment != b.target.ID ||
		m.Environment != b.target.ID || m.Kind != infra.KindRuntime || m.Desired != "deleting" || !sameOperation(m.Pending, o) ||
		m.Pending.Uncertain || m.Importing || !m.MutationUntil.IsZero() || m.PortChange != nil || m.PortsPending || !knownTasks ||
		labels["karakoram.application"] != o.AppID || labels["karakoram.environment"] != b.target.ID ||
		labels["karakoram.owner"] == "" || o.ServiceIdentity != "" && o.ServiceIdentity != service.ID {
		state.Message += "; cleanup authorization or mutation fence prevents escalation"
		return state, nil
	}
	ds, err := b.deployments(ctx, o.AppID)
	if err != nil || ds == nil {
		state.Message += "; deployment inspection unavailable; waiting without escalation"
		return state, nil
	}
	for _, d := range ds {
		if d.Status != "done" && d.Status != "error" && d.Status != "cancelled" {
			state.Message += "; deployment may still commit; waiting without escalation"
			return state, nil
		}
	}
	var queue []deploymentQueueRow
	if err := b.c.get(ctx, "deployment.queueList", nil, &queue); err != nil || queue == nil {
		state.Message += "; deployment queue inspection unavailable; waiting without escalation"
		return state, nil
	}
	for _, row := range queue {
		if row.State == "completed" || row.State == "failed" {
			continue
		}
		if row.Data.ApplicationID == "" || row.Data.ApplicationID == o.AppID {
			state.Message += "; active or queued deployment may still commit; waiting without escalation"
			return state, nil
		}
	}
	// Revalidate after queue inspection. A changed/native-recreated service or
	// journal is not the object authorized by the earlier observation.
	var current application
	if err := b.c.get(ctx, "application.one", map[string]string{"applicationId": o.AppID}, &current); err != nil {
		state.Message += "; fresh application inspection unavailable; waiting without escalation"
		return state, nil
	}
	fresh, valid := parseMetadata(current.Description)
	exact, inspectErr := b.inspectService(ctx, o.AppName)
	if !valid || current.ID != a.ID || current.AppName != a.AppName || current.Environment != a.Environment ||
		fresh.description() != m.description() || inspectErr != nil || exact == nil || exact.ID != service.ID ||
		!b.ownsService(current, fresh, exact) || exact.Spec.Mode.Replicated == nil || exact.Spec.Mode.Replicated.Replicas == nil || *exact.Spec.Mode.Replicated.Replicas != 0 {
		state.Message += "; inspection changed or is stale; waiting without escalation"
		return state, nil
	}
	m.DeleteRemoval = &deleteRemoval{ServiceID: service.ID, Attempted: true}
	if err := b.update(ctx, &current, m, nil); err != nil {
		return state, err
	}
	state.Message += "; escalated removal: submitting final application deletion; awaiting observed absence"
	if err := b.c.post(ctx, "application.delete", map[string]any{"applicationId": o.AppID}, nil); err != nil {
		// Keep the persisted attempt fence for both refusal and uncertain delivery.
		// Subsequent polls retain the diagnosis and can still recognize absence.
		state.Message += "; removal response unavailable; preserving delivery fence"
	}
	return state, nil
}

// A saved delete journal precedes stop delivery. Recover that boundary without
// changing the receipt: stop is idempotent, unlike build/restore/final removal.
// An explicit native zero also confirms legacy receipts without resending stop.
func (b *Backend) ensureDeleteStopped(ctx context.Context, a *application, m *metadata, o operation) error {
	if m.DeleteStopConfirmed || !m.Deployed || m.DeleteRemoval != nil && m.DeleteRemoval.Attempted {
		return nil
	}
	if m.Desired != "deleting" || !sameOperation(m.Pending, o) || a.ID != o.AppID || a.AppName != o.AppName || a.Name != o.Hostname || a.Environment != b.target.ID {
		return restoreConflict("delete stop receipt no longer matches the owned application")
	}
	service, err := b.inspectService(ctx, o.AppName)
	if err != nil {
		return err
	}
	if service == nil {
		return nil
	}
	if !b.ownsService(*a, *m, service) || o.ServiceIdentity != "" && o.ServiceIdentity != service.ID ||
		service.Spec.Mode.Replicated == nil || service.Spec.Mode.Replicated.Replicas == nil {
		return restoreConflict("exact owned-service replica inspection unavailable; preserving the pending delete receipt")
	}
	if *service.Spec.Mode.Replicated.Replicas != 0 {
		if err := b.c.post(ctx, "application.stop", map[string]any{"applicationId": o.AppID}, nil); err != nil {
			if errors.Is(err, infra.ErrNotFound) {
				// Keep the receipt pending: the next poll must still observe
				// application and task absence rather than infer completion.
				return nil
			}
			return err
		}
	}
	m.DeleteStopConfirmed = true
	if err := b.update(ctx, a, *m, nil); err != nil {
		if errors.Is(err, infra.ErrNotFound) {
			return nil
		}
		return err
	}
	a.Description = m.description()
	return nil
}
