// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package engine

import (
	"context"
	"strings"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

// ObserveRuntimeStops adds current platform shutdown diagnostics without treating
// elapsed time as fencing proof. It also runs during deletion, when ordinary
// provider health sweeps are intentionally skipped.
func ObserveRuntimeStops(ctx context.Context, rc *Context, status *domain.ClusterStatus, recordEvents bool) error {
	observer, ok := rc.Backend.(infra.RuntimeStopObserver)
	if !ok {
		return nil
	}
	probe, cancel := context.WithTimeout(ctx, ObservationSweepTimeout)
	defer cancel()
	members := make([]string, len(status.Members))
	for i, m := range status.Members {
		members[i] = m.Name
	}
	failures, err := observer.RuntimeStopFailures(infra.ObservationContext(probe), members)
	if err != nil {
		// A fresh provider snapshot may omit platform diagnostics. Only a
		// successful inventory can clear the persisted stop warning; retaining
		// it also preserves event deduplication after this failed sweep is saved.
		for _, old := range rc.Cluster.Status.Conditions {
			if !strings.HasPrefix(old.Type, "RuntimeStop/") {
				continue
			}
			found := false
			for i := range status.Conditions {
				if status.Conditions[i].Type == old.Type {
					status.Conditions[i] = old
					found = true
					break
				}
			}
			if !found {
				status.Conditions = append(status.Conditions, old)
			}
		}
		return err
	}
	previous := rc.Cluster.Status.Conditions
	// Keep the previous snapshot intact even when status is rc.Cluster.Status.
	conditions := make([]domain.Condition, 0, len(status.Conditions)+len(failures))
	for _, c := range status.Conditions {
		if c.Reason != "BackendStopStalled" {
			conditions = append(conditions, c)
		}
	}
	messages := make([]string, 0, len(failures))
	for _, f := range failures {
		condition := domain.Condition{Type: "RuntimeStop/" + f.Allocation, Status: "False", Reason: "BackendStopStalled", Key: f.Member + "/" + f.Allocation, Message: f.Message, LastTransitionTime: deploymentNow(rc)}
		known := false
		for _, old := range previous {
			if old.Type == condition.Type && old.Reason == condition.Reason && old.Message == condition.Message {
				known = true
				condition.LastTransitionTime = old.LastTransitionTime
				break
			}
		}
		if !known && recordEvents && rc.Events != nil {
			if err := rc.Events.Record(ctx, domain.Event{Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Resource: f.Member, Type: "Warning", Reason: condition.Reason, Message: condition.Message}); err != nil {
				return err
			}
		}
		conditions = append(conditions, condition)
		messages = append(messages, f.Message)
		for i := range status.Members {
			if status.Members[i].Name == f.Member {
				status.Members[i].Health = false
			}
		}
	}
	if len(failures) > 0 {
		if status.Phase == domain.PhaseReady {
			status.Phase = domain.PhaseDegraded
		}
		for i := range conditions {
			c := &conditions[i]
			if readinessCondition(*c) && c.Status == "True" {
				c.Status, c.Reason, c.Message = "False", "BackendStopStalled", failures[0].Message
			}
		}
		status.Message = strings.Join(messages, "; ")
	}
	status.Conditions = conditions
	return nil
}
