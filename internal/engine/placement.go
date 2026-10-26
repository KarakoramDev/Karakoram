// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package engine

import (
	"context"
	"strings"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

// CleanupSupersedesDeployment discards only runtime mutation readiness waits,
// and only when deletion itself provides the backend's durable mutation fence.
// Import outcomes and deletion operations must always be resolved normally.
func CleanupSupersedesDeployment(backend infra.Backend, op infra.Op) bool {
	s, ok := backend.(infra.DeploymentSuperseder)
	if !ok || !s.SupersedesDeploymentsOnDelete() {
		return false
	}
	switch op.Kind {
	case "deploy", "start", "restart", "resources", "scale", "ports":
		return true
	default:
		return false
	}
}

func observePlacementFailures(ctx context.Context, rc *Context, status *domain.ClusterStatus, recordEvents bool) error {
	observer, ok := rc.Backend.(infra.PlacementObserver)
	if !ok || rc.Cluster.Deleting {
		return nil
	}
	members := make([]string, len(status.Members))
	for i, member := range status.Members {
		members[i] = member.Name
	}
	failures, err := observer.PlacementFailures(ctx, members)
	if err != nil {
		return err
	}
	conditions := status.Conditions[:0]
	for _, c := range status.Conditions {
		if c.Reason != "BackendPlacementFailed" {
			conditions = append(conditions, c)
		}
	}
	status.Conditions = conditions
	for _, f := range failures {
		condition := domain.Condition{Type: "Placement/" + f.Member, Status: "False", Reason: "BackendPlacementFailed", Key: f.Member + "/" + f.Node, Message: f.Message, LastTransitionTime: deploymentNow(rc)}
		status.Conditions = append(status.Conditions, condition)
		if status.Phase == domain.PhaseReady {
			status.Phase = domain.PhaseDegraded
		}
		// Retain per-member diagnostics as well as the summary, without hiding
		// independent agent/process observations from the provider snapshot.
		for i := range status.Members {
			if status.Members[i].Name == f.Member {
				status.Members[i].Health = false
			}
		}
		known := false
		for _, old := range rc.Cluster.Status.Conditions {
			if old.Type == condition.Type && old.Reason == condition.Reason && old.Message == condition.Message {
				known = true
				break
			}
		}
		if !known && recordEvents && rc.Events != nil {
			if err := rc.Events.Record(ctx, domain.Event{Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Resource: f.Member, Type: "Warning", Reason: condition.Reason, Message: f.Message}); err != nil {
				return err
			}
		}
	}
	if len(failures) > 0 {
		for i := range status.Conditions {
			c := &status.Conditions[i]
			if (c.Type == "Ready" || c.Type == "Healthy") && c.Status == "True" {
				c.Status, c.Reason, c.Message = "False", "BackendPlacementFailed", failures[0].Message
			}
		}
		messages := make([]string, len(failures))
		for i, f := range failures {
			messages[i] = f.Message
		}
		status.Message = strings.Join(messages, "; ")
	}
	return nil
}
