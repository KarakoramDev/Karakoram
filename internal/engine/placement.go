// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package engine

import (
	"context"
	"fmt"
	"slices"
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

// ReplacementPlacementAvoid fences a replacement away from every surviving
// voter, using runtime placement rather than retained volume-holder counts.
// The result belongs in the durable allocation before an import can be frozen.
// Providers without an authoritative eligible-node inventory keep their existing
// placement behavior. Capacity admission remains the provider's responsibility.
func ReplacementPlacementAvoid(ctx context.Context, rc *Context, survivors, explicit []string) ([]string, error) {
	nodes, ok := rc.Backend.(infra.PlacementNodes)
	if !ok || !rc.Backend.Capabilities().Placement {
		return explicit, nil
	}
	inventory, err := nodes.PlacementSnapshot(ctx)
	if err != nil {
		return nil, err
	}
	avoid := make([]string, 0, len(explicit)+len(survivors))
	for _, group := range explicit {
		canonical := inventory.CanonicalGroup(group)
		if !domain.KnownPlacementGroup(canonical) {
			return nil, fmt.Errorf("replacement waits for authoritative identity of excluded node %s: %w", group, ErrUnavailable)
		}
		avoid = append(avoid, canonical)
	}
	for _, name := range survivors {
		containers, err := rc.Backend.Containers(ctx, name)
		if err != nil {
			return nil, fmt.Errorf("replacement waits for authoritative placement of %s: %w", name, err)
		}
		known := false
		for _, container := range containers {
			canonical := inventory.CanonicalGroup(container.PlacementGroup)
			if domain.KnownPlacementGroup(canonical) {
				known = true
				avoid = append(avoid, canonical)
			}
		}
		if !known {
			return nil, fmt.Errorf("replacement waits for authoritative placement of %s; restore placement observation before retrying: %w", name, ErrUnavailable)
		}
	}
	slices.Sort(avoid)
	avoid = slices.Compact(avoid)
	for _, group := range inventory.Eligible {
		if !slices.Contains(avoid, group) {
			return avoid, nil
		}
	}
	return nil, fmt.Errorf("replacement requires an eligible node without another voting data member; add or reactivate a data node, provide CPU/memory headroom, and retry; refusing co-location: %w", infra.ErrPlacementUnsatisfiable)
}

// ObserveFailureDomains assesses only the engine's voting data identities,
// never proxies, learners, or retained storage. Current occupied domains count
// even if ineligible for new allocations; eligible free domains enable repair.
func ObserveFailureDomains(ctx context.Context, rc *Context, status *domain.ClusterStatus, voters []string) error {
	nodes, ok := rc.Backend.(infra.PlacementNodes)
	if !ok || !rc.Backend.Capabilities().Placement || len(voters) < 2 || rc.Cluster.Deleting {
		return nil
	}
	inventory, err := nodes.PlacementSnapshot(ctx)
	if err != nil {
		return err
	}
	condition := domain.Condition{Type: "FailureDomainsSeparated", Status: "True", Reason: "DistinctNodes", LastTransitionTime: deploymentNow(rc)}
	groups := make(map[string][]string)
	unknown := false
	for _, name := range voters {
		group := ""
		for _, member := range status.Members {
			if member.Name == name {
				group = member.PlacementGroup
				break
			}
		}
		group = inventory.CanonicalGroup(group)
		if !domain.KnownPlacementGroup(group) {
			unknown = true
		} else {
			groups[group] = append(groups[group], name)
		}
	}
	var shared []string
	for group, members := range groups {
		if len(members) > 1 {
			shared = append(shared, group)
		}
	}
	slices.Sort(shared)
	if unknown {
		condition.Status, condition.Reason = "Unknown", "PlacementUnknown"
		condition.Message = "Voting data member placement is incomplete; inspect clusters members before changing membership"
	} else if len(shared) > 0 {
		// Existing voters can remain on nodes no longer eligible for new
		// allocations. Count their occupied domains alongside eligible free
		// domains; eligibility alone is not the current failure-domain count.
		occupied := len(groups)
		for _, group := range inventory.Eligible {
			if _, known := groups[group]; !known {
				groups[group] = nil
			}
		}
		if len(groups) == occupied || len(groups) < len(voters) {
			condition.Status, condition.Reason = "Unknown", "InsufficientEligibleNodes"
			condition.Message = "Voting data members share nodes, but occupied domains plus eligible unoccupied nodes are insufficient for separation; add or reactivate eligible nodes before replacement"
		} else {
			node := shared[0]
			members := slices.Clone(groups[node])
			slices.Sort(members)
			condition.Status, condition.Reason = "False", "SharedNode"
			condition.Message = fmt.Sprintf("Voting data members %s share node %s while an eligible node has no cluster member; after verifying quorum and capacity, repair with: karactl -n %s clusters replace-member %s %s --wait", strings.Join(members, ", "), node, domain.NormalizedNamespace(rc.Cluster.Namespace), rc.Cluster.Name, members[len(members)-1])
		}
	}
	for _, old := range rc.Cluster.Status.Conditions {
		if old.Type == condition.Type && old.Status == condition.Status && old.Reason == condition.Reason && old.Message == condition.Message {
			condition.LastTransitionTime = old.LastTransitionTime
			break
		}
	}
	status.Conditions = append(status.Conditions, condition)
	if condition.Status == "False" {
		if status.Phase == domain.PhaseReady {
			status.Phase = domain.PhaseDegraded
		}
		for i := range status.Conditions {
			c := &status.Conditions[i]
			if (c.Type == "Ready" || c.Type == "Healthy") && c.Status == "True" {
				c.Status, c.Reason, c.Message = "False", condition.Reason, condition.Message
			}
		}
		status.Message = condition.Message
		// The status publisher records SharedNode only when this condition
		// transitions in the same transaction as the persisted status. An
		// observation's input snapshot cannot deduplicate concurrent writers.
	}
	return nil
}
