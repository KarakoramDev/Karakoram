// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package infra

import "context"

// PlacementFailure is a current scheduler rejection, not a runtime failure.
// Message must contain only safe scheduling diagnostics, never job environments.
type PlacementFailure struct {
	Member, Node, Message string
}

// PlacementObserver optionally exposes read-only scheduler diagnostics for
// explicit engine-owned member identities. Hostname prefixes are not ownership
// boundaries. An empty successful sample clears previous failures.
type PlacementObserver interface {
	PlacementFailures(context.Context, []string) ([]PlacementFailure, error)
}

// ResourceUpdatePlanner admits replacement placement before engine-owned
// disruption (for example taking a database offline). It does not reserve
// capacity; SetResources must still check immediately before its own stop.
type ResourceUpdatePlanner interface {
	PlanResources(context.Context, string, Resources) error
}

// UnplacedRuntimeObserver proves a runtime's initial deployment has never
// acquired an allocation. Empty live container inventory is not such proof:
// terminal or garbage-collected allocations may have served database traffic.
type UnplacedRuntimeObserver interface {
	RuntimeNeverPlaced(context.Context, string) (bool, error)
}

// UnplacedResourceUpdater conditionally supersedes an exact placement-blocked
// receipt: an initial deployment with no allocation history, or a stateless
// resource rollout with only older allocations. The backend owns admission and
// scheduling fences; observing then calling SetResources is not conditional.
type UnplacedResourceUpdater interface {
	ResizeUnplacedRuntime(context.Context, string, Op, Resources) (Op, bool, error)
}

// DeploymentSuperseder promises that deleting a service fences an outstanding
// deployment, including delayed writes, without waiting for runtime readiness.
// Engines must still resolve imports and preserve exact service identities.
type DeploymentSuperseder interface {
	SupersedesDeploymentsOnDelete() bool
}
