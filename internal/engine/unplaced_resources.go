// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package engine

import (
	"context"
	"errors"
	"reflect"
	"strings"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

// ResizeUnplacedDeployment lets corrected capacity supersede a placement-blocked
// rollout before waiting for it. Initial deployment requires no allocation history;
// a stateless resource rollout may retain serving predecessors but must not have
// allocated its new version. The backend owns that distinction and its fences.
// The replacement receipt and target resources must be persisted together.
func ResizeUnplacedDeployment(ctx context.Context, rc *Context, name string, desired infra.Resources, applied *infra.Resources, op *infra.Op) (bool, error) {
	if rc.Cluster.Deleting || op == nil || reflect.DeepEqual(*applied, desired) {
		return false, nil
	}
	capability, ok := rc.Backend.(infra.RuntimeResourceDeployment)
	if !ok || !capability.ResourceUpdatesDeployRuntime() {
		return false, nil
	}
	updater, ok := rc.Backend.(infra.UnplacedResourceUpdater)
	if !ok {
		return false, nil
	}
	replacement, changed, err := updater.ResizeUnplacedRuntime(ctx, name, *op, desired)
	if err != nil || !changed {
		var exhausted *infra.DeploymentExhaustedError
		if errors.As(err, &exhausted) && strings.HasPrefix(exhausted.LastError, "UnplacedResizeDeferred:") && rc.Events != nil {
			err = errors.Join(err, rc.Events.Record(ctx, domain.Event{Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Type: "Warning", Reason: "UnplacedResizeDeferred", Message: exhausted.LastError}))
		}
		return false, err
	}
	*op = replacement
	*applied = desired
	return true, nil
}

// ApplyDeploymentResourceRollback restores the provider's applied-resource view
// from durable conditional-resize failure evidence, including failures observed
// only after a later poll or control-plane restart.
func ApplyDeploymentResourceRollback(err error, applied *infra.Resources) bool {
	var exhausted *infra.DeploymentExhaustedError
	if !errors.As(err, &exhausted) || exhausted.PreviousResources == nil {
		return false
	}
	*applied = *exhausted.PreviousResources
	return true
}

// PrepareInitialDeploymentResources brings capacity up to date before an
// imported member's already-authorized ordinary deployment/re-arm. A raced
// bootstrap may have initialized storage before stopping; this is not another
// never-placed shortcut. The normal stop/deploy lifecycle and durable failure
// fence apply. The provider tracks the resource operation before deploying.
func PrepareInitialDeploymentResources(ctx context.Context, rc *Context, name string, spec infra.RuntimeSpec, desired, applied infra.Resources) (infra.Op, bool, error) {
	capability, ok := rc.Backend.(infra.RuntimeResourceDeployment)
	if !ok || !capability.ResourceUpdatesDeployRuntime() || reflect.DeepEqual(desired, applied) {
		return infra.Op{}, false, nil
	}
	if DeploymentBlocked(rc, name, spec) {
		f := rc.Cluster.Status.DeploymentFailures[name]
		return infra.Op{}, false, &infra.DeploymentExhaustedError{Service: name, Version: f.Version, Attempts: f.Attempts, LastError: f.LastError}
	}
	op, err := rc.Backend.SetResources(ctx, name, desired)
	return op, err == nil, err
}
