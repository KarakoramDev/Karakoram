// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package engine

import (
	"context"
	"slices"
	"strings"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

// PublicTCPDeferred gates an actual route mutation, not an idempotent routing
// observation. Nil desired means withdraw every route for this runtime. Initial
// provisioning remains exempt. Callers continue recovery but must not acknowledge
// their desired generation while this returns true.
func PublicTCPDeferred(ctx context.Context, rc *Context, member string, desired *infra.PublicPort) (bool, error) {
	if rc.Cluster.Status.ObservedGeneration == 0 {
		return false, nil
	}
	// Open windows and overrides need no extra platform inventory read on the
	// idle ensure path. Clear a prior wait when permission has changed.
	if allowed, _ := rc.DisruptionAllowed(); allowed {
		for _, condition := range rc.Cluster.Status.Conditions {
			if condition.Type == "MaintenanceWindow" && condition.Status == "False" {
				rc.WaitForWindow()
				break
			}
		}
		return false, nil
	}
	routes, err := rc.Backend.ListPublicPorts(ctx)
	if err != nil {
		return false, err
	}
	matched, count := false, 0
	for _, route := range routes {
		if route.Hostname != member {
			continue
		}
		count++
		if desired != nil && route.Port == desired.Port && route.TargetPort == desired.TargetPort && strings.EqualFold(route.Protocol, desired.Protocol) && strings.EqualFold(route.IPType, desired.IPType) && slices.Equal(route.Allowlist, desired.Allowlist) {
			matched = true
		}
	}
	if desired == nil && count == 0 || desired != nil && matched {
		return false, nil
	}
	return rc.WaitForWindow().RequeueAfter > 0, nil
}
