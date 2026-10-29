// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package engine

import (
	"context"
	"reflect"
	"slices"
	"strings"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

// ExposureAdmission is a durable cluster/component policy receipt, independent
// of runtime route application. Providers must retain it across runtime loss and
// suspension and persist Resolve's result before changing platform routes.
type ExposureAdmission struct {
	Admitted *domain.Exposure `json:"admitted,omitempty"`
}

// Seed migrates known previously applied policy, never current desired policy.
// Call before clearing legacy runtime receipts. It does not replace an existing
// admission, including when desired state has since changed.
func (a *ExposureAdmission) Seed(applied domain.Exposure) {
	if a.Admitted == nil {
		v := applied
		v.Allowlist = slices.Clone(applied.Allowlist)
		a.Admitted = &v
	}
}

// Resolve returns the policy that recovery may apply and whether the desired
// generation remains deferred. Initial provisioning is exempt. Legacy state
// without a recoverable policy starts private: a lost application receipt is
// never permission to publish current desired access outside a window.
// Keep reconciling the returned policy while deferred, but do not acknowledge
// the desired generation. A successful Resolve must be durably saved before
// any external mutation; admission is separate from application completion.
func (a *ExposureAdmission) Resolve(rc *Context, desired domain.Exposure) (effective domain.Exposure, deferred bool) {
	if a.Admitted == nil {
		if rc.Cluster.Status.ObservedGeneration == 0 || rc.Cluster.Status.ObservedGeneration == rc.Cluster.Generation {
			a.Seed(desired)
		} else {
			a.Seed(domain.Exposure{})
		}
	}
	// Private policy has no route whose IP family or allowlist can change.
	equal := !a.Admitted.Public && !desired.Public || reflect.DeepEqual(*a.Admitted, desired)
	if !equal {
		if rc.WaitForWindow().RequeueAfter > 0 {
			return *a.Admitted, true
		}
		v := desired
		v.Allowlist = slices.Clone(desired.Allowlist)
		a.Admitted = &v
	}
	return *a.Admitted, false
}

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
		if desired != nil && route.Port == desired.Port && route.TargetPort == desired.TargetPort && strings.EqualFold(route.Protocol, desired.Protocol) && strings.EqualFold(publicIPFamily(route.IPType), publicIPFamily(desired.IPType)) && slices.Equal(route.Allowlist, desired.Allowlist) {
			matched = true
		}
	}
	if desired == nil && count == 0 || desired != nil && matched {
		return false, nil
	}
	return rc.WaitForWindow().RequeueAfter > 0, nil
}

func publicIPFamily(value string) string {
	switch {
	case strings.EqualFold(value, "IPV4"), strings.EqualFold(value, "IP_V4"):
		return "IPV4"
	case strings.EqualFold(value, "IPV6"), strings.EqualFold(value, "IP_V6"):
		return "IPV6"
	default:
		return value
	}
}
