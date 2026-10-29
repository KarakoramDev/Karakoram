// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package engine

import (
	"slices"
	"strings"

	"github.com/KarakoramDev/Karakoram/internal/domain"
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
	equal := !a.Admitted.Public && !desired.Public ||
		a.Admitted.Public == desired.Public &&
			publicIPFamily(a.Admitted.IPType) == publicIPFamily(desired.IPType) &&
			slices.Equal(a.Admitted.Allowlist, desired.Allowlist) &&
			a.Admitted.AllowAllAddresses == desired.AllowAllAddresses
	if !equal {
		if rc.Cluster.Status.ObservedGeneration != 0 && rc.WaitForWindow().RequeueAfter > 0 {
			return *a.Admitted, true
		}
		v := desired
		v.Allowlist = slices.Clone(desired.Allowlist)
		a.Admitted = &v
	}
	return *a.Admitted, false
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
