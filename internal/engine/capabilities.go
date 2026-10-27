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

// CapabilityRequirer is implemented by providers whose cluster specs need
// optional backend capabilities beyond the engine-neutral ones returned by
// RequiredCapabilities (e.g. public HTTPS for a management UI, horizontal
// scaling for stateless routers). Names are infra capability names.
type CapabilityRequirer interface {
	RequiredCapabilities(domain.ClusterSpec) []string
}

// BackendValidator is implemented by providers with admission requirements on
// the backend's runtime contract, beyond its optional capability flags.
// A nil backend means the namespace backend could not be resolved.
type BackendValidator interface {
	ValidateBackend(infra.Backend) error
}

// RequiredCapabilities returns the backend capabilities a cluster spec needs
// regardless of its engine: public TCP exposure, its allowlist, and
// dedicated CPU for any component.
func RequiredCapabilities(spec domain.ClusterSpec) []string {
	var out []string
	if spec.Exposure.Public {
		out = append(out, "publicTcp")
		if exposureNeedsTCPAllowlist(spec.Exposure) {
			out = append(out, "tcpAllowlist")
		}
	}
	dedicated := func(c domain.ComponentSpec) bool { return strings.EqualFold(c.Resources.CPUMode, infra.CPUDedicated) }
	comps := spec.Components
	if dedicated(comps.Engine) || dedicated(comps.ConfigServer) || dedicated(comps.Router) {
		out = append(out, "dedicatedCpu")
	} else {
		for _, c := range comps.Extra {
			if dedicated(c) {
				out = append(out, "dedicatedCpu")
				break
			}
		}
	}
	return out
}

// exposureNeedsTCPAllowlist excludes explicitly acknowledged unrestricted
// exposure: there is no source-address restriction for the backend to enforce.
func exposureNeedsTCPAllowlist(exposure domain.Exposure) bool {
	return len(exposure.Allowlist) > 0 && !(exposure.AllowAllAddresses && domain.ExposureCoversFamily(exposure))
}

// CheckCapabilities rejects a spec that needs capabilities the backend lacks,
// so a cluster that could never converge is refused at admission. The error
// wraps ErrNotSupported and names the missing capabilities.
func CheckCapabilities(p Provider, spec domain.ClusterSpec, backend string, have infra.Capabilities) error {
	need := RequiredCapabilities(spec)
	if r, ok := p.(CapabilityRequirer); ok {
		need = append(need, r.RequiredCapabilities(spec)...)
	}
	supported := have.List()
	var missing []string
	for _, n := range need {
		if !slices.Contains(supported, n) && !slices.Contains(missing, n) {
			missing = append(missing, n)
		}
	}
	if len(missing) == 0 {
		return nil
	}
	return fmt.Errorf("%w: backend %s does not provide %s", ErrNotSupported, backend, strings.Join(missing, ", "))
}

// CheckDiskGrowthUpdate checks transitions, not initial sizing: creating a new
// component or adding replicas does not grow an existing volume.
func CheckDiskGrowthUpdate(previous, next domain.ClusterSpec, backend string, have infra.Capabilities) error {
	if have.DiskGrowth {
		return nil
	}
	grows := func(old, target domain.ComponentSpec) bool {
		return old.Replicas > 0 && target.Replicas > 0 &&
			(target.Resources.MinDisk > old.Resources.MinDisk || target.Resources.MaxDisk > old.Resources.MaxDisk)
	}
	old, target := previous.Components, next.Components
	if grows(old.Engine, target.Engine) || grows(old.ConfigServer, target.ConfigServer) || grows(old.Router, target.Router) {
		return fmt.Errorf("%w: backend %s does not provide diskGrowth", ErrNotSupported, backend)
	}
	for name, component := range target.Extra {
		if grows(old.Extra[name], component) {
			return fmt.Errorf("%w: backend %s does not provide diskGrowth", ErrNotSupported, backend)
		}
	}
	return nil
}

// CheckExposureSupport refuses only a positively unsupported public IP family.
// Unknown capability does not change admission or convergence behavior.
func CheckExposureSupport(ctx context.Context, backend infra.Backend, exposure domain.Exposure) (domain.Condition, error) {
	if !exposure.Public {
		return domain.Condition{}, nil
	}
	family := exposure.IPType
	if family == "" {
		family = "IPV6"
	}
	condition := domain.Condition{Type: "ExposureSupported", Status: "Unknown", Reason: "PublicIPSupportUnknown",
		Message: "Backend cannot currently confirm public " + family + " availability; routing will be attempted"}
	checker, ok := backend.(infra.PublicIPSupport)
	if !ok {
		return condition, nil
	}
	supported, err := checker.SupportsPublicIP(ctx, family)
	if err != nil {
		return condition, nil
	}
	if supported {
		condition.Status, condition.Reason = "True", "PublicIPAvailable"
		condition.Message = "Backend provides public " + family
		return condition, nil
	}
	condition.Status, condition.Reason = "False", "PublicIPTypeUnsupported"
	condition.Message = "spec.exposure.ipType " + family + " is unavailable in this namespace's backend project; select an available IP family, provision that public address, or disable public exposure"
	return condition, fmt.Errorf("%w: %s", ErrInvalid, condition.Message)
}
