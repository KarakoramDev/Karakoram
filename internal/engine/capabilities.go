// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package engine

import (
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
