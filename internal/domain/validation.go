// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package domain

import (
	"fmt"
	"math/big"
	"net/netip"
	"regexp"
	"slices"
	"strings"
)

var clusterName = regexp.MustCompile(`^[a-z][a-z0-9]{1,11}$`)
var resourceName = regexp.MustCompile(`^[a-z][a-z0-9-]{0,62}$`)
var accountName = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9_.@-]{0,63}$`)

// ValidateClusterName enforces the Zerops-safe 2–12 character cluster naming rule.
func ValidateClusterName(name string) error {
	if !clusterName.MatchString(name) {
		return fmt.Errorf("invalid cluster name %q: use 2–12 lowercase alphanumeric characters starting with a letter", name)
	}
	return nil
}

// ValidateName enforces lowercase DNS-style resource naming.
func ValidateName(name string) error {
	if !resourceName.MatchString(name) {
		return fmt.Errorf("invalid resource name %q", name)
	}
	return nil
}

// ValidateAccountName accepts bounded local or OIDC account identifiers.
func ValidateAccountName(name string) error {
	if !accountName.MatchString(name) {
		return fmt.Errorf("invalid account name %q", name)
	}
	return nil
}

// ValidateDatabaseUserName rejects empty, oversized, or control-character user names.
func ValidateDatabaseUserName(name string) error {
	if name == "" || len(name) > 128 || strings.ContainsAny(name, "\x00\r\n") {
		return fmt.Errorf("invalid database user name")
	}
	return nil
}

// ValidateExposureUpdate preserves unchanged legacy routing on unrelated updates.
// Changing routing still requires the current family and acknowledgement rules.
func ValidateExposureUpdate(previous *Exposure, next Exposure) error {
	if previous != nil && previous.Public == next.Public &&
		exposureIPType(previous.IPType) == exposureIPType(next.IPType) &&
		slices.Equal(previous.Allowlist, next.Allowlist) {
		return nil
	}
	return ValidateExposure(next)
}

func exposureIPType(ipType string) string {
	if ipType == "" {
		return "IPV6"
	}
	return ipType
}

// ValidateExposure rejects unsafe and family-mismatched public firewall rules.
func ValidateExposure(exposure Exposure) error {
	ipType := exposureIPType(exposure.IPType)
	if ipType != "IPV4" && ipType != "IPV6" {
		return InvalidField("spec.exposure.ipType", ReasonInvalid, "spec.exposure.ipType must be IPV4 or IPV6")
	}
	if exposure.Public && len(exposure.Allowlist) == 0 {
		return InvalidField("spec.exposure.allowlist", ReasonRequired, "spec.exposure.allowlist requires at least one CIDR for public exposure")
	}
	var prefixes []netip.Prefix
	if !exposure.AllowAllAddresses {
		prefixes = make([]netip.Prefix, 0, len(exposure.Allowlist))
	}
	for i, cidr := range exposure.Allowlist {
		field := fmt.Sprintf("spec.exposure.allowlist.%d", i)
		prefix, err := netip.ParsePrefix(cidr)
		if err != nil {
			return InvalidField(field, ReasonInvalid, "%s must be a valid CIDR matching spec.exposure.ipType (%s)", field, ipType)
		}
		if prefix.Addr().Is4() != (ipType == "IPV4") {
			return InvalidField(field, ReasonInvalid, "%s must use the %s address family selected by spec.exposure.ipType", field, ipType)
		}
		if prefix.Bits() == 0 && !exposure.AllowAllAddresses {
			return InvalidField(field, ReasonRequired, "%s covers every address; explicitly acknowledge it with spec.exposure.allowAllAddresses: true", field)
		}
		if !exposure.AllowAllAddresses {
			prefixes = append(prefixes, prefix.Masked())
		}
	}
	if prefixesCoverFamily(prefixes) {
		return InvalidField("spec.exposure.allowlist", ReasonRequired, "spec.exposure.allowlist covers every address; explicitly acknowledge it with spec.exposure.allowAllAddresses: true")
	}
	return nil
}

// ExposureCoversFamily reports whether valid CIDRs cover the configured address
// family completely. It ignores Public and AllowAllAddresses; invalid or
// family-mismatched legacy allowlists do not qualify.
func ExposureCoversFamily(exposure Exposure) bool {
	ipType := exposureIPType(exposure.IPType)
	if ipType != "IPV4" && ipType != "IPV6" {
		return false
	}
	prefixes := make([]netip.Prefix, 0, len(exposure.Allowlist))
	for _, cidr := range exposure.Allowlist {
		prefix, err := netip.ParsePrefix(cidr)
		if err != nil || prefix.Addr().Is4() != (ipType == "IPV4") {
			return false
		}
		prefixes = append(prefixes, prefix.Masked())
	}
	return prefixesCoverFamily(prefixes)
}

// CIDR ranges are either disjoint or nested. Sorting by network address, then
// shortest prefix, lets us omit duplicates and contained ranges before summing.
func prefixesCoverFamily(prefixes []netip.Prefix) bool {
	if len(prefixes) == 0 {
		return false
	}
	slices.SortFunc(prefixes, func(a, b netip.Prefix) int {
		if order := a.Addr().Compare(b.Addr()); order != 0 {
			return order
		}
		return a.Bits() - b.Bits()
	})
	var total, size, full big.Int
	full.SetInt64(1)
	full.Lsh(&full, uint(prefixes[0].Addr().BitLen()))
	var previous netip.Prefix
	for _, prefix := range prefixes {
		if previous.IsValid() && previous.Contains(prefix.Addr()) {
			continue
		}
		size.SetInt64(1)
		size.Lsh(&size, uint(prefix.Addr().BitLen()-prefix.Bits()))
		total.Add(&total, &size)
		previous = prefix
	}
	return total.Cmp(&full) >= 0
}
