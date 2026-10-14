// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package domain

import (
	"fmt"
	"net/netip"
	"regexp"
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

// ValidateExposure rejects unsafe and family-mismatched public firewall rules.
func ValidateExposure(exposure Exposure) error {
	ipType := exposure.IPType
	if ipType == "" {
		ipType = "IPV6"
	}
	if ipType != "IPV4" && ipType != "IPV6" {
		return InvalidField("spec.exposure.ipType", ReasonInvalid, "spec.exposure.ipType must be IPV4 or IPV6")
	}
	if exposure.Public && len(exposure.Allowlist) == 0 {
		return InvalidField("spec.exposure.allowlist", ReasonRequired, "spec.exposure.allowlist requires at least one CIDR for public exposure")
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
	}
	return nil
}
