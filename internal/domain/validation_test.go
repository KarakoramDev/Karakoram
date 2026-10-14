// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package domain

import (
	"errors"
	"testing"
)

func TestExposureValidation(t *testing.T) {
	for _, tc := range []struct {
		name, ip, cidr string
		ack            bool
		field, reason  string
	}{
		{"restricted IPv4", "IPV4", "192.0.2.0/24", false, "", ""},
		{"restricted IPv6", "IPV6", "2001:db8::/64", false, "", ""},
		{"all IPv4", "IPV4", "0.0.0.0/0", false, "spec.exposure.allowlist.0", ReasonRequired},
		{"noncanonical all IPv4", "IPV4", "192.0.2.1/0", false, "spec.exposure.allowlist.0", ReasonRequired},
		{"all IPv6", "IPV6", "::/0", false, "spec.exposure.allowlist.0", ReasonRequired},
		{"ack IPv4", "IPV4", "0.0.0.0/0", true, "", ""},
		{"ack IPv6", "IPV6", "::/0", true, "", ""},
		{"IPv4 mismatch", "IPV6", "192.0.2.0/24", false, "spec.exposure.allowlist.0", ReasonInvalid},
		{"IPv6 mismatch", "IPV4", "2001:db8::/64", false, "spec.exposure.allowlist.0", ReasonInvalid},
		{"invalid CIDR", "IPV4", "192.0.2.0/99", false, "spec.exposure.allowlist.0", ReasonInvalid},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateExposure(Exposure{Public: true, IPType: tc.ip, Allowlist: []string{tc.cidr}, AllowAllAddresses: tc.ack})
			if tc.field == "" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			var f *FieldError
			if !errors.As(err, &f) || f.Field != tc.field || f.Reason != tc.reason {
				t.Fatal(err)
			}
		})
	}
}
