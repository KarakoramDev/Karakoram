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

func TestExposureUnionCoverage(t *testing.T) {
	for _, tc := range []struct {
		name, ip string
		cidrs    []string
		all      bool
	}{
		{"IPv4 halves", "IPV4", []string{"0.0.0.0/1", "128.0.0.0/1"}, true},
		{"IPv6 halves", "IPV6", []string{"::/1", "8000::/1"}, true},
		{"IPv4 quarters", "IPV4", []string{"192.0.0.0/2", "64.0.0.0/2", "0.0.0.0/2", "128.0.0.0/2"}, true},
		{"IPv6 quarters", "IPV6", []string{"c000::/2", "4000::/2", "::/2", "8000::/2"}, true},
		{"overlapping full IPv4", "IPV4", []string{"128.0.0.0/1", "64.0.0.0/2", "0.0.0.0/1", "0.0.0.0/1"}, true},
		{"overlapping full IPv6", "IPV6", []string{"8000::/1", "4000::/2", "::/1", "::/1"}, true},
		{"overlapping partial IPv4", "IPV4", []string{"0.0.0.0/1", "0.0.0.0/1", "64.0.0.0/2"}, false},
		{"overlapping partial IPv6", "IPV6", []string{"::/1", "::/1", "4000::/2"}, false},
		{"IPv4 gap", "IPV4", []string{"0.0.0.0/1", "192.0.0.0/2"}, false},
		{"IPv6 gap", "IPV6", []string{"::/1", "c000::/2"}, false},
		{"noncanonical halves", "IPV4", []string{"1.2.3.4/1", "192.0.2.1/1"}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			exposure := Exposure{Public: true, IPType: tc.ip, Allowlist: tc.cidrs}
			if all := ExposureCoversFamily(exposure); all != tc.all {
				t.Fatalf("family coverage = %t, want %t", all, tc.all)
			}
			err := ValidateExposure(exposure)
			if tc.all {
				var field *FieldError
				if !errors.As(err, &field) || field.Field != "spec.exposure.allowlist" || field.Reason != ReasonRequired {
					t.Fatalf("expected full-family acknowledgement: %v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			exposure.AllowAllAddresses = true
			if err := ValidateExposure(exposure); err != nil {
				t.Fatal(err)
			}
		})
	}
}
