// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package domain

import (
	"strings"
	"testing"
)

func TestNamespaceClusterRefRoundTrip(t *testing.T) {
	for _, tc := range []struct {
		name string
		ref  ClusterRef
		key  string
		want ClusterRef
	}{
		{"default", ClusterRef{Namespace: DefaultNamespace, Name: "orders"}, "orders", ClusterRef{Namespace: DefaultNamespace, Name: "orders"}},
		{"omitted", ClusterRef{Name: "orders"}, "orders", ClusterRef{Namespace: DefaultNamespace, Name: "orders"}},
		{"other", ClusterRef{Namespace: "dev", Name: "orders"}, "dev:orders", ClusterRef{Namespace: "dev", Name: "orders"}},
		{"hyphens_and_digits", ClusterRef{Namespace: "project-2", Name: "orders-3"}, "project-2:orders-3", ClusterRef{Namespace: "project-2", Name: "orders-3"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.ref.Key(); got != tc.key {
				t.Fatalf("Key() = %q, want %q", got, tc.key)
			}
			if got := ParseClusterKey(tc.key); got != tc.want {
				t.Fatalf("ParseClusterKey(%q) = %+v, want %+v", tc.key, got, tc.want)
			}
			if got := ParseClusterKey(tc.ref.Key()).Key(); got != tc.key {
				t.Fatalf("canonical round trip = %q, want %q", got, tc.key)
			}
		})
	}
}

func TestValidateNamespaceName(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value string
		valid bool
	}{
		{"minimum_letters", "ab", true},
		{"minimum_digit_suffix", "a0", true},
		{"maximum_letters", strings.Repeat("a", 32), true},
		{"maximum_digit_suffix", "a" + strings.Repeat("-", 30) + "9", true},
		{"interior_hyphen", "team-dev", true},
		{"interior_digits", "team123", true},
		{"consecutive_hyphens", "a--b", true},
		{"default_syntax", DefaultNamespace, true},
		{"empty", "", false},
		{"one_character", "a", false},
		{"too_long", strings.Repeat("a", 33), false},
		{"leading_digit", "1a", false},
		{"leading_hyphen", "-ab", false},
		{"trailing_hyphen", "ab-", false},
		{"uppercase_prefix", "Ab", false},
		{"uppercase_interior", "aBc", false},
		{"uppercase_suffix", "abC", false},
		{"underscore", "team_dev", false},
		{"colon", "team:dev", false},
		{"slash", "team/dev", false},
		{"space", "team dev", false},
		{"newline", "ab\n", false},
		{"non_ascii", "téam", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateNamespaceName(tc.value)
			if (err == nil) != tc.valid {
				t.Fatalf("ValidateNamespaceName(%q) = %v, want valid=%t", tc.value, err, tc.valid)
			}
		})
	}
}
