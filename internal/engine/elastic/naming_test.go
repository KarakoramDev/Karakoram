// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"strings"
	"testing"
)

func TestNames(t *testing.T) {
	for _, tt := range []struct {
		name  string
		valid bool
	}{
		{"ab", true}, {"a1", true}, {"abcdefghijkl", true}, {"a", false}, {"abcdefghijklm", false}, {"1a", false}, {"Ab", false}, {"a-b", false}, {"éx", false}, {"ab\n", false}, {"", false},
	} {
		t.Run("cluster_"+tt.name, func(t *testing.T) {
			if ValidClusterName(tt.name) != tt.valid {
				t.Fatal("cluster name validity")
			}
		})
	}
	for _, tt := range []struct {
		name  string
		valid bool
	}{
		{"a", true}, {"1", true}, {strings.Repeat("a", 25), true}, {strings.Repeat("a", 26), false}, {"ab-c", false}, {"AB", false}, {"", false}, {"a\n", false},
	} {
		t.Run("host_"+tt.name, func(t *testing.T) {
			if ValidHostname(tt.name) != tt.valid {
				t.Fatal("hostname validity")
			}
		})
	}
	for _, tt := range []struct{ got, want string }{
		{NodeHostname("demo", 0), "demon0"}, {NodeHostname("demo", 123), "demon123"}, {MasterHostname("demo", 2), "demom2"}, {VolumeHostname("demom2"), "demom2v"}, {KibanaHostname("demo"), "demokb"}, {LogstashHostname("demo"), "demols"}, {APMHostname("demo"), "demoapm"},
	} {
		t.Run(tt.want, func(t *testing.T) {
			if tt.got != tt.want || !ValidHostname(tt.got) {
				t.Fatalf("got %q", tt.got)
			}
		})
	}
}
