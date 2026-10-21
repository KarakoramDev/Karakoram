// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package myspec

import "testing"

func TestGTIDContains(t *testing.T) {
	const sid = "11111111-2222-3333-4444-000000000001"
	for _, tc := range []struct {
		have, want string
		contains   bool
	}{{sid + ":1-10:12-20," + sid + ":11", sid + ":3-18", true}, {sid + ":1-10:12-20", sid + ":3-18", false}, {sid + ":tag:1-10", sid + ":tag:2-9", true}, {sid + ":tag:1-10", sid + ":other:2-9", false}, {sid + ":1-10", "", true}, {"", sid + ":1", false}} {
		got, err := GTIDContains(tc.have, tc.want)
		if err != nil || got != tc.contains {
			t.Fatalf("%q contains %q: %v %v", tc.have, tc.want, got, err)
		}
	}
	for _, raw := range []string{"bad", sid + ":0", sid + ":3-2", sid + ":tag", sid + ":9223372036854775808"} {
		if _, err := GTIDContains(raw, ""); err == nil {
			t.Fatal("malformed GTID accepted", raw)
		}
	}
}
