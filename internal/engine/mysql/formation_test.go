// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import "testing"

const sid = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
const sid2 = "11111111-2222-3333-4444-555555555555"

func TestGTIDContainment(t *testing.T) {
	for _, tc := range []struct {
		name, have, want string
		contains         bool
	}{
		{"empty", "", "", true}, {"empty contains nothing", sid + ":1-5", "", true}, {"missing source", "", sid + ":1", false},
		{"holes", sid + ":1-5:7-9", sid + ":1-9", false}, {"split adjacent", sid + ":1-5:6-9", sid + ":1-9", true},
		{"unordered overlapping", sid + ":7-9:1-5:4-8", sid + ":1-9", true}, {"repeated source", sid + ":1-5," + sid + ":6-9", sid + ":1-9", true},
		{"multiple uuid", sid + ":1-9," + sid2 + ":1-3", sid + ":2-5," + sid2 + ":3", true},
		{"missing second uuid", sid + ":1-9", sid + ":1," + sid2 + ":1", false},
		{"tag", sid + ":blue:1-5:7-9", sid + ":blue:2-4:7", true}, {"different tag", sid + ":blue:1-9", sid + ":green:1", false},
		{"tag and untagged separate", sid + ":blue:1-9", sid + ":1", false}, {"tag case insensitive", sid + ":Blue:1-9", sid + ":blue:2", true},
		{"uuid case insensitive", "AAAAAAAA-BBBB-CCCC-DDDD-EEEEEEEEEEEE:1-9", sid + ":1", true},
		{"mixed tagged and untagged", sid + ":1-5," + sid + ":tag_1:1-7", sid + ":1-3," + sid + ":tag_1:1-4", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			have, err := parseGTIDSet(tc.have)
			if err != nil {
				t.Fatal(err)
			}
			want, err := parseGTIDSet(tc.want)
			if err != nil {
				t.Fatal(err)
			}
			if got := have.contains(want); got != tc.contains {
				t.Fatalf("contains=%v want=%v", got, tc.contains)
			}
		})
	}
}
func TestGTIDRejectsMalformed(t *testing.T) {
	for _, raw := range []string{"garbage", sid, sid + ":", sid + ":tag", sid + ":0", sid + ":2-1", sid + ":1-2-3", sid + ":-1", sid + ":1:", sid + ":9tag:1", sid + ":bad-tag:1", sid + ":9223372036854775808", sid + ":18446744073709551616", sid + ":1,"} {
		if _, err := parseGTIDSet(raw); err == nil {
			t.Errorf("accepted %q", raw)
		}
	}
}
func TestSelectSuperset(t *testing.T) {
	for _, tc := range []struct {
		name string
		sets map[string]string
		want string
	}{
		{"superset", map[string]string{"m0": sid + ":1-3", "m1": sid + ":1-5", "m2": sid + ":1-4"}, "m1"},
		{"equal deterministic", map[string]string{"m2": sid + ":1-3", "m0": sid + ":1-3"}, "m0"},
		{"incomparable", map[string]string{"m0": sid + ":1-3", "m1": sid + ":1-2:4"}, ""},
		{"two sources incomparable", map[string]string{"m0": sid + ":1", "m1": sid2 + ":1"}, ""},
		{"tag superset", map[string]string{"m0": sid + ":tag:1-5," + sid + ":1", "m1": sid + ":tag:1-4"}, "m0"},
		{"tags incomparable", map[string]string{"m0": sid + ":blue:1", "m1": sid + ":green:1"}, ""},
		{"empty histories", map[string]string{"m0": "", "m1": ""}, "m0"},
		{"invalid set", map[string]string{"m0": "bad", "m1": ""}, ""}, {"no observations", map[string]string{}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			observations := map[string]mysqloffline{}
			for name, set := range tc.sets {
				observations[name] = mysqloffline{GTID: set, UUID: sid}
			}
			got, err := selectSuperset(observations)
			if tc.want == "" {
				if err == nil {
					t.Fatal("expected blocked selection")
				}
				return
			}
			if err != nil || got != tc.want {
				t.Fatalf("got %q %v want %q", got, err, tc.want)
			}
		})
	}
}
func TestSelectSupersetRequiresIdentity(t *testing.T) {
	if _, err := selectSuperset(map[string]mysqloffline{"m0": {GTID: sid + ":1"}}); err == nil {
		t.Fatal("accepted unverified data identity")
	}
}
