// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package myspec

import "testing"

func TestPXCFrozenHistoryContained(t *testing.T) {
	const state = "fc8b3a31-d49d-11f1-a52e-2a8aa2dd75f9"
	const inverse = "0374c5ce-2b62-ee0e-5ad1-d5755d228a06"
	const frozen = "bac91544-d464-11f1-a1f6-8b04c1d7e37c"
	seed := state + ":1-20," + frozen + ":1-711"
	for _, test := range []struct {
		name, member string
		want         bool
	}{
		{"copied inverse accounting", inverse + ":1-20," + frozen + ":1-711", true},
		{"no frozen extras", inverse + ":1", true},
		{"frozen growth", inverse + ":1-20," + frozen + ":1-712", false},
		{"local unique SID", inverse + ":1-20,aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:1", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := PXCFrozenHistoryContained(seed, test.member, state)
			if err != nil || got != test.want {
				t.Fatalf("contained=%t err=%v want%t", got, err, test.want)
			}
		})
	}
	if _, err := PXCFrozenHistoryContained(seed, "invalid", state); err == nil {
		t.Fatal("malformed member history accepted")
	}
}
