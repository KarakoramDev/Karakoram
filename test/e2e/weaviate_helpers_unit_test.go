// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package e2e

import "testing"

// weaviateSequences checks exact membership, not nearVector ordering: distances
// may tie and order is not part of the persistence contract.
func weaviateSequences(values []int, count int) bool {
	if len(values) != count {
		return false
	}
	seen := make([]bool, count)
	for _, value := range values {
		if value < 0 || value >= count || seen[value] {
			return false
		}
		seen[value] = true
	}
	return true
}

func TestWeaviateSequences(t *testing.T) {
	for _, tc := range []struct {
		name   string
		values []int
		count  int
		want   bool
	}{
		{"ordered", []int{0, 1, 2}, 3, true},
		{"unordered", []int{2, 0, 1}, 3, true},
		{"duplicate", []int{0, 0, 2}, 3, false},
		{"missing", []int{0, 1}, 3, false},
		{"extra", []int{0, 1, 2, 3}, 3, false},
		{"negative", []int{-1, 1, 2}, 3, false},
		{"out-of-range", []int{0, 1, 3}, 3, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := weaviateSequences(tc.values, tc.count); got != tc.want {
				t.Fatalf("got %t want %t", got, tc.want)
			}
		})
	}
}
