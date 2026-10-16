// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package agent

import "golang.org/x/sys/unix"

// affinityCPUs counts the CPUs this process may run on, or 0 when unknown.
func affinityCPUs() int {
	var set unix.CPUSet
	if unix.SchedGetaffinity(0, &set) != nil {
		return 0
	}
	return set.Count()
}
