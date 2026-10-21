// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package weaviate

import (
	"fmt"
	"regexp"
)

var clusterName = regexp.MustCompile(`^[a-z][a-z0-9]{1,11}$`)
var hostnamePattern = regexp.MustCompile(`^[a-z0-9]{1,25}$`)

func ValidClusterName(name string) bool { return clusterName.MatchString(name) }
func ValidHostname(name string) bool    { return hostnamePattern.MatchString(name) }

// NodeHostname uses a terminal letter so no index is a prefix of another in
// upstream's substring-based RAFT_JOIN voter matching. Padding is cosmetic;
// the terminator preserves disjointness even after the width is exhausted.
func NodeHostname(cluster string, index int) string { return fmt.Sprintf("%sn%06dx", cluster, index) }
func VolumeHostname(node string) string             { return node + "v" }

// NextMemberIndex preserves a durable high-water mark, including retired names.
func NextMemberIndex(highWater int, allocated []int) int {
	next := highWater
	for _, index := range allocated {
		if index >= next {
			next = index + 1
		}
	}
	return next
}
