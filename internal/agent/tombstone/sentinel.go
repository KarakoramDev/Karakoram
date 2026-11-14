// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

// Package tombstone installs and verifies local resolution for retired members.
package tombstone

const SentinelIPv4 = "239.192.0.1"
const SentinelIPv6 = "ff18::4b4b:1"

// ValidHost accepts only a single provider-neutral member hostname.
func ValidHost(host string) bool {
	if len(host) == 0 || len(host) > 63 || host[0] < 'a' || host[0] > 'z' {
		return false
	}
	for _, c := range host[1:] {
		if c != '-' && (c < 'a' || c > 'z') && (c < '0' || c > '9') {
			return false
		}
	}
	return true
}
