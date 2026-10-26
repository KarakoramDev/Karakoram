// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"strconv"
	"strings"

	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
)

func walPosition(value string) (uint64, bool) {
	hi, lo, ok := strings.Cut(value, "/")
	if !ok {
		return 0, false
	}
	h, err := strconv.ParseUint(hi, 16, 32)
	if err != nil {
		return 0, false
	}
	l, err := strconv.ParseUint(lo, 16, 32)
	return h<<32 | l, err == nil
}

// Samples straddling a primary/timeline change are unknown, not zero lag.
func replicaDistance(primary, replica pgspec.StatusResponse) (uint64, bool) {
	if !primary.Running || primary.Role != "primary" || primary.SystemID == "" || primary.SystemID != replica.SystemID || primary.Timeline < 1 || primary.Timeline != replica.Timeline {
		return 0, false
	}
	head, ok := walPosition(primary.WALPosition)
	if !ok {
		return 0, false
	}
	replay, ok := walPosition(replica.WALPosition)
	if !ok {
		return 0, false
	}
	// The replica may be sampled later than the primary. It has already crossed
	// the observed primary barrier in that case, on the same identity/timeline.
	if replay >= head {
		return 0, true
	}
	return head - replay, true
}
