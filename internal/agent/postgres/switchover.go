// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import "strings"

// Native response bodies are untrusted. Export only a closed vocabulary, never
// raw Patroni text (which can include member identities or service diagnostics).
func classifySwitchoverRejection(body string) string {
	switch strings.ToLower(strings.TrimSpace(body)) {
	case "leader name does not match", "switchover target and source are the same":
		return "switchover_leader_changed"
	case "candidate name does not match with sync_standby", "candidate does not exists", "switchover is not possible: can not find sync_standby", "switchover is not possible: no good candidates have been found":
		return "switchover_candidate_changed"
	case "switchover is already scheduled", "another switchover is already scheduled":
		return "switchover_in_progress"
	case "can't schedule switchover in the paused state", "switchover is possible only to a specific candidate in a paused state":
		return "switchover_paused"
	default:
		return "switchover_rejected"
	}
}

func safeSwitchoverRejection(err error) string {
	switch err.Error() {
	case "switchover_leader_changed", "switchover_candidate_changed", "switchover_in_progress", "switchover_paused", "switchover_unavailable":
		return err.Error()
	case "internal PostgreSQL service unavailable":
		return "switchover_unavailable"
	default:
		return "switchover_rejected"
	}
}
