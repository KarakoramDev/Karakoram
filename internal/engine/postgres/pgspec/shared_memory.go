// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package pgspec

// SharedMemoryDefault returns the managed PostgreSQL/Patroni default for the
// parameters whose standby values must not be lower than the primary's values.
func SharedMemoryDefault(name string) (int64, bool) {
	switch name {
	case "max_connections":
		return 100, true
	case "max_prepared_transactions":
		return 0, true
	case "max_locks_per_transaction":
		return 64, true
	case "max_wal_senders":
		return 10, true
	case "max_worker_processes":
		return 8, true
	default:
		return 0, false
	}
}

// RestartRequest keeps ordinary restarts replica-only. PrimaryFirst is reserved
// for a verified shared-memory decrease, which cannot switch to an old-value peer.
type RestartRequest struct {
	PrimaryFirst bool `json:"primaryFirst,omitempty"`
}
