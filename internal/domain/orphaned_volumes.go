// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package domain

import "time"

// OrphanedVolume records a verified volume detached from cluster lifecycle work.
// Its hostname is unique within its namespace; the service ID fences deletion.
type OrphanedVolume struct {
	Namespace     string    `json:"namespace,omitempty"`
	Hostname      string    `json:"hostname"`
	ServiceID     string    `json:"serviceId"`
	Cluster       string    `json:"cluster"`
	Member        string    `json:"member"`
	FirstError    string    `json:"firstError"`
	LastError     string    `json:"lastError"`
	FirstFailedAt time.Time `json:"firstFailedAt"`
	OrphanedAt    time.Time `json:"orphanedAt"`
	LastAttemptAt time.Time `json:"lastAttemptAt"`
	NextAttemptAt time.Time `json:"nextAttemptAt"`
	Attempts      int       `json:"attempts"`
	LastProcessID string    `json:"lastProcessId"`
	// Janitor bookkeeping is persisted separately, never exposed in API JSON.
	ProcessPending  bool `json:"-"`
	JanitorFailures int  `json:"-"`
}
