// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"errors"
	"testing"
)

func TestSwitchoverRejectionClassification(t *testing.T) {
	for _, tc := range []struct{ body, want string }{
		{"leader name does not match", "switchover_leader_changed"},
		{"Switchover target and source are the same", "switchover_leader_changed"},
		{"candidate name does not match with sync_standby", "switchover_candidate_changed"},
		{"candidate does not exists", "switchover_candidate_changed"},
		{"switchover is not possible: no good candidates have been found", "switchover_candidate_changed"},
		{"switchover is not possible: can not find sync_standby", "switchover_candidate_changed"},
		{"Another switchover is already scheduled", "switchover_in_progress"},
		{"Can't schedule switchover in the paused state", "switchover_paused"},
		{"failed to write failover key into DCS", "switchover_rejected"},
		{"untrusted native diagnostic with private details", "switchover_rejected"},
	} {
		t.Run(tc.body, func(t *testing.T) {
			got := classifySwitchoverRejection(tc.body)
			if got != tc.want || safeSwitchoverRejection(errors.New(got)) != tc.want {
				t.Fatalf("got %s, want %s", got, tc.want)
			}
		})
	}
	if got := safeSwitchoverRejection(errors.New("private native diagnostic")); got != "switchover_rejected" {
		t.Fatal(got)
	}
}
