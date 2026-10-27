// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package myspec

import "testing"

func TestWriterEligibility(t *testing.T) {
	for _, test := range []struct {
		name, state, role                     string
		galera, readOnly, superReadOnly, want bool
	}{
		{name: "single", state: "ONLINE", role: "PRIMARY", want: true},
		{name: "GR primary", state: "ONLINE", role: "PRIMARY", want: true},
		{name: "GR secondary", state: "ONLINE", role: "SECONDARY"},
		{name: "GR recovering", state: "RECOVERING", role: "PRIMARY"},
		{name: "PXC synced", state: "Synced", role: "PRIMARY", galera: true, want: true},
		{name: "PXC donor", state: "Donor/Desynced", role: "PRIMARY", galera: true},
		{name: "PXC joiner", state: "Joining", role: "PRIMARY", galera: true},
		{name: "PXC not ready", state: "Synced", galera: true},
		{name: "PXC read only", state: "Synced", role: "PRIMARY", galera: true, readOnly: true},
		{name: "PXC super read only", state: "Synced", role: "PRIMARY", galera: true, superReadOnly: true},
		{name: "GR rejects Galera vocabulary", state: "Synced", role: "PRIMARY"},
	} {
		t.Run(test.name, func(t *testing.T) {
			st := StatusResponse{MemberState: test.state, MemberRole: test.role, ReadOnly: test.readOnly, SuperReadOnly: test.superReadOnly}
			if got := st.WriterEligible(test.galera); got != test.want {
				t.Fatalf("eligibility=%t want %t", got, test.want)
			}
		})
	}
}
