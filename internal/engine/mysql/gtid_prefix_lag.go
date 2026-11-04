// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"slices"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

const pxcPrefixInFlightWindow = 5 * time.Second

type gtidPrefixLag struct {
	StateUUID    string                           `json:"stateUUID"`
	SID          string                           `json:"sid"`
	Members      []string                         `json:"members"`
	Epoch        uint64                           `json:"epoch"`
	Group        string                           `json:"group"`
	Identities   map[string]gtidMemberIdentity    `json:"identities"`
	FirstSeen    time.Time                        `json:"firstSeen"`
	LastSample   time.Time                        `json:"lastSample"`
	Samples      int                              `json:"samples"`
	Observations map[string]gtidPrefixObservation `json:"observations"`
}

type gtidPrefixObservation struct {
	LastCommitted int64  `json:"lastCommitted"`
	GTIDExecuted  string `json:"gtidExecuted"`
}

// Two independent complete samples of the same stationary discrepancy beyond
// the in-flight window turn apparent prefix lag at equal wsrep positions into
// a fail-safe divergence. Progress in either read invalidates the suspicion:
// repeated torn samples under writes are not evidence of one persistent gap.
// Cache hits retain their original timestamp and cannot count again.
func observePXCEqualPositionLag(s *state, observations map[string]myspec.GaleraStatusResponse, sampleAt time.Time, eligible bool) (*myspec.GTIDQuorumError, bool, bool) {
	sid, members := "", []string(nil)
	if eligible {
		sid, members = myspec.PXCEqualPositionPrefixDifferences(observations)
	}
	previous := s.GTIDPrefixLag
	if len(members) == 0 {
		s.GTIDPrefixLag = nil
		return nil, false, previous != nil
	}
	stateUUID := observations[members[0]].StateUUID
	matches := previous != nil && previous.StateUUID == stateUUID && previous.SID == sid && slices.Equal(previous.Members, members)
	if matches {
		matches = previous.Epoch == s.Formation.FormationEpoch && previous.Group == s.Formation.GroupName && len(previous.Identities) == len(s.data()) && len(previous.Observations) == len(observations)
		for _, m := range s.data() {
			identity, ok := previous.Identities[m.Name]
			if !ok || identity != (gtidMemberIdentity{ServerUUID: m.ServerUUID, ServiceID: m.ServiceID, VolumeID: m.VolumeID, NativeUUID: observations[m.Name].ServerUUID}) {
				matches = false
				break
			}
			status := observations[m.Name]
			if previous.Observations[m.Name] != (gtidPrefixObservation{LastCommitted: status.LastCommitted, GTIDExecuted: status.GTIDExecuted}) {
				matches = false
				break
			}
		}
	}
	if !matches {
		s.GTIDPrefixLag = &gtidPrefixLag{StateUUID: stateUUID, SID: sid, Members: members, Epoch: s.Formation.FormationEpoch, Group: s.Formation.GroupName, Identities: gtidMemberIdentities(s, observations), FirstSeen: sampleAt, LastSample: sampleAt, Samples: 1}
		s.GTIDPrefixLag.Observations = make(map[string]gtidPrefixObservation, len(observations))
		for name, status := range observations {
			s.GTIDPrefixLag.Observations[name] = gtidPrefixObservation{LastCommitted: status.LastCommitted, GTIDExecuted: status.GTIDExecuted}
		}
		return nil, true, true
	}
	changed := sampleAt.After(previous.LastSample)
	if changed {
		previous.LastSample = sampleAt
		previous.Samples++
	}
	if previous.Samples >= 2 && sampleAt.Sub(previous.FirstSeen) >= pxcPrefixInFlightWindow {
		return &myspec.GTIDQuorumError{Diverged: true, Members: append([]string(nil), members...)}, true, changed
	}
	return nil, true, changed
}
