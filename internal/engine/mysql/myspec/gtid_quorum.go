// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package myspec

import (
	"fmt"
	"sort"
	"strings"
)

// GTIDQuorumError distinguishes proven conflicting histories from an unavailable
// healthy baseline. Members contains names only, never agent errors or secrets.
type GTIDQuorumError struct {
	Diverged bool
	Members  []string
}

func (e *GTIDQuorumError) Error() string {
	if e.Diverged {
		return fmt.Sprintf("PXC GTID divergence affecting members %s; freeze incremental/PITR continuity across the unsafe interval, repair from trusted quorum history and take a fresh full backup", strings.Join(e.Members, ", "))
	}
	return fmt.Sprintf("PXC GTID quorum comparison unavailable for members %s; new GTID-continuity claims require a healthy same-history quorum (verified archived history and full backups remain available)", strings.Join(e.Members, ", "))
}

// CheckGTIDQuorum requires a majority of configured members with healthy,
// same-Galera-history, pairwise prefix-compatible executed sets. Only the current state
// namespace may lag; restored historical namespaces must agree exactly.
// A singleton establishes its own baseline.
// Observations must include every configured member, using zero values on errors.
func CheckGTIDQuorum(observations map[string]GaleraStatusResponse) error {
	return CheckGTIDQuorumBaseline(observations, nil)
}

// CheckGTIDQuorumBaseline permits only the frozen, directed member differences
// explicitly acknowledged by a full backup. Missing/changed identities fail
// closed; an acknowledgement never permits a new or growing difference.
func CheckGTIDQuorumBaseline(observations, acknowledged map[string]GaleraStatusResponse) error {
	names := make([]string, 0, len(observations))
	for name := range observations {
		names = append(names, name)
	}
	sort.Strings(names)
	n := len(names)
	contains := make([][]bool, n)
	sets := make([]map[string][]gtidInterval, n)
	valid := make([]bool, n)
	ackSets := make([]map[string][]gtidInterval, n)
	for i, name := range names {
		s := observations[name]
		set, err := gtidIntervals(s.GTIDExecuted)
		sets[i] = set
		valid[i] = s.Eligible() && ValidUUID(s.StateUUID) && err == nil
		contains[i] = make([]bool, n)
		if ack, ok := acknowledged[name]; ok && ack.StateUUID == s.StateUUID {
			ackSets[i], _ = gtidIntervals(ack.GTIDExecuted)
		}
	}
	for i, name := range names {
		current := activeClusterSID(observations, names, sets, valid, observations[name].StateUUID)
		for j, other := range names {
			if valid[i] && valid[j] && observations[name].StateUUID == observations[other].StateUUID {
				frozenEqual := frozenGTIDsEqual(sets[i], sets[j], current)
				if !frozenEqual {
					if ackSets[i] != nil && ackSets[j] != nil {
						frozenEqual = frozenDeltasContained(sets[i], sets[j], ackSets[i], ackSets[j], current) && frozenDeltasContained(sets[j], sets[i], ackSets[j], ackSets[i], current)
					}
				}
				contains[i][j] = frozenEqual && gtidPrefix(sets[i][current], sets[j][current])
			}
		}
	}
	// Strict containment is a DAG; equal sets are ordered by member name.
	chains := make([][]int, n)
	var chain func(int) []int
	chain = func(i int) []int {
		if chains[i] != nil {
			return chains[i]
		}
		best := []int{}
		for j := range names {
			if i != j && contains[i][j] && (!contains[j][i] || j < i) {
				if candidate := chain(j); len(candidate) > len(best) {
					best = candidate
				}
			}
		}
		chains[i] = append(append([]int{}, best...), i)
		return chains[i]
	}
	var baseline []int
	for i := range names {
		if valid[i] {
			if candidate := chain(i); len(candidate) > len(baseline) {
				baseline = candidate
			}
		}
	}
	if n == 0 || len(baseline) < n/2+1 {
		return &GTIDQuorumError{Members: names}
	}
	var affected []string
	for i, name := range names {
		if !valid[i] {
			continue
		}
		for _, j := range baseline {
			if !contains[i][j] && !contains[j][i] {
				affected = append(affected, name)
				break
			}
		}
	}
	if len(affected) > 0 {
		return &GTIDQuorumError{Diverged: true, Members: affected}
	}
	return nil
}

// Only the current Galera state namespace may advance during observations.
// Historical restored namespaces must agree exactly; a node-local extra SID is
// not normal replication lag even when its complete set is a strict superset.
func frozenGTIDsEqual(a, b map[string][]gtidInterval, current string) bool {
	for sid, ranges := range a {
		if sid == current {
			continue
		}
		other := b[sid]
		if len(ranges) != len(other) {
			return false
		}
		for i, r := range ranges {
			if r != other[i] {
				return false
			}
		}
	}
	for sid := range b {
		if sid != current {
			if _, ok := a[sid]; !ok {
				return false
			}
		}
	}
	return true
}

// gtidPrefix requires the shorter set to equal the longer set truncated at
// the shorter set's highest sequence; an interior omission is not write lag.
func gtidPrefix(longer, shorter []gtidInterval) bool {
	if len(longer) < len(shorter) {
		return false
	}
	for i, r := range shorter {
		if r.lo != longer[i].lo || r.hi > longer[i].hi || i < len(shorter)-1 && r.hi != longer[i].hi {
			return false
		}
	}
	return true
}

// PXC can use the group UUID or its bytewise complement. Select the inverse
// only when it is shared by every eligible member and the direct namespace is
// absent. If both exist, neither is proven active: do not exempt frozen history.
func activeClusterSID(observations map[string]GaleraStatusResponse, names []string, sets []map[string][]gtidInterval, valid []bool, state string) string {
	direct := strings.ToLower(state)
	inverse := inverseUUID(direct)
	directSeen, inverseSeen, eligible := false, 0, 0
	for i, name := range names {
		s := observations[name]
		if !valid[i] || !strings.EqualFold(s.StateUUID, state) {
			continue
		}
		set := sets[i]
		eligible++
		directSeen = directSeen || len(set[direct]) > 0
		if len(set[inverse]) > 0 {
			inverseSeen++
		}
	}
	if !directSeen && inverseSeen == eligible && eligible > 0 {
		return inverse
	}
	if inverseSeen == 0 {
		return direct
	}
	return ""
}

func inverseUUID(uuid string) string {
	const hex = "0123456789abcdef"
	out := []byte(strings.ToLower(uuid))
	for i, c := range out {
		if n := strings.IndexByte(hex, c); n >= 0 {
			out[i] = hex[15-n]
		}
	}
	return string(out)
}

func frozenDeltasContained(a, b, ackA, ackB map[string][]gtidInterval, active string) bool {
	for sid, ranges := range a {
		if sid == active {
			continue
		}
		// Compare directed differences, not full sets: shared history can grow
		// or be replaced by a restore without acknowledging an unsafe delta.
		if !gtidDifferenceContained(ranges, b[sid], ackA[sid], ackB[sid]) {
			return false
		}
	}
	return true
}
