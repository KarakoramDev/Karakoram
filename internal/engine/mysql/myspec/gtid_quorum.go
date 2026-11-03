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
	// Detail is a controller-generated, secret-safe observation discriminator.
	Detail string
}

func (e *GTIDQuorumError) Error() string {
	if e.Diverged {
		return fmt.Sprintf("PXC GTID divergence affecting members %s; freeze incremental/PITR continuity across the unsafe interval, repair from trusted quorum history and take a fresh full backup", strings.Join(e.Members, ", "))
	}
	return fmt.Sprintf("PXC GTID quorum comparison unavailable for members %s; majority-short%s; new GTID-continuity claims require a healthy same-history quorum (verified archived history and full backups remain available)", strings.Join(e.Members, ", "), e.Detail)
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

// PXCActiveSIDAdvance identifies the namespace that actually gained GTIDs
// across a fenced replicated probe. Presence alone cannot distinguish SST-copied
// historical namespaces from this process's cached native assignment.
func PXCActiveSIDAdvance(before, after, state string) (string, error) {
	if !ValidUUID(state) {
		return "", fmt.Errorf("invalid Galera state UUID")
	}
	a, err := gtidIntervals(before)
	if err != nil {
		return "", err
	}
	b, err := gtidIntervals(after)
	if err != nil {
		return "", err
	}
	if !gtidContains(b, a) {
		return "", fmt.Errorf("executed history shrank during certification")
	}
	direct := strings.ToLower(state)
	inverse := inverseUUID(direct)
	directAdvanced := !gtidDifferenceContained(b[direct], a[direct], nil, nil)
	inverseAdvanced := !gtidDifferenceContained(b[inverse], a[inverse], nil, nil)
	if directAdvanced && inverseAdvanced {
		return "", fmt.Errorf("both cluster SID mappings advanced")
	}
	if directAdvanced {
		return direct, nil
	}
	if inverseAdvanced {
		return inverse, nil
	}
	return "", nil
}

// PXCEqualPositionPrefixDifferences is a fail-safe suspicion, not an immediate
// divergence verdict. A caller must confirm it in independent observations:
// wsrep status and executed GTIDs are separate reads and may briefly disagree.
func PXCEqualPositionPrefixDifferences(observations map[string]GaleraStatusResponse) (string, []string) {
	names := make([]string, 0, len(observations))
	for name := range observations {
		names = append(names, name)
	}
	sort.Strings(names)
	if len(names) < 2 {
		return "", nil
	}
	first := observations[names[0]]
	state := strings.ToLower(first.StateUUID)
	if !ValidUUID(state) {
		return "", nil
	}
	inverse := inverseUUID(state)
	sets := make([]map[string][]gtidInterval, len(names))
	directPresent, inversePresent := 0, 0
	for i, name := range names {
		status := observations[name]
		if !status.Eligible() || !strings.EqualFold(status.StateUUID, state) || status.LastCommitted != first.LastCommitted || status.LastCommitted < 0 {
			return "", nil
		}
		set, err := gtidIntervals(status.GTIDExecuted)
		if err != nil {
			return "", nil
		}
		sets[i] = set
		if len(set[state]) > 0 {
			directPresent++
		}
		if len(set[inverse]) > 0 {
			inversePresent++
		}
	}
	sid := state
	if directPresent < len(names)/2+1 {
		if directPresent != 0 || inversePresent != len(names) {
			return "", nil
		}
		sid = inverse
	}
	best, bestCount := 0, 0
	for i := range names {
		count := 0
		for j := range names {
			if gtidPrefix(sets[i][sid], sets[j][sid]) && gtidPrefix(sets[j][sid], sets[i][sid]) {
				count++
			}
		}
		if count > bestCount {
			best, bestCount = i, count
		}
	}
	if bestCount == len(names) {
		return "", nil
	}
	var affected []string
	for i, name := range names {
		if bestCount < len(names)/2+1 || !gtidPrefix(sets[i][sid], sets[best][sid]) || !gtidPrefix(sets[best][sid], sets[i][sid]) {
			affected = append(affected, name)
		}
	}
	return sid, affected
}

// PXCCurrentSIDMappingCompatible bounds what a non-fencing executed-set sample
// can prove before acknowledging history. Every member must share the current
// direct/inverse namespace presence, and at most one of those namespaces may
// differ by nested prefixes; the other must be identical frozen history.
// This does not certify native future assignment or supersede mixed probe
// evidence. In particular, identical copied histories are not a new probe.
func PXCCurrentSIDMappingCompatible(observations map[string]GaleraStatusResponse) bool {
	if len(observations) == 0 {
		return false
	}
	var state, direct, inverse string
	var baseDirect, baseInverse, longestDirect, longestInverse []gtidInterval
	directEqual, inverseEqual, directPrefix, inversePrefix := true, true, true, true
	for _, status := range observations {
		if !status.Eligible() || !ValidUUID(status.StateUUID) {
			return false
		}
		set, err := gtidIntervals(status.GTIDExecuted)
		if err != nil {
			return false
		}
		if state == "" {
			state = strings.ToLower(status.StateUUID)
			direct, inverse = state, inverseUUID(state)
			baseDirect, baseInverse = set[direct], set[inverse]
			longestDirect, longestInverse = baseDirect, baseInverse
			if len(baseDirect) == 0 && len(baseInverse) == 0 {
				return false
			}
			continue
		}
		if !strings.EqualFold(status.StateUUID, state) {
			return false
		}
		currentDirect, currentInverse := set[direct], set[inverse]
		if (len(currentDirect) == 0) != (len(baseDirect) == 0) || (len(currentInverse) == 0) != (len(baseInverse) == 0) {
			return false
		}
		directEqual = directEqual && gtidPrefix(baseDirect, currentDirect) && gtidPrefix(currentDirect, baseDirect)
		inverseEqual = inverseEqual && gtidPrefix(baseInverse, currentInverse) && gtidPrefix(currentInverse, baseInverse)
		if !gtidPrefix(longestDirect, currentDirect) {
			if gtidPrefix(currentDirect, longestDirect) {
				longestDirect = currentDirect
			} else {
				directPrefix = false
			}
		}
		if !gtidPrefix(longestInverse, currentInverse) {
			if gtidPrefix(currentInverse, longestInverse) {
				longestInverse = currentInverse
			} else {
				inversePrefix = false
			}
		}
	}
	return len(baseDirect) > 0 && directPrefix && inverseEqual ||
		len(baseInverse) > 0 && inversePrefix && directEqual
}
