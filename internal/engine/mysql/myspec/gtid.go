// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package myspec

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

type gtidInterval struct{ lo, hi uint64 }

var gtidTag = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,31}$`)

// GTIDContains compares normalized UUID/tag transaction intervals without
// assuming file names or ordering imply transaction coverage.
func GTIDContains(have, want string) (bool, error) {
	a, err := gtidIntervals(have)
	if err != nil {
		return false, err
	}
	b, err := gtidIntervals(want)
	if err != nil {
		return false, err
	}
	return gtidContains(a, b), nil
}

func gtidContains(a, b map[string][]gtidInterval) bool {
	for sid, ranges := range b {
		available := a[sid]
		i := 0
		for _, r := range ranges {
			for i < len(available) && available[i].hi < r.lo {
				i++
			}
			if i == len(available) || available[i].lo > r.lo || available[i].hi < r.hi {
				return false
			}
		}
	}
	return true
}
func gtidIntervals(raw string) (map[string][]gtidInterval, error) {
	out := map[string][]gtidInterval{}
	if strings.TrimSpace(raw) == "" {
		return out, nil
	}
	for _, block := range strings.Split(raw, ",") {
		parts := strings.Split(strings.TrimSpace(block), ":")
		if len(parts) < 2 || !ValidUUID(parts[0]) {
			return nil, fmt.Errorf("invalid GTID source")
		}
		sid := strings.ToLower(parts[0])
		start := 1
		if _, err := strconv.ParseUint(parts[1], 10, 64); err != nil && !strings.Contains(parts[1], "-") {
			if !gtidTag.MatchString(parts[1]) {
				return nil, fmt.Errorf("invalid GTID tag")
			}
			sid += ":" + parts[1]
			start++
		}
		if start >= len(parts) {
			return nil, fmt.Errorf("missing GTID interval")
		}
		for _, part := range parts[start:] {
			bounds := strings.Split(part, "-")
			if len(bounds) > 2 {
				return nil, fmt.Errorf("invalid GTID interval")
			}
			lo, err := strconv.ParseUint(bounds[0], 10, 64)
			if err != nil || lo == 0 || lo > 9223372036854775807 {
				return nil, fmt.Errorf("invalid GTID sequence")
			}
			hi := lo
			if len(bounds) == 2 {
				hi, err = strconv.ParseUint(bounds[1], 10, 64)
				if err != nil || hi < lo || hi > 9223372036854775807 {
					return nil, fmt.Errorf("invalid GTID interval")
				}
			}
			out[sid] = append(out[sid], gtidInterval{lo, hi})
		}
	}
	for sid, rs := range out {
		sort.Slice(rs, func(i, j int) bool { return rs[i].lo < rs[j].lo })
		merged := rs[:0]
		for _, r := range rs {
			if len(merged) > 0 && r.lo <= merged[len(merged)-1].hi+1 {
				if r.hi > merged[len(merged)-1].hi {
					merged[len(merged)-1].hi = r.hi
				}
			} else {
				merged = append(merged, r)
			}
		}
		out[sid] = merged
	}
	return out, nil
}
