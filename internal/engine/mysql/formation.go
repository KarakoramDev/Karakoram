// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"fmt"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	"sort"
	"strconv"
	"strings"
	"unicode"
)

type interval struct{ lo, hi uint64 }
type gtidSet map[string][]interval

// parseGTIDSet normalizes repeated SID/tag blocks and overlapping intervals.
// Tags partition a UUID's transaction stream; they must never be conflated.
func parseGTIDSet(raw string) (gtidSet, error) {
	result := gtidSet{}
	if strings.TrimSpace(raw) == "" {
		return result, nil
	}
	for _, block := range strings.Split(raw, ",") {
		fields := strings.Split(strings.TrimSpace(block), ":")
		if len(fields) < 2 || !myspec.ValidUUID(fields[0]) {
			return nil, fmt.Errorf("invalid GTID source")
		}
		key := strings.ToLower(fields[0])
		start := 1
		if _, err := strconv.ParseUint(fields[1], 10, 64); err != nil && !strings.Contains(fields[1], "-") {
			tag := fields[1]
			if len(tag) == 0 || len(tag) > 32 {
				return nil, fmt.Errorf("invalid GTID tag")
			}
			for i, c := range tag {
				if !(c == '_' || unicode.IsLetter(c) && c < 128 || i > 0 && c >= '0' && c <= '9') {
					return nil, fmt.Errorf("invalid GTID tag")
				}
			}
			key += ":" + strings.ToLower(tag)
			start = 2
		}
		if start >= len(fields) {
			return nil, fmt.Errorf("missing GTID intervals")
		}
		for _, part := range fields[start:] {
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
			result[key] = append(result[key], interval{lo, hi})
		}
	}
	for key, values := range result {
		sort.Slice(values, func(i, j int) bool { return values[i].lo < values[j].lo })
		merged := values[:0]
		for _, v := range values {
			if len(merged) > 0 && v.lo <= merged[len(merged)-1].hi+1 {
				if v.hi > merged[len(merged)-1].hi {
					merged[len(merged)-1].hi = v.hi
				}
			} else {
				merged = append(merged, v)
			}
		}
		result[key] = merged
	}
	return result, nil
}
func (s gtidSet) contains(other gtidSet) bool {
	for key, want := range other {
		have := s[key]
		i := 0
		for _, w := range want {
			for i < len(have) && have[i].hi < w.lo {
				i++
			}
			if i == len(have) || have[i].lo > w.lo || have[i].hi < w.hi {
				return false
			}
		}
	}
	return true
}
func selectSuperset(observations map[string]mysqloffline) (string, error) {
	if len(observations) == 0 {
		return "", fmt.Errorf("no offline observations")
	}
	sets := map[string]gtidSet{}
	names := make([]string, 0, len(observations))
	for name, observation := range observations {
		if !myspec.ValidUUID(observation.UUID) {
			return "", fmt.Errorf("member %s has no verified server identity", name)
		}
		set, err := parseGTIDSet(observation.GTID)
		if err != nil {
			return "", fmt.Errorf("member %s has invalid GTID set: %w", name, err)
		}
		sets[name] = set
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		all := true
		for _, other := range names {
			if !sets[name].contains(sets[other]) {
				all = false
				break
			}
		}
		if all {
			return name, nil
		}
	}
	return "", fmt.Errorf("offline GTID sets are incomparable; no member contains every transaction")
}
func publishRevision(s *state) {
	s.ConfigVersion++
	for _, m := range s.Members {
		m.ConfigVersion = s.ConfigVersion
	}
}
func (p *Core) reconcileFormation(ctx context.Context, rc *engine.Context, s *state) (bool, error) {
	if s.Topology == "cluster" {
		return p.reconcileGaleraFormation(ctx, rc, s)
	}
	if s.Formation.State == "adopting" {
		seed := s.find(s.Formation.Seed)
		if seed == nil {
			return true, engine.ErrNotFound
		}
		applied, err := configApplied(ctx, rc, seed)
		if err != nil || !applied {
			return true, err
		}
		a, err := rc.Agents.Dial(ctx, rc.Cluster.Ref(), seed.Name)
		if err != nil {
			return true, err
		}
		if err = a.Process(ctx, agentapi.ProcessAction{Process: "mysqld", Action: "restart", RequestID: fmt.Sprintf("adopt-%s-%d", s.UID, s.Formation.FormationEpoch)}); err != nil {
			return true, err
		}
		s.Formation.State = "bootstrapping"
		publishRevision(s)
		return true, nil
	}
	if s.Topology != "group" {
		return false, nil
	}
	f := &s.Formation
	if r := f.Reformation; r != nil && r.State == "observing" {
		observations := map[string]mysqloffline{}
		for _, m := range s.data() {
			if m.Stage != "deployed" && m.Stage != "joined" {
				return true, nil
			}
			a, err := rc.Agents.Dial(ctx, rc.Cluster.Ref(), m.Name)
			if err != nil {
				r.Blocked = "Cannot observe every stopped member"
				return true, nil
			}
			var out myspec.OfflineGTIDResponse
			probe, cancel := context.WithTimeout(ctx, engine.ObservationTimeout)
			err = a.Engine(probe, "mysql", myspec.ActionOfflineGTID, struct{}{}, &out)
			cancel()
			if err != nil || !out.Stopped || !out.DataIdentity.Initialized || m.ServerUUID != "" && out.ServerUUID != m.ServerUUID {
				r.Blocked = "Every member must provide a stopped, initialized offline GTID observation"
				return true, nil
			}
			observations[m.Name] = mysqloffline{GTID: out.GTIDExecuted, UUID: out.ServerUUID}
		}
		seed, err := selectSuperset(observations)
		r.Observations = observations
		if err != nil {
			r.Blocked = err.Error()
			return true, nil
		}
		r.Blocked = ""
		r.State = "bootstrapping"
		f.Seed = seed
		f.FormationEpoch++
		f.State = "bootstrapping"
		for _, m := range s.data() {
			m.Offline = m.Name != seed
		}
		publishRevision(s)
		return true, nil
	}
	if f.State == "unformed" {
		for _, m := range s.data() {
			if m.Stage != "deployed" && m.Stage != "joined" {
				return false, nil
			}
		}
		seed := s.find(f.Seed)
		if seed == nil || seed.Initialized {
			return true, engine.Invalid("initial bootstrap seed is not fresh")
		}
		f.State = "bootstrapping"
		publishRevision(s)
		return true, nil
	}
	if f.State == "bootstrapping" {
		st, err := live(ctx, rc, f.Seed)
		if err != nil {
			return true, nil
		}
		if !onlinePrimary(st) || st.DataIdentity.FormationEpoch < f.FormationEpoch {
			seed := s.find(f.Seed)
			if seed == nil {
				return true, engine.ErrNotFound
			}
			applied, err := configApplied(ctx, rc, seed)
			if err != nil || !applied {
				return true, err
			}
			if st.DataIdentity.FormationEpoch >= f.FormationEpoch {
				if st.MemberState != "RECOVERING" {
					f.Blocked = "Bootstrap epoch was consumed without an ONLINE primary; automatic re-bootstrap is forbidden"
				}
				return true, nil
			}
			a, err := rc.Agents.Dial(ctx, rc.Cluster.Ref(), f.Seed)
			if err != nil {
				return true, err
			}
			err = a.Engine(ctx, "mysql", myspec.ActionBootstrap, myspec.GroupRequest{GroupName: f.GroupName, FormationEpoch: f.FormationEpoch, Bootstrap: true}, nil)
			return true, err
		}
		f.Blocked = ""
		seed := s.find(f.Seed)
		seed.Initialized = true
		seed.ServerUUID = st.ServerUUID
		seed.Stage = "joined"
		f.State = "formed"
		for _, m := range s.data() {
			m.Offline = false
		}
		if f.Reformation != nil {
			f.Reformation.State = "formed"
		}
		publishRevision(s)
		return true, nil
	}
	if f.State == "formed" {
		for _, m := range s.data() {
			st, err := live(ctx, rc, m.Name)
			if err != nil || st.MemberState == "ONLINE" || st.MemberState == "RECOVERING" {
				continue
			}
			applied, err := configApplied(ctx, rc, m)
			if err != nil || !applied {
				return true, err
			}
			if !m.Initialized && !m.CloneDone {
				if m.Clone == nil {
					for _, donor := range s.data() {
						status, err := live(ctx, rc, donor.Name)
						if err == nil && onlinePrimary(status) {
							m.Clone = &myspec.CloneRequest{Receipt: fmt.Sprintf("initial-%s-%s", s.UID, m.Name), Donor: myspec.Peer{Name: donor.Name, Host: donor.Name, Port: 3306}, Version: m.Version}
							return true, nil
						}
					}
					return true, nil
				}
				a, err := rc.Agents.Dial(ctx, rc.Cluster.Ref(), m.Name)
				if err != nil {
					return true, err
				}
				if err = a.Engine(ctx, "mysql", myspec.ActionClone, *m.Clone, nil); err != nil {
					return true, err
				}
				m.CloneDone = true
				// Revoke the per-member destructive clone permission before join.
				publishRevision(s)
				return true, nil
			}
			a, err := rc.Agents.Dial(ctx, rc.Cluster.Ref(), m.Name)
			if err != nil {
				return true, err
			}
			return true, a.Engine(ctx, "mysql", myspec.ActionStartGroup, myspec.GroupRequest{GroupName: f.GroupName, FormationEpoch: f.FormationEpoch}, nil)
		}
	}
	return false, nil
}
