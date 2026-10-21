// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
)

var sharedMemoryKeys = [...]string{"max_connections", "max_prepared_transactions", "max_locks_per_transaction", "max_wal_senders", "max_worker_processes"}

type rollingPhase struct {
	Direction string          `json:"direction"`
	Options   json.RawMessage `json:"options"`
}

// Every member renders the same phase, including members not yet restarted:
// a cached configuration pull must not patch an earlier value back into DCS.
type rollingState struct {
	Target   json.RawMessage `json:"target"`
	Phases   []rollingPhase  `json:"phases"`
	Phase    int             `json:"phase"`
	Group    int             `json:"group,omitempty"`
	Version  int64           `json:"version,omitempty"`
	Primary  string          `json:"primary,omitempty"`
	Timeline int             `json:"timeline,omitempty"`
	Barrier  string          `json:"barrier,omitempty"`
}

func sharedMemoryValue(parameters map[string]string, key string) (int64, error) {
	value, _ := pgspec.SharedMemoryDefault(key)
	if raw, ok := parameters[key]; ok {
		var err error
		value, err = strconv.ParseInt(raw, 10, 64)
		if err != nil || value < 0 {
			return 0, engine.Invalid("PostgreSQL parameter %s requires a nonnegative integer", key)
		}
	}
	return value, nil
}

func (p *Core) sharedMemoryOptions(raw json.RawMessage) (EngineOptions, error) {
	options, err := p.parseOptions(raw)
	if err != nil {
		return options, err
	}
	parameters, err := p.profileParameters(raw)
	if err != nil {
		return options, err
	}
	for key, value := range parameters {
		if _, ok := pgspec.SharedMemoryDefault(key); ok {
			if options.Parameters == nil {
				options.Parameters = map[string]string{}
			}
			options.Parameters[key] = value
		}
	}
	return options, nil
}

func (p *Core) planSharedMemory(rc *engine.Context, s *state) (*rollingState, error) {
	next, err := p.sharedMemoryOptions(rc.Cluster.Spec.EngineOptions)
	if err != nil {
		return nil, err
	}
	peak := make(map[string]int64, len(sharedMemoryKeys))
	for _, key := range sharedMemoryKeys {
		peak[key], err = sharedMemoryValue(next.Parameters, key)
		if err != nil {
			return nil, err
		}
	}
	increase, decrease := false, false
	for _, m := range s.data() {
		old, err := p.sharedMemoryOptions(m.Options)
		if err != nil {
			return nil, err
		}
		for _, key := range sharedMemoryKeys {
			before, err := sharedMemoryValue(old.Parameters, key)
			if err != nil {
				return nil, err
			}
			after, _ := sharedMemoryValue(next.Parameters, key)
			increase = increase || after > before
			decrease = decrease || after < before
			if before > peak[key] {
				peak[key] = before
			}
		}
	}
	if !increase && !decrease {
		return nil, nil
	}
	journal := &rollingState{Target: append(json.RawMessage(nil), rc.Cluster.Spec.EngineOptions...)}
	if s.Topology == "distributed" {
		journal.Group = s.maintenanceGroups()[0].id
	}
	for _, direction := range []string{"increase", "decrease"} {
		if direction == "increase" && !increase || direction == "decrease" && !decrease {
			continue
		}
		options, err := p.sharedMemoryOptions(rc.Cluster.Spec.EngineOptions)
		if err != nil {
			return nil, err
		}
		if options.Parameters == nil {
			options.Parameters = map[string]string{}
		}
		for _, key := range sharedMemoryKeys {
			value, _ := sharedMemoryValue(next.Parameters, key)
			if direction == "increase" {
				value = peak[key]
			}
			options.Parameters[key] = strconv.FormatInt(value, 10)
		}
		raw, err := json.Marshal(options)
		if err != nil {
			return nil, err
		}
		journal.Phases = append(journal.Phases, rollingPhase{Direction: direction, Options: raw})
	}
	return journal, nil
}

func sharedMemoryMatches(st pgspec.StatusResponse, parameters map[string]string) bool {
	for _, key := range sharedMemoryKeys {
		expected, err := sharedMemoryValue(parameters, key)
		actual, ok := st.SharedMemoryParameters[key]
		if err != nil || !ok || actual != expected {
			return false
		}
	}
	return true
}

func walAtLeast(position, barrier string) bool {
	parse := func(value string) (uint64, error) {
		hi, lo, ok := strings.Cut(value, "/")
		if !ok {
			return 0, fmt.Errorf("invalid WAL position")
		}
		h, err := strconv.ParseUint(hi, 16, 32)
		if err != nil {
			return 0, err
		}
		l, err := strconv.ParseUint(lo, 16, 32)
		return h<<32 | l, err
	}
	actual, err := parse(position)
	if err != nil {
		return false
	}
	expected, err := parse(barrier)
	return err == nil && actual >= expected
}

func (p *Core) reconcileSharedMemory(ctx context.Context, rc *engine.Context, s *state) (bool, error) {
	if s.Rolling == nil {
		if !s.Formed || !patroniTopology(s.Topology) || s.Replacement != nil {
			return false, nil
		}
		for _, m := range s.data() {
			if m.Stage != "joined" || m.Removing || m.Retirement != "" {
				return false, nil
			}
		}
		var err error
		s.Rolling, err = p.planSharedMemory(rc, s)
		if err != nil || s.Rolling == nil {
			return false, err
		}
		return true, nil // Persist direction and both mixed-change phases first.
	}
	journal := s.Rolling
	if journal.Phase >= len(journal.Phases) {
		for _, m := range s.data() {
			m.Options = append(json.RawMessage(nil), journal.Target...)
		}
		s.Rolling = nil
		s.ConfigVersion++ // Let ordinary convergence apply newer intent/proxy changes.
		return true, nil
	}
	phase := journal.Phases[journal.Phase]
	options, err := ParseOptions(phase.Options)
	if err != nil {
		return true, err
	}
	if journal.Version == 0 {
		s.ConfigVersion++
		journal.Version = s.ConfigVersion
		for _, m := range s.Members {
			m.ConfigVersion = journal.Version
		}
		return true, nil
	}
	// A decreasing writer cannot restart until every existing proxy has
	// acknowledged the smaller shared capacity budget.
	for _, m := range s.Members {
		if m.Role == "proxy" && desiredComponent(rc, m).Replicas > 0 {
			applied, err := proxyApplied(ctx, rc, m, journal.Version, false)
			if err != nil || !applied {
				return true, err
			}
		}
	}
	// Acknowledging every pull also rules out a delayed old-phase PATCH before
	// any restart. All periodic pulls now render this journal's phase options.
	for _, m := range s.data() {
		a, err := rc.Agents.Dial(ctx, rc.Cluster.Ref(), m.Name)
		if err != nil {
			return true, err
		}
		status, err := engine.ObserveAgentStatus(ctx, a, "PostgreSQL", m.Name)
		if err != nil {
			return true, err
		}
		if status.ConfigVersion < journal.Version {
			return true, reloadPublished(ctx, rc, s, m)
		}
	}
	group := s.group(journal.Group)
	statuses := make(map[string]pgspec.StatusResponse, len(group.data()))
	primary := ""
	for _, m := range group.data() {
		st, err := live(ctx, rc, m.Name)
		if err != nil {
			return true, err
		}
		if !st.Running || len(st.SharedMemoryParameters) != len(sharedMemoryKeys) {
			return true, nil
		}
		statuses[m.Name] = st
		if st.Role == "primary" && st.Leader == m.Name && st.EtcdHealthy {
			if primary != "" {
				return true, engine.ErrUnavailable
			}
			primary = m.Name
		} else if st.Role != "replica" || !st.Streaming {
			return true, nil
		}
	}
	if primary == "" {
		return true, engine.ErrUnavailable
	}
	writer := statuses[primary]
	if phase.Direction == "decrease" {
		if !sharedMemoryMatches(writer, options.Parameters) {
			if wait := rc.WaitForWindow(); wait.RequeueAfter > 0 {
				return true, nil
			}
			journal.Primary, journal.Barrier, journal.Timeline = "", "", 0
			return true, pgAction(ctx, rc, primary, pgspec.ActionRestart, pgspec.RestartRequest{PrimaryFirst: true}, nil)
		}
		if journal.Primary != primary || journal.Timeline != writer.Timeline || journal.Barrier == "" {
			if writer.Timeline <= 0 || !walAtLeast(writer.WALPosition, "0/0") {
				return true, nil
			}
			journal.Primary, journal.Timeline, journal.Barrier = primary, writer.Timeline, writer.WALPosition
			return true, nil
		}
	}
	for _, m := range group.data() {
		st := statuses[m.Name]
		if m.Name == primary || sharedMemoryMatches(st, options.Parameters) {
			continue
		}
		if phase.Direction == "decrease" && (st.Timeline != journal.Timeline || !walAtLeast(st.WALPosition, journal.Barrier)) {
			return true, nil
		}
		if wait := rc.WaitForWindow(); wait.RequeueAfter > 0 {
			return true, nil
		}
		return true, pgAction(ctx, rc, m.Name, pgspec.ActionRestart, pgspec.RestartRequest{}, nil)
	}
	if !sharedMemoryMatches(writer, options.Parameters) {
		if wait := rc.WaitForWindow(); wait.RequeueAfter > 0 {
			return true, nil
		}
		if len(group.data()) == 1 {
			return true, pgAction(ctx, rc, primary, pgspec.ActionRestart, pgspec.RestartRequest{}, nil)
		}
		// Every standby is already at the increased values. Only those peers
		// may take over before restarting the former primary as a replica.
		return true, switchAway(ctx, rc, s, primary)
	}
	groups := s.maintenanceGroups()
	for i, g := range groups {
		if g.id == journal.Group && i+1 < len(groups) {
			journal.Group = groups[i+1].id
			journal.Primary, journal.Timeline, journal.Barrier = "", 0, ""
			return true, nil
		}
	}
	journal.Group = groups[0].id
	journal.Phase++
	journal.Version, journal.Primary, journal.Timeline, journal.Barrier = 0, "", 0, ""
	if journal.Phase == len(journal.Phases) {
		for _, m := range s.data() {
			m.Options = append(json.RawMessage(nil), journal.Target...)
		}
		s.Rolling = nil
		s.ConfigVersion++
	}
	return true, nil
}

// needsDisruption distinguishes reloadable config (including proxy-only changes
// and certificates) from changes that require moving the writable primary.
func needsDisruption(p *Core, rc *engine.Context, m *member, desired domain.ComponentSpec) bool {
	if m.Version != rc.Cluster.Spec.Version || !reflect.DeepEqual(m.Resources, desired.Resources) || p.profilesAdded(m.Options, rc.Cluster.Spec.EngineOptions) {
		return true
	}
	old, err := p.parseOptions(m.Options)
	if err != nil {
		return true
	}
	next, err := p.parseOptions(rc.Cluster.Spec.EngineOptions)
	if err != nil {
		return true
	}
	// Composed engines can change extension preloads without adding a core
	// profile. Those libraries are postmaster settings, not reload-only input.
	oldPreloads, err := p.profilePreloads(m.Options, major(m.Version))
	if err != nil {
		return true
	}
	nextPreloads, err := p.profilePreloads(rc.Cluster.Spec.EngineOptions, major(rc.Cluster.Spec.Version))
	if err != nil || !reflect.DeepEqual(oldPreloads, nextPreloads) {
		return true
	}
	for _, key := range sharedMemoryKeys {
		if old.Parameters[key] != next.Parameters[key] {
			return true
		}
	}
	for _, key := range []string{"shared_buffers", "wal_buffers"} {
		if old.Parameters[key] != next.Parameters[key] {
			return true
		}
	}
	return false
}
