// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"context"
	"fmt"

	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
)

// A group retirement is irreversible once drained. Finish its durable intent
// even if a later spec increases workerGroups again; new identities never reuse
// the retired indexes. SQL removal is observed before retrying an ambiguous RPC.
type citusScaleState struct {
	Group   int    `json:"group"`
	Stage   string `json:"stage"`
	Host    string `json:"host,omitempty"`
	Port    int    `json:"port,omitempty"`
	Started bool   `json:"started,omitempty"`
}

func coordinatorCatalog(ctx context.Context, rc *engine.Context, s *state) (*member, pgspec.CitusStatusResponse, error) {
	var catalog pgspec.CitusStatusResponse
	coordinator, _, err := s.group(0).primary(ctx, rc)
	if err != nil {
		return nil, catalog, err
	}
	err = pgAction(ctx, rc, coordinator.Name, pgspec.ActionCitusStatus, struct{}{}, &catalog)
	if err == nil && !catalog.IsCoordinator {
		err = engine.ErrUnavailable
	}
	return coordinator, catalog, err
}

func (p *Core) reconcileCitusScaleDown(ctx context.Context, rc *engine.Context, s *state) (bool, error) {
	if s.Topology != "distributed" {
		return false, nil
	}
	if s.CitusScale == nil {
		if s.Replacement != nil || s.Rolling != nil || s.RestoreRecovery != nil {
			return false, nil
		}
		o, err := ParseOptions(rc.Cluster.Spec.EngineOptions)
		if err != nil {
			return false, err
		}
		highest := o.citusOptions().WorkerGroups
		for _, m := range s.data() {
			if m.Group > highest {
				highest = m.Group
			}
		}
		if highest <= o.citusOptions().WorkerGroups {
			return false, nil
		}
		coordinatorReady := false
		for _, m := range s.group(0).data() {
			coordinatorReady = coordinatorReady || m.Stage == "joined" && m.ServiceID != ""
		}
		if s.Suspended || !coordinatorReady {
			return false, nil // Let initial creation/resume provision the coordinator.
		}
		if rc.WaitForWindow().RequeueAfter > 0 {
			return true, nil
		}
		s.CitusScale = &citusScaleState{Group: highest, Stage: "drain"}
		return true, nil // Checkpoint intent before the first native mutation.
	}
	cp := s.CitusScale
	switch cp.Stage {
	case "drain":
		if !cp.Started && rc.WaitForWindow().RequeueAfter > 0 {
			return true, nil
		}
		coordinator, catalog, err := coordinatorCatalog(ctx, rc, s)
		if err != nil {
			return true, err
		}
		var node *pgspec.CitusNode
		for i := range catalog.Nodes {
			n := &catalog.Nodes[i]
			if n.GroupID == cp.Group && n.Role == "primary" {
				if node != nil {
					return true, engine.ErrUnavailable
				}
				node = n
			}
		}
		if node == nil {
			cp.Stage = "fence" // No registration means no shards on this group.
			return true, nil
		}
		if cp.Host != node.Host || cp.Port != node.Port {
			cp.Host, cp.Port = node.Host, node.Port
			return true, nil // Persist the exact SQL target before draining.
		}
		if err := pgAction(ctx, rc, coordinator.Name, pgspec.ActionCitusDrainNode, pgspec.CitusNodeRequest{Host: cp.Host, Port: cp.Port}, nil); err != nil {
			return true, err
		}
		cp.Started = true
		cp.Stage = "fence"
		return true, nil
	case "fence":
		if !cp.Started {
			if rc.WaitForWindow().RequeueAfter > 0 {
				return true, nil
			}
			cp.Started = true
		}
		// Reobserve after draining: failover during the RPC may have moved the
		// registration. Drain that new target before fencing the group.
		_, catalog, err := coordinatorCatalog(ctx, rc, s)
		if err != nil {
			return true, err
		}
		for _, node := range catalog.Nodes {
			if node.GroupID == cp.Group && node.Role == "primary" && (node.Host != cp.Host || node.Port != cp.Port) {
				cp.Host, cp.Port, cp.Stage = node.Host, node.Port, "drain"
				return true, nil
			}
		}
		changed := false
		for _, m := range s.group(cp.Group).data() {
			if !m.Offline {
				m.Offline = true
				changed = true
			}
		}
		if changed {
			s.ConfigVersion++
			for _, m := range s.group(cp.Group).data() {
				m.ConfigVersion = s.ConfigVersion
			}
			return true, nil
		}
		// Offline rendering removes Patroni from desired processes. Explicit stop
		// also stops a paused/orphaned postmaster; it is idempotent after a lost reply.
		for _, m := range s.group(cp.Group).data() {
			if m.ServiceID == "" {
				continue
			}
			if err := reloadPublished(ctx, rc, s, m); err != nil {
				return true, err
			}
			if err := pgAction(ctx, rc, m.Name, "upgrade-stop", struct{}{}, nil); err != nil {
				return true, err
			}
		}
		cp.Stage = "remove"
		return true, nil
	case "remove":
		coordinator, catalog, err := coordinatorCatalog(ctx, rc, s)
		if err != nil {
			return true, err
		}
		for _, node := range catalog.Nodes {
			if node.GroupID != cp.Group {
				continue
			}
			if err := pgAction(ctx, rc, coordinator.Name, pgspec.ActionCitusRemoveNode, pgspec.CitusNodeRequest{Host: node.Host, Port: node.Port}, nil); err != nil {
				return true, err
			}
			return true, nil // Observe absence before advancing, including lost replies.
		}
		for _, m := range s.group(cp.Group).data() {
			m.Removing = true
		}
		cp.Stage = "reset"
		return true, nil
	case "reset":
		// Clear only this stopped group's Patroni subtree before retiring its
		// last authenticated runtime. A future scale-up can bootstrap a fresh
		// system identifier without reusing stale DCS initialization state.
		for _, m := range s.group(cp.Group).data() {
			if m.ServiceID != "" {
				if err := pgAction(ctx, rc, m.Name, pgspec.ActionDCSReset, pgspec.DCSResetRequest{}, nil); err != nil {
					return true, err
				}
				break
			}
		}
		cp.Stage = "retire"
		return true, nil
	case "retire":
		for _, m := range s.group(cp.Group).data() {
			_, err := p.retireMember(ctx, rc, s, m)
			return true, err
		}
		delete(s.GroupFormed, cp.Group)
		if s.GroupRepositoryIncarnations == nil {
			s.GroupRepositoryIncarnations = map[int]int{}
		}
		s.GroupRepositoryIncarnations[cp.Group]++
		s.CitusScale = nil
		return true, nil
	default:
		return true, fmt.Errorf("unknown Citus scale checkpoint %q", cp.Stage)
	}
}

func (p *Core) reconcileCitusRegistration(ctx context.Context, rc *engine.Context, s *state) (bool, error) {
	if s.Topology != "distributed" {
		return false, nil
	}
	observations := map[string]pgspec.StatusResponse{}
	for _, group := range s.groups() {
		leader, status, err := group.primary(ctx, rc)
		if err != nil {
			return true, err
		}
		observations[leader.Name] = status
	}
	if citusTopology(ctx, rc, s, observations).Status != "True" {
		return true, nil
	}
	changed := false
	for _, group := range s.groups() {
		if group.id != 0 && !group.formed() {
			if s.GroupFormed == nil {
				s.GroupFormed = map[int]bool{}
			}
			s.GroupFormed[group.id] = true
			changed = true
		}
	}
	if changed {
		s.ConfigVersion++
		return true, nil
	}
	return false, nil
}
