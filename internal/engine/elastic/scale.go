// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/elastic/esapi"
)

func (p *Provider) allocate(rc *engine.Context, s *state, kind string) (*member, error) {
	m := &member{Kind: kind, Stage: stagePlanned, ConfigVersion: s.ConfigVersion}
	switch kind {
	case kindNode, kindMaster:
		m.Index = s.NextIndex[kind]
		s.NextIndex[kind]++
		if kind == kindNode {
			m.Name = NodeHostname(rc.Cluster.Name, m.Index)
		} else {
			m.Name = MasterHostname(rc.Cluster.Name, m.Index)
		}
		m.Volume = VolumeHostname(m.Name)
	case kindKibana:
		m.Name = KibanaHostname(rc.Cluster.Name)
	case kindLogstash:
		m.Name = LogstashHostname(rc.Cluster.Name)
	case kindAPM:
		m.Name = APMHostname(rc.Cluster.Name)
	default:
		return nil, invalid("unknown member kind")
	}
	if !ValidHostname(m.Name) || m.Volume != "" && !ValidHostname(m.Volume) {
		return nil, invalid("member hostname exceeds platform bounds")
	}
	s.Members = append(s.Members, m)
	p.bumpConfig(s)
	return m, nil
}
func removeRecord(s *state, name string) {
	s.Members = slices.DeleteFunc(s.Members, func(m *member) bool { return m.Name == name })
}
func (p *Provider) scale(ctx context.Context, rc *engine.Context, s *state) (bool, error) {
	for _, m := range s.esNodes() {
		if m.Replaces != "" {
			return false, nil
		}
	}
	for _, kind := range []string{kindMaster, kindNode} {
		desired := rc.Cluster.Spec.Components.Extra[kind].Replicas
		if kind == kindNode {
			desired = rc.Cluster.Spec.Components.Engine.Replicas
		}
		ms := s.members(kind)
		if len(ms) < desired {
			s.Phase = "scaling"
			_, err := p.allocate(rc, s, kind)
			if err != nil {
				return true, err
			}
			p.bumpConfig(s)
			if err = save(rc.Cluster, s); err != nil {
				return true, err
			}
			for _, m := range s.esNodes() {
				if m.Stage == stageJoined {
					p.reloadMembers(ctx, rc, m.Name)
				}
			}
			return true, nil
		}
		if len(ms) > desired {
			m := ms[len(ms)-1]
			m.Removing = true
			s.Phase = "scaling"
			return true, nil
		}
	}
	return false, nil
}

// cancelRemoval restores a scale-down member while it can still serve shards.
// Replacement removals and nodes already voting-excluded must finish retirement.
func (p *Provider) cancelRemoval(ctx context.Context, rc *engine.Context, s *state, m *member) (bool, error) {
	if m.Stage != stageJoined && m.Stage != stageDeployed && m.Stage != stageDraining {
		return false, nil
	}
	desired := rc.Cluster.Spec.Components.Engine.Replicas
	if m.Kind == kindMaster {
		desired = rc.Cluster.Spec.Components.Extra[kindMaster].Replicas
	}
	if desired < len(s.members(m.Kind)) {
		return false, nil
	}
	for _, other := range s.esNodes() {
		if other.Replaces == m.Name {
			return false, nil
		}
	}
	if m.AllocationExcluded {
		c, err := p.es(ctx, rc, s)
		if err != nil {
			return true, err
		}
		defer c.Close()
		names, err := c.AllocationExcludeNames(ctx)
		if err != nil {
			return true, err
		}
		names = slices.DeleteFunc(names, func(name string) bool { return name == m.Name })
		if err = c.SetAllocationExcludeNames(ctx, names); err != nil {
			return true, err
		}
	}
	if rc.Events != nil {
		message := fmt.Sprintf("Removal of %s cancelled because the desired node count includes it again; shard allocation is restored", m.Name)
		if err := rc.Events.Record(ctx, domain.Event{Namespace: rc.Cluster.Ref().Namespace, Cluster: rc.Cluster.Name, Type: "Normal", Reason: "ShardDrainCancelled", Message: message, CreatedAt: now(rc)}); err != nil {
			return true, err
		}
	}
	m.Removing, m.AllocationExcluded = false, false
	m.DrainIdleObservations, m.DrainShardCount, m.DrainWarning = 0, 0, ""
	if m.Stage == stageDraining {
		m.Stage = stageJoined
	}
	s.DrainBlocked = ""
	s.Phase = "scaling"
	return true, nil
}

func blockDrain(ctx context.Context, rc *engine.Context, s *state, m *member, reason string) error {
	message := fmt.Sprintf("Shard drain blocked for %s: %s. Restore or raise the desired data-node count to cancel an unfinished drain, then raise it further if more capacity is needed; or lower index replicas to an intentional redundancy policy. Restore unavailable primaries before retrying. Never force removal.", m.Name, reason)
	if s.DrainBlocked == message {
		return nil
	}
	if rc.Events != nil && m.DrainWarning != message {
		if err := rc.Events.Record(ctx, domain.Event{Namespace: rc.Cluster.Ref().Namespace, Cluster: rc.Cluster.Name, Type: "Warning", Reason: "ShardDrainBlocked", Message: message, CreatedAt: now(rc)}); err != nil {
			return err
		}
	}
	m.DrainWarning = message
	s.DrainBlocked = message
	return nil
}
func (p *Provider) removeStep(ctx context.Context, rc *engine.Context, s *state, m *member) error {
	c, err := p.es(ctx, rc, s)
	if err != nil {
		return err
	}
	defer c.Close()
	eligible := masterEligible(rc.Cluster.Spec, m)
	// Recheck primary safety at every destructive checkpoint, including resumed
	// removals: zero shard counts do not prove an unavailable primary is safe.
	switch m.Stage {
	case stageExcluded, stageDeleting, "delete-volume":
		h, err := c.Health(ctx, esapi.HealthOptions{})
		if err != nil {
			return err
		}
		if h.Status != "green" && h.Status != "yellow" {
			return blockDrain(ctx, rc, s, m, "unassigned primary shards prevent safe removal")
		}
		s.DrainBlocked = ""
	}
	switch m.Stage {
	case stageJoined, stageDeployed:
		remaining := 0
		masters := 0
		for _, other := range s.esNodes() {
			if other.Name == m.Name || other.Removing {
				continue
			}
			if other.Kind == kindNode {
				remaining++
			}
			if masterEligible(rc.Cluster.Spec, other) {
				masters++
			}
		}
		if eligible && masters < 1 {
			return invalid("cannot remove last master")
		}
		if m.Kind == kindNode {
			replicas, err := c.MaxIndexReplicas(ctx, remaining)
			if err != nil {
				return err
			}
			if remaining < replicas+1 {
				return blockDrain(ctx, rc, s, m, fmt.Sprintf("insufficient data nodes for index replicas (%d remaining, at least %d required)", remaining, replicas+1))
			}
		}
		s.DrainBlocked = ""
		names, err := c.AllocationExcludeNames(ctx)
		if err != nil {
			return err
		}
		if !slices.Contains(names, m.Name) {
			m.AllocationExcluded = true
			names = append(names, m.Name)
		}
		if err = c.SetAllocationExcludeNames(ctx, names); err != nil {
			return err
		}
		m.Stage = stageDraining
		return nil
	case stageDraining:
		counts, err := c.ShardCounts(ctx)
		if err != nil {
			return err
		}
		h, err := c.Health(ctx, esapi.HealthOptions{})
		if err != nil {
			return err
		}
		if h.Status != "green" && h.Status != "yellow" {
			m.DrainIdleObservations = 0
			return blockDrain(ctx, rc, s, m, "unassigned primary shards prevent safe removal")
		}
		shards := counts[m.Name]
		if h.Status == "yellow" && shards != 0 && h.RelocatingShards == 0 && h.InitializingShards == 0 {
			if shards != m.DrainShardCount {
				m.DrainIdleObservations = 0
			}
			if m.DrainIdleObservations < 3 {
				m.DrainIdleObservations++
			}
		} else {
			m.DrainIdleObservations = 0
		}
		m.DrainShardCount = shards
		if m.DrainIdleObservations >= 3 {
			return blockDrain(ctx, rc, s, m, "unassigned replica shards prevent the remaining shards from draining")
		}
		s.DrainBlocked = ""
		if counts[m.Name] != 0 || h.RelocatingShards != 0 || h.InitializingShards != 0 {
			return nil
		}
		// Dedicated masters hold no shards and are omitted by _cat/allocation.
		// Data nodes missing a row still require confirmation that they left.
		if _, present := counts[m.Name]; !present && m.Kind != kindMaster {
			nodes, err := c.Nodes(ctx)
			if err != nil {
				return err
			}
			for _, node := range nodes {
				if node.Name == m.Name {
					return nil
				}
			}
		}
		if eligible {
			if err = c.AddVotingExclusion(ctx, []string{m.Name}, 10*time.Second); err != nil {
				return err
			}
		}
		m.Stage = stageExcluded
		return nil
	case stageExcluded:
		if m.PublicPort > 0 {
			if err = rc.Backend.DeletePublicPorts(ctx, m.Name); err != nil {
				return err
			}
			m.PublicPort = 0
			return nil
		}
		return p.issueAction(ctx, rc, s, &pending{Action: "stop-removal", Member: m.Name})
	case stageDeleting:
		return p.issueAction(ctx, rc, s, &pending{Action: "delete-runtime", Member: m.Name})
	case "delete-volume":
		if m.VolumeOrphaned {
			m.Stage = "removed"
			return nil
		}
		volume, err := rc.Backend.GetService(ctx, m.Volume)
		if err != nil {
			return err
		}
		if volume == nil {
			m.Stage = "removed"
			return nil
		}
		return p.issueAction(ctx, rc, s, &pending{Action: "delete-volume", Member: m.Name})
	case "removed":
		nodes, err := c.Nodes(ctx)
		if err != nil {
			return err
		}
		for _, node := range nodes {
			if node.Name == m.Name {
				return nil
			}
		}
		names, err := c.AllocationExcludeNames(ctx)
		if err != nil {
			return err
		}
		if m.AllocationExcluded && slices.Contains(names, m.Name) {
			names = slices.DeleteFunc(names, func(name string) bool { return name == m.Name })
			return c.SetAllocationExcludeNames(ctx, names)
		}
		if eligible {
			if err = c.ClearVotingExclusions(ctx, true); err != nil {
				return err
			}
		}
		if err = deleteMemberSecrets(ctx, rc, m.Name); err != nil {
			return err
		}
		removeRecord(s, m.Name)
		p.bumpConfig(s)
		if err = save(rc.Cluster, s); err != nil {
			return err
		}
		for _, node := range s.esNodes() {
			p.reloadMembers(ctx, rc, node.Name)
		}
		return nil
	default:
		return invalid("cannot remove member at stage %q", m.Stage)
	}
}
func (p *Provider) deleteComponent(ctx context.Context, rc *engine.Context, s *state, m *member) error {
	if err := verifyMemberServices(ctx, rc, m); err != nil {
		return err
	}
	service, err := rc.Backend.GetService(ctx, m.Name)
	if err != nil {
		return err
	}
	if service != nil {
		if m.Subdomain {
			return p.issueAction(ctx, rc, s, &pending{Action: "subdomain-off", Member: m.Name})
		}
		return p.issueAction(ctx, rc, s, &pending{Action: "delete-runtime", Member: m.Name})
	}
	if err = deleteMemberSecrets(ctx, rc, m.Name); err != nil {
		return err
	}
	removeRecord(s, m.Name)
	p.bumpConfig(s)
	return nil
}

// skipOccupiedPlans never adopts a previous cluster's detached storage by name.
// A token or recorded identity means this is a resumable plan, not a new name.
func (p *Provider) skipOccupiedPlans(ctx context.Context, rc *engine.Context, s *state) error {
	for _, m := range s.esNodes() {
		if m.Stage != stagePlanned || m.ServiceID != "" || m.VolumeID != "" {
			continue
		}
		tok, err := rc.Secrets.GetAgentToken(ctx, rc.Cluster.Ref(), m.Name)
		if err != nil && !missing(err) {
			return err
		}
		if tok != nil {
			continue
		}
		for {
			occupied := false
			for _, name := range []string{m.Name, m.Volume} {
				service, err := rc.Backend.GetService(ctx, name)
				if err != nil {
					return err
				}
				occupied = occupied || service != nil
				if rc.Store != nil {
					orphan, err := rc.Store.GetOrphanedVolume(ctx, rc.Cluster.Ref().Namespace, name)
					if err != nil && !missing(err) {
						return err
					}
					occupied = occupied || orphan != nil
				}
			}
			if !occupied {
				break
			}
			old := m.Name
			m.Index = s.NextIndex[m.Kind]
			s.NextIndex[m.Kind]++
			if m.Kind == kindNode {
				m.Name = NodeHostname(rc.Cluster.Name, m.Index)
			} else {
				m.Name = MasterHostname(rc.Cluster.Name, m.Index)
			}
			m.Volume = VolumeHostname(m.Name)
			if !ValidHostname(m.Name) || !ValidHostname(m.Volume) {
				return invalid("member hostname exceeds platform bounds")
			}
			for i, name := range s.InitialMasterNodes {
				if name == old {
					s.InitialMasterNodes[i] = m.Name
				}
			}
		}
	}
	return nil
}
