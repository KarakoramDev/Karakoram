// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"context"
	"reflect"
	"slices"
	"sort"
	"strconv"

	"github.com/KarakoramDev/Karakoram/internal/engine"
)

func (p *Provider) rollingStart(rc *engine.Context, s *state, reason, version, operationID string) {
	if s.Rolling != nil {
		return
	}
	s.Rolling = &rolling{Reason: reason, Version: version, OperationID: operationID, StartedAt: now(rc)}
	s.Phase = "rolling"
}
func (p *Provider) rollingStep(ctx context.Context, rc *engine.Context, s *state) (bool, string, error) {
	if busy, err := p.observePending(ctx, rc, s); busy || err != nil {
		return false, "waiting for platform operation", err
	}
	if busy, _, err := p.retryPending(ctx, rc, s); busy || err != nil {
		return false, "waiting for platform retry", err
	}
	r := s.Rolling
	if r == nil {
		return true, "", nil
	}
	if len(r.Order) == 0 {
		c, err := p.es(ctx, rc, s)
		if err != nil {
			return false, "", err
		}
		master, err := c.MasterNode(ctx)
		c.Close()
		if err != nil {
			return false, "", err
		}
		nodes := s.esNodes()
		targetVersion := r.Version
		if targetVersion == "" {
			targetVersion = rc.Cluster.Spec.Version
		}
		changed := map[string]bool{}
		for _, m := range s.Members {
			include := !m.Removing && (isES(m) || component(rc.Cluster.Spec, m).Replicas > 0)
			if include && r.Reason == "upgrade" {
				include = m.Version != targetVersion
			}
			if include && (r.Reason == "config" || r.Reason == "resources") {
				desired, err := p.desiredSpec(ctx, rc, s, m, targetVersion)
				if err != nil {
					return false, "", err
				}
				include = desired.RestartToken != m.ConfigHash
			}
			changed[m.Name] = include
		}
		sort.SliceStable(nodes, func(i, j int) bool {
			rank := func(m *member) int {
				if m.Name == master {
					return 2
				}
				if masterEligible(rc.Cluster.Spec, m) {
					return 1
				}
				return 0
			}
			a, b := rank(nodes[i]), rank(nodes[j])
			if a != b {
				return a < b
			}
			return nodes[i].Index < nodes[j].Index
		})
		for _, m := range nodes {
			if changed[m.Name] {
				r.Order = append(r.Order, m.Name)
			}
		}
		for _, kind := range []string{kindKibana, kindLogstash, kindAPM} {
			for _, m := range s.members(kind) {
				if changed[m.Name] {
					r.Order = append(r.Order, m.Name)
				}
			}
		}
		if len(r.Order) == 0 {
			s.Rolling = nil
			return true, "no rolling changes", nil
		}
		return false, "planned rolling order", nil
	}
	if r.Current == "" {
		for _, name := range r.Order {
			if !slices.Contains(r.Done, name) && s.find(name) != nil {
				r.Current = name
				r.Step = "prepare"
				break
			}
		}
		if r.Current == "" {
			s.Rolling = nil
			return true, "rolling pass complete", nil
		}
	}
	m := s.find(r.Current)
	if m == nil {
		r.Done = append(r.Done, r.Current)
		r.Current = ""
		return false, "member removed", nil
	}
	version := r.Version
	if version == "" {
		version = rc.Cluster.Spec.Version
	}
	if !isES(m) {
		switch r.Step {
		case "prepare":
			if r.Reason == "upgrade" {
				err := p.issueAction(ctx, rc, s, &pending{Action: "stop", Member: m.Name})
				if err == nil || s.Retry != nil {
					r.Step = "apply"
				}
				return false, "stopping component for upgrade", err
			}
			r.Step = "apply"
			return false, "preparing component", nil
		case "apply":
			resources := runtimeResources(m, component(rc.Cluster.Spec, m).Resources)
			if !reflect.DeepEqual(m.Resources, resources) {
				return false, "resizing component", p.issueAction(ctx, rc, s, &pending{Action: "resources", Member: m.Name, Resources: resources})
			}
			if r.Reason == "upgrade" {
				err := p.deploy(ctx, rc, s, m, "upgrade", version)
				if err == nil || s.Retry != nil {
					r.Step = "rejoin"
				}
				return false, "upgrading component", err
			}
			if err := p.publishConfig(ctx, rc, s, m, version, rollingNonce(r)); err != nil {
				return false, "", err
			}
			r.Step = "rejoin"
			p.reloadMembers(ctx, rc, m.Name)
			return false, "restarting component", nil
		case "rejoin":
			if !p.componentReady(ctx, rc, m) {
				return false, "waiting for component readiness", nil
			}
			m.Version = version
			m.Stage = stageJoined
			r.Done = append(r.Done, m.Name)
			r.Current = ""
			return false, "component ready", nil
		}
		return false, "", invalid("unknown rolling component step")
	}
	c, err := p.es(ctx, rc, s)
	if err != nil {
		return false, "", err
	}
	defer c.Close()
	switch r.Step {
	case "prepare":
		ok, err := p.healthy(ctx, c)
		if err != nil || !ok {
			return false, "waiting for shard health", err
		}
		value := "primaries"
		if err = c.SetAllocationEnable(ctx, &value); err != nil {
			return false, "", err
		}
		r.Step = "flush"
		return false, "replica allocation paused", nil
	case "flush":
		_ = c.Flush(ctx)
		r.Step = "apply"
		return false, "flushed indices", nil
	case "apply":
		resources := runtimeResources(m, component(rc.Cluster.Spec, m).Resources)
		if !reflect.DeepEqual(m.Resources, resources) {
			return false, "resizing node", p.issueAction(ctx, rc, s, &pending{Action: "resources", Member: m.Name, Resources: resources})
		}
		if r.Reason == "upgrade" {
			err := p.deploy(ctx, rc, s, m, "upgrade", version)
			if err == nil || s.Retry != nil {
				r.Step = "rejoin"
			}
			return false, "upgrading node", err
		}
		if err = p.publishConfig(ctx, rc, s, m, version, rollingNonce(r)); err != nil {
			return false, "", err
		}
		r.Step = "rejoin"
		p.reloadMembers(ctx, rc, m.Name)
		return false, "restarting node", nil
	case "rejoin":
		nodes, err := c.Nodes(ctx)
		if err != nil {
			return false, "", err
		}
		joined := false
		for _, node := range nodes {
			joined = joined || node.Name == m.Name && node.Version == version
		}
		if !joined || !p.componentReady(ctx, rc, m) {
			return false, "waiting for node rejoin", nil
		}
		m.Version = version
		m.Stage = stageJoined
		r.Step = "allocation"
		return false, "node rejoined", nil
	case "allocation":
		if err = c.SetAllocationEnable(ctx, nil); err != nil {
			return false, "", err
		}
		r.Step = "recover"
		return false, "allocation restored", nil
	case "recover":
		ok, err := p.healthy(ctx, c)
		if err != nil || !ok {
			return false, "waiting for shard recovery", err
		}
		r.Done = append(r.Done, m.Name)
		r.Current = ""
		return false, "node recovered", nil
	default:
		return false, "", invalid("unknown rolling step")
	}
}
func rollingNonce(r *rolling) string {
	if r.Reason == "restart" {
		return r.OperationID + "/" + strconv.FormatInt(r.StartedAt.UnixNano(), 10)
	}
	return ""
}
