// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"errors"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"reflect"
	"time"
)

// rollingState freezes the desired artifacts/resources and order across CP restarts.
// Each side effect has a durable preceding stage and an observation checkpoint.
type rollingState struct {
	From        string                          `json:"from"`
	To          string                          `json:"to"`
	Major       bool                            `json:"major"`
	RequestedAt time.Time                       `json:"requestedAt"`
	Order       []string                        `json:"order"`
	Index       int                             `json:"index"`
	Stage       string                          `json:"stage"`
	Targets     map[string]domain.ComponentSpec `json:"targets"`
	Op          *infra.Op                       `json:"op,omitempty"`
	Parameters  map[string]string               `json:"restartParameters,omitempty"`
}

func rollingOrder(ctx context.Context, rc *engine.Context, s *state) ([]string, error) {
	var order []string
	primary := ""
	for _, m := range s.Members {
		if m.Role == "proxy" {
			order = append(order, m.Name)
			continue
		}
		st, err := live(ctx, rc, m.Name)
		if err != nil || st.MemberState != "ONLINE" {
			return nil, engine.ErrUnavailable
		}
		if onlinePrimary(st) {
			if primary != "" {
				return nil, engine.ErrUnavailable
			}
			primary = m.Name
		} else {
			order = append(order, m.Name)
		}
	}
	if primary == "" {
		return nil, engine.ErrUnavailable
	}
	return append(order, primary), nil
}
func rollingNeedsWindow(m *member, version string, target domain.ComponentSpec, parameters map[string]string) bool {
	return m.Version != version || target.Replicas < m.Replicas ||
		!reflect.DeepEqual(m.Resources, target.Resources) ||
		m.Role == "data" && !reflect.DeepEqual(m.Parameters, parameters)
}

func (p *Core) reconcileRolling(ctx context.Context, rc *engine.Context, s *state) (bool, error) {
	options, err := ParseOptions(rc.Cluster.Spec.EngineOptions)
	if err != nil {
		return true, err
	}
	parameters := restartParameters(options.Parameters)
	if s.Rolling == nil {
		if s.Formation.State != "formed" || s.Replacement != nil {
			return false, nil
		}
		changed := false
		needsWindow := false
		from := ""
		for _, m := range s.Members {
			if m.Stage != "joined" {
				return false, nil
			}
			desired := desiredComponent(rc, m)
			changed = changed || m.Version != rc.Cluster.Spec.Version || m.Replicas != desired.Replicas || !reflect.DeepEqual(m.Resources, desired.Resources)
			changed = changed || m.Role == "data" && !reflect.DeepEqual(m.Parameters, parameters)
			needsWindow = needsWindow || rollingNeedsWindow(m, rc.Cluster.Spec.Version, desired, parameters)
			if m.Role == "data" {
				from = m.Version
			}
		}
		if !changed {
			return false, nil
		}
		if needsWindow && rc.WaitForWindow().RequeueAfter > 0 {
			return false, nil
		}
		for _, m := range s.Members {
			target := desiredComponent(rc, m).Resources
			if m.Resources.MinDisk != target.MinDisk || m.Resources.MaxDisk != target.MaxDisk {
				if err := engine.RequireDiskGrowth(rc.Backend); err != nil {
					return true, err
				}
			}
			if planner, ok := rc.Backend.(infra.ResourceUpdatePlanner); ok && !reflect.DeepEqual(m.Resources, target) {
				if err := planner.PlanResources(ctx, m.Name, target); err != nil {
					return true, err
				}
			}
		}
		if err := maintenanceAllowed(ctx, rc, s); err != nil {
			return true, err
		}
		if from != rc.Cluster.Spec.Version && !upgradeAllowed(from, rc.Cluster.Spec.Version) {
			return true, engine.Invalid("unsupported MySQL version transition")
		}
		order, err := rollingOrder(ctx, rc, s)
		if err != nil {
			return true, err
		}
		u := &rollingState{From: from, To: rc.Cluster.Spec.Version, Major: mysqlMajor(from) != mysqlMajor(rc.Cluster.Spec.Version), RequestedAt: now(rc), Order: order, Stage: "backup", Targets: map[string]domain.ComponentSpec{}}
		u.Parameters = parameters
		for _, m := range s.Members {
			u.Targets[m.Name] = desiredComponent(rc, m)
		}
		s.Rolling = u
		return true, nil
	}
	u := s.Rolling
	if u.To != rc.Cluster.Spec.Version {
		return true, engine.Invalid("cannot change version during MySQL maintenance")
	}
	if u.Stage == "backup" {
		if u.Major {
			if err := requireUpgradeBackup(ctx, rc, u); err != nil {
				return true, err
			}
		}
		u.Stage = "prepare"
		return true, nil
	}
	if u.Index >= len(u.Order) {
		s.Rolling = nil
		return true, nil
	}
	m := s.find(u.Order[u.Index])
	if m == nil {
		return true, engine.ErrNotFound
	}
	target := u.Targets[m.Name]
	switch u.Stage {
	case "prepare":
		if m.Version == u.To && m.Replicas == target.Replicas && reflect.DeepEqual(m.Resources, target.Resources) && (m.Role != "data" || reflect.DeepEqual(m.Parameters, u.Parameters)) {
			u.Index++
			return true, nil
		}
		if rollingNeedsWindow(m, u.To, target, u.Parameters) && rc.WaitForWindow().RequeueAfter > 0 {
			return false, nil
		}
		if m.Resources.MinDisk != target.Resources.MinDisk || m.Resources.MaxDisk != target.Resources.MaxDisk {
			if err := engine.RequireDiskGrowth(rc.Backend); err != nil {
				s.Rolling = nil
				return true, err
			}
		}
		if planner, ok := rc.Backend.(infra.ResourceUpdatePlanner); ok && !reflect.DeepEqual(m.Resources, target.Resources) {
			if err := planner.PlanResources(ctx, m.Name, target.Resources); err != nil {
				// No disruption has started; discard the frozen target so a
				// subsequent desired generation can supersede this rejection.
				if errors.Is(err, infra.ErrRejected) {
					s.Rolling = nil
				}
				return true, err
			}
		}
		if u.Major && m.Role == "data" {
			m.SlowShutdown = true
			publishRevision(s)
		}
		u.Stage = "handoff"
	case "handoff":
		if rollingNeedsWindow(m, u.To, target, u.Parameters) && rc.WaitForWindow().RequeueAfter > 0 {
			return false, nil
		}
		if planner, ok := rc.Backend.(infra.ResourceUpdatePlanner); ok && !reflect.DeepEqual(m.Resources, target.Resources) {
			if err := planner.PlanResources(ctx, m.Name, target.Resources); err != nil {
				if errors.Is(err, infra.ErrRejected) {
					m.SlowShutdown = false
					publishRevision(s)
					s.Rolling = nil
				}
				return true, err
			}
		}
		if u.Major && m.Role == "data" {
			ok, err := configApplied(ctx, rc, m)
			if err != nil || !ok {
				return true, err
			}
		}
		if s.Topology == "group" && m.Role == "data" && !u.Major {
			st, err := live(ctx, rc, m.Name)
			if err != nil {
				return true, err
			}
			if onlinePrimary(st) {
				return true, switchAway(ctx, rc, s, m.Name)
			}
		}
		// For mixed-major groups the old primary leaves rather than promoting an
		// ineligible higher-version secondary with set_as_primary. GR elects it.
		m.Offline = true
		publishRevision(s)
		u.Stage = "offline"
	case "offline":
		ok, err := configApplied(ctx, rc, m)
		if err != nil || !ok {
			return true, err
		}
		if clustered(s.Topology) && m.Role == "data" {
			if err := survivorsOnline(ctx, rc, s, m.Name); err != nil {
				return true, err
			}
		}
		op, err := rc.Backend.Stop(ctx, m.Name)
		if err != nil {
			return true, err
		}
		u.Op = &op
		u.Stage = "stopped"
	case "stopped":
		done, err := rollingOpDone(ctx, rc, u)
		if err != nil || !done {
			return true, err
		}
		runtime, err := engine.ObserveRetiringService(ctx, rc.Backend, m.Name, m.ServiceID, m.Volume, m.VolumeID, false)
		if err != nil {
			return true, err
		}
		if runtime != nil && runtime.Status != infra.StatusStopped {
			return true, nil
		}
		if err = verifyMember(ctx, rc, m); err != nil {
			return true, err
		}
		if m.Volume != "" && (m.Resources.MinDisk != target.Resources.MinDisk || m.Resources.MaxDisk != target.Resources.MaxDisk) {
			op, err := rc.Backend.SetResources(ctx, m.Volume, domain.Resources{MinDisk: target.Resources.MinDisk, MaxDisk: target.Resources.MaxDisk})
			if err != nil {
				return true, err
			}
			u.Op = &op
		} else {
			u.Op = nil
		}
		u.Stage = "disk"
	case "disk":
		if u.Op != nil {
			done, err := rollingOpDone(ctx, rc, u)
			if err != nil || !done {
				return true, err
			}
		}
		op, err := rc.Backend.SetResources(ctx, m.Name, target.Resources)
		if err != nil {
			return true, err
		}
		u.Op = &op
		u.Stage = "resources"
	case "resources":
		done, err := rollingOpDone(ctx, rc, u)
		if err != nil || !done {
			return true, err
		}
		if m.Role == "proxy" && m.Replicas != target.Replicas {
			op, err := rc.Backend.SetContainerCount(ctx, m.Name, target.Replicas, target.Replicas)
			if err != nil {
				return true, err
			}
			u.Op = &op
			u.Stage = "replicas"
			return true, nil
		}
		u.Stage = "replicas"
		u.Op = nil
	case "replicas":
		if u.Op != nil {
			done, err := rollingOpDone(ctx, rc, u)
			if err != nil || !done {
				return true, err
			}
		}
		m.Version = u.To
		m.Resources = target.Resources
		m.Replicas = target.Replicas
		if m.Role == "data" {
			m.Parameters = u.Parameters
		}
		publishRevision(s)
		u.Stage = "deploy"
	case "deploy":
		spec, err := p.runtimeSpec(rc, m)
		if err != nil {
			return true, err
		}
		if engine.DeploymentBlocked(rc, m.Name, spec) {
			return true, nil
		}
		op, err := engine.Deploy(ctx, rc, m.Name, spec)
		if err != nil {
			return true, err
		}
		u.Op = &op
		u.Stage = "deployed"
	case "deployed":
		done, err := rollingOpDone(ctx, rc, u)
		if err != nil || !done {
			return true, err
		}
		m.Offline = false
		m.SlowShutdown = false
		publishRevision(s)
		u.Stage = "online"
	case "online":
		if clustered(s.Topology) && m.Role == "data" {
			// Existing data identities are rejoined, never cloned across versions.
			st, err := live(ctx, rc, m.Name)
			if err != nil {
				return true, err
			}
			if st.MemberState != "ONLINE" {
				_, err = p.reconcileFormation(ctx, rc, s)
				return true, err
			}
		}
		ok, err := ready(ctx, rc, m)
		if err != nil || !ok {
			return true, err
		}
		u.Index++
		u.Op = nil
		u.Stage = "prepare"
	default:
		return true, engine.Invalid("unknown MySQL rolling checkpoint %s", u.Stage)
	}
	return true, nil
}

func rollingOpDone(ctx context.Context, rc *engine.Context, u *rollingState) (bool, error) {
	if u.Op == nil {
		return false, engine.Invalid("missing MySQL maintenance receipt")
	}
	st, err := rc.Backend.OpStatus(ctx, *u.Op)
	if err != nil {
		return false, err
	}
	if st.Failed {
		return false, engine.ErrUnavailable
	}
	return st.Done, nil
}
