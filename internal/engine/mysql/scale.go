// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

// maintenanceAllowed excludes physical backups and all topology/restore owners.
func maintenanceAllowed(ctx context.Context, rc *engine.Context, s *state) error {
	bs, err := loadBackupState(rc.Cluster)
	if err != nil {
		return err
	}
	if bs.RestoreOperation != nil || s.GaleraTLS != nil || s.Suspended || s.Formation.State != "formed" || s.Formation.Reformation != nil && s.Formation.Reformation.State != "formed" {
		return engine.ErrUnavailable
	}
	if rc.Store != nil {
		backups, err := rc.Store.ListBackups(ctx, rc.Cluster.Ref())
		if err != nil {
			return err
		}
		for _, b := range backups {
			if b.Phase == domain.PhasePending || b.Phase == domain.PhaseRunning {
				return engine.ErrUnavailable
			}
		}
	}
	return nil
}

func (p *Core) adoptSingleton(ctx context.Context, rc *engine.Context, s *state) error {
	if s.Topology != "single" || rc.Cluster.Spec.Topology.Type != "group" || len(s.data()) != 1 || rc.Cluster.Spec.Components.Extra["proxy"].Replicas < 1 {
		return engine.ErrNotSupported
	}
	if err := maintenanceAllowed(ctx, rc, s); err != nil {
		return err
	}
	if s.Pending != nil || s.Rolling != nil || s.Replacement != nil {
		return engine.ErrUnavailable
	}
	seed := s.data()[0]
	st, err := live(ctx, rc, seed.Name)
	if err != nil || st.MemberState != "ONLINE" {
		return engine.ErrUnavailable
	}
	group, err := newUUID()
	if err != nil {
		return err
	}
	s.Topology = "group"
	s.Formation = formation{GroupName: group, FormationEpoch: s.Formation.FormationEpoch + 1, Seed: seed.Name, State: "adopting", Adopting: true}
	publishRevision(s)
	return nil
}

func (p *Core) reconcileMembership(ctx context.Context, rc *engine.Context, s *state) (bool, error) {
	if s.Replacement != nil {
		cp := s.Replacement
		if cp.Stage == "join" {
			if _, err := p.advanceReplacement(ctx, rc, s); err != nil {
				return true, err
			}
			if cp.Stage == "join" {
				return cp.Recovery != "", nil
			}
		}
		_, err := p.advanceReplacement(ctx, rc, s)
		return true, err
	}
	for _, m := range s.data() {
		if m.Retirement != "" {
			_, err := p.retireMember(ctx, rc, s, m)
			return true, err
		}
	}
	want := rc.Cluster.Spec.Components.Engine.Replicas
	if s.Formation.State != "formed" || len(s.data()) == want {
		return false, nil
	}
	if err := maintenanceAllowed(ctx, rc, s); err != nil {
		return true, err
	}
	if s.Pending != nil || s.Rolling != nil {
		return true, engine.ErrUnavailable
	}
	if !clustered(s.Topology) || want != 3 && want != 5 {
		return true, engine.ErrNotSupported
	}
	if len(s.data()) < want {
		s.allocate(rc.Cluster)
		publishRevision(s)
		return true, nil
	}
	// Highest identity is removed first. No writer is ever removed in place.
	data := s.data()
	target := data[len(data)-1]
	for _, m := range data {
		if m.Index > target.Index {
			target = m
		}
	}
	target.Retirement = "handoff"
	return true, nil
}
