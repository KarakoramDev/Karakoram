// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package openbao

import (
	"context"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

// Every native and platform transition is separated by a persisted step.
func (p *Provider) removeStep(ctx context.Context, rc *engine.Context, s *internalState, m *member) error {
	switch m.RemoveStep {
	case "", "step-down", "remove-peer", "verify-peer":
		token, err := p.ensureManagementToken(ctx, rc, s)
		if err != nil {
			return err
		}
		c, active, err := p.activeClient(ctx, rc, s, token)
		if err != nil {
			return err
		}
		if !p.postgresStorage(rc.Cluster) {
			peers, err := c.RaftConfiguration(ctx)
			if err != nil {
				return err
			}
			present := false
			for _, peer := range peers {
				if peer.NodeID == m.Name {
					present = true
				}
			}
			if !present {
				m.RemoveStep = "stop"
				return nil
			}
			if !removalQuorum(s, m.Name) {
				s.QuorumBlocked = "Removing " + m.Name + " would lose the live Raft majority"
				return nil
			}
			s.QuorumBlocked = ""
			if active.Name == m.Name {
				if m.RemoveStep != "step-down" {
					if err := c.StepDown(ctx); err != nil {
						return err
					}
					m.RemoveStep = "step-down"
				}
				return nil
			}
			if m.RemoveStep == "verify-peer" {
				return nil
			}
			if err := c.RemovePeer(ctx, m.Name); err != nil {
				return err
			}
			m.RemoveStep = "verify-peer"
			return nil
		}
		if active.Name == m.Name && len(s.Members) > 1 {
			if err := c.StepDown(ctx); err != nil {
				return err
			}
			return nil
		}
		m.RemoveStep = "stop"
		return nil
	case "stop":
		// Dead members are removable by node id. Platform deletion is the fallback
		// stop fence when the local supervisor cannot be contacted.
		a, err := p.agent(ctx, rc, m.Name)
		if err == nil {
			err = a.Maintenance(ctx, agentapi.Maintenance{Enabled: true})
			if err == nil {
				_ = a.Process(ctx, agentapi.ProcessAction{Process: "openbao", Action: "stop"})
			}
			a.Close()
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		m.RemoveStep = "delete-runtime"
		return nil
	case "delete-runtime":
		if err := verifyMemberServices(ctx, rc, m); err != nil {
			return err
		}
		if err := rc.Backend.DeletePublicPorts(ctx, m.Name); err != nil {
			return err
		}
		return p.submit(ctx, rc, s, &pending{Action: "delete-runtime", Member: m.Name})
	case "delete-volume":
		return p.submit(ctx, rc, s, &pending{Action: "delete-volume", Member: m.Name})
	}
	return engine.ErrConflict
}
