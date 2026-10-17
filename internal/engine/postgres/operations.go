// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"time"
)

func (p *Core) convertTopology(ctx context.Context, rc *engine.Context, s *state, result engine.Result) (engine.Result, error) {
	if s.Topology != "single" || rc.Cluster.Spec.Topology.Type != "ha" {
		return result, engine.ErrInvalid
	}
	s.ConversionSeed = s.data()[0].Name
	s.Topology = "ha"
	s.ConfigVersion++
	s.Formed = false
	s.allocate(rc.Cluster)
	for _, m := range s.Members {
		if m.Role == "data" {
			m.Topology = "ha"
			m.EtcdState = "new"
			if m.ServiceID != "" {
				m.Stage = "imported"
			}
			m.ConfigVersion = s.ConfigVersion
		}
	}
	return result, nil
}
func (p *Core) scaleDown(ctx context.Context, rc *engine.Context, s *state, result engine.Result) (engine.Result, error) {
	data := s.data()
	m := data[len(data)-1]
	if m.ServiceID != "" {
		fenced, err := fenceRetirement(ctx, rc, s, m)
		if err != nil || !fenced {
			return result, err
		}
	}
	if m.Retirement != "removed-etcd" {
		if err := removeEtcdMember(ctx, rc, s, m); err != nil {
			return result, err
		}
		m.Removing = true
		m.Retirement = "removed-etcd"
	}
	_, err := p.retireMember(ctx, rc, s, m)
	return result, err
}

// CanClaimOperation leaves Sync-owned maintenance ahead of queued imperative
// work: upgrades, directional restarts and terminal replacement recovery.
func (*Core) CanClaimOperation(c *domain.Cluster) (bool, error) {
	s, err := load(c)
	if err != nil {
		return false, err
	}
	return s.Upgrade == nil && s.Rolling == nil && (s.Replacement == nil || s.Replacement.Recovery == ""), nil
}

// RunOperation executes bounded, checkpointed rolling restarts.
func (p *Core) RunOperation(ctx context.Context, rc *engine.Context, op *domain.Operation) (result engine.OpResult, err error) {
	allowed, err := p.CanClaimOperation(rc.Cluster)
	if err != nil {
		return engine.OpResult{}, err
	}
	if !allowed {
		return engine.OpResult{}, engine.Invalid("PostgreSQL maintenance owns the cluster; imperative operations must wait")
	}
	switch op.Kind {
	case domain.OperationBackup, domain.OperationDeleteBackup, domain.OperationRestore, "initial-restore":
		return p.runBackupOperation(ctx, rc, op)
	case domain.OperationReplaceMember:
		return p.replaceMember(ctx, rc, op)
	case "reinit-member":
		return p.reinitMember(ctx, rc, op)
	case domain.OperationRotateTLS:
		return p.rotateTLS(ctx, rc, op)
	case domain.OperationRestart:
		// Rolling restart below.
	default:
		return engine.OpResult{}, engine.ErrNotSupported
	}
	s, err := load(rc.Cluster)
	if err != nil {
		return engine.OpResult{}, err
	}
	defer func() {
		if e := save(rc.Cluster, s); err == nil {
			err = e
		}
	}()
	var checkpoint struct {
		Order   []string `json:"order"`
		Index   int      `json:"index"`
		Waiting bool     `json:"waiting"`
	}
	if len(op.Checkpoint) > 0 {
		if err = json.Unmarshal(op.Checkpoint, &checkpoint); err != nil {
			return engine.OpResult{}, err
		}
	}
	if len(checkpoint.Order) == 0 {
		primary := ""
		for _, m := range s.Members {
			st, e := live(ctx, rc, m.Name)
			if e != nil || !st.Running {
				return engine.OpResult{}, engine.ErrUnavailable
			}
			if st.Role == "primary" {
				primary = m.Name
			} else {
				checkpoint.Order = append(checkpoint.Order, m.Name)
			}
		}
		if primary != "" {
			checkpoint.Order = append(checkpoint.Order, primary)
		}
	}
	if checkpoint.Index >= len(checkpoint.Order) {
		return engine.OpResult{Done: true}, nil
	}
	m := s.find(checkpoint.Order[checkpoint.Index])
	if m == nil {
		return engine.OpResult{}, engine.ErrNotFound
	}
	if checkpoint.Waiting {
		ok, err := ready(ctx, rc, m)
		if err != nil {
			return engine.OpResult{}, err
		}
		if ok {
			checkpoint.Index++
			checkpoint.Waiting = false
		}
	} else {
		st, err := live(ctx, rc, m.Name)
		if err != nil {
			return engine.OpResult{}, err
		}
		a, err := rc.Agents.Dial(ctx, rc.Cluster.Ref(), m.Name)
		if err != nil {
			return engine.OpResult{}, err
		}
		if st.Role == "primary" && s.Topology == "ha" {
			if err = switchAway(ctx, rc, s, m.Name); err != nil {
				return engine.OpResult{}, err
			}
		} else {
			process := "postgres"
			if m.Role == "proxy" {
				process = "proxysql"
			}
			if m.Role == "data" && s.Topology == "ha" {
				err = a.Engine(ctx, "postgres", "restart", struct{}{}, nil)
			} else {
				err = a.Process(ctx, agentapi.ProcessAction{Process: process, Action: "restart"})
			}
			if err != nil {
				return engine.OpResult{}, err
			}
			checkpoint.Waiting = true
		}
	}
	raw, err := json.Marshal(checkpoint)
	if err != nil {
		return engine.OpResult{}, err
	}
	return engine.OpResult{Checkpoint: raw, Step: "restart", Message: fmt.Sprintf("Restarting member %d of %d", checkpoint.Index+1, len(checkpoint.Order)), RequeueAfter: 2 * time.Second}, nil
}
