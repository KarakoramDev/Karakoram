// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"encoding/json"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"time"
)

func (*Core) CanClaimOperation(c *domain.Cluster) (bool, error) {
	s, err := load(c)
	if err != nil {
		return false, err
	}
	bs, err := loadBackupState(c)
	if err != nil {
		return false, err
	}
	if bs.RestoreOperation != nil {
		return false, nil
	}
	if s.Pending != nil || s.GaleraTLS != nil || s.Suspended || s.Formation.State != "formed" || s.Replacement != nil || s.TLSRotation != nil || s.Rolling != nil && s.Rolling.Stage != "backup" {
		return false, nil
	}
	for _, m := range s.Members {
		if m.Stage != "joined" || m.ImportBatch != nil {
			return false, nil
		}
	}
	return true, nil
}

// RunOperation's closed dispatch prevents unsupported maintenance from mutating data.
func (p *Core) RunOperation(ctx context.Context, rc *engine.Context, op *domain.Operation) (engine.OpResult, error) {
	p.accessMu.Lock()
	defer p.accessMu.Unlock()
	if err := p.finishPendingAccess(ctx, rc); err != nil {
		return engine.OpResult{}, err
	}
	switch op.Kind {
	case "recover-quorum":
		if p.Name() != "pxc" {
			return engine.OpResult{}, engine.ErrNotSupported
		}
		return p.recoverGaleraQuorum(ctx, rc, op)
	case domain.OperationBackup, domain.OperationDeleteBackup, domain.OperationRestore, "initial-restore":
		return p.runBackupOperation(ctx, rc, op)
	case domain.OperationReplaceMember:
		return p.replaceMember(ctx, rc, op)
	case domain.OperationRotateTLS:
		return p.rotateTLS(ctx, rc, op)
	case domain.OperationRestart:
	default:
		return engine.OpResult{}, engine.ErrNotSupported
	}
	s, err := load(rc.Cluster)
	if err != nil {
		return engine.OpResult{}, err
	}
	if err := maintenanceAllowed(ctx, rc, s); err != nil {
		return engine.OpResult{}, err
	}
	if s.Rolling != nil || s.Replacement != nil || s.TLSRotation != nil || s.GaleraTLS != nil || s.Pending != nil {
		return engine.OpResult{}, engine.ErrUnavailable
	}
	var cp struct {
		Order   []string `json:"order"`
		Index   int      `json:"index"`
		Waiting bool     `json:"waiting"`
	}
	if len(op.Checkpoint) > 0 {
		if err = json.Unmarshal(op.Checkpoint, &cp); err != nil {
			return engine.OpResult{}, err
		}
	}
	if len(cp.Order) == 0 {
		primary := ""
		for _, m := range s.Members {
			if m.Role == "proxy" {
				cp.Order = append(cp.Order, m.Name)
				continue
			}
			st, e := live(ctx, rc, m.Name)
			if e != nil || st.MemberState != "ONLINE" {
				return engine.OpResult{}, engine.ErrUnavailable
			}
			if onlinePrimary(st) {
				primary = m.Name
			} else {
				cp.Order = append(cp.Order, m.Name)
			}
		}
		if primary != "" {
			cp.Order = append(cp.Order, primary)
		}
		raw, e := json.Marshal(cp)
		return engine.OpResult{Checkpoint: raw, Step: "restart", RequeueAfter: 2 * time.Second}, e
	}
	if cp.Index >= len(cp.Order) {
		return engine.OpResult{Done: true}, nil
	}
	m := s.find(cp.Order[cp.Index])
	if m == nil {
		return engine.OpResult{}, engine.ErrNotFound
	}
	if cp.Waiting {
		ok, e := ready(ctx, rc, m)
		if e != nil {
			return engine.OpResult{}, e
		}
		if ok {
			cp.Index++
			cp.Waiting = false
		}
	} else {
		a, e := rc.Agents.Dial(ctx, rc.Cluster.Ref(), m.Name)
		if e != nil {
			return engine.OpResult{}, e
		}
		if s.Topology == "group" && m.Role == "data" {
			st, e := live(ctx, rc, m.Name)
			if e != nil {
				return engine.OpResult{}, e
			}
			if onlinePrimary(st) {
				if e := switchAway(ctx, rc, s, m.Name); e != nil {
					return engine.OpResult{}, e
				}
				raw, e := json.Marshal(cp)
				return engine.OpResult{Checkpoint: raw, Step: "switchover", RequeueAfter: 2 * time.Second}, e
			}
		}
		if s.Topology == "cluster" && m.Role == "data" {
			if e := survivorsOnline(ctx, rc, s, m.Name); e != nil {
				return engine.OpResult{}, e
			}
		}
		process := "mysqld"
		if m.Role == "proxy" {
			process = "proxysql"
		}
		if e = a.Process(ctx, agentapi.ProcessAction{Process: process, Action: "restart", RequestID: op.ID + "/restart/" + m.Name}); e != nil {
			return engine.OpResult{}, e
		}
		cp.Waiting = true
	}
	raw, err := json.Marshal(cp)
	return engine.OpResult{Checkpoint: raw, Step: "restart", RequeueAfter: 2 * time.Second}, err
}
