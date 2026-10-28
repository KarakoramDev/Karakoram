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
	if m.Retirement == "" && !m.Removing {
		if wait := rc.WaitForWindow(); wait.RequeueAfter > 0 {
			return wait, nil
		}
	}
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
	// Initial recovery is what establishes formation. Requiring Formed here
	// waits on Sync, which cannot acknowledge formation until that same restore
	// publishes its application receipt.
	initialRestore := false
	if c.Spec.DataSource != nil {
		bs, err := loadBackupState(c)
		if err != nil {
			return false, err
		}
		initialRestore = bs.InitialQueued && !bs.InitialApplied
	}
	if s.Topology == "distributed" {
		if s.Pending != nil || len(s.data()) != s.desiredDataCount(&engine.Context{Cluster: c}) {
			return false, nil
		}
		for _, m := range s.data() {
			if m.Stage != "joined" || !initialRestore && !s.group(m.Group).formed() {
				return false, nil
			}
		}
	}
	for _, m := range s.Members {
		if m.UnplacedRetirement != nil || m.ImportBatch != nil || m.RuntimeRecovery.Op != nil || m.RuntimeRecovery.ImportPlan != nil {
			return false, nil
		}
	}
	return s.CitusScale == nil && s.CitusOperation == "" && s.Rebalance == nil && s.RestoreRecovery == nil && s.Upgrade == nil && s.Rolling == nil && (s.Replacement == nil || s.Replacement.Recovery == ""), nil
}

// RunOperation executes bounded, checkpointed rolling restarts.
func (p *Core) RunOperation(ctx context.Context, rc *engine.Context, op *domain.Operation) (result engine.OpResult, err error) {
	previousOperation := rc.Operation
	rc.Operation = op
	defer func() { rc.Operation = previousOperation }()
	allowed, err := p.CanClaimOperation(rc.Cluster)
	if err != nil {
		return engine.OpResult{}, err
	}
	if rc.Cluster.Spec.Topology.Type == "distributed" && rc.Cluster.Spec.DataSource != nil && op.Kind != domain.OperationUpgradeAgents {
		bs, readErr := loadBackupState(rc.Cluster)
		if readErr != nil {
			return engine.OpResult{}, readErr
		}
		if bs.InitialQueued && !bs.InitialApplied {
			initial, readErr := rc.Store.GetRestore(ctx, rc.Cluster.Ref(), "initial-restore")
			if readErr != nil {
				return engine.OpResult{}, readErr
			}
			if !restoreOperation(op) || initial.OperationID != op.ID {
				return engine.OpResult{}, engine.Invalid("initial PostgreSQL recovery owns the cluster")
			}
		}
	}
	if !allowed {
		// Replacement join deliberately runs Sync to provision its new member.
		// Its own frozen import is admission fencing for other operations, not
		// a reason to fail the already-running, non-destructive join observer.
		s, loadErr := load(rc.Cluster)
		if loadErr == nil && op.Kind == "rebalance" && s.Rebalance != nil && s.Rebalance.Operation == op.ID {
			return p.runCitusRebalance(ctx, rc, op, false)
		}
		if loadErr == nil && s.CitusOperation != "" && (restoreOperation(op) || op.Kind == domain.OperationBackup) && s.CitusOperation == op.ID {
			return p.runBackupOperation(ctx, rc, op)
		}
		if loadErr != nil {
			return engine.OpResult{}, loadErr
		}
		ownedJoin := op.Kind == domain.OperationReplaceMember && s.Upgrade == nil && s.Rolling == nil && s.RestoreRecovery == nil &&
			s.Replacement != nil && s.Replacement.Operation == op.ID && s.Replacement.Stage == "join" && s.Replacement.Recovery == ""
		if !ownedJoin {
			return engine.OpResult{}, engine.Invalid("PostgreSQL maintenance owns the cluster; imperative operations must wait")
		}
	}
	switch op.Kind {
	case domain.OperationRotateSystemUser:
		return p.rotateDataUser(ctx, rc, op)
	case "rebalance":
		return p.runCitusRebalance(ctx, rc, op, false)
	case domain.OperationUpgradeAgents:
		return p.upgradeAgents(ctx, rc, op, false)
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
		if s.Topology == "distributed" {
			for _, m := range s.Members {
				if m.Role == "proxy" {
					checkpoint.Order = append(checkpoint.Order, m.Name)
				}
			}
			for _, group := range s.maintenanceGroups() {
				primary, _, err := group.primary(ctx, rc)
				if err != nil {
					return engine.OpResult{}, err
				}
				for _, m := range group.data() {
					if m != primary {
						checkpoint.Order = append(checkpoint.Order, m.Name)
					}
				}
				checkpoint.Order = append(checkpoint.Order, primary.Name)
			}
		} else {
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
		if wait := rc.WaitForWindow(); wait.RequeueAfter > 0 {
			raw, err := json.Marshal(checkpoint)
			return engine.OpResult{Checkpoint: raw, Step: "restart", Message: "Waiting for maintenance window", RequeueAfter: wait.RequeueAfter}, err
		}
		st, err := live(ctx, rc, m.Name)
		if err != nil {
			return engine.OpResult{}, err
		}
		a, err := rc.Agents.Dial(ctx, rc.Cluster.Ref(), m.Name)
		if err != nil {
			return engine.OpResult{}, err
		}
		if st.Role == "primary" && patroniTopology(s.Topology) && rc.Cluster.Spec.Components.Engine.Replicas > 1 {
			if err = switchAway(ctx, rc, s, m.Name); err != nil {
				return engine.OpResult{}, err
			}
		} else {
			process := "postgres"
			if m.Role == "proxy" {
				process = "proxysql"
			}
			if m.Role == "data" && patroniTopology(s.Topology) {
				err = a.Engine(ctx, "postgres", "restart", struct{}{}, nil)
			} else {
				err = a.Process(ctx, agentapi.ProcessAction{Process: process, Action: "restart", RequestID: op.ID + "/restart/" + m.Name})
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
