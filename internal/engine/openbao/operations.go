// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package openbao

import (
	"context"
	"encoding/json"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"slices"
	"time"
)

type operationState struct {
	Done      []string `json:"done,omitempty"`
	Waiting   string   `json:"waiting,omitempty"`
	Stage     string   `json:"stage,omitempty"`
	NewMember string   `json:"newMember,omitempty"`
}

func operationResult(cp operationState, done bool) (engine.OpResult, error) {
	b, err := json.Marshal(cp)
	return engine.OpResult{Done: done, Step: cp.Stage, Checkpoint: b, RequeueAfter: time.Second}, err
}

func (p *Provider) RunOperation(ctx context.Context, rc *engine.Context, op *domain.Operation) (result engine.OpResult, err error) {
	if r, handled, e := p.runBackupOperation(ctx, rc, op); handled {
		return r, e
	}
	switch op.Kind {
	case domain.OperationRestart, domain.OperationRotateTLS, domain.OperationReplaceMember:
	default:
		return result, engine.ErrNotSupported
	}
	var cp operationState
	if len(op.Checkpoint) > 0 {
		if err = json.Unmarshal(op.Checkpoint, &cp); err != nil {
			return result, err
		}
	}
	// Operations own convergence; external concurrent Sync is fenced below.
	if _, err = p.Sync(ctx, rc); err != nil {
		return result, err
	}
	s, err := loadState(rc.Cluster)
	if err != nil {
		return result, err
	}
	defer func() {
		if e := s.save(rc.Cluster); err == nil {
			err = e
		}
	}()
	if s.Pending != nil || s.Retry != nil {
		return operationResult(cp, false)
	}
	if op.Kind == domain.OperationReplaceMember {
		var payload struct {
			Member string `json:"member"`
		}
		if err = json.Unmarshal(op.Payload, &payload); err != nil {
			return result, err
		}
		old := s.find(payload.Member)
		switch cp.Stage {
		case "":
			if old == nil || old.Removing {
				return result, engine.ErrInvalid
			}
			m, e := allocateNewMember(ctx, rc, s)
			if e != nil {
				return result, e
			}
			cp.NewMember = m.Name
			cp.Stage = "join"
			s.Replacing = true
			s.ConfigVersion++
			for _, peer := range s.Members {
				activateConfig(rc.Cluster, s, peer)
			}
		case "join":
			m := s.find(cp.NewMember)
			if m == nil {
				return result, engine.ErrNotFound
			}
			if m.Healthy && (m.Voter || p.postgresStorage(rc.Cluster)) && m.Stage == "ready" {
				if old != nil {
					old.Removing = true
				}
				cp.Stage = "remove"
			}
		case "remove":
			if old == nil {
				s.Replacing = false
				cp.Stage = "complete"
				return operationResult(cp, true)
			}
		case "complete":
			return operationResult(cp, true)
		}
		return operationResult(cp, false)
	}
	if cp.Waiting != "" {
		m := s.find(cp.Waiting)
		if m == nil {
			return result, engine.ErrNotFound
		}
		if m.Change != nil {
			return operationResult(cp, false)
		}
		cp.Done = append(cp.Done, cp.Waiting)
		cp.Waiting = ""
	}
	for _, active := range []bool{false, true} {
		for _, m := range ordered(s) {
			if slices.Contains(cp.Done, m.Name) || m.Active != active {
				continue
			}
			if !m.Healthy || m.Change != nil {
				return operationResult(cp, false)
			}
			fp, e := p.restartFingerprint(ctx, rc)
			if e != nil {
				return result, e
			}
			kind := "restart"
			if op.Kind == domain.OperationRotateTLS {
				kind = "tls"
			}
			m.Change = &memberChange{Kind: kind, Version: m.DesiredVersion, Fingerprint: fp}
			cp.Waiting = m.Name
			cp.Stage = "rolling"
			return operationResult(cp, false)
		}
	}
	cp.Stage = "complete"
	return operationResult(cp, true)
}

func (*Provider) SyncDuringOperation(*domain.Operation) bool { return false }
func (*Provider) CanClaimOperation(c *domain.Cluster) (bool, error) {
	s, err := loadState(c)
	if err != nil {
		return false, err
	}
	if s.Pending != nil || s.Retry != nil || s.Unsealing != "" || s.Replacing {
		return false, nil
	}
	for _, m := range s.Members {
		if m.Removing || m.Change != nil || m.ResumeDeploy != nil || m.UnplacedRetirement != nil || m.RuntimeRecovery.Op != nil || m.RuntimeRecovery.ImportPlan != nil || m.ImportPlan != nil {
			return false, nil
		}
	}
	return true, nil
}
