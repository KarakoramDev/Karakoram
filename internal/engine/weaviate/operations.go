// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package weaviate

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"time"
)

type operationState struct {
	Stage  string `json:"stage"`
	Member string `json:"member,omitempty"`
}

func operationResult(cp operationState, done bool) (engine.OpResult, error) {
	raw, e := json.Marshal(cp)
	return engine.OpResult{Done: done, Step: cp.Stage, Checkpoint: raw, RequeueAfter: time.Second}, e
}
func (p *Provider) RunOperation(ctx context.Context, rc *engine.Context, op *domain.Operation) (result engine.OpResult, err error) {
	previousOperation := rc.Operation
	rc.Operation = op
	defer func() { rc.Operation = previousOperation }()
	s, e := load(rc.Cluster)
	if e != nil {
		return result, e
	}
	defer func() {
		if e := s.save(rc.Cluster); err == nil {
			err = e
		}
	}()
	if busy, e := drainRuntimeRecovery(ctx, rc, s); busy || e != nil {
		return engine.OpResult{Checkpoint: op.Checkpoint, RequeueAfter: time.Second}, e
	}
	if busy, e := p.advancePending(ctx, rc, s); busy || e != nil {
		return engine.OpResult{Checkpoint: op.Checkpoint, RequeueAfter: time.Second}, e
	}
	var cp operationState
	if len(op.Checkpoint) > 0 {
		if e = json.Unmarshal(op.Checkpoint, &cp); e != nil {
			return result, e
		}
	}
	var payload struct {
		Member  string `json:"member"`
		Backup  string `json:"backup"`
		Restore string `json:"restore"`
	}
	if len(op.Payload) > 0 {
		if e = json.Unmarshal(op.Payload, &payload); e != nil {
			return result, invalid("invalid operation payload")
		}
	}
	switch op.Kind {
	case domain.OperationBackup:
		return p.runBackup(ctx, rc, s, op, cp, payload.Backup)
	case domain.OperationDeleteBackup:
		return p.runDeleteBackup(ctx, rc, s, op, cp, payload.Backup)
	case domain.OperationRestore:
		return p.runRestore(ctx, rc, s, op, cp, payload.Restore)
	case domain.OperationRotateSystemUser:
		return p.runRotation(ctx, rc, s, op, false)
	case domain.OperationRotateTLS:
		return p.runRotation(ctx, rc, s, op, true)
	case domain.OperationRestart:
		if cp.Stage == "" {
			if payload.Member != "" && s.find(payload.Member) == nil {
				return result, invalid("unknown member")
			}
			if s.Rolling != nil && s.Rolling.OperationID != op.ID {
				return result, engine.ErrConflict
			}
			if e = p.startRolling(ctx, rc, s, "restart", op.ID, payload.Member); e != nil {
				return result, e
			}
			cp.Stage = "rolling"
			return operationResult(cp, false)
		}
		if s.Rolling != nil {
			e = p.rollingStep(ctx, rc, s)
			r, _ := operationResult(cp, false)
			return r, e
		}
		cp.Stage = "complete"
		return operationResult(cp, true)
	case domain.OperationReplaceMember:
		if cp.Stage == "" {
			m := s.find(payload.Member)
			if m == nil {
				return result, invalid("unknown member")
			}
			if !m.RuntimeRecovery.VolumeLost {
				ready, e := p.clusterReady(ctx, rc, s)
				if e != nil {
					return result, e
				}
				if !ready {
					return result, fmt.Errorf("%w: replacement requires healthy synchronized peers and Raft quorum", engine.ErrConflict)
				}
			} else {
				safe, e := p.volumeRecoverySafe(ctx, rc, s, m)
				if e != nil {
					return result, e
				}
				if !safe {
					return result, fmt.Errorf("%w: lost initialized volume has no proven surviving replicas; restore original volume or a backup", engine.ErrConflict)
				}
			}
			cp.Member = m.Name
			cp.Stage = "stop"
			if m.RuntimeRecovery.VolumeLost {
				cp.Stage = "delete"
			}
			return operationResult(cp, false)
		}
		m := s.find(cp.Member)
		if m == nil {
			return result, engine.ErrNotFound
		}
		switch cp.Stage {
		case "stop":
			if op.Actor != "system" {
				if wait := rc.WaitForWindow(); wait.RequeueAfter > 0 {
					result, err := operationResult(cp, false)
					result.RequeueAfter = wait.RequeueAfter
					return result, err
				}
			}
			if e = verifyMemberServices(ctx, rc, m); e != nil && !m.RuntimeRecovery.VolumeLost {
				return result, e
			}
			s.Pending = &pending{Action: "stop", Member: m.Name}
			cp.Stage = "delete"
		case "delete":
			s.Pending = &pending{Action: "delete-runtime", Member: m.Name}
			cp.Stage = "volume"
		case "volume":
			if m.RuntimeRecovery.VolumeLost && m.VolumeID != "" {
				// Only the admission proof above authorizes replacing lost data.
				s.Pending = &pending{Action: "delete-volume", Member: m.Name}
				cp.Stage = "recover-volume"
			} else {
				cp.Stage = "provision"
			}
		case "recover-volume":
			m.RuntimeRecovery = engine.RuntimeRecovery{}
			m.CleanupStep = ""
			cp.Stage = "provision"
		case "provision":
			// A healthy runtime replacement retains its original volume; a proven
			// lost-data replacement reuses its name but gets a fresh volume ID.
			if m.Stage == "planned" {
				e = p.importMember(ctx, rc, s, m)
			} else if m.Stage == "imported" {
				s.Pending = &pending{Action: "deploy", Member: m.Name}
			} else {
				cp.Stage = "ack"
			}
		case "ack":
			if _, _, e = p.memberReady(ctx, rc, m); e != nil {
				return operationResult(cp, false)
			}
			ready, e := p.clusterReady(ctx, rc, s)
			if e != nil {
				return result, e
			}
			if !ready {
				return operationResult(cp, false)
			}
			m.Stage = "ready"
			m.CleanupStep = ""
			cp.Stage = "complete"
			return operationResult(cp, true)
		default:
			return result, invalid("unknown replacement checkpoint")
		}
		r, _ := operationResult(cp, false)
		return r, e
	default:
		return result, fmt.Errorf("%w: operation %s", engine.ErrNotSupported, op.Kind)
	}
}
