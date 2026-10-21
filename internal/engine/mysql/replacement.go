// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"time"
)

type replacementState struct {
	Operation string `json:"operation"`
	Old       string `json:"old"`
	New       string `json:"new"`
	Stage     string `json:"stage"`
	Recovery  string `json:"recovery,omitempty"`
}

func replacementResult(cp *replacementState) engine.OpResult {
	raw, _ := json.Marshal(cp)
	return engine.OpResult{Checkpoint: raw, Step: cp.Stage, RequeueAfter: 2 * time.Second}
}

func (p *Core) replaceMember(ctx context.Context, rc *engine.Context, op *domain.Operation) (result engine.OpResult, err error) {
	s, err := load(rc.Cluster)
	if err != nil {
		return result, err
	}
	defer func() {
		if e := save(rc.Cluster, s); err == nil {
			err = e
		}
	}()
	if s.LastReplacement == op.ID {
		return engine.OpResult{Done: true}, nil
	}
	if !clustered(s.Topology) {
		return result, engine.Invalid("MySQL member replacement requires group topology")
	}
	if s.Replacement == nil {
		var previous replacementState
		if len(op.Checkpoint) != 0 {
			if err := json.Unmarshal(op.Checkpoint, &previous); err != nil {
				return result, err
			}
			if previous.Stage == "done" {
				return engine.OpResult{Done: true, Checkpoint: op.Checkpoint}, nil
			}
		}
		if err := maintenanceAllowed(ctx, rc, s); err != nil {
			return result, err
		}
		if s.Rolling != nil || s.Pending != nil || len(s.data()) != rc.Cluster.Spec.Components.Engine.Replicas {
			return result, engine.ErrUnavailable
		}
		var payload struct {
			Member string `json:"member"`
		}
		if err := json.Unmarshal(op.Payload, &payload); err != nil {
			return result, engine.Invalid("invalid MySQL replacement request")
		}
		old := s.find(payload.Member)
		if old == nil || old.Role != "data" {
			return result, engine.Invalid("replacement target must be a MySQL data member")
		}
		index := s.NextIndex
		s.NextIndex++
		name := fmt.Sprintf("%sm%d", rc.Cluster.Name, index)
		s.Members = append(s.Members, &member{Name: name, Index: index, Role: "data", Volume: name + "v", Stage: "planned", ConfigVersion: s.ConfigVersion})
		s.Replacement = &replacementState{Operation: op.ID, Old: old.Name, New: name, Stage: "join"}
		publishRevision(s)
	}
	if s.Replacement.Operation != op.ID {
		return result, engine.ErrConflict
	}
	return p.advanceReplacement(ctx, rc, s)
}

func (p *Core) advanceReplacement(ctx context.Context, rc *engine.Context, s *state) (engine.OpResult, error) {
	cp := s.Replacement
	switch cp.Stage {
	case "join":
		next := s.find(cp.New)
		if next == nil {
			return engine.OpResult{}, engine.ErrNotFound
		}
		if cp.Recovery == "rollback" {
			if busy, err := p.observePending(ctx, rc, s); busy || err != nil {
				return replacementResult(cp), err
			}
			if next.ImportBatch != nil {
				op, err := resumePlannedImport(ctx, rc, next)
				if err != nil {
					return replacementResult(cp), err
				}
				if op != nil {
					p.track(s, next, "import", *op)
					return replacementResult(cp), nil
				}
				next.ImportBatch = nil
			}
			if next.DeployOp != nil {
				status, err := rc.Backend.OpStatus(ctx, *next.DeployOp)
				if err != nil || !status.Done && !status.Failed {
					return replacementResult(cp), err
				}
				next.DeployOp = nil
			}
			next.Removing, next.Offline = true, true
			next.Retirement = "runtime-stop"
			if next.Stage == "planned" && next.ServiceID == "" && next.VolumeID == "" {
				next.Retirement = "credentials"
			}
			cp.Stage = "retire"
			publishRevision(s)
			return replacementResult(cp), nil
		}
		if next.Stage != "joined" {
			return replacementResult(cp), nil
		}
		st, err := live(ctx, rc, next.Name)
		if err != nil {
			return replacementResult(cp), err
		}
		if st.MemberState == "ONLINE" {
			cp.Stage = "retire"
		}
	case "retire":
		target := cp.Old
		if cp.Recovery == "rollback" {
			target = cp.New
		}
		old := s.find(target)
		if old != nil {
			if old.Retirement == "" {
				old.Retirement = "handoff"
				return replacementResult(cp), nil
			}
			done, err := p.retireMember(ctx, rc, s, old)
			if err != nil || !done {
				return replacementResult(cp), err
			}
		}
		cp.Stage = "done"
	case "done":
		result := replacementResult(cp)
		result.Done = true
		s.LastReplacement = cp.Operation
		s.Replacement = nil
		return result, nil
	default:
		return replacementResult(cp), engine.Invalid("unknown MySQL replacement checkpoint")
	}
	return replacementResult(cp), nil
}

func cancelReplacement(rc *engine.Context, op *domain.Operation) (engine.OpResult, error) {
	s, err := load(rc.Cluster)
	if err != nil {
		return engine.OpResult{}, err
	}
	cp := s.Replacement
	if cp == nil {
		return engine.OpResult{Done: true}, nil
	}
	if cp.Operation != op.ID {
		return engine.OpResult{}, engine.ErrConflict
	}
	if cp.Recovery == "" {
		old := s.find(cp.Old)
		if cp.Stage == "join" || cp.Stage == "retire" && old != nil && old.Retirement == "" {
			cp.Recovery = "rollback"
		} else {
			cp.Recovery = "complete"
		}
	}
	if err := save(rc.Cluster, s); err != nil {
		return engine.OpResult{}, err
	}
	result := replacementResult(cp)
	result.Done = true
	return result, nil
}
