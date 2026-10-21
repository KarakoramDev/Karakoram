// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
)

// The operation ID is also the coordinator's promotion-durable receipt. A start
// whose response is lost can therefore be replayed without scheduling a new job.
type citusRebalanceCheckpoint struct {
	Operation string `json:"operation"`
	Started   bool   `json:"started,omitempty"`
	JobID     int64  `json:"jobId,omitempty"`
	State     string `json:"state,omitempty"`
}

func (p *Core) runCitusRebalance(ctx context.Context, rc *engine.Context, op *domain.Operation, cancel bool) (engine.OpResult, error) {
	s, err := load(rc.Cluster)
	if err != nil {
		return engine.OpResult{}, err
	}
	if s.Topology != "distributed" {
		return engine.OpResult{}, engine.Invalid("rebalance requires distributed PostgreSQL")
	}
	cp := s.Rebalance
	if cp == nil {
		cp = &citusRebalanceCheckpoint{Operation: op.ID}
		if len(op.Checkpoint) > 0 {
			if err := json.Unmarshal(op.Checkpoint, cp); err != nil {
				return engine.OpResult{}, err
			}
		}
		if cp.Operation != op.ID {
			return engine.OpResult{}, engine.ErrConflict
		}
		s.Rebalance = cp
		// Establish the formation fence before the first external mutation.
		if err := save(rc.Cluster, s); err != nil {
			return engine.OpResult{}, err
		}
		if !cancel {
			return citusRebalanceResult(cp, false), nil
		}
	}
	if cp.Operation != op.ID {
		return engine.OpResult{}, engine.ErrConflict
	}
	leader, _, err := s.group(0).primary(ctx, rc)
	if err != nil {
		return citusRebalanceResult(cp, false), err
	}
	if !cp.Started {
		var params struct {
			Strategy string `json:"strategy"`
		}
		if len(op.Payload) > 0 {
			if err := json.Unmarshal(op.Payload, &params); err != nil {
				return citusRebalanceResult(cp, false), err
			}
		}
		var response pgspec.CitusRebalanceResponse
		if err := pgAction(ctx, rc, leader.Name, "citus-rebalance-start", pgspec.CitusRebalanceStartRequest{Receipt: op.ID, Strategy: params.Strategy}, &response); err != nil {
			return citusRebalanceResult(cp, false), err
		}
		cp.Started, cp.JobID, cp.State = true, response.JobID, response.State
		// Never poll until the returned job identity has crossed a durable checkpoint.
		if err := save(rc.Cluster, s); err != nil {
			return citusRebalanceResult(cp, false), err
		}
		return citusRebalanceResult(cp, false), nil
	}
	terminal := cp.State == "finished" || cp.State == "cancelled" || cp.State == "failed"
	if !terminal {
		if cancel {
			if err := pgAction(ctx, rc, leader.Name, "citus-rebalance-stop", pgspec.CitusRebalanceRequest{JobID: cp.JobID}, nil); err != nil {
				return citusRebalanceResult(cp, false), err
			}
		}
		var response pgspec.CitusRebalanceResponse
		if err := pgAction(ctx, rc, leader.Name, "citus-rebalance-status", pgspec.CitusRebalanceRequest{JobID: cp.JobID}, &response); err != nil {
			return citusRebalanceResult(cp, false), err
		}
		cp.State = response.State
		terminal = cp.State == "finished" || cp.State == "cancelled" || cp.State == "failed"
	}
	if terminal {
		s.Rebalance = nil
	}
	if err := save(rc.Cluster, s); err != nil {
		return citusRebalanceResult(cp, false), err
	}
	if cp.State == "failed" && !cancel {
		return citusRebalanceResult(cp, true), fmt.Errorf("citus rebalance job %d failed", cp.JobID)
	}
	return citusRebalanceResult(cp, terminal), nil
}

func citusRebalanceResult(cp *citusRebalanceCheckpoint, done bool) engine.OpResult {
	raw, _ := json.Marshal(cp)
	return engine.OpResult{Checkpoint: raw, Step: "rebalance", Done: done, RequeueAfter: 2 * time.Second}
}
