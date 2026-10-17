// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"context"
	"encoding/json"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

func (p *Core) reinitMember(ctx context.Context, rc *engine.Context, op *domain.Operation) (engine.OpResult, error) {
	s, err := load(rc.Cluster)
	if err != nil {
		return engine.OpResult{}, err
	}
	var request struct {
		Member string `json:"member"`
	}
	if err := json.Unmarshal(op.Payload, &request); err != nil {
		return engine.OpResult{}, engine.Invalid("invalid reinitialization request")
	}
	m := s.find(request.Member)
	if s.Topology != "ha" || m == nil || m.Role != "data" || m.Removing {
		return engine.OpResult{}, engine.Invalid("reinitialization requires an active HA data member")
	}
	st, err := live(ctx, rc, m.Name)
	if err != nil {
		return engine.OpResult{}, err
	}
	if st.Role == "primary" {
		return engine.OpResult{}, engine.Invalid("cannot reinitialize the primary; switch over first")
	}
	if len(op.Checkpoint) == 0 {
		a, err := rc.Agents.Dial(ctx, rc.Cluster.Ref(), m.Name)
		if err != nil {
			return engine.OpResult{}, err
		}
		if err := a.Engine(ctx, "postgres", "reinit", struct{}{}, nil); err != nil {
			return engine.OpResult{}, err
		}
		return engine.OpResult{Checkpoint: json.RawMessage(`{"requested":true}`), Step: "rebuild", RequeueAfter: 2 * time.Second}, nil
	}
	return engine.OpResult{Done: st.Running && st.Role == "replica" && st.Streaming, Step: "rebuild", RequeueAfter: 2 * time.Second}, nil
}

func (p *Core) rotateTLS(ctx context.Context, rc *engine.Context, op *domain.Operation) (engine.OpResult, error) {
	if len(op.Checkpoint) == 0 {
		s, err := load(rc.Cluster)
		if err != nil {
			return engine.OpResult{}, err
		}
		if err := ensureCertificates(ctx, rc, s, true); err != nil {
			return engine.OpResult{}, err
		}
		if err := save(rc.Cluster, s); err != nil {
			return engine.OpResult{}, err
		}
		return engine.OpResult{Checkpoint: json.RawMessage(`{"staged":true}`), Step: "reload", RequeueAfter: 2 * time.Second}, nil
	}
	if _, err := p.Sync(ctx, rc); err != nil {
		return engine.OpResult{}, err
	}
	s, err := load(rc.Cluster)
	if err != nil {
		return engine.OpResult{}, err
	}
	for _, m := range s.Members {
		if m.TLSPending || m.ConfigVersion != s.ConfigVersion || m.Stage != "joined" {
			return engine.OpResult{Step: "reload", RequeueAfter: 2 * time.Second}, nil
		}
	}
	return engine.OpResult{Done: true}, nil
}
