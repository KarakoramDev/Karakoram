// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

func (*Provider) CanCancelOperation(_ context.Context, op *domain.Operation) error {
	if op.Kind != domain.OperationUpgradeAgents {
		return engine.ErrNotSupported
	}
	var cp operationState
	if len(op.Checkpoint) > 0 {
		if err := json.Unmarshal(op.Checkpoint, &cp); err != nil {
			return err
		}
	}
	if cp.Waiting != "" {
		return fmt.Errorf("agent upgrade can only be cancelled between members")
	}
	return nil
}

func (p *Provider) CancelOperation(ctx context.Context, rc *engine.Context, op *domain.Operation) (engine.OpResult, error) {
	if op.Kind != domain.OperationUpgradeAgents {
		return engine.OpResult{}, engine.ErrNotSupported
	}
	s, err := load(rc.Cluster)
	if err != nil {
		return engine.OpResult{}, err
	}
	// A cancellation racing an accepted disruption finishes that member only.
	if r := s.Rolling; r != nil && r.OperationID == op.ID && r.Current != "" && r.Step != "prepare" {
		result, err := p.RunOperation(ctx, rc, op)
		if err != nil {
			return result, err
		}
		var cp operationState
		if err := json.Unmarshal(result.Checkpoint, &cp); err != nil {
			return result, err
		}
		if cp.Waiting != "" {
			result.Done = false
			return result, nil
		}
		s, err = load(rc.Cluster)
		if err != nil {
			return result, err
		}
	}
	if s.Rolling != nil && s.Rolling.OperationID == op.ID {
		s.Rolling = nil
	}
	if err := save(rc.Cluster, s); err != nil {
		return engine.OpResult{}, err
	}
	return engine.OpResult{Done: true, Step: "agents-cancelled"}, nil
}
