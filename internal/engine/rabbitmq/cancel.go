// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package rabbitmq

import (
	"context"
	"fmt"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

func (*Provider) CanCancelOperation(_ context.Context, op *domain.Operation) error {
	if op.Kind != domain.OperationUpgradeAgents {
		return fmt.Errorf("operation %s does not support safe cancellation", op.Kind)
	}
	var cp operationState
	if err := decodeJSON(op.Checkpoint, &cp); err != nil {
		return err
	}
	if cp.Waiting != "" {
		return fmt.Errorf("agent upgrade can only be cancelled between members")
	}
	return nil
}

func (p *Provider) CancelOperation(ctx context.Context, rc *engine.Context, op *domain.Operation) (engine.OpResult, error) {
	if op.Kind != domain.OperationUpgradeAgents {
		return engine.OpResult{}, fmt.Errorf("operation %s does not support safe cancellation", op.Kind)
	}
	var cp operationState
	if err := decodeJSON(op.Checkpoint, &cp); err != nil {
		return engine.OpResult{}, err
	}
	if cp.Waiting == "" {
		return opResult(cp, true, "agents-cancelled")
	}
	// Finish a member accepted concurrently with cancellation, never select another.
	result, err := p.RunOperation(ctx, rc, op)
	if err != nil {
		return result, err
	}
	cp = operationState{}
	if err := decodeJSON(result.Checkpoint, &cp); err != nil {
		return result, err
	}
	result.Done = cp.Waiting == ""
	return result, nil
}
